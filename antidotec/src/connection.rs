use self::crdts::Crdt;
use crate::protos::{antidote::*, ApbMessage, ApbMessageCode, MessageCodeError};
use async_std::io::BufReader;
use async_std::{
    io::{self, prelude::*},
    net::TcpStream,
};
use protobuf::ProtobufError;
use std::mem;
use std::{convert::TryFrom, u32};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AntidoteError {
    #[error("unknown error")]
    Unknown = 0,
    #[error("the request has timed out")]
    Timeout = 1,
    #[error("not enough permissions")]
    NoPermissions = 2,
    #[error("the request has been aborted")]
    Aborted = 3,
}

impl From<u32> for AntidoteError {
    fn from(code: u32) -> Self {
        match code {
            1 => Self::Timeout,
            2 => Self::NoPermissions,
            3 => Self::Aborted,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("couldn't write or read from the connection")]
    Io(#[from] io::Error),
    #[error("failed to write or read protobuf message")]
    Protobuf(#[from] ProtobufError),
    #[error("unexpected message code, expected: {expected}, found: {found}")]
    CodeMismatch { expected: u8, found: u8 },
    #[error("received message code is not known")]
    UnknownCode(#[from] MessageCodeError),
    #[error("antidote replied with an error")]
    Antidote(#[from] AntidoteError),
    #[error("antdote replied with an error message: ({0}) {1}")]
    AntidoteErrResp(AntidoteError, String),
}

type TxId = Vec<u8>;

macro_rules! checkr {
    ($resp:expr) => {{
        let resp = $resp;
        if !resp.get_success() {
            let errcode = resp.get_errorcode();
            return Err(Error::Antidote(AntidoteError::from(errcode)));
        }

        resp
    }};
}

#[derive(Debug)]
pub struct Connection {
    stream: TcpStream,
    scratchpad: Vec<u8>,
    dropped: Option<TxId>,
}

impl Connection {
    pub async fn new(address: &str) -> Result<Self, Error> {
        let stream = TcpStream::connect(address).await?;
        let _ = stream.set_nodelay(true);

        Ok(Self {
            stream,
            scratchpad: Vec::new(),
            dropped: None,
        })
    }

    pub async fn transaction(&mut self) -> Result<Transaction<'_>, Error> {
        self.transaction_with_locks(TransactionLocks::new()).await
    }

    pub async fn transaction_with_locks(
        &mut self,
        locks: TransactionLocks,
    ) -> Result<Transaction<'_>, Error> {
        // Dangling transactions leading to errors, shouldn't bubble up.
        if let Err(error) = self.abort_pending_transaction().await {
            tracing::warn!(?error, "aborting dangling transaction");
        }

        let mut transaction = ApbStartTransaction::new();

        let mut properties = ApbTxnProperties::default();
        properties.set_exclusive_locks(protobuf::RepeatedField::from_vec(locks.exclusive));
        properties.set_shared_locks(protobuf::RepeatedField::from_vec(locks.shared));

        transaction.set_properties(properties);

        self.send(transaction).await?;
        let response = checkr!(self.recv::<ApbStartTransactionResp>().await?);

        Ok(Transaction {
            connection: self,
            txid: Vec::from(response.get_transaction_descriptor()),
        })
    }

    async fn send<P>(&mut self, request: P) -> Result<(), Error>
    where
        P: ApbMessage,
    {
        let code = P::code();

        let message_size = request.compute_size() + 1 /* code byte */;

        self.scratchpad.clear();

        let mut header: [u8; 5] = [0; 5];
        header[0..4].copy_from_slice(&message_size.to_be_bytes());
        header[4] = code as u8;

        self.scratchpad.extend_from_slice(&header[..]);
        request.write_to_vec(&mut self.scratchpad)?;
        self.stream.write_all(&self.scratchpad[..]).await?;

        Ok(())
    }

    async fn recv<R>(&mut self) -> Result<R, Error>
    where
        R: ApbMessage,
    {
        /* Unfortunatly we cannot reuse our buffer here (unless implementing
        a buffered stream ourselves). However since we are acting only on a
        request/response scheme, we should not drop any data by creating
        a BufReader each time. */
        let mut stream = BufReader::new(&mut self.stream);

        let mut size_buffer: [u8; 4] = [0; 4];
        stream.read_exact(&mut size_buffer).await?;
        let message_size = u32::from_be_bytes(size_buffer);
        self.scratchpad.resize(message_size as usize, 0);

        assert_eq!((&self.scratchpad[..]).len(), message_size as usize);
        stream.read_exact(&mut self.scratchpad[..]).await?;

        let code = ApbMessageCode::try_from(self.scratchpad[0])?;
        if code == ApbMessageCode::ApbErrorResp {
            let msg: ApbErrorResp = protobuf::parse_from_bytes(&self.scratchpad[1..])?;

            return Err(Error::AntidoteErrResp(
                AntidoteError::from(msg.get_errcode()),
                String::from_utf8_lossy(msg.get_errmsg()).into(),
            ));
        }

        if code != R::code() {
            return Err(Error::CodeMismatch {
                expected: R::code() as u8,
                found: code as u8,
            });
        }

        Ok(protobuf::parse_from_bytes(&self.scratchpad[1..])?)
    }

    async fn abort_pending_transaction(&mut self) -> Result<(), Error> {
        let txid = match self.dropped.take() {
            Some(txid) => txid,
            None => return Ok(()),
        };

        tracing::warn!(?txid, "aborting");
        let mut message = ApbAbortTransaction::new();
        message.set_transaction_descriptor(txid);

        self.send(message).await?;
        self.recv::<ApbOperationResp>().await?;
        Ok(())
    }

    pub async fn close(&mut self) -> Result<(), Error> {
        self.abort_pending_transaction().await
    }
}

pub struct Transaction<'a> {
    connection: &'a mut Connection,
    txid: TxId,
}

impl Transaction<'_> {
    pub async fn commit(mut self) -> Result<(), Error> {
        let mut message = ApbCommitTransaction::new();
        message.set_transaction_descriptor(self.txid.clone());

        self.connection.send(message).await?;
        let result = self.connection.recv::<ApbCommitResp>().await;

        /* Don't drop to avoid calling abort */
        self.txid = Vec::new();
        mem::forget(self);

        checkr!(result?);
        Ok(())
    }

    pub async fn read(
        &mut self,
        bucket: impl Into<RawIdent>,
        queries: impl IntoIterator<Item = ReadQuery>,
    ) -> Result<ReadReply, Error> {
        let bucket = bucket.into();

        let mut message = ApbReadObjects::new();
        message.set_transaction_descriptor(self.txid.clone());

        let bound_objects: Vec<_> = queries
            .into_iter()
            .map(|q| {
                let mut bound = ApbBoundObject::new();
                bound.set_bucket(bucket.clone());
                bound.set_field_type(q.ty);
                bound.set_key(q.key);

                bound
            })
            .collect();

        if bound_objects.is_empty() {
            return Ok(ReadReply { objects: Vec::new() });
        }

        message.set_boundobjects(protobuf::RepeatedField::from(bound_objects));

        self.connection.send(message).await?;
        let mut response: ApbReadObjectsResp =
            checkr!(self.connection.recv::<ApbReadObjectsResp>().await?);

        Ok(ReadReply {
            objects: response.take_objects().into_iter().map(Some).collect(),
        })
    }

    pub async fn update(
        &mut self,
        bucket: impl Into<RawIdent>,
        queries: impl IntoIterator<Item = UpdateQuery>,
    ) -> Result<(), Error> {
        let bucket = bucket.into();

        let mut message = ApbUpdateObjects::new();
        message.set_transaction_descriptor(self.txid.clone());

        let bound_objects: Vec<_> = queries
            .into_iter()
            .map(|q| {
                let mut bound = ApbBoundObject::new();
                bound.set_bucket(bucket.clone());
                bound.set_field_type(q.ty);
                bound.set_key(q.key);

                let mut op = ApbUpdateOp::new();
                op.set_boundobject(bound);
                op.set_operation(q.update);

                op
            })
            .collect();

        if bound_objects.is_empty() {
            return Ok(());
        }

        message.set_updates(protobuf::RepeatedField::from(bound_objects));

        self.connection.send(message).await?;
        checkr!(self.connection.recv::<ApbOperationResp>().await?);

        Ok(())
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        assert!(self.connection.dropped.is_none());
        assert!(!self.txid.is_empty());

        tracing::warn!(?self.txid, "dropped, will be aborted");
        self.connection.dropped = Some(mem::replace(&mut self.txid, Vec::new()));
    }
}

#[derive(Debug)]
pub struct TransactionLocks {
    pub exclusive: Vec<RawIdent>,
    pub shared: Vec<RawIdent>,
}

impl TransactionLocks {
    pub fn new() -> Self {
        Self {
            exclusive: Vec::new(),
            shared: Vec::new(),
        }
    }

    pub fn with_capacity(exclusive: usize, shared: usize) -> Self {
        Self {
            exclusive: Vec::with_capacity(exclusive),
            shared: Vec::with_capacity(shared),
        }
    }

    pub fn push_exclusive(&mut self, ident: impl Into<RawIdent>) -> &mut Self {
        self.exclusive.push(ident.into());
        self
    }

    pub fn push_shared(&mut self, ident: impl Into<RawIdent>) -> &mut Self {
        self.shared.push(ident.into());
        self
    }
}

pub type RawIdent = Vec<u8>;
pub type RawIdentSlice<'a> = &'a [u8];

pub struct ReadQuery {
    key: RawIdent,
    ty: CRDT_type,
}

pub struct ReadReply {
    objects: Vec<Option<ApbReadObjectResp>>,
}

impl ReadReply {
    pub fn counter(&mut self, index: usize) -> crdts::Counter {
        self.object(CRDT_type::COUNTER, index)
            .unwrap()
            .into_counter()
    }

    pub fn lwwreg(&mut self, index: usize) -> Option<crdts::LwwReg> {
        let reg = self.object(CRDT_type::LWWREG, index).unwrap().into_lwwreg();

        if !reg.is_empty() {
            Some(reg)
        } else {
            None
        }
    }

    pub fn mvreg(&mut self, index: usize) -> Option<crdts::MvReg> {
        let reg = self.object(CRDT_type::MVREG, index).unwrap().into_mvreg();

        if !reg.is_empty() {
            Some(reg)
        } else {
            None
        }
    }

    pub fn gmap(&mut self, index: usize) -> Option<crdts::GMap> {
        let gmap = self.object(CRDT_type::GMAP, index).unwrap().into_gmap();

        if gmap.is_empty() {
            None
        } else {
            Some(gmap)
        }
    }

    pub fn rrmap(&mut self, index: usize) -> Option<crdts::RrMap> {
        let rrmap = self.object(CRDT_type::RRMAP, index).unwrap().into_rrmap();

        if rrmap.is_empty() {
            None
        } else {
            Some(rrmap)
        }
    }

    pub fn rwset(&mut self, index: usize) -> Option<crdts::RwSet> {
        let rwset = self.object(CRDT_type::RWSET, index).unwrap().into_rwset();

        if rwset.is_empty() {
            None
        } else {
            Some(rwset)
        }
    }

    fn object(&mut self, ty: CRDT_type, index: usize) -> Option<Crdt> {
        match self.objects.get_mut(index) {
            Some(slot) => slot.take().map(|o| Crdt::from_read(ty, o)),
            None => None,
        }
    }
}

pub struct UpdateQuery {
    key: RawIdent,
    ty: CRDT_type,
    update: ApbUpdateOperation,
}

pub mod counter {
    use super::{
        ApbCounterUpdate, ApbUpdateOperation, CRDT_type, RawIdent, ReadQuery, UpdateQuery,
    };

    pub type Counter = i32;

    pub fn get(key: impl Into<RawIdent>) -> ReadQuery {
        ReadQuery {
            key: key.into(),
            ty: CRDT_type::COUNTER,
        }
    }

    pub fn inc(key: impl Into<RawIdent>, value: Counter) -> UpdateQuery {
        let mut inc = ApbCounterUpdate::new();
        inc.set_inc(value as i64);

        let mut update = ApbUpdateOperation::new();
        update.set_counterop(inc);

        UpdateQuery {
            key: key.into(),
            ty: CRDT_type::COUNTER,
            update,
        }
    }
}

pub mod lwwreg {
    use super::{ApbRegUpdate, ApbUpdateOperation, CRDT_type, RawIdent, ReadQuery, UpdateQuery};

    pub type LwwReg = Vec<u8>;

    pub fn get(key: impl Into<RawIdent>) -> ReadQuery {
        ReadQuery {
            key: key.into(),
            ty: CRDT_type::LWWREG,
        }
    }

    pub fn set(key: impl Into<RawIdent>, value: Vec<u8>) -> UpdateQuery {
        let mut set = ApbRegUpdate::new();
        set.set_value(value);

        let mut update = ApbUpdateOperation::new();
        update.set_regop(set);

        UpdateQuery {
            key: key.into(),
            ty: CRDT_type::LWWREG,
            update,
        }
    }

    pub use crate::encoding::lwwreg::*;
}

pub mod mvreg {
    use super::{
        ApbCrdtReset, ApbRegUpdate, ApbUpdateOperation, CRDT_type, RawIdent, ReadQuery, UpdateQuery,
    };

    pub type MvReg = Vec<Vec<u8>>;

    pub fn get(key: impl Into<RawIdent>) -> ReadQuery {
        ReadQuery {
            key: key.into(),
            ty: CRDT_type::MVREG,
        }
    }

    pub fn set(key: impl Into<RawIdent>, reg: Vec<u8>) -> UpdateQuery {
        let mut set = ApbRegUpdate::new();
        set.set_value(reg);

        let mut update = ApbUpdateOperation::new();
        update.set_regop(set);

        UpdateQuery {
            key: key.into(),
            ty: CRDT_type::MVREG,
            update,
        }
    }

    pub fn reset(key: impl Into<RawIdent>) -> UpdateQuery {
        let mut update = ApbUpdateOperation::new();
        update.set_resetop(ApbCrdtReset::new());

        UpdateQuery {
            key: key.into(),
            ty: CRDT_type::MVREG,
            update,
        }
    }

    pub use crate::encoding::mvreg::*;
}

pub mod gmap {
    use super::crdts::Crdt;
    use super::{
        ApbMapKey, ApbMapNestedUpdate, ApbMapUpdate, ApbUpdateOperation, CRDT_type, RawIdent,
        ReadQuery, UpdateQuery,
    };
    use std::collections::HashMap;

    pub type GMap = HashMap<RawIdent, Crdt>;

    pub struct UpdateBuilder {
        key: RawIdent,
        updates: Vec<ApbMapNestedUpdate>,
    }

    impl UpdateBuilder {
        pub fn push(mut self, query: UpdateQuery) -> Self {
            let mut nested = ApbMapNestedUpdate::new();
            let mut key = ApbMapKey::new();
            key.set_field_type(query.ty);
            key.set_key(query.key);

            nested.set_update(query.update);
            nested.set_key(key);

            self.updates.push(nested);
            self
        }

        pub fn build(self) -> UpdateQuery {
            let mut updates = ApbMapUpdate::new();
            updates.set_updates(protobuf::RepeatedField::from(self.updates));

            let mut update = ApbUpdateOperation::new();
            update.set_mapop(updates);

            UpdateQuery {
                key: self.key,
                update,
                ty: CRDT_type::GMAP,
            }
        }
    }

    pub fn update(key: impl Into<RawIdent>, capacity: usize) -> UpdateBuilder {
        UpdateBuilder {
            key: key.into(),
            updates: Vec::with_capacity(capacity),
        }
    }

    pub fn get(key: impl Into<RawIdent>) -> ReadQuery {
        ReadQuery {
            key: key.into(),
            ty: CRDT_type::GMAP,
        }
    }
}

pub mod rrmap {
    use super::crdts::Crdt;
    use super::{
        ApbCrdtReset, ApbMapKey, ApbMapNestedUpdate, ApbMapUpdate, ApbUpdateOperation, CRDT_type,
        RawIdent, ReadQuery, UpdateQuery,
    };
    use std::collections::HashMap;

    pub type RrMap = HashMap<RawIdent, Crdt>;

    pub struct UpdateBuilder {
        key: RawIdent,
        updates: Vec<ApbMapNestedUpdate>,
        removed: Vec<ApbMapKey>,
    }

    impl UpdateBuilder {
        pub fn push(mut self, query: UpdateQuery) -> Self {
            let mut nested = ApbMapNestedUpdate::new();
            let mut key = ApbMapKey::new();
            key.set_field_type(query.ty);
            key.set_key(query.key);

            nested.set_update(query.update);
            nested.set_key(key);

            self.updates.push(nested);
            self
        }

        pub fn remove_mvreg(mut self, ident: impl Into<RawIdent>) -> Self {
            let mut key = ApbMapKey::new();
            key.set_field_type(CRDT_type::MVREG);
            key.set_key(ident.into());

            self.removed.push(key);
            self
        }

        pub fn remove_rwset(mut self, ident: impl Into<RawIdent>) -> Self {
            let mut key = ApbMapKey::new();
            key.set_field_type(CRDT_type::RWSET);
            key.set_key(ident.into());

            self.removed.push(key);
            self
        }

        pub fn build(self) -> UpdateQuery {
            let mut updates = ApbMapUpdate::new();
            updates.set_updates(protobuf::RepeatedField::from(self.updates));
            updates.set_removedKeys(protobuf::RepeatedField::from(self.removed));

            let mut update = ApbUpdateOperation::new();
            update.set_mapop(updates);

            UpdateQuery {
                key: self.key,
                update,
                ty: CRDT_type::RRMAP,
            }
        }
    }

    pub fn reset(key: impl Into<RawIdent>) -> UpdateQuery {
        let mut update = ApbUpdateOperation::new();
        update.set_resetop(ApbCrdtReset::new());

        UpdateQuery {
            key: key.into(),
            ty: CRDT_type::RRMAP,
            update,
        }
    }

    pub fn update(key: impl Into<RawIdent>) -> UpdateBuilder {
        UpdateBuilder {
            key: key.into(),
            updates: Vec::new(),
            removed: Vec::new(),
        }
    }

    pub fn get(key: impl Into<RawIdent>) -> ReadQuery {
        ReadQuery {
            key: key.into(),
            ty: CRDT_type::RRMAP,
        }
    }
}

pub mod rwset {
    use super::{
        ApbCrdtReset, ApbSetUpdate, ApbSetUpdate_SetOpType, ApbUpdateOperation, CRDT_type,
        RawIdent, ReadQuery, UpdateQuery,
    };
    use std::collections::HashSet;
    pub type RwSet = HashSet<Vec<u8>>;

    pub fn reset(key: impl Into<RawIdent>) -> UpdateQuery {
        let mut update = ApbUpdateOperation::new();
        update.set_resetop(ApbCrdtReset::new());

        UpdateQuery {
            key: key.into(),
            ty: CRDT_type::RWSET,
            update,
        }
    }
    pub struct InsertBuilder {
        key: RawIdent,
        inserts: Vec<Vec<u8>>,
    }

    impl InsertBuilder {
        pub fn add(mut self, value: Vec<u8>) -> Self {
            self.inserts.push(value);
            self
        }

        pub fn build(self) -> UpdateQuery {
            let mut updates = ApbSetUpdate::new();
            updates.set_optype(ApbSetUpdate_SetOpType::ADD);
            updates.set_adds(protobuf::RepeatedField::from(self.inserts));

            let mut update = ApbUpdateOperation::new();
            update.set_setop(updates);

            UpdateQuery {
                key: self.key,
                update,
                ty: CRDT_type::RWSET,
            }
        }
    }

    pub fn insert(key: impl Into<RawIdent>) -> InsertBuilder {
        InsertBuilder {
            key: key.into(),
            inserts: Vec::new(),
        }
    }

    pub struct RemoveBuilder {
        key: RawIdent,
        inserts: Vec<Vec<u8>>,
    }

    impl RemoveBuilder {
        pub fn remove(mut self, value: Vec<u8>) -> Self {
            self.inserts.push(value);
            self
        }

        pub fn build(self) -> UpdateQuery {
            let mut updates = ApbSetUpdate::new();
            updates.set_optype(ApbSetUpdate_SetOpType::REMOVE);
            updates.set_rems(protobuf::RepeatedField::from(self.inserts));

            let mut update = ApbUpdateOperation::new();
            update.set_setop(updates);

            UpdateQuery {
                key: self.key,
                update,
                ty: CRDT_type::RWSET,
            }
        }
    }

    pub fn remove(key: impl Into<RawIdent>) -> RemoveBuilder {
        RemoveBuilder {
            key: key.into(),
            inserts: Vec::new(),
        }
    }

    pub fn get(key: impl Into<RawIdent>) -> ReadQuery {
        ReadQuery {
            key: key.into(),
            ty: CRDT_type::RWSET,
        }
    }
}

pub mod crdts {
    pub use super::{
        counter::Counter, gmap::GMap, lwwreg::LwwReg, mvreg::MvReg, rrmap::RrMap, rwset::RwSet,
        RawIdent,
    };
    use super::{ApbReadObjectResp, CRDT_type};
    use std::collections::{HashMap, HashSet};

    #[derive(Debug)]
    pub enum Crdt {
        Counter(Counter),
        LwwReg(LwwReg),
        MvReg(MvReg),
        GMap(GMap),
        RwSet(RwSet),
        RrMap(RrMap),
    }

    impl Crdt {
        pub fn into_counter(self) -> Counter {
            match self {
                Self::Counter(c) => c,
                _ => self.expected("counter"),
            }
        }

        pub fn into_lwwreg(self) -> LwwReg {
            match self {
                Self::LwwReg(r) => r,
                _ => self.expected("lwwreg"),
            }
        }

        pub fn into_gmap(self) -> GMap {
            match self {
                Self::GMap(m) => m,
                _ => self.expected("gmap"),
            }
        }

        pub fn into_rrmap(self) -> RrMap {
            match self {
                Self::RrMap(m) => m,
                _ => self.expected("rrmap"),
            }
        }

        pub fn into_rwset(self) -> RwSet {
            match self {
                Self::RwSet(m) => m,
                _ => self.expected("rwset"),
            }
        }

        pub fn into_mvreg(self) -> MvReg {
            match self {
                Self::MvReg(m) => m,
                _ => self.expected("mvreg"),
            }
        }

        fn expected(&self, name: &str) -> ! {
            panic!("expected {}, found: {:#?}", name, self)
        }

        pub(super) fn from_read(ty: CRDT_type, mut read: ApbReadObjectResp) -> Self {
            use Crdt::*;

            match ty {
                CRDT_type::COUNTER => Counter(read.take_counter().get_value()),
                CRDT_type::LWWREG => LwwReg(read.take_reg().take_value()),
                CRDT_type::MVREG => MvReg(read.take_mvreg().take_values().into_vec()),
                CRDT_type::GMAP => GMap(Self::map(read)),
                CRDT_type::RRMAP => RrMap(Self::map(read)),
                CRDT_type::RWSET => RwSet(Self::set(read)),
                _ => unimplemented!(),
            }
        }

        fn set(mut read: ApbReadObjectResp) -> HashSet<Vec<u8>> {
            let entries = read.take_set().take_value();
            let mut output = HashSet::new();

            for entry in entries.into_vec() {
                output.insert(entry);
            }

            output
        }

        fn map(mut read: ApbReadObjectResp) -> HashMap<RawIdent, Crdt> {
            let entries = read.take_map().take_entries();

            let mut map = HashMap::with_capacity(entries.len());
            for mut entry in entries.into_iter() {
                let mut entry_key = entry.take_key();
                let key = entry_key.take_key();

                map.insert(
                    key,
                    Self::from_read(entry_key.get_field_type(), entry.take_value()),
                );
            }

            map
        }
    }
}

#[macro_export]
macro_rules! chain {
    ($head:expr, ($tail:expr),+) => {
        std::iter::once($head).chain(chain!($($tail),+))
    };
    ($item:expr) => {
        std::iter::once($item)
    };
}

#[macro_export]
macro_rules! reads {
    ($($item:expr),*) => {
        $crate::chain!($($item),*)
    };
}

#[macro_export]
macro_rules! updates {
    ($($item:expr),*) => {
        $crate::chain!($($item),*)
    };
}
