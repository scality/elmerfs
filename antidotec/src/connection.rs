use self::crdts::Crdt;
use crate::protos::{antidote::*, ApbMessage, ApbMessageCode, MessageCodeError};
use async_std::{
    io::{self, prelude::*},
    net::TcpStream,
};
pub use prost::bytes::Bytes;
use prost::{DecodeError, EncodeError, Message};
use std::convert::TryInto;
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
    #[error("failed to write protobuf message")]
    ProtobufEncode(#[from] EncodeError),
    #[error("failed to read protobuf message")]
    ProtobufDecode(#[from] DecodeError),
    #[error("unexpected message code, expected: {expected}, found: {found}")]
    CodeMismatch { expected: u8, found: u8 },
    #[error("received message code is not known")]
    UnknownCode(#[from] MessageCodeError),
    #[error("antidote replied with an error")]
    Antidote(#[from] AntidoteError),
    #[error("antdote replied with an error message: ({0}) {1}")]
    AntidoteErrResp(AntidoteError, String),
}

type TxId = Bytes;

macro_rules! checkr {
    ($resp:expr) => {{
        let resp = $resp;
        if !resp.success {
            let errcode = resp.errorcode.unwrap();
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
            scratchpad: Vec::with_capacity(128 * 1024),
            dropped: None,
        })
    }

    pub async fn transaction(&mut self) -> Result<Transaction<'_>, Error> {
        self.transaction_with_locks(TransactionLocks::new()).await
    }

    #[tracing::instrument(skip(self))]
    pub async fn transaction_with_locks(
        &mut self,
        locks: TransactionLocks,
    ) -> Result<Transaction<'_>, Error> {
        // Dangling transactions leading to errors, shouldn't bubble up.
        if let Err(error) = self.abort_pending_transaction().await {
            tracing::warn!(?error, "aborting dangling transaction");
        }

        let transaction = ApbStartTransaction {
            properties: Some(ApbTxnProperties {
                exclusive_locks: locks.exclusive,
                shared_locks: locks.shared,
                ..ApbTxnProperties::default()
            }),
            ..ApbStartTransaction::default()
        };
        self.send(transaction).await?;
        let response = checkr!(self.recv::<ApbStartTransactionResp>().await?);

        Ok(Transaction {
            connection: self,
            txid: response.transaction_descriptor.unwrap(),
        })
    }

    #[inline]
    async fn send<P>(&mut self, request: P) -> Result<(), Error>
    where
        P: ApbMessage,
    {
        const HEADER_SIZE: u32 = 5;
        let message_size = request.encoded_len();
        let code = P::code();

        let payload_size = (message_size as u32) + HEADER_SIZE;
        if self.scratchpad.len() < payload_size as usize {
            self.scratchpad.resize(payload_size as usize, 0);
        }

        {
            let (header, payload) = self.scratchpad.split_at_mut(HEADER_SIZE as usize);
            header[0..4].copy_from_slice(&(message_size as u32 + 1).to_be_bytes());
            header[4] = code as u8;

            let mut command = &mut payload[..message_size as usize];
            request.encode(&mut command)?;
        }

        self.stream
            .write_all(&self.scratchpad[..payload_size as usize])
            .await?;
        Ok(())
    }

    #[inline]
    async fn recv<R>(&mut self) -> Result<R, Error>
    where
        R: Default + ApbMessage,
    {
        const MESSAGE_SIZE_BYTES: usize = 4;

        assert!(self.scratchpad.len() >= MESSAGE_SIZE_BYTES);
        let mut n = 0;

        while n < MESSAGE_SIZE_BYTES {
            n += self.stream.read(&mut self.scratchpad[n..]).await?;
        }

        let message_site_bytes = self.scratchpad[..MESSAGE_SIZE_BYTES].try_into().unwrap();
        let message_size = u32::from_be_bytes(message_site_bytes) as usize;

        let payload_size = message_size + MESSAGE_SIZE_BYTES;
        if payload_size > self.scratchpad.len() {
            self.scratchpad.resize(payload_size as usize, 0);
        }

        // We may have to read the end of the message.
        while n < payload_size {
            n += self.stream.read(&mut self.scratchpad[n..]).await?;
        }

        let message_bytes = &self.scratchpad[MESSAGE_SIZE_BYTES..payload_size];
        let code = ApbMessageCode::try_from(message_bytes[0])?;
        if code == ApbMessageCode::ApbErrorResp {
            let msg: ApbErrorResp = ApbErrorResp::decode(&self.scratchpad[1..])?;

            return Err(Error::AntidoteErrResp(
                AntidoteError::from(msg.errcode),
                String::from_utf8_lossy(&msg.errmsg[..]).into(),
            ));
        }

        if code != R::code() {
            return Err(Error::CodeMismatch {
                expected: R::code() as u8,
                found: code as u8,
            });
        }

        Ok(R::decode(&message_bytes[1..])?)
    }

    async fn abort_pending_transaction(&mut self) -> Result<(), Error> {
        let txid = match self.dropped.take() {
            Some(txid) => txid,
            None => return Ok(()),
        };

        tracing::warn!(?txid, "aborting");
        let message = ApbAbortTransaction {
            transaction_descriptor: txid,
        };

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
    #[tracing::instrument(skip(self))]
    pub async fn commit(mut self) -> Result<(), Error> {
        let mut message = ApbCommitTransaction::default();
        message.transaction_descriptor = self.txid.clone();

        self.connection.send(message).await?;
        let result = self.connection.recv::<ApbCommitResp>().await;

        /* Don't drop to avoid calling abort */
        self.txid = TxId::new();
        mem::forget(self);

        checkr!(result?);
        Ok(())
    }

    #[tracing::instrument(skip(self, bucket, queries))]
    pub async fn read(
        &mut self,
        bucket: impl Into<RawIdent>,
        queries: impl IntoIterator<Item = ReadQuery>,
    ) -> Result<ReadReply, Error> {
        let bucket = bucket.into();

        let bound_objects: Vec<_> = queries
            .into_iter()
            .map(|q| ApbBoundObject {
                bucket: bucket.clone(),
                r#type: q.ty as i32,
                key: q.key,
            })
            .collect();

        if bound_objects.is_empty() {
            return Ok(ReadReply {
                objects: Vec::new(),
            });
        }

        let message = ApbReadObjects {
            transaction_descriptor: self.txid.clone(),
            boundobjects: bound_objects,
            ..ApbReadObjects::default()
        };
        self.connection.send(message).await?;

        let response: ApbReadObjectsResp =
            checkr!(self.connection.recv::<ApbReadObjectsResp>().await?);

        Ok(ReadReply {
            objects: response.objects.into_iter().map(Some).collect(),
        })
    }

    #[tracing::instrument(skip(self, bucket, queries))]
    pub async fn update(
        &mut self,
        bucket: impl Into<RawIdent>,
        queries: impl IntoIterator<Item = UpdateQuery>,
    ) -> Result<(), Error> {
        let bucket = bucket.into();

        let updates: Vec<_> = queries
            .into_iter()
            .map(|q| ApbUpdateOp {
                boundobject: ApbBoundObject {
                    bucket: bucket.clone(),
                    r#type: q.ty as i32,
                    key: q.key,
                },
                operation: q.update,
            })
            .collect();

        if updates.is_empty() {
            return Ok(());
        }

        let message = ApbUpdateObjects {
            transaction_descriptor: self.txid.clone(),
            updates,
        };
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
        self.connection.dropped = Some(self.txid.clone());
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

pub type RawIdent = Bytes;

pub struct ReadQuery {
    key: RawIdent,
    ty: CrdtType,
}

pub struct ReadReply {
    objects: Vec<Option<ApbReadObjectResp>>,
}

impl ReadReply {
    pub fn counter(&mut self, index: usize) -> crdts::Counter {
        self.object(CrdtType::Counter, index)
            .unwrap()
            .into_counter()
    }

    pub fn lwwreg(&mut self, index: usize) -> Option<crdts::LwwReg> {
        let reg = self.object(CrdtType::Lwwreg, index).unwrap().into_lwwreg();

        if !reg.is_empty() {
            Some(reg)
        } else {
            None
        }
    }

    pub fn mvreg(&mut self, index: usize) -> Option<crdts::MvReg> {
        let reg = self.object(CrdtType::Mvreg, index).unwrap().into_mvreg();

        if !reg.is_empty() {
            Some(reg)
        } else {
            None
        }
    }

    pub fn gmap(&mut self, index: usize) -> Option<crdts::GMap> {
        let gmap = self.object(CrdtType::Gmap, index).unwrap().into_gmap();

        if gmap.is_empty() {
            None
        } else {
            Some(gmap)
        }
    }

    pub fn rrmap(&mut self, index: usize) -> Option<crdts::RrMap> {
        let rrmap = self.object(CrdtType::Rrmap, index).unwrap().into_rrmap();

        if rrmap.is_empty() {
            None
        } else {
            Some(rrmap)
        }
    }

    pub fn rwset(&mut self, index: usize) -> Option<crdts::RwSet> {
        let rwset = self.object(CrdtType::Rwset, index).unwrap().into_rwset();

        if rwset.is_empty() {
            None
        } else {
            Some(rwset)
        }
    }

    fn object(&mut self, ty: CrdtType, index: usize) -> Option<Crdt> {
        match self.objects.get_mut(index) {
            Some(slot) => slot.take().map(|o| Crdt::from_read(ty, o)),
            None => None,
        }
    }
}

pub struct UpdateQuery {
    key: RawIdent,
    ty: CrdtType,
    update: ApbUpdateOperation,
}

pub mod counter {
    use super::{ApbCounterUpdate, ApbUpdateOperation, CrdtType, RawIdent, ReadQuery, UpdateQuery};

    pub type Counter = i32;

    pub fn get(key: impl Into<RawIdent>) -> ReadQuery {
        ReadQuery {
            key: key.into(),
            ty: CrdtType::Counter,
        }
    }

    pub fn inc(key: impl Into<RawIdent>, value: Counter) -> UpdateQuery {
        let update = ApbUpdateOperation {
            counterop: Some(ApbCounterUpdate {
                inc: Some(value as i64),
            }),
            ..ApbUpdateOperation::default()
        };
        UpdateQuery {
            key: key.into(),
            ty: CrdtType::Counter,
            update,
        }
    }
}

pub mod lwwreg {
    use super::{ApbRegUpdate, ApbUpdateOperation, CrdtType, RawIdent, ReadQuery, UpdateQuery};
    use prost::bytes::Bytes;

    pub type LwwReg = Bytes;

    pub fn get(key: impl Into<RawIdent>) -> ReadQuery {
        ReadQuery {
            key: key.into(),
            ty: CrdtType::Lwwreg,
        }
    }

    pub fn set(key: impl Into<RawIdent>, value: Bytes) -> UpdateQuery {
        UpdateQuery {
            key: key.into(),
            ty: CrdtType::Lwwreg,
            update: ApbUpdateOperation {
                regop: Some(ApbRegUpdate { value }),
                ..ApbUpdateOperation::default()
            },
        }
    }

    pub use crate::encoding::lwwreg::*;
}

pub mod mvreg {
    use prost::bytes::Bytes;

    use super::{
        ApbCrdtReset, ApbRegUpdate, ApbUpdateOperation, CrdtType, RawIdent, ReadQuery, UpdateQuery,
    };

    pub type MvReg = Vec<Bytes>;

    pub fn get(key: impl Into<RawIdent>) -> ReadQuery {
        ReadQuery {
            key: key.into(),
            ty: CrdtType::Mvreg,
        }
    }

    pub fn set(key: impl Into<RawIdent>, value: Bytes) -> UpdateQuery {
        UpdateQuery {
            key: key.into(),
            ty: CrdtType::Mvreg,
            update: ApbUpdateOperation {
                regop: Some(ApbRegUpdate { value }),
                ..ApbUpdateOperation::default()
            },
        }
    }

    pub fn reset(key: impl Into<RawIdent>) -> UpdateQuery {
        UpdateQuery {
            key: key.into(),
            ty: CrdtType::Mvreg,
            update: ApbUpdateOperation {
                resetop: Some(ApbCrdtReset::default()),
                ..ApbUpdateOperation::default()
            },
        }
    }

    pub use crate::encoding::mvreg::*;
}

pub mod gmap {
    use super::crdts::Crdt;
    use super::{
        ApbMapKey, ApbMapNestedUpdate, ApbMapUpdate, ApbUpdateOperation, CrdtType, RawIdent,
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
            self.updates.push(ApbMapNestedUpdate {
                key: ApbMapKey {
                    r#type: query.ty as i32,
                    key: query.key,
                },
                update: query.update,
                ..ApbMapNestedUpdate::default()
            });
            self
        }

        pub fn build(self) -> UpdateQuery {
            UpdateQuery {
                key: self.key,
                ty: CrdtType::Gmap,
                update: ApbUpdateOperation {
                    mapop: Some(ApbMapUpdate {
                        updates: self.updates,
                        ..ApbMapUpdate::default()
                    }),
                    ..ApbUpdateOperation::default()
                },
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
            ty: CrdtType::Gmap,
        }
    }
}

pub mod rrmap {
    use super::crdts::Crdt;
    use super::{
        ApbCrdtReset, ApbMapKey, ApbMapNestedUpdate, ApbMapUpdate, ApbUpdateOperation, CrdtType,
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
            self.updates.push(ApbMapNestedUpdate {
                key: ApbMapKey {
                    r#type: query.ty as i32,
                    key: query.key,
                },
                update: query.update,
            });
            self
        }

        pub fn remove_mvreg(mut self, ident: impl Into<RawIdent>) -> Self {
            self.removed.push(ApbMapKey {
                r#type: CrdtType::Mvreg as i32,
                key: ident.into(),
            });
            self
        }

        pub fn remove_rwset(mut self, ident: impl Into<RawIdent>) -> Self {
            self.removed.push(ApbMapKey {
                r#type: CrdtType::Rwset as i32,
                key: ident.into(),
            });
            self
        }

        pub fn build(self) -> UpdateQuery {
            UpdateQuery {
                key: self.key,
                ty: CrdtType::Rrmap,
                update: ApbUpdateOperation {
                    mapop: Some(ApbMapUpdate {
                        updates: self.updates,
                        removed_keys: self.removed,
                    }),
                    ..ApbUpdateOperation::default()
                },
            }
        }
    }

    pub fn reset(key: impl Into<RawIdent>) -> UpdateQuery {
        UpdateQuery {
            key: key.into(),
            ty: CrdtType::Rrmap,
            update: ApbUpdateOperation {
                resetop: Some(ApbCrdtReset::default()),
                ..ApbUpdateOperation::default()
            },
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
            ty: CrdtType::Rrmap,
        }
    }
}

pub mod rwset {
    use super::{
        apb_set_update::SetOpType, ApbCrdtReset, ApbSetUpdate, ApbUpdateOperation, CrdtType,
        RawIdent, ReadQuery, UpdateQuery,
    };
    use prost::bytes::Bytes;
    use std::collections::HashSet;
    pub type RwSet = HashSet<Bytes>;

    pub fn reset(key: impl Into<RawIdent>) -> UpdateQuery {
        UpdateQuery {
            key: key.into(),
            ty: CrdtType::Rwset,
            update: ApbUpdateOperation {
                resetop: Some(ApbCrdtReset::default()),
                ..ApbUpdateOperation::default()
            },
        }
    }
    pub struct InsertBuilder {
        key: RawIdent,
        inserts: Vec<Bytes>,
    }

    impl InsertBuilder {
        pub fn add(mut self, value: Bytes) -> Self {
            self.inserts.push(value);
            self
        }

        pub fn build(self) -> UpdateQuery {
            UpdateQuery {
                key: self.key,
                ty: CrdtType::Rwset,
                update: ApbUpdateOperation {
                    setop: Some(ApbSetUpdate {
                        optype: SetOpType::Add as i32,
                        adds: self.inserts,
                        ..ApbSetUpdate::default()
                    }),
                    ..ApbUpdateOperation::default()
                },
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
        removes: Vec<RawIdent>,
    }

    impl RemoveBuilder {
        pub fn remove(mut self, key: RawIdent) -> Self {
            self.removes.push(key);
            self
        }

        pub fn build(self) -> UpdateQuery {
            UpdateQuery {
                key: self.key,
                ty: CrdtType::Rwset,
                update: ApbUpdateOperation {
                    setop: Some(ApbSetUpdate {
                        optype: SetOpType::Remove as i32,
                        rems: self.removes,
                        ..ApbSetUpdate::default()
                    }),
                    ..ApbUpdateOperation::default()
                },
            }
        }
    }

    pub fn remove(key: impl Into<RawIdent>) -> RemoveBuilder {
        RemoveBuilder {
            key: key.into(),
            removes: Vec::new(),
        }
    }

    pub fn get(key: impl Into<RawIdent>) -> ReadQuery {
        ReadQuery {
            key: key.into(),
            ty: CrdtType::Rwset,
        }
    }
}

pub mod crdts {
    use prost::bytes::Bytes;

    pub use super::{
        counter::Counter, gmap::GMap, lwwreg::LwwReg, mvreg::MvReg, rrmap::RrMap, rwset::RwSet,
        RawIdent,
    };
    use super::{ApbReadObjectResp, CrdtType};
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

        pub(super) fn from_read(ty: CrdtType, mut read: ApbReadObjectResp) -> Self {
            use Crdt::*;

            match ty {
                CrdtType::Counter => Counter(read.counter.take().unwrap().value),
                CrdtType::Lwwreg => LwwReg(read.reg.take().unwrap().value),
                CrdtType::Mvreg => MvReg(read.mvreg.take().unwrap().values),
                CrdtType::Gmap => GMap(Self::map(read)),
                CrdtType::Rrmap => RrMap(Self::map(read)),
                CrdtType::Rwset => RwSet(Self::set(read)),
                _ => unimplemented!(),
            }
        }

        fn set(mut read: ApbReadObjectResp) -> HashSet<Bytes> {
            let entries = read.set.take().unwrap().value;
            entries.into_iter().collect()
        }

        fn map(mut read: ApbReadObjectResp) -> HashMap<RawIdent, Crdt> {
            let entries = read.map.take().unwrap().entries;

            let mut map = HashMap::with_capacity(entries.len());
            for entry in entries {
                let map_key = entry.key;
                let ty = Self::ty_from_i32(map_key.r#type).expect("crdt type");

                map.insert(map_key.key, Self::from_read(ty, entry.value));
            }

            map
        }

        fn ty_from_i32(i: i32) -> Option<CrdtType> {
            match i {
                3 => Some(CrdtType::Counter),
                4 => Some(CrdtType::Orset),
                5 => Some(CrdtType::Lwwreg),
                6 => Some(CrdtType::Mvreg),
                8 => Some(CrdtType::Gmap),
                10 => Some(CrdtType::Rwset),
                11 => Some(CrdtType::Rrmap),
                12 => Some(CrdtType::Fatcounter),
                13 => Some(CrdtType::FlagEw),
                14 => Some(CrdtType::FlagDw),
                15 => Some(CrdtType::Bcounter),
                _ => None,
            }
        }
    }
}

#[macro_export]
macro_rules! reads {
    ($head:expr, $($tail:expr),+) => {
        std::iter::once($head).chain(reads!($($tail),*))
    };
    ($item:expr) => {
        std::iter::once($item)
    };
}

#[macro_export]
macro_rules! updates {
    ($head:expr, $($tail:expr),+) => {
        std::iter::once($head).chain(updates!($($tail),*))
    };
    ($item:expr) => {
        std::iter::once($item)
    };
}
