use crate::key::{Bucket, KeyWriter, Ty};
use antidotec::{lwwreg, Connection, Error, RawIdent, TransactionLocks};
use std::ops::Range;
use async_std::sync::Mutex;
use std::mem;

const BATCH_SIZE: u64 = 1 << 16;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum InodeKind {
    Regular = 0,
    Directory = 1,
    Symlink = 2,
}


pub use self::InodeKind::*;

pub fn with_kind(kind: InodeKind, id: u64) -> u64 {
    assert!(id < (1 << (64 - 8*mem::size_of::<InodeKind>())));

    (id << 8*mem::size_of::<InodeKind>()) | kind as u64
}

pub fn kind(ino: u64) -> InodeKind {
    let x = ino & ((1 << 8*mem::size_of::<InodeKind>()) - 1) as u64;

    match x {
        _ if ino == 1 => InodeKind::Directory,
        0 => InodeKind::Regular,
        1 => InodeKind::Directory,
        2 => InodeKind::Symlink,
        _ => unreachable!(),
    }
}

pub fn file_type(ino: u64) -> fuse::FileType {
    match self::kind(ino) {
        InodeKind::Regular => fuse::FileType::RegularFile,
        InodeKind::Directory => fuse::FileType::Directory,
        InodeKind::Symlink => fuse::FileType::Symlink,
    }
}

#[derive(Debug)]
pub struct InoGenerator {
    bucket: Bucket,
    range: Mutex<Range<u64>>,
}

impl InoGenerator {
    pub async fn load(connection: &mut Connection, bucket: Bucket) -> Result<Self, Error> {
        let (next, max) = Self::allocate_range(connection, bucket).await?;

        Ok(Self {
            bucket,
            range: Mutex::new(next..max),
        })
    }

    pub async fn next(&self, kind: InodeKind, connection: &mut Connection) -> Result<u64, Error> {
        let ino = self.next_id(connection).await?;
        Ok(self::with_kind(kind, ino))
    }

    pub async fn next_id(&self, connection: &mut Connection) -> Result<u64, Error> {
        let mut range = self.range.lock().await;

        let found = loop {
            let next = range.start + 1;
            *range = next..range.end;

            if next >= range.end {
                let (next, max) = Self::allocate_range(connection, self.bucket).await?;
                *range = next..max;
            } else {
                break next;
            }
        };

        Ok(found)
    }

    pub async fn allocate_range(
        connection: &mut Connection,
        bucket: Bucket,
    ) -> Result<(u64, u64), Error> {
        let mut tx = connection
            .transaction_with_locks(TransactionLocks {
                exclusive: vec![key().into()],
                shared: vec![],
            })
            .await?;

        let mut reply = tx.read(bucket, vec![lwwreg::get(key())]).await?;
        let result = match reply.lwwreg(0) {
            None => {
                let next = 2;
                let max = next + BATCH_SIZE;
                tx.update(bucket, vec![lwwreg::set_u64(key(), max)]).await?;

                (next, max)
            }
            Some(previous_max) => {
                let next = lwwreg::read_u64(&previous_max);
                let max = next + BATCH_SIZE;
                tx.update(bucket, vec![lwwreg::set_u64(key(), max)]).await?;

                (next, max)
            }
        };

        tx.commit().await?;
        Ok(result)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Key {}

impl Key {
    fn new() -> Self {
        Self {}
    }
}

pub fn key() -> Key {
    Key::new()
}

impl Into<RawIdent> for Key {
    fn into(self) -> RawIdent {
        KeyWriter::with_capacity(Ty::InoCounter, 0).into()
    }
}
