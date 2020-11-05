use crate::key::{Bucket, KeyWriter, Ty};
use antidotec::{lwwreg, Connection, Error, RawIdent, TransactionLocks};
use std::ops::Range;
use async_std::sync::Mutex;

const BATCH_SIZE: u64 = 1 << 16;

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

    pub async fn next(&self, connection: &mut Connection) -> Result<u64, Error> {
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
