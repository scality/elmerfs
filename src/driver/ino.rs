use crate::key::{Bucket, KeyWriter, Ty};
use crate::view::View;
use antidotec::{counter, Error, RawIdent, Transaction};
use std::mem;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
pub struct InoGenerator {
    bucket: Bucket,
    view: View,
    counter: AtomicU64,
}

impl InoGenerator {
    pub async fn load(tx: &mut Transaction<'_>, view: View, bucket: Bucket) -> Result<Self, Error> {
        let next_ino = Self::stored_ino(tx, view, bucket).await?;

        Ok(Self {
            view,
            bucket,
            counter: AtomicU64::new(next_ino),
        })
    }

    pub fn next(&self) -> u64 {
        let next_ino = self.counter.fetch_sub(1, Ordering::Relaxed);
        assert!(next_ino > 1 && next_ino < (1 << 48));

        (next_ino << 16) | self.view as u64
    }

    pub async fn checkpoint(&self, tx: &mut Transaction<'_>) -> Result<(), Error> {
        let key = key(self.view);

        let stored = Self::stored_ino(tx, self.view, self.bucket).await?;
        let current = self.counter.load(Ordering::Relaxed);

        let inc = -(stored.checked_sub(current).unwrap() as i32);
        tx.update(self.bucket, vec![counter::inc(key, inc)]).await?;

        Ok(())
    }

    async fn stored_ino(
        tx: &mut Transaction<'_>,
        view: View,
        bucket: Bucket,
    ) -> Result<u64, Error> {
        let key = key(view);

        let mut reply = tx.read(bucket, vec![counter::get(key)]).await?;

        let offset = i32::max_value() as u32;
        let counter = match reply.counter(0) {
            0 => {
                let start_value = i32::max_value();
                tx.update(bucket, vec![counter::inc(key, start_value)])
                    .await?;

                start_value as u32 + offset
            }
            x => x as u32 + offset,
        };

        /* Note that Antidote support only 32bit counters */
        Ok(counter as u64)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Key(View);

impl Key {
    fn new(view: View) -> Self {
        Self(view)
    }
}

pub fn key(view: View) -> Key {
    Key::new(view)
}

impl Into<RawIdent> for Key {
    fn into(self) -> RawIdent {
        KeyWriter::with_capacity(Ty::InoCounter, mem::size_of::<View>())
            .write_u16(self.0)
            .into()
    }
}
