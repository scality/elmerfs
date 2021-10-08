use crate::{
    key::{KeyWriter, Ty},
    Config,
    model::inode::{Ino, InodeKind, InoDesc}
};
use antidotec::{lwwreg, Connection, Error, RawIdent};
use async_std::sync::Mutex;
use std::{ops::Range, sync::Arc};

const RANGE_SIZE: u64 = 1024;

#[derive(Debug)]
pub struct InoGenerator {
    config: Arc<Config>,
    range: Mutex<Range<u64>>,
}

impl InoGenerator {
    pub async fn load(connection: &mut Connection, config: Arc<Config>) -> Result<Self, Error> {
        let (next, max) = Self::allocate_range(&config, connection).await?;

        Ok(Self {
            config,
            range: Mutex::new(next..max),
        })
    }

    pub async fn next(&self, kind: InodeKind, connection: &mut Connection) -> Result<Ino, Error> {
        let sequence = self.next_sequence(connection).await?;
        let ino_desc = InoDesc {
            cluster_id: self.config.cluster_id,
            node_id: self.config.node_id,
            kind,
            sequence
        };
        Ok(Ino::encode(ino_desc))
    }

    pub async fn next_sequence(&self, connection: &mut Connection) -> Result<u64, Error> {
        let mut range = self.range.lock().await;

        let found = loop {
            let next = range.start + 1;
            *range = next..range.end;

            if next >= range.end {
                let (next, max) = Self::allocate_range(&self.config, connection).await?;
                *range = next..max;
            } else {
                break next;
            }
        };

        Ok(found)
    }

    pub async fn allocate_range(
        config: &Config,
        connection: &mut Connection,
    ) -> Result<(u64, u64), Error> {
        let mut tx = connection.transaction().await?;
        let key = key(config.cluster_id, config.node_id);

        let mut reply = tx.read(config.bucket(), vec![lwwreg::get(key)]).await?;
        let result = match reply.lwwreg(0) {
            None => {
                let next = 2;
                let max = next + RANGE_SIZE;
                tx.update(config.bucket(), vec![lwwreg::set_u64(key, max)])
                    .await?;

                (next, max)
            }
            Some(previous_max) => {
                let next = lwwreg::read_u64(&previous_max);
                let max = next + RANGE_SIZE;
                tx.update(config.bucket(), vec![lwwreg::set_u64(key, max)])
                    .await?;

                (next, max)
            }
        };

        tx.commit().await?;
        Ok(result)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Key {
    cluster_id: u8,
    node_id: u8,
}

impl Key {
    fn new(cluster_id: u8, node_id: u8) -> Self {
        Self {
            cluster_id,
            node_id,
        }
    }
}

pub fn key(cluster_id: u8, node_id: u8) -> Key {
    Key::new(cluster_id, node_id)
}

impl Into<RawIdent> for Key {
    fn into(self) -> RawIdent {
        KeyWriter::with_capacity(Ty::InoCounter, 2)
            .write_u8(self.cluster_id)
            .write_u8(self.node_id)
            .into()
    }
}
