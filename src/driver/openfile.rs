use std::collections::hash_map::Entry;
use std::{sync::Arc};

use std::collections::HashMap;
use antidotec::{Connection, Transaction, TxId};
use async_std::sync::Mutex;
use super::{DriverError, PAGE_SIZE, page::{PageCache}};
use super::buffer::WriteBuffer;

const DATA_CACHE_PER_TX: u64 = 16 * PAGE_SIZE;

pub(crate) struct Openfile {
    pub txid: TxId,
    pub write_buffer: Arc<Mutex<WriteBuffer>>,
    pub pages: Arc<Mutex<PageCache>>,
    open_count: u32,
}

pub(crate) struct OpenfileRef<'c> {
    pub tx: Transaction<'c>,
    pub write_buffer: Arc<Mutex<WriteBuffer>>,
    pub pages: Arc<Mutex<PageCache>>,
}

impl Drop for OpenfileRef<'_> {
    fn drop(&mut self) {
        self.tx.dangle()
    }
}

pub(crate) struct Openfiles {
    entries: HashMap<u64, Openfile>,
}

impl Openfiles {
    pub(crate) fn new(capacity: u32) -> Self {
        Self {
            entries: HashMap::with_capacity(capacity as usize)
        }
    }

    pub(crate) async fn open<'c>(&mut self, connection: &'c mut Connection, ino: u64) -> Result<(), DriverError> {
        match self.entries.entry(ino) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().open_count += 1;
                Ok(())
            },
            Entry::Vacant(entry) => {
                let tx = connection.transaction().await?;

                entry.insert(Openfile {
                    txid: tx.into_raw(),
                    write_buffer: Arc::new(Mutex::new(WriteBuffer::new(PAGE_SIZE))),
                    pages: Arc::new(Mutex::new(PageCache::new(DATA_CACHE_PER_TX))),
                    open_count: 1
                });

                Ok(())
            }
        }
    }

    pub(crate) fn get<'c>(&self, ino: u64, connection: &'c mut Connection) -> OpenfileRef<'c> {
        let openfile = &self.entries[&ino];
        OpenfileRef {
            tx: Transaction::from_raw(openfile.txid.clone(), connection),
            write_buffer: openfile.write_buffer.clone(),
            pages: openfile.pages.clone()
        }
    }

    pub(crate) fn close(&mut self, ino: u64) -> Option<Openfile> {
        match self.entries.entry(ino) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().open_count -= 1;
                if entry.get_mut().open_count == 0 {
                    return Some(entry.remove())
                }

                None
            },
            Entry::Vacant(_) => {
                tracing::warn!("closing an already closed file.");
                None
            }
        }
    }
}
