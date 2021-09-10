use antidotec::{
    reads, updates, AntidoteError, Bytes, BytesMut, Connection, Error, Transaction, TxId,
};
use async_std::channel::{self, Receiver, Sender};
use async_std::task;
use core::slice;
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tracing_futures::Instrument;

use crate::driver::{EBADFD, EINVAL, EIO, ENOENT};
use crate::model::inode::{self, Inode};
use crate::time;
use crate::Bucket;

use super::buffer::{Flush, WriteBuffer, WriteSlice, WriteSlice};
use super::page::{PageDriver, PageWriter};
use super::pool::ConnectionPool;
use super::Driver;
use super::{page::PageCache, DriverError, PAGE_SIZE};

#[derive(Debug)]
struct OpenfileEntry {
    handle: OpenfileHandle,
    open_count: u32,
}

#[derive(Debug)]
pub(crate) struct Openfiles {
    bucket: Bucket,
    connection_pool: Arc<ConnectionPool>,
    entries: HashMap<u64, OpenfileEntry>,
}

impl Openfiles {
    pub(crate) fn new(bucket: Bucket, connection_pool: Arc<ConnectionPool>) -> Self {
        Self {
            bucket,
            connection_pool,
            entries: HashMap::with_capacity(1024),
        }
    }

    pub async fn open(&mut self, ino: u64) -> Result<OpenfileHandle, DriverError> {
        use std::collections::hash_map::Entry;

        match self.entries.entry(ino) {
            Entry::Vacant(entry) => {
                let writer = PageWriter::new(self.bucket, PAGE_SIZE);
                let handle =
                    Openfile::spawn(ino, self.bucket, self.connection_pool.clone(), writer).await?;

                Ok(entry
                    .insert(OpenfileEntry {
                        handle,
                        open_count: 1,
                    })
                    .handle
                    .clone())
            }
            Entry::Occupied(mut entry) => {
                let entry = entry.get_mut();
                entry.open_count += 1;
                Ok(entry.handle.clone())
            }
        }
    }

    pub fn find(&self, ino: u64) -> Option<OpenfileHandle> {
        self.entries.get(&ino).map(|e| e.handle.clone())
    }

    pub fn get(&self, fh: u64) -> Result<OpenfileHandle, DriverError> {
        self.entries
            .get(&fh)
            .ok_or(EBADFD)
            .map(|e| e.handle.clone())
    }

    pub async fn close(&mut self, fh: u64) -> Result<(), DriverError> {
        use std::collections::hash_map::Entry;

        match self.entries.entry(fh) {
            Entry::Vacant(_) => {
                tracing::error!(ino = fh, "Closing an already closed file.");
                Err(EIO)
            }
            Entry::Occupied(mut entry) => {
                if entry.get().open_count == 1 {
                    let (_, entry) = entry.remove_entry();
                    entry.handle.shutdown().await?;
                } else {
                    entry.get_mut().open_count -= 1;
                }

                Ok(())
            }
        }
    }
}

#[derive(Debug)]
enum Command {
    Write {
        payload: WriteSlice,
    },
    Read {
        offset: u64,
        len: u64,
        response_sender: Sender<Result<Bytes, DriverError>>,
    },
    Truncate {
        txid: TxId,
        new_size: u64,
        response_sender: Sender<Result<(), DriverError>>,
    },
    ReadAttr {
        response_sender: Sender<Result<Box<Inode>, DriverError>>,
    },
    Flush {
        response_sender: Sender<Result<(), DriverError>>,
    },
    Exit {
        response_sender: Sender<Result<(), DriverError>>,
    },
}

#[derive(Debug, Copy, Clone)]
enum Mode {
    Idle,
    Write,
    Read,
}

struct Openfile {
    bucket: Bucket,
    ino: u64,
    pool: Arc<ConnectionPool>,
    write_buffer: WriteBuffer,
    cache_txid: Option<TxId>,
    cache: PageCache,
    cached_size: u64,
    driver: PageDriver,
    commands: Receiver<Command>,
    write_error: Option<DriverError>,
    mode: Mode,
}

impl Openfile {
    pub async fn spawn(
        ino: u64,
        bucket: Bucket,
        driver: PageDriver,
        pool: Arc<ConnectionPool>,
    ) -> Result<OpenfileHandle, DriverError> {
        let page_size = driver.page_size();
        let (cmd_sender, cmd_receiver) = channel::bounded(128);

        let openfile = Openfile {
            bucket,
            ino,
            pool,
            write_buffer: WriteBuffer::new(PAGE_SIZE),
            cache_txid: None,
            cache: PageCache::new(6 * page_size),
            driver: PageDriver::new(ino, bucket, page_size),
            commands: cmd_receiver,
            mode: Mode::Idle,
        };

        let _ = task::spawn(Self::run(openfile));
        Ok(OpenfileHandle {
            sender: cmd_sender,
            ino,
        })
    }

    async fn run(mut self) {
        loop {
            let command = match self.commands.recv().await {
                Ok(command) => command,
                Err(_) => {
                    tracing::error!(ino = self.ino, "No more openfile clients. Exiting.");
                    break;
                }
            };

            match command {
                Command::Write { payload } => {
                    let result = self.write(payload).await;
                    self.write_error = self.write_error.or(result.err());
                }
                Command::Read {
                    offset,
                    len,
                    response_sender,
                } => {}
                Command::Flush { response_sender } => {}
                Command::ReadAttr { response_sender } => {}
                Command::Truncate {
                    txid,
                    new_size,
                    response_sender,
                } => {}
                Command::Exit { response_sender } => {}
            }
        }
    }

    async fn write(&mut self, write_slice: WriteSlice) -> Result<(), DriverError> {
        self.request_mode(Mode::Write).await?;

        if let Some(slices) = self.write_buffer.push(write_slice) {
            Self::handle_flush(
                self.bucket,
                self.ino,
                &self.pool,
                &mut self.cache,
                &mut self.driver,
                slices,
            )
            .await
        } else {
            Ok(())
        }
    }

    async fn read(&mut self, offset: u64, len: u64) -> Result<Bytes, DriverError> {
        self.request_mode(Mode::Read).await?;

        let pool = self.pool.clone();
        let mut connection = pool.acquire().await?;

        let mut tx = match self.cache_txid.clone() {
            Some(txid) => DangleTx(Transaction::from_raw(txid, &mut connection)),
            None => {
                self.flush_pending_writes().await?;
                let mut tx = connection.transaction().await?;

                let mut reply = tx.read(self.bucket, reads!(inode::read(self.ino))).await?;
                self.cached_size = inode::decode_size(self.ino, &mut reply, 0).unwrap_or(0);

                DangleTx(tx)
            }
        };

        let mut output = BytesMut::with_capacity(len as usize);
        let truncated_len = len.min(self.cached_size);
        self.driver
            .read(&mut tx, &mut self.cache, offset, truncated_len, &mut output)
            .await?;

        output.resize(len as usize, 0);
        Ok(output.freeze())
    }

    async fn flush(&mut self) -> Result<(), DriverError> {
        self.request_mode(Mode::Write).await?;
        self.flush_pending_writes().await
    }

    async fn request_mode(&mut self, new_mode: Mode) -> Result<(), DriverError> {
        let old_mode = std::mem::replace(&mut self.mode, new_mode);
        match (old_mode, new_mode) {
            (Mode::Read, Mode::Write) => {
                self.flush_cache().await?;
            },
            (Mode::Write, Mode::Read) => {
                self.flush_pending_writes().await?;
                self.flush_cache().await?;
            },
            _ => {}
        }

        Ok(())
    }

    async fn flush_pending_writes(&mut self) -> Result<(), DriverError> {
        let slices = self.write_buffer.flush();
        Self::handle_flush(
            self.bucket,
            self.ino,
            &self.pool,
            &mut self.cache,
            &mut self.driver,
            slices,
        )
        .await
    }

    async fn handle_flush(
        bucket: Bucket,
        ino: u64,
        pool: &ConnectionPool,
        cache: &mut PageCache,
        driver: &mut PageDriver,
        slices: Flush<'_>,
    ) -> Result<(), DriverError> {
        let mut connection = pool.acquire().await?;
        let mut tx = connection.transaction().await?;

        if let Some(extent) = slices.extent() {
            driver.write(&mut tx, cache, &*slices).await?;

            let now = time::now();
            let mut reply = tx.read(bucket, reads!(inode::read(ino))).await?;
            let old_size = inode::decode_size(ino, &mut reply, 0).ok_or(ENOENT)?;

            let new_size = old_size.max(extent.end);
            tx.update(bucket, updates!(inode::update_size(now, ino, new_size)))
                .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    async fn flush_cache(&mut self) -> Result<(), DriverError> {
        self.cached_size = 0;
        self.cache.clear();

        if let Some(txid) = self.cache_txid.take() {
            let mut connection = self.pool.acquire().await?;
            let tx = Transaction::from_raw(txid, &mut connection);
            tx.commit().await?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct OpenfileHandle {
    ino: u64,
    sender: Sender<Command>,
}

impl OpenfileHandle {
    pub(crate) fn fh(&self) -> u64 {
        self.ino
    }

    pub(crate) async fn write(&self, payload: WriteSlice) -> Result<(), DriverError> {
        self.sender
            .send(Command::Write { payload })
            .await
            .map_err(|_| EIO)
    }

    pub(crate) async fn read(&self, offset: u64, len: u64) -> Result<Bytes, DriverError> {
        let (response_sender, response_receiver) = channel::bounded(1);
        self.send(Command::Read {
            offset,
            len,
            response_sender,
        })
        .await;
        self.recv(response_receiver).await
    }

    pub(crate) async fn truncate(
        &self,
        transaction: &mut Transaction<'_>,
        new_size: u64,
    ) -> Result<(), DriverError> {
        let (response_sender, response_receiver) = channel::bounded(1);
        self.send(Command::Truncate {
            txid: transaction.id(),
            new_size,
            response_sender,
        })
        .await;
        self.recv(response_receiver).await
    }

    pub(crate) async fn read_attr(&self) -> Result<Box<Inode>, DriverError> {
        let (response_sender, response_receiver) = channel::bounded(1);
        self.send(Command::ReadAttr { response_sender }).await;
        self.recv(response_receiver).await
    }

    pub(crate) async fn flush(&self) -> Result<(), DriverError> {
        let (response_sender, response_receiver) = channel::bounded(1);
        self.send(Command::Flush { response_sender }).await;
        self.recv(response_receiver).await
    }

    pub(crate) async fn shutdown(&self) -> Result<(), DriverError> {
        let (response_sender, response_receiver) = channel::bounded(1);
        self.send(Command::Exit { response_sender }).await;
        self.recv(response_receiver).await
    }

    async fn recv<T>(&self, receiver: Receiver<Result<T, DriverError>>) -> Result<T, DriverError> {
        match receiver.recv().await {
            Ok(result) => result,
            Err(_) => {
                tracing::error!(ino = self.ino, "response channel closed. Replying EIO.");
                Err(EIO)
            }
        }
    }

    async fn send(&self, command: Command) {
        let _ = self.sender.send(command).await;
    }
}

struct DangleTx<'c>(pub Transaction<'c>);

impl Drop for DangleTx<'_> {
    fn drop(&mut self) {
        self.0.dangle();
    }
}

impl<'c> Deref for DangleTx<'c> {
    type Target = Transaction<'c>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for DangleTx<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
