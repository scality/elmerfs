use antidotec::{AntidoteError, Bytes, Connection, Error, Transaction, TxId, reads, updates};
use async_std::channel::{self, Receiver, Sender};
use async_std::task;
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use crate::driver::{EBADFD, EINVAL, EIO, ENOENT};
use crate::model::inode::{self, Inode};
use crate::time;
use crate::Bucket;

use super::buffer::{WriteBuffer, WritePayload};
use super::page::PageWriter;
use super::pool::ConnectionPool;
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
        payload: WritePayload,
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

struct Openfile {
    txid: Option<TxId>,
    bucket: Bucket,
    ino: u64,
    pool: Arc<ConnectionPool>,
    page_cache: PageCache,
    writer: PageWriter,
    write_buffer: WriteBuffer,
    not_commited_bytes: u64,
    commit_threshold: u64,
    writes_count: u64,
    writes_count_threshold: u64,
    size: u64,
    last_write_offset: u64,
    commands: Receiver<Command>,
    write_error_flag: bool,
}

impl Openfile {
    pub async fn spawn(
        ino: u64,
        bucket: Bucket,
        pool: Arc<ConnectionPool>,
        writer: PageWriter,
    ) -> Result<OpenfileHandle, DriverError> {
        let page_size = writer.page_size();
        let (cmd_sender, cmd_receiver) = channel::bounded(128);

        let openfile = Openfile {
            txid: None,
            bucket,
            ino,
            pool,
            writer,
            page_cache: PageCache::new(4 * page_size),
            not_commited_bytes: 0,
            last_write_offset: 0,
            commit_threshold: 4*page_size,
            writes_count: 0,
            writes_count_threshold: 16,
            commands: cmd_receiver,
            write_buffer: WriteBuffer::new(page_size),
            write_error_flag: false,
            size: 0,
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
                    let result = self.write_cached(payload).await;
                    let _ = self.ack_result(result);
                }
                Command::Read {
                    offset,
                    len,
                    response_sender,
                } => {
                    let result = self.read(offset, len).await;
                    let _ = response_sender.send(self.ack_result(result)).await;
                }
                Command::Flush { response_sender } => {
                    let result = self.flush().await;
                    let _ = response_sender.send(self.ack_result(result)).await;
                }
                Command::ReadAttr { response_sender } => {
                    let result = self.read_attr().await;
                    let _ = response_sender.send(self.ack_result(result)).await;
                }
                Command::Truncate {
                    txid,
                    new_size,
                    response_sender,
                } => {
                    let result = self.truncate(txid, new_size).await;
                    let _ = response_sender.send(self.ack_result(result)).await;
                }
                Command::Exit { response_sender } => {
                    let exit_result = if self.write_error_flag {
                        let result = self.abort().await;
                        Err(result.err().unwrap_or(EIO))
                    } else {
                        self.flush().await
                    };

                    let _ = response_sender.send(exit_result).await;
                    break;
                }
            }
        }
    }

    fn ack_result<T: Debug>(&mut self, result: Result<T, DriverError>) -> Result<T, DriverError> {
        match &result {
            Ok(_) => {}
            Err(DriverError::Antidote(Error::Antidote(AntidoteError::Aborted))) => {
                tracing::error!(ino = self.ino, "aborted tx");
                self.txid = None;
            }
            Err(DriverError::Antidote(Error::Antidote(AntidoteError::Unknown))) => {
                tracing::error!(ino = self.ino, "unknown tx error");
                self.txid = None;
            }
            Err(error) => {
                tracing::error!(ino = self.ino, ?error, "write error");
            }
        }

        if result.is_err() {
            self.write_error_flag = true;
        }
        result
    }

    async fn write_cached(&mut self, payload: WritePayload) -> Result<(), DriverError> {
        let pool = self.pool.clone();
        let mut connection = pool.acquire().await?;
        self.write_cached_with(&mut connection, payload).await
    }

    async fn write_cached_with(&mut self, connection: &mut Connection, payload: WritePayload) -> Result<(), DriverError> {
        if payload.bytes.len() == 0 {
            return Ok(());
        }

        tracing::debug!(ino = self.ino, "writing {} bytes.", payload.bytes.len());
        {
            let write_end = payload.start_offset + payload.bytes.len() as u64;
            self.size = self.size.max(write_end);
            self.last_write_offset = self.last_write_offset.max(write_end);
            tracing::debug!(ino = self.ino, "new size {} bytes.", self.size);

            self.not_commited_bytes += payload.bytes.len() as u64;

            if let Some(payload) = self
                .write_buffer
                .try_append(payload.start_offset, &payload.bytes)
            {
                tracing::debug!(ino = self.ino, "write {} gathered bytes.", payload.bytes.len());

                self.write_with(connection, payload).await?
            }
        }

        if self.not_commited_bytes > self.commit_threshold
            || self.writes_count > self.writes_count_threshold {
            tracing::debug!(ino = self.ino, "exceeded commit threshold ({} bytes).", self.not_commited_bytes);

            self.commit_with(connection).await?;
        }

        Ok(())
    }

    async fn write_with(&mut self, connection: &mut Connection, payload: WritePayload) -> Result<(), DriverError> {
        let txid = self.txid(connection).await?;
        let mut tx = DangleTx(Transaction::from_raw(txid, connection));

        self.writes_count += 1;
        self.writer
            .write(
                &mut tx,
                &mut self.page_cache,
                self.ino,
                payload.start_offset,
                payload.bytes,
            )
            .await?;

        Ok(())
    }

    async fn read(&mut self, offset: u64, len: u64) -> Result<Bytes, DriverError> {
        let pool = self.pool.clone();
        let mut connection = pool.acquire().await?;

        self.flush_with(&mut connection).await?; /* Ensures that we have no pending writes. */
        let txid = self.txid(&mut connection).await?;

        let mut connection = self.pool.acquire().await?;
        let mut tx = DangleTx(Transaction::from_raw(txid, &mut *connection));

        let read_end = (offset + len as u64).min(self.size);

        if offset > self.size {
            return Err(EINVAL);
        }

        let mut bytes = Vec::with_capacity(len as usize);
        let truncated_len = read_end - offset;
        self.writer
            .read(
                &mut tx,
                &mut self.page_cache,
                self.ino,
                offset,
                truncated_len,
                &mut bytes,
            )
            .await?;

        let padding = len.saturating_sub(bytes.len() as u64);
        bytes.resize(bytes.len() + padding as usize, 0);
        assert_eq!(bytes.len() as u64, len);

        Ok(Bytes::from(bytes))
    }

    async fn read_attr(&mut self) -> Result<Box<Inode>, DriverError> {
        /* No need to flush until a read happens. */
        let pool = self.pool.clone();
        let mut connection = pool.acquire().await?;

        let txid = self.txid(&mut connection).await?;

        let inode = {
            let mut tx = DangleTx(Transaction::from_raw(txid, &mut *connection));

            let mut reply = tx.read(self.bucket, vec![inode::read(self.ino)]).await?;

            inode::decode(self.ino, &mut reply, 0)
        };

        match inode {
            Some(inode) => Ok(Box::new(inode)),
            None => {
                /* The inode got removed, abort our pending writes. */
                self.abort_with(&mut connection).await?;
                Err(ENOENT)
            }
        }
    }

    async fn truncate(&mut self, txid: TxId, new_size: u64) -> Result<(), DriverError> {
        let pool = self.pool.clone();
        let mut connection = pool.acquire().await?;

        self.flush_with(&mut connection).await?; /* Ensure that pending writes does not ovewrite our removals */

        let result = {
            let mut tx = DangleTx(Transaction::from_raw(txid, &mut *connection));

            if new_size < self.size {
                self.writer
                    .remove(&mut tx, self.ino, new_size..self.size)
                    .await
            } else {
                Ok(())
            }
        };

        result
    }

    async fn flush(&mut self) -> Result<(), DriverError> {
        let pool = self.pool.clone();
        let mut connection = pool.acquire().await?;

        self.flush_with(&mut connection).await
    }


    async fn flush_with(&mut self, connection: &mut Connection) -> Result<(), DriverError> {
        let payload = self.write_buffer.flush();

        if payload.bytes.len() > 0 {
            tracing::debug!(ino = self.ino, "flushing {} bytes", payload.bytes.len());


            self.write_with(connection, payload).await?;
            self.commit_with(connection).await?;
        }

        match self.write_error_flag {
            true => {
                self.write_error_flag = false;
                Err(EIO)
            }
            false => Ok(()),
        }
    }

    async fn txid(&mut self, connection: &mut Connection) -> Result<TxId, DriverError> {
        if let Some(txid) = self.txid.clone() {
            return Ok(txid);
        }

        self.refresh_tx(connection).await
    }

    async fn abort(&mut self) -> Result<(), DriverError> {
        let pool = self.pool.clone();
        let mut connection = pool.acquire().await?;

        self.abort_with(&mut connection).await
    }

    async fn abort_with(&mut self, connection: &mut Connection) -> Result<(), DriverError> {
        if let Some(txid) = self.txid.take() {
            let tx = Transaction::from_raw(txid, &mut *connection);
            drop(tx); /* Drop aborts */
            self.write_error_flag = true;
        }

        Ok(())
    }

    async fn commit_with(&mut self, connection: &mut Connection) -> Result<(), DriverError> {
        /* If the commit fails, we won't be able to recover the data. Reset the counter
        in all cases. */
        let cahed_size = self.size;
        match self.txid.take() {
            Some(txid) => {
                let mut tx = Transaction::from_raw(txid, &mut *connection);

                let ts = time::now();
                tx.update(
                    self.bucket,
                    updates!(inode::update_size(ts, self.ino, cahed_size)),
                )
                .await?;

                tx.commit().await?;
            }
            None if self.not_commited_bytes > 0 => {
                let mut tx = connection.transaction().await?;

                /* Ifthere are pending writes that are above the read file size, extend the file. Otherwise it means that content was wrote inside
                   the actual commited range and the read file size is the right one. */
                let size = if self.last_write_offset >= self.size {
                    self.size
                } else {
                    cahed_size
                };
                self.size = size;

                let ts = time::now();
                tx.update(
                    self.bucket,
                    updates!(inode::update_size(ts, self.ino, cahed_size)),
                )
                .await?;

                tx.commit().await?;
            },
            _ => {}
        }

        self.page_cache.clear();
        self.not_commited_bytes = 0;
        self.last_write_offset = 0;
        self.writes_count = 0;

        Ok(())

    }

    async fn refresh_tx(&mut self, connection: &mut Connection) -> Result<TxId, DriverError> {
        self.commit_with(connection).await?;

        let mut tx = connection.transaction().await?;

        let mut reply = tx.read(self.bucket, reads!(inode::read(self.ino))).await?;
        self.size = inode::decode_size(self.ino, &mut reply, 0).ok_or(ENOENT)?;

        tracing::debug!(ino = self.ino, "read size {} bytes", self.size);
        self.txid = Some(tx.into_raw());

        Ok(self.txid.clone().unwrap())
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

    pub(crate) async fn write(&self, payload: WritePayload) -> Result<(), DriverError> {
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

    pub(crate) async fn truncate(&self, transaction: &mut Transaction<'_>, new_size: u64) -> Result<(), DriverError> {
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
