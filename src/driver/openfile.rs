use antidotec::{reads, updates, Bytes, BytesMut, Transaction, TxId};
use async_std::channel::{self, Receiver, Sender};
use async_std::task;
use fuse::FileAttr;
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::Duration;

use crate::driver::{EBADFD, EINVAL, EIO, ENOENT};
use crate::model::inode;
use crate::time;
use crate::Bucket;

use super::buffer::{Flush, WriteBuffer, WriteSlice};
use super::page::{PageCache, PageDriver};
use super::pool::ConnectionPool;
use super::{DriverError, PAGE_SIZE};

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
                let cache = PageCache::new(PAGE_SIZE * 16);
                let driver = PageDriver::new(ino, self.bucket, PAGE_SIZE, cache);
                let handle =
                    Openfile::spawn(ino, self.bucket, driver, self.connection_pool.clone()).await?;

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

#[derive(Debug, Default)]
pub struct WriteAttrsDesc {
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub size: Option<u64>,
    pub atime: Option<Duration>,
    pub mtime: Option<Duration>,
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
    WriteAttrs {
        desc: Box<WriteAttrsDesc>,
        response_sender: Sender<Result<FileAttr, DriverError>>,
    },
    ReadAttrs {
        response_sender: Sender<Result<FileAttr, DriverError>>,
    },
    Sync {
        response_sender: Sender<Result<(), DriverError>>,
    },
    ClearOnExit,
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
    cached_size: u64,
    driver: PageDriver<PageCache>,
    commands: Receiver<Command>,
    write_error: Option<DriverError>,
    mode: Mode,
    clear_on_exit: bool,
}

impl Openfile {
    pub async fn spawn(
        ino: u64,
        bucket: Bucket,
        driver: PageDriver<PageCache>,
        pool: Arc<ConnectionPool>,
    ) -> Result<OpenfileHandle, DriverError> {
        let (cmd_sender, cmd_receiver) = channel::bounded(128);

        let openfile = Openfile {
            bucket,
            ino,
            pool,
            write_buffer: WriteBuffer::new(PAGE_SIZE),
            cache_txid: None,
            cached_size: 0,
            driver,
            commands: cmd_receiver,
            mode: Mode::Idle,
            write_error: None,
            clear_on_exit: false,
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
                    let result = self.handle_write(payload).await;
                    if result.is_err() {
                        tracing::error!(
                            ino = self.ino,
                            ?result,
                            "failed to asynchronosly write some data."
                        );
                    }
                    self.write_error = self.write_error.or(result.err());
                }
                Command::Read {
                    offset,
                    len,
                    response_sender,
                } => {
                    let result = self.handle_read(offset, len).await;
                    let _ = response_sender.send(result).await;
                }
                Command::Sync { response_sender } => {
                    let result = self.handle_sync().await;
                    let _ = response_sender.send(result).await;
                }
                Command::ReadAttrs { response_sender } => {
                    let result = self.handle_read_attrs().await;
                    let _ = response_sender.send(result).await;
                }
                Command::WriteAttrs {
                    desc,
                    response_sender,
                } => {
                    let result = self.handle_write_attrs(desc).await;
                    let _ = response_sender.send(result).await;
                }
                Command::ClearOnExit => {
                    self.clear_on_exit = true;
                }
                Command::Exit { response_sender } => {
                    let result = self.handle_exit().await;
                    let _ = response_sender.send(result).await;
                    break;
                }
            }
        }
    }

    async fn handle_write(&mut self, write_slice: WriteSlice) -> Result<(), DriverError> {
        self.request_mode(Mode::Write).await?;

        let txid = self.txid().await?;
        let flush_result = if let Some(slices) = self.write_buffer.push(write_slice) {
            let mut connection = self.pool.acquire().await?;
            let mut tx = DangleTx(Transaction::from_raw(txid, &mut connection));
            Self::write_slices_and_update_size(
                self.ino,
                self.bucket,
                &mut tx,
                &mut self.driver,
                &mut self.cached_size,
                slices,
            )
            .await
            .map(|_| true)
        } else {
            Ok(false)
        };

        match flush_result {
            Err(error) => {
                let _ = self.abort().await;
                Err(error)
            }
            Ok(flushed) if flushed => self.commit().await,
            _ => Ok(()),
        }
    }

    async fn handle_read(&mut self, offset: u64, len: u64) -> Result<Bytes, DriverError> {
        self.request_mode(Mode::Read).await?;

        if offset > self.cached_size {
            return Err(EINVAL);
        }

        let pool = self.pool.clone();
        let mut connection = pool.acquire().await?;
        let txid = self.txid().await?;

        let mut tx = DangleTx(Transaction::from_raw(txid, &mut connection));

        let mut output = BytesMut::with_capacity(len as usize);
        let truncated_len = len.min(self.cached_size);
        self.driver
            .read((&mut *tx).into(), offset, truncated_len, &mut output)
            .await?;

        output.resize(len as usize, 0);
        Ok(output.freeze())
    }

    async fn handle_sync(&mut self) -> Result<(), DriverError> {
        self.request_mode(Mode::Write).await?;
        self.write_error.take().map(Err).unwrap_or(Ok(()))?;
        self.flush().await
    }

    async fn handle_write_attrs(
        &mut self,
        attrs: Box<WriteAttrsDesc>,
    ) -> Result<FileAttr, DriverError> {
        macro_rules! update {
            ($target:expr, $v:expr) => {
                $target = $v.unwrap_or($target);
            };
        }
        self.request_mode(Mode::Write).await?;

        /* For simplicity, we flush all pending writes. This allow us, in case of a truncate, to remove
        the pages correctly without having to fiddle with the in memory buffer. */
        self.flush().await?;

        let inode = {
            let pool = self.pool.clone();
            let mut connection = pool.acquire().await?;
            let txid = self.txid().await?;

            let mut tx = DangleTx(Transaction::from_raw(txid, &mut connection));

            /* We must have a consistent view of the cached size. It correspond to the same transaction. */
            if let Some(new_size) = attrs.size {
                let old_size = self.cached_size;
                self.cached_size = new_size;

                if old_size > new_size {
                    self.driver
                        .truncate((&mut *tx).into(), new_size, old_size)
                        .await?;
                }
            }

            let mut reply = tx.read(self.bucket, vec![inode::read(self.ino)]).await?;
            let mut inode = inode::decode(self.ino, &mut reply, 0).ok_or(ENOENT)?;

            let owner = {
                let mut new_owner = inode.owner;
                if let Some(new_uid) = attrs.uid {
                    new_owner.uid = new_uid;
                }

                if let Some(new_gid) = attrs.gid {
                    new_owner.gid = new_gid;
                }

                new_owner
            };
            let update = inode::UpdateAttrsDesc {
                mode: attrs.mode,
                owner: Some(owner),
                atime: attrs.atime,
                mtime: attrs.mtime,
                size: Some(self.cached_size),
            };

            tx.update(self.bucket, updates!(inode::update_attrs(self.ino, update)))
                .await?;
            update!(inode.mode, attrs.mode);
            update!(inode.owner.uid, attrs.uid);
            update!(inode.owner.gid, attrs.gid);
            update!(inode.atime, attrs.atime);
            update!(inode.mtime, attrs.mtime);
            inode.size = self.cached_size;

            inode
        };

        Ok(inode.attr(self.ino))
    }

    async fn handle_read_attrs(&mut self) -> Result<FileAttr, DriverError> {
        self.request_mode(Mode::Read).await?;

        let pool = self.pool.clone();
        let mut connection = pool.acquire().await?;
        let txid = self.txid().await?;

        let mut tx = DangleTx(Transaction::from_raw(txid, &mut connection));
        let mut reply = tx.read(self.bucket, vec![inode::read(self.ino)]).await?;
        let inode = inode::decode(self.ino, &mut reply, 0).ok_or(ENOENT)?;

        Ok(inode.attr(self.ino))
    }

    async fn handle_exit(&mut self) -> Result<(), DriverError> {
        self.handle_sync().await?;
        if self.clear_on_exit {
            self.handle_write_attrs(Box::new(WriteAttrsDesc {
                size: Some(0),
                ..WriteAttrsDesc::default()
            }))
            .await?;

            self.commit().await?;
        }

        Ok(())
    }

    async fn flush(&mut self) -> Result<(), DriverError> {
        let flush_result = {
            let txid = self.txid().await?;
            let slices = self.write_buffer.flush();

            let mut connection = self.pool.acquire().await?;
            let mut tx = DangleTx(Transaction::from_raw(txid, &mut connection));

            Self::write_slices_and_update_size(
                self.ino,
                self.bucket,
                (&mut *tx).into(),
                &mut self.driver,
                &mut self.cached_size,
                slices,
            )
            .await
        };

        match flush_result {
            error @ Err(_) => {
                let _ = self.abort().await;
                error
            }
            Ok(_) => self.commit().await,
        }
    }

    async fn write_slices_and_update_size(
        ino: u64,
        bucket: Bucket,
        tx: &mut Transaction<'_>,
        driver: &mut PageDriver<PageCache>,
        cached_size: &mut u64,
        slices: Flush<'_>,
    ) -> Result<(), DriverError> {
        if let Some(extent) = slices.extent() {
            driver.write(tx.into(), &slices).await?;

            let now = time::now();
            if extent.end > *cached_size {
                *cached_size = extent.end;
                tx.update(bucket, updates!(inode::update_size(now, ino, extent.end)))
                    .await?;
            } else {
                tx.update(bucket, updates!(inode::update_size(now, ino, *cached_size)))
                    .await?;
            }
        }

        Ok(())
    }

    async fn request_mode(&mut self, new_mode: Mode) -> Result<(), DriverError> {
        let old_mode = std::mem::replace(&mut self.mode, new_mode);
        match (old_mode, new_mode) {
            (Mode::Write, Mode::Read) => {
                self.flush().await?;
            }
            _ => {}
        }

        Ok(())
    }

    async fn commit(&mut self) -> Result<(), DriverError> {
        self.driver.invalidate_cache()?;
        self.cached_size = 0;

        if let Some(txid) = self.cache_txid.take() {
            let mut connection = self.pool.acquire().await?;
            let tx = Transaction::from_raw(txid, &mut connection);
            tx.commit().await?;
        }

        Ok(())
    }

    async fn abort(&mut self) -> Result<(), DriverError> {
        self.driver.invalidate_cache()?;
        self.cached_size = 0;

        if let Some(txid) = self.cache_txid.take() {
            let mut connection = self.pool.acquire().await?;
            let tx = Transaction::from_raw(txid, &mut connection);
            tx.abort().await?;
        }

        Ok(())
    }

    async fn txid(&mut self) -> Result<TxId, DriverError> {
        match self.cache_txid.clone() {
            Some(txid) => Ok(txid),
            None => {
                let mut connection = self.pool.acquire().await?;
                let mut tx = DangleTx(connection.transaction().await?);

                let mut reply = tx.read(self.bucket, reads!(inode::read(self.ino))).await?;
                self.cached_size = inode::decode_size(self.ino, &mut reply, 0).ok_or(ENOENT)?;
                self.driver.invalidate_cache()?;

                Ok(tx.id())
            }
        }
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

    pub(crate) async fn write_attrs(
        &self,
        desc: Box<WriteAttrsDesc>,
    ) -> Result<FileAttr, DriverError> {
        let (response_sender, response_receiver) = channel::bounded(1);
        self.send(Command::WriteAttrs {
            desc,
            response_sender,
        })
        .await;
        self.recv(response_receiver).await
    }

    pub(crate) async fn read_attrs(&self) -> Result<FileAttr, DriverError> {
        let (response_sender, response_receiver) = channel::bounded(1);
        self.send(Command::ReadAttrs { response_sender }).await;
        self.recv(response_receiver).await
    }

    pub(crate) async fn sync(&self) -> Result<(), DriverError> {
        let (response_sender, response_receiver) = channel::bounded(1);
        self.send(Command::Sync { response_sender }).await;
        self.recv(response_receiver).await
    }

    pub(crate) async fn clear_on_exit(&self) {
        self.send(Command::ClearOnExit).await;
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
