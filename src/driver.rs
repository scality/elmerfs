mod dir;
pub(crate) mod ino;
mod lock;
mod page;
mod pool;

pub use self::pool::AddressBook;

use self::dir::DirDriver;
use self::ino::InoGenerator;
use self::lock::PageLocks;
use self::page::PageWriter;
use self::pool::ConnectionPool;
use crate::key::Bucket;
use crate::model::{
    inode::{self, Inode, Owner},
    symlink,
    dentries,
};
use crate::view::{NameRef, View};
use antidotec::{self, Connection, Transaction, TransactionLocks};
use async_std::sync::Arc;
use async_std::task;
use fuse::*;
use nix::errno::Errno;
use std::fmt::Debug;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;

pub use self::dir::ListingFlavor;

const ROOT_INO: u64 = 1;
const MAX_CONNECTIONS: usize = 32;
const PAGE_SIZE: u64 = 64 * 1024;

const ENOENT: Error = Error::Sys(Errno::ENOENT);



macro_rules! transaction {
    ($cfg:expr, $connection:expr) => {
        transaction!($cfg, { shared: [], exclusive: [] })
    };

    ($cfg:expr, $connection:expr, { shared: [$($shared:expr),*] }) => {
        transaction!($cfg, $connection, { shared: [$($shared),*], exclusive: [] })
    };

    ($cfg:expr, $connection:expr, { exclusive: [$($excl:expr),*] }) => {
        transaction!($cfg, $connection, { shared: [], exclusive: [$($excl),*] })
    };

    ($cfg:expr, $connection:expr, { shared: [$($shared:expr),*], exclusive: [$($excl:expr),*] }) => {{
        if $cfg.locks {
            $connection.transaction_with_locks(TransactionLocks {
                shared: vec![$($shared.into()),*],
                exclusive: vec![$($excl.into()),*]
            })
        } else {
            $connection.transaction_with_locks(TransactionLocks {
                shared: vec![],
                exclusive: vec![]
            })
        }
    }};
}

#[derive(Error, Debug)]
pub(crate) enum Error {
    #[error("driver replied with: {0}")]
    Sys(Errno),

    #[error("io error with antidote: {0}")]
    Antidote(#[from] antidotec::Error),

    #[error("internal syscall failed: {0}")]
    Nix(#[from] nix::Error),
}
pub(crate) type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct Config {
    pub bucket: Bucket,
    pub addresses: Arc<AddressBook>,
    pub locks: bool,
    pub listing_flavor: dir::ListingFlavor,
}

#[derive(Debug)]
pub(crate) struct Driver {
    cfg: Config,
    ino_counter: Arc<InoGenerator>,
    pool: Arc<ConnectionPool>,
    pages: PageWriter,
    page_locks: PageLocks,
    dirs: DirDriver,
}

impl Driver {
    pub async fn new(cfg: Config) -> Result<Self> {
        let pages = PageWriter::new(cfg.bucket, PAGE_SIZE);
        let pool = Arc::new(ConnectionPool::with_capacity(
            cfg.addresses.clone(),
            MAX_CONNECTIONS,
        ));
        let ino_counter = {
            let mut connection = pool.acquire().await?;
            Self::make_root(&cfg, &mut connection).await?;
            let ino_counter = Self::load_ino_counter(&cfg, &mut connection).await?;

            ino_counter
        };

        let dirs = DirDriver::new(cfg.clone(), pool.clone());

        Ok(Self {
            cfg,
            ino_counter: Arc::new(ino_counter),
            pages,
            pool,
            page_locks: PageLocks::new(PAGE_SIZE),
            dirs,
        })
    }

    #[tracing::instrument(skip(connection))]
    pub(crate) async fn load_ino_counter(
        cfg: &Config,
        connection: &mut Connection,
    ) -> Result<InoGenerator> {
        let counter = InoGenerator::load(connection, cfg.bucket).await?;
        Ok(counter)
    }

    #[tracing::instrument(skip(connection))]
    pub(crate) async fn make_root(cfg: &Config, connection: &mut Connection) -> Result<()> {
        let mut tx = transaction!(cfg, connection, { exclusive: [inode::key(ROOT_INO)] }).await?;

        match Self::attr_of(cfg, &mut tx, ROOT_INO).await {
            Ok(_) => {
                tx.commit().await?;
                return Ok(());
            }
            Err(Error::Sys(Errno::ENOENT)) => {}
            Err(error) => return Err(error),
        };

        let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let root_inode = Inode {
            ino: ROOT_INO,
            parent: 1,
            atime: t,
            ctime: t,
            mtime: t,
            owner: Owner { uid: 0, gid: 0 },
            mode: 0o777,
            size: 0,
            nlink: 3,
        };

        tx.update(
            cfg.bucket,
            vec![
                inode::create(&root_inode),
                dentries::create(View { uid: 0 }, ROOT_INO, ROOT_INO),
            ],
        )
        .await?;
        tx.commit().await?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn getattr(&self, view: View, ino: u64) -> Result<FileAttr> {
        let mut connection = self.pool.acquire().await?;

        let mut tx = transaction!(self.cfg, connection, { shared: [inode::key(ino)] }).await?;

        let attrs = Self::attr_of(&self.cfg, &mut tx, ino).await?;

        tx.commit().await?;
        Ok(attrs)
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn setattr(
        &self,
        view: View,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<Duration>,
        mtime: Option<Duration>,
    ) -> Result<FileAttr> {
        macro_rules! update {
            ($target:expr, $v:ident) => {
                $target = $v.unwrap_or($target);
            };
        }

        /* Note that here we don't lock any pages when truncating. It is expected
        as while concurrent read/write or write/write to the same register
        might lead to invalid output even if they concerns different ranges,
        here we are discarding without being dependant on a previously read
        value. */

        let mut connection = self.pool.acquire().await?;
        let mut tx = transaction!(self.cfg, connection, { exclusive: [inode::key(ino)] }).await?;

        let inode = {
            let mut reply = tx.read(self.cfg.bucket, vec![inode::read(ino)]).await?;
            let mut inode = inode::decode(ino, &mut reply, 0).ok_or(ENOENT)?;

            update!(inode.mode, mode);
            update!(inode.owner.uid, uid);
            update!(inode.owner.gid, gid);
            update!(inode.atime, atime);
            update!(inode.mtime, mtime);

            let update = if let Some(new_size) = size {
                if new_size < inode.size {
                    tracing::debug!("truncate DOWN from 0x{:x} to 0x{:x}", inode.size, new_size);

                    let remove_range = new_size..inode.size;
                    self.pages.remove(&mut tx, ino, remove_range).await?;
                } else {
                    tracing::debug!("truncate UP from 0x{:X} to 0x{:X}", inode.size, new_size);
                }

                inode.size = new_size;
                inode::update_stats_and_size(&inode)
            } else {
                inode::update_stats(&inode)
            };

            tx.update(self.cfg.bucket, std::iter::once(update)).await?;

            inode
        };

        tx.commit().await?;
        Ok(inode.attr())
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn lookup(
        &self,
        view: View,
        parent_ino: u64,
        name: NameRef,
    ) -> Result<FileAttr> {
        let mut connection = self.pool.acquire().await?;
        let mut tx = transaction!(self.cfg, connection, {
            shared: [dentries::key(parent_ino)]
        })
        .await?;

        let entries = self.dirs.load(view, &mut tx, parent_ino).await?;
        let attrs = match entries.get(&name) {
            Some(entry) => Self::attr_of(&self.cfg, &mut tx, entry.ino).await,
            None => Err(Error::Sys(Errno::ENOENT)),
        };

        tx.commit().await?;
        attrs
    }

    async fn attr_of(cfg: &Config, tx: &mut Transaction<'_>, ino: u64) -> Result<FileAttr> {
        let mut reply = tx.read(cfg.bucket, vec![inode::read(ino)]).await?;
        let inode = inode::decode(ino, &mut reply, 0).ok_or(ENOENT)?;
        Ok(inode.attr())
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn opendir(&self, view: View, ino: u64) -> Result<()> {
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn releasedir(&self, view: View, ino: u64) -> Result<()> {
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn readdir(
        &self,
        view: View,
        ino: u64,
        offset: i64,
    ) -> Result<Vec<ReadDirEntry>> {
        assert!(offset >= 0);
        let mut connection = self.pool.acquire().await?;
        let mut tx = transaction!(self.cfg, connection, { shared: [dentries::key(ino)] }).await?;

        let entries = {
            let entries = self.dirs.load(view, &mut tx, ino).await?;

            let mut mapped_entries = Vec::with_capacity(entries.len());
            for entry in entries.iter_from(self.cfg.listing_flavor, offset as usize) {
                let entry = entry?;

                mapped_entries.push(ReadDirEntry {
                    name: entry.name.into_owned(),
                    ino: entry.ino,
                    kind: ino::file_type(ino)
                });
            }

            mapped_entries
        };

        tx.commit().await?;
        Ok(entries)
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn mkdir(
        &self,
        view: View,
        owner: Owner,
        mode: u32,
        parent_ino: u64,
        name: NameRef,
    ) -> Result<FileAttr> {
        let mut connection = self.pool.acquire().await?;
        let ino = self.ino_counter.next(ino::Directory, &mut connection).await?;

        let mut tx = transaction!(self.cfg, connection, {
            exclusive: [
                inode::key(parent_ino),
                dentries::key(parent_ino)
            ]
        })
        .await?;

        let attr = {
            let mut reply = tx
                .read(
                    self.cfg.bucket,
                    vec![inode::read(parent_ino), dentries::read(parent_ino)],
                )
                .await?;

            let mut parent_inode = inode::decode(parent_ino, &mut reply, 0).ok_or(ENOENT)?;
            let entries = dentries::decode(&mut reply, 1).ok_or(ENOENT)?;
            let entries = self.dirs.view(view, &mut tx, entries, parent_ino).await?;

            if entries.contains_key(&name) {
                return Err(Error::Sys(Errno::EEXIST));
            }

            let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            let inode = Inode {
                ino,
                parent: parent_ino,
                atime: t,
                ctime: t,
                mtime: t,
                owner,
                mode,
                size: 0,
                nlink: 2,
            };
            parent_inode.mtime = t;
            parent_inode.atime = t;
            parent_inode.size += 1;

            let attr = inode.attr();

            let name = name.canonicalize(view);
            tx.update(
                self.cfg.bucket,
                vec![
                    dentries::add_entry(
                        parent_ino,
                        &dentries::Entry::new(name, ino),
                    ),
                    dentries::create(view, parent_ino, ino),
                    inode::create(&inode),
                    inode::update_stats_and_size(&parent_inode),
                ],
            )
            .await?;

            attr
        };

        tx.commit().await?;
        Ok(attr)
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn rmdir(
        self: Arc<Driver>,
        view: View,
        parent_ino: u64,
        name: NameRef,
    ) -> Result<()> {
        let mut connection = self.pool.acquire().await?;
        let mut tx = transaction!(self.cfg, connection, {
            exclusive: [
                inode::key(parent_ino),
                dentries::key(parent_ino)
            ]
        })
        .await?;

        let ino = {
            let mut reply = tx
                .read(
                    self.cfg.bucket,
                    vec![inode::read(parent_ino), dentries::read(parent_ino)],
                )
                .await?;

            let mut parent_inode = inode::decode(parent_ino, &mut reply, 0).ok_or(ENOENT)?;
            let entries = dentries::decode(&mut reply, 1).ok_or(ENOENT)?;
            let entries = self.dirs.view(view, &mut tx, entries, parent_ino).await?;
            let entry = entries.get(&name).ok_or(ENOENT)?;

            let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            parent_inode.atime = t;
            parent_inode.mtime = t;
            parent_inode.size = parent_inode.size.saturating_sub(1);

            let dentry = entry.into_dentry();
            tx.update(
                self.cfg.bucket,
                vec![
                    inode::decr_link_count(entry.ino, 1),
                    dentries::remove_entry(parent_ino, &dentry),
                    inode::update_stats_and_size(&parent_inode),
                ],
            )
            .await?;

            entry.ino
        };

        tx.commit().await?;
        self.schedule_delete(ino);
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn mknod(
        &self,
        view: View,
        owner: Owner,
        mode: u32,
        parent_ino: u64,
        name: NameRef,
        _rdev: u32,
    ) -> Result<FileAttr> {
        let mut connection = self.pool.acquire().await?;
        let ino = self.ino_counter.next(ino::Regular, &mut connection).await?;

        let mut tx = transaction!(self.cfg, connection, {
            exclusive: [
                inode::key(parent_ino),
                dentries::key(parent_ino)
            ]
        })
        .await?;

        let attr = {
            let mut reply = tx
                .read(
                    self.cfg.bucket,
                    vec![inode::read(parent_ino), dentries::read(parent_ino)],
                )
                .await?;

            let mut parent = inode::decode(parent_ino, &mut reply, 0).ok_or(ENOENT)?;
            let entries = dentries::decode(&mut reply, 1).ok_or(ENOENT)?;
            let entries = self.dirs.view(view, &mut tx, entries, parent_ino).await?;

            if entries.contains_key(&name) {
                return Err(Error::Sys(Errno::EEXIST));
            }

            let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            let inode = Inode {
                ino,
                parent: parent_ino,
                atime: t,
                ctime: t,
                mtime: t,
                owner,
                mode,
                size: 0,
                nlink: 1,
            };
            parent.mtime = t;
            parent.ctime = t;
            parent.size += 1;

            let attr = inode.attr();
            let name = name.canonicalize(view);
            tx.update(
                self.cfg.bucket,
                vec![
                    inode::update_stats_and_size(&parent),
                    dentries::add_entry(
                        parent_ino,
                        &dentries::Entry::new(name, ino),
                    ),
                    inode::create(&inode),
                ],
            )
            .await?;

            attr
        };

        tx.commit().await?;
        Ok(attr)
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn unlink(&self, view: View, parent_ino: u64, name: NameRef) -> Result<()> {
        let mut connection = self.pool.acquire().await?;
        let mut tx = transaction!(self.cfg, connection, {
            exclusive: [
                inode::key(parent_ino),
                dentries::key(parent_ino)
            ]
        })
        .await?;

        let ino = {
            let mut reply = tx
                .read(
                    self.cfg.bucket,
                    vec![inode::read(parent_ino), dentries::read(parent_ino)],
                )
                .await?;

            let mut parent_inode = inode::decode(parent_ino, &mut reply, 0).ok_or(ENOENT)?;
            let entries = dentries::decode(&mut reply, 1).ok_or(ENOENT)?;
            let entries = self.dirs.view(view, &mut tx, entries, parent_ino).await?;
            let entry = entries.get(&name).ok_or(ENOENT)?;

            let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            parent_inode.mtime = t;
            parent_inode.ctime = t;
            parent_inode.size -= 1;

            let dentry = entry.into_dentry();
            tx.update(
                self.cfg.bucket,
                vec![
                    dentries::remove_entry(parent_ino, &dentry),
                    inode::decr_link_count(entry.ino, 1),
                ],
            )
            .await?;

            entry.ino
        };

        tx.commit().await?;
        self.schedule_delete(ino);
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn open(&self, view: View, ino: u64) -> Result<()> {
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn release(&self, view: View, ino: u64) -> Result<()> {
        Ok(())
    }

    #[tracing::instrument(skip(self, bytes), fields(offset, len = bytes.len()))]
    pub(crate) async fn write(
        &self,
        view: View,
        ino: u64,
        bytes: &[u8],
        offset: u64,
    ) -> Result<()> {
        let byte_range = offset..(offset + bytes.len() as u64);
        let lock = self.page_locks.lock(ino, byte_range).await;

        let result = self.write_nolock(ino, bytes, offset).await;

        self.page_locks.unlock(lock).await;
        result
    }

    pub(crate) async fn write_nolock(&self, ino: u64, bytes: &[u8], offset: u64) -> Result<()> {
        let mut connection = self.pool.acquire().await?;
        let mut tx = transaction!(self.cfg, connection, { exclusive: [inode::key(ino)] }).await?;

        self.pages.write(&mut tx, ino, offset, bytes).await?;

        let mut reply = tx.read(self.cfg.bucket, vec![inode::read(ino)]).await?;
        let mut inode = inode::decode(ino, &mut reply, 0).ok_or(ENOENT)?;

        let wrote_above_size = (offset + bytes.len() as u64).saturating_sub(inode.size);

        let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        inode.atime = t;
        inode.mtime = t;

        let update = if wrote_above_size > 0 {
            inode.size += wrote_above_size;

            tracing::debug!(extended = inode.size);
            inode::update_stats_and_size(&inode)
        } else {
            inode::update_stats(&inode)
        };

        tx.update(self.cfg.bucket, std::iter::once(update)).await?;
        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn read(&self, ino: u64, offset: u64, len: u32) -> Result<Vec<u8>> {
        let byte_range = offset..(offset + len as u64);
        let lock = self.page_locks.lock(ino, byte_range).await;

        let result = self.read_nolock(ino, offset, len).await;

        self.page_locks.unlock(lock).await;
        result
    }

    async fn read_nolock(&self, ino: u64, offset: u64, len: u32) -> Result<Vec<u8>> {
        let len = len as usize;
        let mut connection = self.pool.acquire().await?;
        let mut tx = transaction!(self.cfg, connection, { shared: [inode::key(ino)] }).await?;

        let mut reply = tx.read(self.cfg.bucket, vec![inode::read(ino)]).await?;
        let inode = inode::decode(ino, &mut reply, 0).ok_or(ENOENT)?;

        let mut bytes = Vec::with_capacity(len);
        let read_end = (offset + len as u64).min(inode.size);

        if offset > inode.size {
            return Err(Error::Sys(Errno::EINVAL));
        }

        let truncated_len = read_end - offset;
        self.pages
            .read(&mut tx, ino, offset, truncated_len, &mut bytes)
            .await?;

        let padding = len.saturating_sub(bytes.len());
        tracing::debug!(?padding, output_len = bytes.len());
        bytes.resize(bytes.len() + padding, 0);
        assert!(bytes.len() == len);

        tx.commit().await?;
        Ok(bytes)
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn rename(
        &self,
        view: View,
        parent_ino: u64,
        name: NameRef,
        new_parent_ino: u64,
        new_name: NameRef,
    ) -> Result<()> {
        let parents_to_lock = self
            .up_until_common_ancestor(view, parent_ino, new_parent_ino)
            .await?;
        tracing::trace!(?parents_to_lock);

        let mut connection = self.pool.acquire().await?;
        let mut tx = connection
            .transaction_with_locks(TransactionLocks {
                shared: vec![],
                exclusive: parents_to_lock
                    .into_iter()
                    .map(|ino| dentries::key(ino).into())
                    .collect(),
            })
            .await?;

        let (mut parent, mut new_parent, parent_entries, new_parent_entries) = {
            let mut reply = tx
                .read(
                    self.cfg.bucket,
                    vec![
                        inode::read(parent_ino),
                        inode::read(new_parent_ino),
                        dentries::read(parent_ino),
                        dentries::read(new_parent_ino),
                    ],
                )
                .await?;

            (
                inode::decode(parent_ino, &mut reply, 0).ok_or(ENOENT)?,
                inode::decode(new_parent_ino, &mut reply, 1).ok_or(ENOENT)?,
                dentries::decode(&mut reply, 2).ok_or(ENOENT)?,
                dentries::decode(&mut reply, 3).ok_or(ENOENT)?,
            )
        };

        let parent_entries = self.dirs.view(view, &mut tx, parent_entries, parent_ino).await?;
        let new_parent_entries = self
            .dirs
            .view(view, &mut tx, new_parent_entries, new_parent_ino)
            .await?;

        let entry = parent_entries.get(&name).ok_or(ENOENT)?;
        let target_entry = new_parent_entries.get(&new_name);

        let (mut inode, target) = {
            let reads = match target_entry {
                Some(target_entry) => vec![inode::read(entry.ino), inode::read(target_entry.ino)],
                None => vec![inode::read(entry.ino)],
            };
            let mut reply = tx.read(self.cfg.bucket, reads).await?;

            let inode = inode::decode(entry.ino, &mut reply, 0).ok_or(ENOENT)?;
            let target = target_entry.and_then(|e| inode::decode(e.ino, &mut reply, 1));

            (inode, target)
        };

        /* Checks if target is a dir and empty. If it is the case, we have
        to delete it */
        match &target {
            Some(target) if ino::kind(target.ino) == ino::Directory && target.size == 0 => {
                let target_entry = target_entry.unwrap();
                let target_dentry = target_entry.into_dentry();

                tx.update(
                    self.cfg.bucket,
                    vec![
                        inode::remove(target_entry.ino),
                        dentries::remove(target_entry.ino),
                        dentries::remove_entry(new_parent_ino, &target_dentry),
                    ],
                )
                .await?;
            }
            Some(target) if target.nlink == 1 => {
                let target_entry = target_entry.unwrap();
                let target_dentry = target_entry.into_dentry();

                tx.update(
                    self.cfg.bucket,
                    vec![
                        inode::remove(target.ino),
                        dentries::remove_entry(new_parent_ino, &target_dentry),
                        symlink::remove(target.ino),
                    ],
                )
                .await?;
            }
            _ => {}
        }

        /* At this point we are sure that target does not exists
        and we are ready to perform the rename */
        let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        new_parent.size += 1;
        new_parent.atime = t;
        new_parent.mtime = t;

        parent.size -= 1;
        parent.atime = t;
        parent.mtime = t;

        inode.atime = t;
        inode.parent = new_parent_ino;

        let ino = entry.ino;
        let dentry_to_remove = entry.into_dentry();
        let new_name = new_name.canonicalize(view);
        let new_dentry = &dentries::Entry::new(new_name, ino);

        tx.update(
            self.cfg.bucket,
            vec![
                inode::update_stats_and_size(&parent),
                inode::update_stats_and_size(&new_parent),
                inode::update_stats(&inode),
                dentries::remove_entry(parent_ino, &dentry_to_remove),
                dentries::add_entry(new_parent_ino, new_dentry),
            ],
        )
        .await?;

        tx.commit().await?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn link(
        &self,
        view: View,
        ino: u64,
        new_parent_ino: u64,
        new_name: NameRef,
    ) -> Result<FileAttr> {
        let mut connection = self.pool.acquire().await?;
        let mut tx = transaction!(self.cfg, connection, {
            exclusive: [
                inode::key(ino),
                inode::key(new_parent_ino),
                dentries::key(new_parent_ino)
            ]
        })
        .await?;

        let (mut inode, mut parent, entries) = {
            let mut reply = tx
                .read(
                    self.cfg.bucket,
                    vec![
                        inode::read(ino),
                        inode::read(new_parent_ino),
                        dentries::read(new_parent_ino),
                    ],
                )
                .await?;

            let inode = inode::decode(ino, &mut reply, 0).ok_or(ENOENT)?;
            let parent = inode::decode(new_parent_ino, &mut reply, 1).ok_or(ENOENT)?;
            let entries = dentries::decode(&mut reply, 2).ok_or(ENOENT)?;
            let entries = self.dirs.view(view, &mut tx, entries, new_parent_ino).await?;

            (inode, parent, entries)
        };

        if entries.get(&new_name).is_some() {
            return Err(Error::Sys(Errno::EEXIST));
        }

        let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        parent.mtime = t;
        parent.atime = t;
        parent.size += 1;

        let new_name = new_name.canonicalize(view);
        tx.update(
            self.cfg.bucket,
            vec![
                inode::update_stats_and_size(&parent),
                dentries::add_entry(
                    new_parent_ino,
                    &dentries::Entry::new(new_name, ino),
                ),
                inode::incr_link_count(ino, 1),
            ],
        )
        .await?;

        inode.nlink += 1;
        tx.commit().await?;
        Ok(inode.attr())
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn read_link(&self, view: View, ino: u64) -> Result<String> {
        let mut connection = self.pool.acquire().await?;
        let mut tx = transaction!(self.cfg, connection, { shared: [symlink::key(ino)] }).await?;

        let mut reply = tx.read(self.cfg.bucket, vec![symlink::read(ino)]).await?;

        let link = symlink::decode(&mut reply, 0).ok_or(ENOENT)?;

        tx.commit().await?;
        Ok(link)
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn symlink(
        &self,
        view: View,
        parent_ino: u64,
        owner: Owner,
        name: NameRef,
        link: String,
    ) -> Result<FileAttr> {
        let mut connection = self.pool.acquire().await?;
        let ino = self.ino_counter.next(ino::Symlink, &mut connection).await?;

        let mut tx = transaction!(self.cfg, connection, {
            exclusive: [
                inode::key(parent_ino),
                dentries::key(parent_ino)
            ]
        })
        .await?;

        let (mut parent, entries) = {
            let mut reply = tx
                .read(
                    self.cfg.bucket,
                    vec![inode::read(parent_ino), dentries::read(parent_ino)],
                )
                .await?;

            let parent = inode::decode(parent_ino, &mut reply, 0).ok_or(ENOENT)?;
            let entries = dentries::decode(&mut reply, 1).ok_or(ENOENT)?;
            let entries = self.dirs.view(view, &mut tx, entries, parent_ino).await?;

            (parent, entries)
        };

        if entries.contains_key(&name) {
            return Err(Error::Sys(Errno::EEXIST));
        }

        let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let inode = inode::Inode {
            ino,
            parent: parent_ino,
            atime: t,
            ctime: t,
            mtime: t,
            owner,
            mode: 0o644,
            size: link.len() as u64,
            nlink: 1,
        };
        parent.size += 1;
        parent.mtime = t;
        parent.atime = t;

        let name = name.canonicalize(view);
        tx.update(
            self.cfg.bucket,
            vec![
                inode::create(&inode),
                inode::update_stats_and_size(&parent),
                dentries::add_entry(parent_ino, &dentries::Entry::new(name, ino)),
                symlink::create(ino, link),
            ],
        )
        .await?;

        tx.commit().await?;
        Ok(inode.attr())
    }

    fn schedule_delete(&self, ino: u64) {
        #[tracing::instrument(skip(cfg, pool))]
        async fn delete_later(
            cfg: Config,
            pool: Arc<ConnectionPool>,
            pages: PageWriter,
            ino: u64,
        ) -> Result<bool> {
            let mut connection = pool.acquire().await?;
            let mut tx = transaction!(cfg, connection, { exclusive: [inode::key(ino)] }).await?;

            let inode = {
                let mut reply = tx.read(cfg.bucket, vec![inode::read(ino)]).await?;
                inode::decode(ino, &mut reply, 0).ok_or(ENOENT)?
            };

            let must_be_removed =
                (ino::kind(ino) == ino::Directory && inode.nlink <= 1) || inode.nlink == 0;

            if must_be_removed {
                tx.update(
                    cfg.bucket,
                    vec![
                        inode::remove(ino),
                        dentries::remove(ino),
                        symlink::remove(ino),
                    ],
                )
                .await?;

                if ino::kind(ino) == ino::Regular {
                    /* At this point we should be (locally) the only one
                    seeing this file, don't bother locking up the pages */
                    pages.remove(&mut tx, ino, 0..inode.size).await?;
                }
            }

            tx.commit().await?;
            Ok(must_be_removed)
        }

        let cfg = self.cfg.clone();
        let pool = self.pool.clone();
        let pages = self.pages;
        task::spawn(delete_later(cfg, pool, pages, ino));
    }

    pub async fn up_until_common_ancestor(
        &self,
        view: View,
        mut lhs_parent: u64,
        mut rhs_parent: u64,
    ) -> Result<Vec<u64>> {
        let mut connection = self.pool.acquire().await?;
        let mut tx = connection.transaction().await?;

        let dotdot = NameRef::Partial("..".into());

        let mut parents = Vec::with_capacity(1);
        while lhs_parent != rhs_parent {
            parents.push(lhs_parent);
            parents.push(rhs_parent);

            let mut reply = tx
                .read(
                    self.cfg.bucket,
                    vec![dentries::read(lhs_parent), dentries::read(rhs_parent)],
                )
                .await?;

            let lhs_entries = dentries::decode(&mut reply, 0).ok_or(ENOENT)?;
            let lhs_entries = self.dirs.view(view, &mut tx, lhs_entries, lhs_parent).await?;

            let rhs_entries = dentries::decode(&mut reply, 1).ok_or(ENOENT)?;
            let rhs_entries = self.dirs.view(view, &mut tx, rhs_entries, rhs_parent).await?;

            lhs_parent = lhs_entries.get(&dotdot).unwrap().ino;
            rhs_parent = rhs_entries.get(&dotdot).unwrap().ino;
        }
        assert_eq!(lhs_parent, rhs_parent);
        parents.push(lhs_parent);

        tx.commit().await?;
        Ok(parents)
    }
}

#[derive(Debug)]
pub(crate) struct ReadDirEntry {
    pub(crate) ino: u64,
    pub(crate) kind: FileType,
    pub(crate) name: String,
}
