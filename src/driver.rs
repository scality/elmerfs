mod ino;
mod page;
mod pool;

pub use self::pool::AddressBook;

use self::ino::InoGenerator;
use self::page::PageWriter;
use crate::key::Bucket;
use crate::model::{
    dir,
    symlink,
    inode::{self, Inode, Owner},
};
use crate::view::{NameRef, View};
use self::pool::ConnectionPool;
use antidotec::{self, Connection, Transaction, TransactionLocks};
use async_std::sync::Arc;
use async_std::task;
use fuse::*;
use nix::errno::Errno;
use std::fmt::Debug;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tracing::debug;

const ROOT_INO: u64 = 1;
const MAX_CONNECTIONS: usize = 32;
const PAGE_SIZE: usize = 4 * 1024;

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
}
pub(crate) type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct Config {
    pub view: View,
    pub bucket: Bucket,
    pub addresses: Arc<AddressBook>,
    pub locks: bool,
}

#[derive(Debug)]
pub(crate) struct Driver {
    cfg: Config,
    ino_counter: Arc<InoGenerator>,
    pool: Arc<ConnectionPool>,
    pages: PageWriter,
}

impl Driver {
    pub async fn new(cfg: Config) -> Result<Self> {
        let pages = PageWriter::new(cfg.bucket, PAGE_SIZE);
        let pool = ConnectionPool::with_capacity(cfg.addresses.clone(), MAX_CONNECTIONS);

        let ino_counter = {
            let mut connection = pool.acquire().await?;
            Self::make_root(&cfg, &mut connection).await?;
            let ino_counter = Self::load_ino_counter(&cfg, &mut connection).await?;

            ino_counter
        };

        Ok(Self {
            cfg,
            ino_counter: Arc::new(ino_counter),
            pages,
            pool: Arc::new(pool),
        })
    }

    #[tracing::instrument(skip(connection))]
    pub(crate) async fn load_ino_counter(
        cfg: &Config,
        connection: &mut Connection,
    ) -> Result<InoGenerator> {
        let mut tx = transaction!(cfg, connection, { exclusive: [ino::key(cfg.view)] }).await?;

        let counter = InoGenerator::load(&mut tx, cfg.view, cfg.bucket).await?;

        tx.commit().await?;
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
            kind: inode::Kind::Directory,
            parent: 1,
            atime: t,
            ctime: t,
            mtime: t,
            owner: Owner { uid: 0, gid: 0 },
            mode: 0777,
            size: 0,
            nlink: 3,
        };

        tx.update(
            cfg.bucket,
            vec![
                inode::create(&root_inode),
                dir::create(cfg.view, ROOT_INO, ROOT_INO),
            ],
        )
        .await?;
        tx.commit().await?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn getattr(&self, ino: u64) -> Result<FileAttr> {
        let mut connection = self.pool.acquire().await?;

        let mut tx = transaction!(self.cfg, connection, { shared: [inode::key(ino)] }).await?;

        let attrs = Self::attr_of(&self.cfg, &mut tx, ino).await?;

        tx.commit().await?;
        Ok(attrs)
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn setattr(
        &self,
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

        let mut connection = self.pool.acquire().await?;
        let mut tx = transaction!(self.cfg, connection, { exclusive: [inode::key(ino)] }).await?;

        let inode = {
            let mut reply = tx.read(self.cfg.bucket, vec![inode::read(ino)]).await?;
            let mut inode = inode::decode(ino, &mut reply, 0).ok_or(ENOENT)?;

            update!(inode.mode, mode);
            update!(inode.owner.uid, uid);
            update!(inode.owner.gid, gid);
            update!(inode.size, size);
            update!(inode.atime, atime);
            update!(inode.mtime, mtime);

            tx.update(self.cfg.bucket, vec![inode::update_stats(&inode)])
                .await?;

            inode
        };

        tx.commit().await?;
        Ok(inode.attr())
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn lookup(&self, parent_ino: u64, name: NameRef) -> Result<FileAttr> {
        let mut connection = self.pool.acquire().await?;
        let mut tx = transaction!(self.cfg, connection, { shared: [dir::key(parent_ino)] }).await?;

        let entries = {
            let mut reply = tx
                .read(self.cfg.bucket, vec![dir::read(parent_ino)])
                .await?;

            dir::decode(self.cfg.view, &mut reply, 0).ok_or(ENOENT)?
        };
        debug!(?entries);

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
    pub(crate) async fn opendir(&self, ino: u64) -> Result<()> {
        // FIXME: For now we are stateless, meaning that we do not track open
        // close calls. just perform a simple getattr as a dummy check.
        self.getattr(ino).await.map(|_| ())
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn releasedir(&self, ino: u64) -> Result<()> {
        self.getattr(ino).await.map(|_| ())
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn readdir(&self, ino: u64, offset: i64) -> Result<Vec<ReadDirEntry>> {
        let mut connection = self.pool.acquire().await?;
        let mut tx = transaction!(self.cfg, connection, { shared: [dir::key(ino)] }).await?;

        let entries = {
            let entries = {
                let mut reply = tx.read(self.cfg.bucket, vec![dir::read(ino)]).await?;

                match dir::decode(self.cfg.view, &mut reply, 0) {
                    Some(entries) => entries,
                    None => {
                        tx.commit().await?;
                        return Ok(Vec::new());
                    }
                }
            };

            let mut names = Vec::with_capacity(entries.len());
            let mut attr_reads = Vec::with_capacity(entries.len());

            for entry in entries.iter() {
                if entry.name.prefix == "." || entry.name.prefix == ".." {
                    names.push((format!("{}", entry.name.prefix), ino));
                } else {
                    names.push((format!("{}", entry.name), ino));
                }
                attr_reads.push(inode::read(ino));
            }

            let mut reply = tx.read(self.cfg.bucket, attr_reads).await?;

            let mut entries = Vec::with_capacity(names.len());
            assert!(offset >= 0);
            let names = names.into_iter().enumerate().skip(offset as usize);
            for (index, (name, ino)) in names {
                // We share the same view as when we read the directory entries
                // as we share the same transaction. An non existing entry at
                // this step means a bug.
                let inode = inode::decode(ino, &mut reply, index).unwrap();
                entries.push(ReadDirEntry {
                    ino,
                    kind: inode.kind.to_file_type(),
                    name,
                });
            }

            entries
        };

        tx.commit().await?;
        Ok(entries)
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn mkdir(
        &self,
        owner: Owner,
        mode: u32,
        parent_ino: u64,
        name: NameRef,
    ) -> Result<FileAttr> {
        let ino = self.next_ino()?;

        let mut connection = self.pool.acquire().await?;
        let mut tx = transaction!(self.cfg, connection, {
            exclusive: [
                inode::key(parent_ino),
                dir::key(parent_ino)
            ]
        })
        .await?;

        let attr = {
            let mut reply = tx
                .read(
                    self.cfg.bucket,
                    vec![inode::read(parent_ino), dir::read(parent_ino)],
                )
                .await?;

            let mut parent_inode = inode::decode(parent_ino, &mut reply, 0).ok_or(ENOENT)?;
            let entries = dir::decode(self.cfg.view, &mut reply, 1).ok_or(ENOENT)?;
            if entries.contains_key(&name) {
                return Err(Error::Sys(Errno::EEXIST));
            }

            let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            let inode = Inode {
                ino,
                kind: inode::Kind::Directory,
                parent: parent_ino,
                atime: t,
                ctime: t,
                mtime: t,
                owner,
                mode,
                size: 0,
                nlink: 2,
            };
            parent_inode.size += 1;
            parent_inode.mtime = t;
            parent_inode.atime = t;

            let attr = inode.attr();

            let name = name.canonicalize(self.cfg.view);
            tx.update(
                self.cfg.bucket,
                vec![
                    dir::add_entry(parent_ino, &dir::Entry::new(name, ino)),
                    dir::create(self.cfg.view, parent_ino, ino),
                    inode::create(&inode),
                    inode::update_stats(&parent_inode),
                ],
            )
            .await?;

            attr
        };

        tx.commit().await?;
        Ok(attr)
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn rmdir(self: Arc<Driver>, parent_ino: u64, name: NameRef) -> Result<()> {
        let mut connection = self.pool.acquire().await?;
        let mut tx = transaction!(self.cfg, connection, {
            exclusive: [
                inode::key(parent_ino),
                dir::key(parent_ino)
            ]
        })
        .await?;

        let ino = {
            let mut reply = tx
                .read(
                    self.cfg.bucket,
                    vec![inode::read(parent_ino), dir::read(parent_ino)],
                )
                .await?;

            let mut parent_inode = inode::decode(parent_ino, &mut reply, 0).ok_or(ENOENT)?;
            let entries = dir::decode(self.cfg.view, &mut reply, 1).ok_or(ENOENT)?;
            let entry = entries.get(&name).ok_or(ENOENT)?;

            let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            parent_inode.atime = t;
            parent_inode.mtime = t;
            parent_inode.size -= 1;

            let name = name.canonicalize(self.cfg.view);
            tx.update(
                self.cfg.bucket,
                vec![
                    inode::decr_link_count(entry.ino, 1),
                    dir::remove_entry(parent_ino, &dir::Entry::new(name, entry.ino)),
                ],
            )
            .await?;

            entry.ino
        };

        tx.commit().await?;
        self.clone().schedule_delete(ino);
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn mknod(
        &self,
        owner: Owner,
        mode: u32,
        parent_ino: u64,
        name: NameRef,
        _rdev: u32,
    ) -> Result<FileAttr> {
        let ino = self.next_ino()?;

        let mut connection = self.pool.acquire().await?;
        let mut tx = transaction!(self.cfg, connection, {
            exclusive: [
                inode::key(parent_ino),
                dir::key(parent_ino)
            ]
        })
        .await?;

        let attr = {
            let mut reply = tx
                .read(
                    self.cfg.bucket,
                    vec![inode::read(parent_ino), dir::read(parent_ino)],
                )
                .await?;

            let mut parent = inode::decode(parent_ino, &mut reply, 0).ok_or(ENOENT)?;
            let entries = dir::decode(self.cfg.view, &mut reply, 1).ok_or(ENOENT)?;
            if entries.contains_key(&name) {
                return Err(Error::Sys(Errno::EEXIST));
            }

            let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            let inode = Inode {
                ino,
                kind: inode::Kind::Regular,
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
            parent.atime = t;
            parent.size += 1;

            let attr = inode.attr();
            let name = name.canonicalize(self.cfg.view);
            tx.update(
                self.cfg.bucket,
                vec![
                    inode::update_stats(&parent),
                    dir::add_entry(parent_ino, &dir::Entry::new(name, ino)),
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
    pub(crate) async fn unlink(&self, parent_ino: u64, name: NameRef) -> Result<()> {
        let mut connection = self.pool.acquire().await?;
        let mut tx = transaction!(self.cfg, connection, {
            exclusive: [
                inode::key(parent_ino),
                dir::key(parent_ino)
            ]
        })
        .await?;

        let ino = {
            let mut reply = tx
                .read(
                    self.cfg.bucket,
                    vec![inode::read(parent_ino), dir::read(parent_ino)],
                )
                .await?;

            let mut parent_inode = inode::decode(parent_ino, &mut reply, 0).ok_or(ENOENT)?;
            let entries = dir::decode(self.cfg.view, &mut reply, 1).ok_or(ENOENT)?;
            let entry = entries.get(&name).ok_or(ENOENT)?;

            let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            parent_inode.atime = t;
            parent_inode.mtime = t;
            parent_inode.size -= 1;

            tx.update(
                self.cfg.bucket,
                vec![
                    dir::remove_entry(parent_ino, entry),
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
    pub(crate) async fn open(&self, ino: u64) -> Result<()> {
        self.getattr(ino).await.map(|_| ())
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn release(&self, ino: u64) -> Result<()> {
        self.getattr(ino).await.map(|_| ())
    }

    #[tracing::instrument(skip(self, bytes))]
    pub(crate) async fn write(&self, ino: u64, bytes: &[u8], offset: u64) -> Result<()> {
        let mut connection = self.pool.acquire().await?;
        let mut tx = transaction!(self.cfg, connection, { exclusive: [inode::key(ino)] }).await?;

        self.pages
            .write(&mut tx, ino, offset as usize, bytes)
            .await?;

        let mut reply = tx.read(self.cfg.bucket, vec![inode::read(ino)]).await?;
        let mut inode = inode::decode(ino, &mut reply, 0).ok_or(ENOENT)?;

        let wrote = (offset + bytes.len() as u64).saturating_sub(inode.size);

        let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        inode.atime = t;
        inode.mtime = t;
        inode.size += wrote as u64;

        tracing::trace!(?inode);
        tx.update(self.cfg.bucket, vec![inode::update_stats(&inode)])
            .await?;

        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn read(&self, ino: u64, offset: u64, len: u32) -> Result<Vec<u8>> {
        // Manual trace to avoid priting content result.
        tracing::trace!(ino, offset, len);

        let len = len as usize;
        let mut connection = self.pool.acquire().await?;
        let mut tx = transaction!(self.cfg, connection, { shared: [inode::key(ino)] }).await?;

        let mut reply = tx.read(self.cfg.bucket, vec![inode::read(ino)]).await?;
        let mut inode = inode::decode(ino, &mut reply, 0).ok_or(ENOENT)?;

        let mut bytes = Vec::with_capacity(len as usize);
        self.pages
            .read(&mut tx, ino, offset as usize, len as usize, &mut bytes)
            .await?;

        let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        inode.atime = t;
        debug!(len, read_len = bytes.len(), requested_len = len);

        /* FIXME! Update the inode while reading fast seems to make the transaction
        fails.

        tx.update(self.cfg.bucket, vec![inode::update(&inode)])
          .await?; */

        tx.commit().await?;
        Ok(bytes)
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn rename(
        &self,
        parent_ino: u64,
        name: NameRef,
        new_parent_ino: u64,
        new_name: NameRef,
    ) -> Result<()> {
        let mut connection = self.pool.acquire().await?;
        let mut tx = transaction!(self.cfg, connection, {
            exclusive: [
                inode::key(parent_ino),
                inode::key(new_parent_ino)
            ]
        })
        .await?;

        let (mut parent, mut new_parent, parent_entries, new_parent_entries) = {
            let mut reply = tx
                .read(
                    self.cfg.bucket,
                    vec![
                        inode::read(parent_ino),
                        inode::read(new_parent_ino),
                        dir::read(parent_ino),
                        dir::read(new_parent_ino),
                    ],
                )
                .await?;

            (
                inode::decode(parent_ino, &mut reply, 0).ok_or(ENOENT)?,
                inode::decode(new_parent_ino, &mut reply, 1).ok_or(ENOENT)?,
                dir::decode(self.cfg.view, &mut reply, 2).ok_or(ENOENT)?,
                dir::decode(self.cfg.view, &mut reply, 3).ok_or(ENOENT)?,
            )
        };
        debug!(?parent, ?parent_entries, ?new_parent, ?new_parent_entries);

        let entry = parent_entries.get(&name).ok_or(ENOENT)?;
        let target_entry = new_parent_entries.get(&new_name);
        debug!(?entry, ?target_entry);

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
        debug!(?inode, ?target);

        /* Checks if target is a dir and empty. If it is the case, we have
        to delete it */
        match &target {
            Some(target) if target.kind == inode::Kind::Directory && target.size == 0 => {
                debug!("target is an empty dir, removing");

                let target_entry = target_entry.unwrap();
                tx.update(
                    self.cfg.bucket,
                    vec![
                        inode::remove(target_entry.ino),
                        dir::remove(target_entry.ino),
                        dir::remove_entry(new_parent_ino, target_entry),
                    ],
                )
                .await?;
            }
            Some(target) if target.nlink == 1 => {
                debug!("target is an existing link");

                let target_entry = target_entry.unwrap();
                tx.update(
                    self.cfg.bucket,
                    vec![
                        inode::remove(target.ino),
                        dir::remove_entry(new_parent_ino, target_entry),
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
        debug!(?parent, ?parent_entries, ?new_parent, ?new_parent_entries);

        let ino = entry.ino;

        let new_name = new_name.canonicalize(self.cfg.view);
        let new_entry = &dir::Entry::new(new_name, ino);

        tx.update(
            self.cfg.bucket,
            vec![
                inode::update_stats(&parent),
                inode::update_stats(&new_parent),
                inode::update_stats(&inode),
                dir::remove_entry(parent_ino, entry),
                dir::add_entry(new_parent_ino, new_entry),
            ],
        )
        .await?;

        tx.commit().await?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn link(
        &self,
        ino: u64,
        new_parent_ino: u64,
        new_name: NameRef,
    ) -> Result<FileAttr> {
        let mut connection = self.pool.acquire().await?;
        let mut tx = transaction!(self.cfg, connection, {
            exclusive: [
                inode::key(ino),
                inode::key(new_parent_ino),
                dir::key(new_parent_ino)
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
                        dir::read(new_parent_ino),
                    ],
                )
                .await?;

            let inode = inode::decode(ino, &mut reply, 0).ok_or(ENOENT)?;
            let parent = inode::decode(new_parent_ino, &mut reply, 1).ok_or(ENOENT)?;
            let entries = dir::decode(self.cfg.view, &mut reply, 2).ok_or(ENOENT)?;

            (inode, parent, entries)
        };

        if entries.get(&new_name).is_some() {
            return Err(Error::Sys(Errno::EEXIST));
        }

        let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        parent.mtime = t;
        parent.atime = t;

        let new_name = new_name.canonicalize(self.cfg.view);
        tx.update(
            self.cfg.bucket,
            vec![
                inode::update_stats(&parent),
                dir::add_entry(new_parent_ino, &dir::Entry::new(new_name, ino)),
                inode::incr_link_count(ino, 1),
            ],
        )
        .await?;

        inode.nlink += 1;
        tx.commit().await?;
        Ok(inode.attr())
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn read_link(&self, ino: u64) -> Result<String> {
        let mut connection = self.pool.acquire().await?;
        let mut tx =
            transaction!(self.cfg, connection, { shared: [symlink::key(ino)] }).await?;

        let mut reply = tx
            .read(self.cfg.bucket, vec![symlink::read(ino)])
            .await?;

        let link = symlink::decode(&mut reply, 0).ok_or(ENOENT)?;

        tx.commit().await?;
        Ok(link)
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn symlink(
        &self,
        parent_ino: u64,
        owner: Owner,
        name: NameRef,
        link: String,
    ) -> Result<FileAttr> {
        let ino = self.next_ino()?;

        let mut connection = self.pool.acquire().await?;
        let mut tx = transaction!(self.cfg, connection, {
            exclusive: [
                inode::key(parent_ino),
                dir::key(parent_ino)
            ]
        })
        .await?;

        let (mut parent, entries) = {
            let mut reply = tx
                .read(
                    self.cfg.bucket,
                    vec![inode::read(parent_ino), dir::read(parent_ino)],
                )
                .await?;

            let parent = inode::decode(parent_ino, &mut reply, 0).ok_or(ENOENT)?;
            let entries = dir::decode(self.cfg.view, &mut reply, 1).ok_or(ENOENT)?;

            (parent, entries)
        };

        if entries.contains_key(&name) {
            return Err(Error::Sys(Errno::EEXIST));
        }

        let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let inode = inode::Inode {
            ino,
            kind: inode::Kind::Symlink,
            parent: parent_ino,
            atime: t,
            ctime: t,
            mtime: t,
            owner,
            mode: 0644,
            size: link.len() as u64,
            nlink: 1,
        };
        parent.size += 1;
        parent.mtime = t;
        parent.atime = t;

        let name = name.canonicalize(self.cfg.view);
        tx.update(
            self.cfg.bucket,
            vec![
                inode::create(&inode),
                inode::update_stats(&parent),
                dir::add_entry(parent_ino, &dir::Entry::new(name, ino)),
                symlink::create(ino, link),
            ],
        )
        .await?;

        tx.commit().await?;
        Ok(inode.attr())
    }

    fn schedule_delete(&self, ino: u64) {
        #[tracing::instrument(skip(cfg, pool))]
        async fn delete_later(cfg: Config, pool: Arc<ConnectionPool>, ino: u64) -> Result<bool> {
            let mut connection = pool.acquire().await?;
            let mut tx = transaction!(cfg, connection, { exclusive: [inode::key(ino)] }).await?;

            let inode = {
                let mut reply = tx.read(cfg.bucket, vec![inode::read(ino)]).await?;

                inode::decode(ino, &mut reply, 0).ok_or(ENOENT)?
            };

            let must_be_removed =
                (inode.kind == inode::Kind::Directory && inode.nlink == 1) || inode.nlink == 0;

            if must_be_removed {
                tx.update(
                    cfg.bucket,
                    vec![
                        inode::remove(ino),
                        dir::remove(ino),
                        symlink::remove(ino),
                    ],
                )
                .await?;
            }

            tx.commit().await?;
            Ok(must_be_removed)
        }

        let cfg = self.cfg.clone();
        let pool = self.pool.clone();
        task::spawn(delete_later(cfg, pool, ino));
    }

    #[tracing::instrument(skip(self))]
    pub(crate) fn next_ino(&self) -> Result<u64> {
        #[tracing::instrument(skip(cfg, counter, pool))]
        async fn checkpoint(
            cfg: Config,
            counter: Arc<InoGenerator>,
            pool: Arc<ConnectionPool>,
        ) -> Result<()> {
            let mut connection = pool.acquire().await?;

            let mut tx = transaction!(cfg, connection, { exclusive: [ino::key(cfg.view)] }).await?;

            counter.checkpoint(&mut tx).await?;

            tx.commit().await?;
            Ok(())
        }

        let ino = self.ino_counter.next();

        let counter = self.ino_counter.clone();
        let pool = self.pool.clone();
        let cfg = self.cfg.clone();
        task::spawn(checkpoint(cfg, counter, pool));

        Ok(ino)
    }
}

#[derive(Debug)]
pub(crate) struct ReadDirEntry {
    pub(crate) ino: u64,
    pub(crate) kind: FileType,
    pub(crate) name: String,
}
