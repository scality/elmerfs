use crate::inode::{self, Inode, Owner};
use crate::key::{Bucket, InoCounter};
use crate::page::PageDriver;
use crate::{pool::ConnectionPool, AddressBook};
use antidotec::{self, counter, crdts, Connection, Transaction, TransactionLocks};
use async_std::sync::Arc;
use async_std::task;
use fuse::*;
use nix::errno::Errno;
use std::fmt::Debug;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tracing::debug;

const ROOT_INO: u64 = 1;
const INO_COUNTER: InoCounter = InoCounter::new(0);
const MAX_CONNECTIONS: usize = 32;

macro_rules! expect_inode {
    ($ino:expr, $map:expr) => {
        match $map {
            Some(map) => inode::decode($ino, map),
            None => return Err(Error::Sys(Errno::ENOENT)),
        }
    };
}

macro_rules! transaction {
    ($driver:expr, $connection:expr) => {
        transaction!($driver, { shared: [], exclusive: [] })
    };

    ($driver:expr, $connection:expr, { shared: [$($shared:expr),*] }) => {
        transaction!($driver, $connection, { shared: [$($shared),*], exclusive: [] })
    };

    ($driver:expr, $connection:expr, { exclusive: [$($excl:expr),*] }) => {
        transaction!($driver, $connection, { shared: [], exclusive: [$($excl),*] })
    };

    ($driver:expr, $connection:expr, { shared: [$($shared:expr),*], exclusive: [$($excl:expr),*] }) => {{
        if $driver.antidote_locks {
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

    #[error("could not allocate a new inode number")]
    InoAllocFailed,
}
pub(crate) type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub struct Config {
    pub bucket: Bucket,
    pub addresses: AddressBook,
    pub use_distributed_locks: bool,
}

#[derive(Debug)]
pub(crate) struct Driver {
    bucket: Bucket,
    pages: PageDriver,
    pool: ConnectionPool,
    antidote_locks: bool,
}

impl Driver {
    pub async fn new(cfg: Config, pages: PageDriver) -> Result<Self> {
        let Config {
            bucket,
            addresses,
            use_distributed_locks,
        } = cfg;
        Ok(Self {
            bucket,
            pages,
            pool: ConnectionPool::with_capacity(addresses, MAX_CONNECTIONS),
            antidote_locks: use_distributed_locks,
        })
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn configure(&self) -> Result<()> {
        let mut connection = self.pool.acquire().await?;

        self.ensure_ino_counter(&mut connection).await?;
        self.ensure_root_dir(&mut connection).await?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn ensure_ino_counter(&self, connection: &mut Connection) -> Result<()> {
        let mut tx = transaction!(self, connection, { exclusive: [INO_COUNTER] }).await?;

        {
            let mut reply = tx
                .read(self.bucket, vec![counter::get(INO_COUNTER)])
                .await?;

            if reply.counter(0) != 0 {
                tx.commit().await?;
                return Ok(());
            }

            let inc = i32::min_value();
            tx.update(self.bucket, vec![counter::inc(INO_COUNTER, inc)])
                .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn ensure_root_dir(&self, connection: &mut Connection) -> Result<()> {
        match self.getattr(ROOT_INO).await {
            Ok(_) => return Ok(()),
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
            nlink: 2,
        };

        let mut entries = inode::DirEntries::new();
        entries.insert(String::from("."), ROOT_INO);
        entries.insert(String::from(".."), ROOT_INO);

        let mut tx =
            transaction!(self, connection, { exclusive: [inode::Key::new(ROOT_INO)] }).await?;
        {
            tx.update(
                self.bucket,
                vec![
                    inode::update(&root_inode, inode::NLinkInc(3)),
                    inode::update_dir(ROOT_INO, &entries),
                ],
            )
            .await?;
        }
        tx.commit().await?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn getattr(&self, ino: u64) -> Result<FileAttr> {
        let mut connection = self.pool.acquire().await?;

        let mut tx = transaction!(self, connection, { shared: [inode::Key::new(ino)] }).await?;

        let attrs = self.attr_of(&mut tx, ino).await?;

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
        let mut tx = transaction!(self, connection, { exclusive: [inode::Key::new(ino)] }).await?;

        let inode = {
            let mut reply = tx.read(self.bucket, vec![inode::read(ino)]).await?;
            let mut inode = expect_inode!(ino, reply.rrmap(0));

            update!(inode.mode, mode);
            update!(inode.owner.uid, uid);
            update!(inode.owner.gid, gid);
            update!(inode.size, size);
            update!(inode.atime, atime);
            update!(inode.mtime, mtime);

            tx.update(self.bucket, vec![inode::update_stats(&inode)])
                .await?;

            inode
        };

        tx.commit().await?;
        Ok(inode.attr())
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn lookup(&self, parent_ino: u64, name: &str) -> Result<FileAttr> {
        let mut connection = self.pool.acquire().await?;
        let mut tx = transaction!(self, connection, {
            shared: [inode::Key::dir_entries(parent_ino)]
        })
        .await?;

        let entries = {
            let mut reply = tx
                .read(self.bucket, vec![inode::read_dir(parent_ino)])
                .await?;

            match reply.rrmap(0) {
                Some(map) => inode::decode_dir(map),
                None => {
                    return Err(Error::Sys(Errno::ENOENT));
                }
            }
        };

        let attrs = match entries.get(name) {
            Some(ino) => self.attr_of(&mut tx, *ino).await,
            None => Err(Error::Sys(Errno::ENOENT)),
        };

        tx.commit().await?;
        attrs
    }

    async fn attr_of(&self, tx: &mut Transaction<'_>, ino: u64) -> Result<FileAttr> {
        let mut reply = tx.read(self.bucket, vec![inode::read(ino)]).await?;
        Ok(expect_inode!(ino, reply.rrmap(0)).attr())
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
        let mut tx =
            transaction!(self, connection, { shared: [inode::Key::dir_entries(ino)] }).await?;

        let entries = {
            let entries = {
                let mut reply = tx.read(self.bucket, vec![inode::read_dir(ino)]).await?;

                match reply.rrmap(0) {
                    Some(map) => inode::decode_dir(map),
                    None => {
                        // FIXME: An API that prevents this kind of "I need to
                        // remember to properly close the transaction" would
                        // be better.
                        tx.commit().await?;
                        return Ok(vec![]);
                    }
                }
            };

            let mut names = Vec::with_capacity(entries.len());
            let mut attr_reads = Vec::with_capacity(entries.len());

            for (name, ino) in entries {
                names.push((name, ino));
                attr_reads.push(inode::read(ino));
            }

            let mut reply = tx.read(self.bucket, attr_reads).await?;

            let mut entries = Vec::with_capacity(names.len());
            assert!(offset >= 0);
            let names = names.into_iter().enumerate().skip(offset as usize);
            for (index, (name, ino)) in names {
                // We share the same view as when we read the directory entries
                // as we share the same transaction. An non existing entry at
                // this step means a bug.
                let inode = reply.rrmap(index).unwrap();
                let inode = inode::decode(ino, inode);

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
        name: String,
    ) -> Result<FileAttr> {
        let mut connection = self.pool.acquire().await?;
        let ino = self.generate_ino(&mut connection).await?;

        let mut tx = transaction!(self, connection, {
            exclusive: [
                inode::Key::new(parent_ino),
                inode::Key::dir_entries(parent_ino)
            ]
        })
        .await?;

        let attr = {
            let mut reply = tx
                .read(
                    self.bucket,
                    vec![inode::read(parent_ino), inode::read_dir(parent_ino)],
                )
                .await?;

            let mut parent_inode = expect_inode!(parent_ino, reply.rrmap(0));

            let mut entries = inode::decode_dir(reply.rrmap(1).unwrap_or(crdts::RrMap::new()));

            if entries.contains_key(&name) {
                return Err(Error::Sys(Errno::EEXIST));
            }
            entries.insert(name, ino);

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

            let mut default_entries = inode::DirEntries::new();
            default_entries.insert(String::from("."), ino);
            default_entries.insert(String::from(".."), parent_ino);

            tx.update(
                self.bucket,
                vec![
                    inode::update_stats(&parent_inode),
                    inode::update_dir(parent_ino, &entries),
                    inode::update(&inode, inode::NLinkInc(inode.nlink as i32)),
                    inode::update_dir(ino, &default_entries),
                ],
            )
            .await?;

            attr
        };

        tx.commit().await?;
        Ok(attr)
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn rmdir(self: Arc<Driver>, parent_ino: u64, name: String) -> Result<()> {
        let mut connection = self.pool.acquire().await?;
        let mut tx = transaction!(self, connection, {
            exclusive: [
                inode::Key::new(parent_ino),
                inode::Key::dir_entries(parent_ino)
            ]
        })
        .await?;

        let ino = {
            let mut reply = tx
                .read(
                    self.bucket,
                    vec![inode::read(parent_ino), inode::read_dir(parent_ino)],
                )
                .await?;

            let mut parent_inode = match reply.rrmap(0) {
                Some(inode) => inode::decode(parent_ino, inode),
                None => {
                    return Err(Error::Sys(Errno::ENOENT));
                }
            };

            let mut entries = inode::decode_dir(reply.rrmap(1).unwrap_or_default());

            let ino = match entries.remove(&name) {
                Some(ino) => ino,
                None => {
                    return Err(Error::Sys(Errno::ENOENT));
                }
            };

            let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            parent_inode.atime = t;
            parent_inode.mtime = t;
            parent_inode.size -= 1;

            tx.update(
                self.bucket,
                vec![
                    inode::decr_link_count(ino, 1),
                    inode::remove_dir_entry(parent_ino, name),
                ],
            )
            .await?;

            ino
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
        name: String,
        _rdev: u32,
    ) -> Result<FileAttr> {
        let mut connection = self.pool.acquire().await?;
        let ino = self.generate_ino(&mut connection).await?;

        let mut tx = transaction!(self, connection, {
            exclusive: [
                inode::Key::new(parent_ino),
                inode::Key::dir_entries(parent_ino)
            ]
        })
        .await?;

        let attr = {
            let mut reply = tx
                .read(
                    self.bucket,
                    vec![inode::read(parent_ino), inode::read_dir(parent_ino)],
                )
                .await?;

            let mut parent = expect_inode!(parent_ino, reply.rrmap(0));
            let mut entries = inode::decode_dir(reply.rrmap(1).unwrap_or(crdts::GMap::new()));

            if entries.contains_key(&name) {
                return Err(Error::Sys(Errno::EEXIST));
            }

            entries.insert(name, ino);

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
            parent.size = entries.len() as u64;
            parent.mtime = t;
            parent.atime = t;
            parent.size += 1;

            let attr = inode.attr();

            tx.update(
                self.bucket,
                vec![
                    inode::update_stats(&parent),
                    inode::update_dir(parent_ino, &entries),
                    inode::update(&inode, inode::NLinkInc(inode.nlink as i32)),
                ],
            )
            .await?;

            attr
        };

        tx.commit().await?;
        Ok(attr)
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn unlink(self: Arc<Driver>, parent_ino: u64, name: String) -> Result<()> {
        let mut connection = self.pool.acquire().await?;

        let mut tx = transaction!(self, connection, {
            exclusive: [
                inode::Key::new(parent_ino),
                inode::Key::dir_entries(parent_ino)
            ]
        })
        .await?;

        let ino = {
            let mut reply = tx
                .read(
                    self.bucket,
                    vec![inode::read(parent_ino), inode::read_dir(parent_ino)],
                )
                .await?;

            let mut parent_inode = expect_inode!(parent_ino, reply.rrmap(0));
            let mut entries = inode::decode_dir(reply.rrmap(1).unwrap_or(crdts::GMap::new()));

            let ino = match entries.remove(&name) {
                Some(ino) => ino,
                None => {
                    return Err(Error::Sys(Errno::ENOENT));
                }
            };

            let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            parent_inode.atime = t;
            parent_inode.mtime = t;
            parent_inode.size -= 1;

            tx.update(
                self.bucket,
                vec![
                    inode::remove_dir_entry(parent_ino, name),
                    inode::decr_link_count(ino, 1),
                ],
            )
            .await?;

            ino
        };

        tx.commit().await?;
        self.clone().schedule_delete(ino);
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
        let mut tx = transaction!(self, connection, { exclusive: [inode::Key::new(ino)] }).await?;

        self.pages
            .write(&mut tx, ino, offset as usize, bytes)
            .await?;

        let mut reply = tx.read(self.bucket, vec![inode::read(ino)]).await?;
        let mut inode = expect_inode!(ino, reply.rrmap(0));

        let wrote = (offset + bytes.len() as u64).saturating_sub(inode.size);

        let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        inode.atime = t;
        inode.mtime = t;
        inode.size += wrote as u64;

        tracing::trace!(?inode);
        tx.update(self.bucket, vec![inode::update_stats(&inode)])
            .await?;

        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn read(&self, ino: u64, offset: u64, len: u32) -> Result<Vec<u8>> {
        // Manual trace to avoid priting content result.
        tracing::trace!(ino, offset, len);

        let len = len as usize;
        let mut connection = self.pool.acquire().await?;
        let mut tx = transaction!(self, connection, { shared: [inode::Key::new(ino)] }).await?;

        let mut reply = tx.read(self.bucket, vec![inode::read(ino)]).await?;
        let mut inode = expect_inode!(ino, reply.rrmap(0));

        let mut bytes = Vec::with_capacity(len as usize);
        self.pages
            .read(&mut tx, ino, offset as usize, len as usize, &mut bytes)
            .await?;

        let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        inode.atime = t;
        debug!(len, read_len = bytes.len(), requested_len = len);

        /* FIXME! Update the inode while reading fast seems to make the transaction
        fails.

        tx.update(self.bucket, vec![inode::update(&inode)])
          .await?; */

        tx.commit().await?;
        Ok(bytes)
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn rename(
        &self,
        parent_ino: u64,
        name: String,
        new_parent_ino: u64,
        new_name: String,
    ) -> Result<()> {
        let mut connection = self.pool.acquire().await?;
        let mut tx = transaction!(self, connection, {
            exclusive: [
                inode::Key::new(parent_ino),
                inode::Key::new(new_parent_ino)
            ]
        })
        .await?;

        let (mut parent, mut parent_entries, mut new_parent, mut new_parent_entries) = {
            let mut reply = tx
                .read(
                    self.bucket,
                    vec![
                        inode::read(parent_ino),
                        inode::read_dir(parent_ino),
                        inode::read(new_parent_ino),
                        inode::read_dir(new_parent_ino),
                    ],
                )
                .await?;

            (
                expect_inode!(parent_ino, reply.rrmap(0)),
                inode::decode_dir(reply.rrmap(1).unwrap_or_default()),
                expect_inode!(new_parent_ino, reply.rrmap(2)),
                inode::decode_dir(reply.rrmap(3).unwrap_or_default()),
            )
        };
        debug!(?parent, ?parent_entries, ?new_parent, ?new_parent_entries);

        let ino = match parent_entries.get(&name) {
            Some(ino) => *ino,
            None => return Err(Error::Sys(Errno::ENOENT)),
        };
        let target_ino = new_parent_entries.get(&new_name).copied();
        debug!(?ino, ?target_ino);

        /* If we have to deal with the same link, this rename does nothing */
        if parent_ino == new_parent_ino && Some(ino) == target_ino && name == new_name {
            debug!("noop operation");
            return Ok(());
        }

        let (mut inode, target) = {
            let reads = if let Some(target_ino) = target_ino {
                vec![inode::read(ino), inode::read(target_ino)]
            } else {
                vec![inode::read(ino)]
            };
            let mut reply = tx.read(self.bucket, reads).await?;

            let inode = expect_inode!(ino, reply.rrmap(0));
            let target = if let Some(target_ino) = target_ino {
                Some(expect_inode!(target_ino, reply.rrmap(1)))
            } else {
                None
            };

            (inode, target)
        };
        debug!(?inode, ?target);

        /* Checks if target is a dir and empty. If it is the case, we have
        to delete it */
        match &target {
            Some(target) if target.kind == inode::Kind::Directory && target.size == 0 => {
                debug!("target is an empty dir, removing");
                tx.update(
                    self.bucket,
                    vec![
                        inode::remove(target.ino),
                        inode::remove_dir(target.ino),
                        inode::remove_dir_entry(new_parent_ino, new_name.clone()),
                    ],
                )
                .await?;
            }
            Some(target) if target.nlink == 1 => {
                debug!("target is an existing link");

                tx.update(
                    self.bucket,
                    vec![inode::remove(target.ino), inode::remove_link(target.ino)],
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
        new_parent_entries.insert(new_name, ino);

        parent.size -= 1;
        parent.atime = t;
        parent.mtime = t;
        parent_entries.remove(&name);

        if parent_ino == new_parent_ino {
            /* Be sure that we remove the old name when updating entries */
            new_parent_entries.remove(&name);
        }

        inode.atime = t;
        debug!(?parent, ?parent_entries, ?new_parent, ?new_parent_entries);

        tx.update(
            self.bucket,
            vec![
                inode::update_stats(&parent),
                inode::remove_dir_entry(parent_ino, name),
                inode::update_stats(&new_parent),
                inode::update_dir(new_parent_ino, &new_parent_entries),
                inode::update_stats(&inode),
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
        new_name: String,
    ) -> Result<FileAttr> {
        let mut connection = self.pool.acquire().await?;
        let mut tx = transaction!(self, connection, {
            exclusive: [
                inode::Key::new(ino),
                inode::Key::new(new_parent_ino),
                inode::Key::dir_entries(new_parent_ino)
            ]
        })
        .await?;

        let (mut inode, mut parent, mut entries) = {
            let mut reply = tx
                .read(
                    self.bucket,
                    vec![
                        inode::read(ino),
                        inode::read(new_parent_ino),
                        inode::read_dir(new_parent_ino),
                    ],
                )
                .await?;

            let inode = expect_inode!(ino, reply.rrmap(0));
            let parent = expect_inode!(new_parent_ino, reply.rrmap(1));
            let entries = inode::decode_dir(reply.rrmap(2).unwrap_or_default());

            (inode, parent, entries)
        };

        if entries.contains_key(&new_name) {
            return Err(Error::Sys(Errno::EEXIST));
        }

        let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        parent.mtime = t;
        parent.atime = t;

        entries.insert(new_name, ino);

        tx.update(
            self.bucket,
            vec![
                inode::update_stats(&parent),
                inode::update_dir(new_parent_ino, &entries),
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
        let mut tx = transaction!(self, connection, { shared: [inode::Key::symlink(ino)] }).await?;

        let mut reply = tx.read(self.bucket, vec![inode::read_link(ino)]).await?;
        let link = match reply.lwwreg(0) {
            Some(reg) => inode::decode_link(reg),
            None => return Err(Error::Sys(Errno::ENOENT)),
        };

        tx.commit().await?;
        Ok(link)
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn symlink(
        &self,
        parent_ino: u64,
        owner: Owner,
        name: String,
        link: String,
    ) -> Result<FileAttr> {
        let mut connection = self.pool.acquire().await?;
        let ino = self.generate_ino(&mut connection).await?;

        let mut tx = transaction!(self, connection, {
            exclusive: [
                inode::Key::new(parent_ino),
                inode::Key::dir_entries(parent_ino)
            ]
        })
        .await?;

        let (mut parent, mut entries) = {
            let mut reply = tx
                .read(
                    self.bucket,
                    vec![inode::read(parent_ino), inode::read_dir(parent_ino)],
                )
                .await?;

            let parent = expect_inode!(parent_ino, reply.rrmap(0));
            let entries = inode::decode_dir(reply.rrmap(1).unwrap_or_default());

            (parent, entries)
        };

        if entries.contains_key(&name) {
            return Err(Error::Sys(Errno::EEXIST));
        }
        entries.insert(name, ino);

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

        tx.update(
            self.bucket,
            vec![
                inode::update(&inode, inode::NLinkInc(inode.nlink as i32)),
                inode::update_stats(&parent),
                inode::update_dir(parent_ino, &entries),
                inode::set_link(ino, link),
            ],
        )
        .await?;

        tx.commit().await?;
        Ok(inode.attr())
    }

    fn schedule_delete(self: Arc<Driver>, ino: u64) {
        task::spawn(async move {
            let result = self.delete_later(ino).await;
            tracing::trace!(?result);
        });
    }

    #[tracing::instrument(skip(self))]
    async fn delete_later(&self, ino: u64) -> Result<bool> {
        let mut connection = self.pool.acquire().await?;
        let mut tx = transaction!(self, connection, { exclusive: [inode::Key::new(ino)] }).await?;

        let inode = {
            let mut reply = tx.read(self.bucket, vec![inode::read(ino)]).await?;

            expect_inode!(ino, reply.rrmap(0))
        };

        let must_be_removed =
            (inode.kind == inode::Kind::Directory && inode.nlink == 1) || inode.nlink == 0;

        if must_be_removed {
            tx.update(
                self.bucket,
                vec![inode::remove(ino), inode::remove_dir(ino)],
            )
            .await?;
        }

        tx.commit().await?;
        Ok(must_be_removed)
    }

    #[tracing::instrument(skip(self, connection))]
    pub(crate) async fn generate_ino(&self, connection: &mut Connection) -> Result<u64> {
        let mut tx = transaction!(self, connection, { exclusive: [INO_COUNTER] }).await?;

        let ino = {
            let mut reply = tx
                .read(self.bucket, vec![counter::get(INO_COUNTER)])
                .await?;

            let ino_counter = reply.counter(0) as u64;
            let (ino, inc) = match ino_counter.checked_add(1) {
                Some(0) => {
                    // If we reached 0 we need to skip 0 and 1 (ROOT ino).
                    (2, 2)
                }
                Some(ino) => (ino, 1),
                None => {
                    return Err(Error::InoAllocFailed);
                }
            };

            tx.update(self.bucket, vec![counter::inc(INO_COUNTER, inc)])
                .await?;

            ino as u64
        };

        tx.commit().await?;
        Ok(ino)
    }
}

#[derive(Debug)]
pub(crate) struct ReadDirEntry {
    pub(crate) ino: u64,
    pub(crate) kind: FileType,
    pub(crate) name: String,
}
