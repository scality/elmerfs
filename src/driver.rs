mod buffer;
mod dir;
pub(crate) mod ino;
mod openfile;
mod page;
mod pool;

use crate::driver::openfile::WriteAttrsDesc;
use crate::time;

use self::buffer::WriteSlice;
pub use self::pool::AddressBook;

use self::dir::DirDriver;
use self::ino::InoGenerator;
use self::openfile::{OpenfileHandle, Openfiles};
use self::pool::ConnectionPool;
use crate::config::Config;
use crate::model::{
    dentries,
    inode::{self, Owner, InodeKind, Ino},
    symlink,
};
use crate::view::{Name, NameRef, View};
use antidotec::{self, reads, updates, Bytes, Connection, Error, Transaction, TransactionLocks};
use async_std::sync::{Arc, Mutex};
use fuse::*;
use nix::errno::Errno;
use std::fmt::Debug;
use std::time::Duration;
use thiserror::Error;

pub use self::dir::ListingFlavor;

pub(crate) const MAX_CONNECTIONS: usize = 32;
pub(crate) const ENOENT: DriverError = DriverError::Sys(Errno::ENOENT);
pub(crate) const EINVAL: DriverError = DriverError::Sys(Errno::EINVAL);
pub(crate) const EEXIST: DriverError = DriverError::Sys(Errno::EEXIST);
pub(crate) const ENOTEMPTY: DriverError = DriverError::Sys(Errno::ENOTEMPTY);
pub(crate) const EIO: DriverError = DriverError::Sys(Errno::EIO);
pub(crate) const EBADFD: DriverError = DriverError::Sys(Errno::EBADFD);

pub(crate) const FUSE_MAX_WRITE: u64 = 128 * 1024;

pub type FileHandle = Ino;

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
        if $cfg.driver.use_locks {
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
pub enum DriverError {
    #[error("driver replied with: {0}")]
    Sys(Errno),

    #[error("io error with antidote: {0}")]
    Antidote(#[from] Error),

    #[error("internal syscall failed: {0}")]
    Nix(#[from] nix::Error),
}

impl DriverError {
    pub fn should_retry(&self) -> bool {
        match self {
            Self::Antidote(Error::Antidote(antidotec::AntidoteError::Aborted)) => true,
            _ => false,
        }
    }
}

pub(crate) type Result<T> = std::result::Result<T, DriverError>;

pub(crate) struct Driver {
    config: Arc<Config>,
    ino_counter: Arc<InoGenerator>,
    pool: Arc<ConnectionPool>,
    openfiles: Arc<Mutex<Openfiles>>,
    dirs: DirDriver,
}

impl Driver {
    pub async fn new(config: Arc<Config>) -> Result<Self> {
        let pool = Arc::new(ConnectionPool::with_capacity(
            config.antidote.addresses.clone(),
            MAX_CONNECTIONS,
        ));
        let openfiles = Arc::new(Mutex::new(Openfiles::new(config.clone(), pool.clone())));

        let ino_counter = {
            let mut connection = pool.acquire().await?;
            let ino_counter = InoGenerator::load(&mut connection, config.clone()).await?;
            ino_counter
        };

        let dirs = DirDriver::new(config.clone(), pool.clone());

        Ok(Self {
            config,
            ino_counter: Arc::new(ino_counter),
            pool,
            openfiles,
            dirs,
        })
    }

    pub async fn bootstrap(config: Arc<Config>) -> Result<()> {
        let pool = Arc::new(ConnectionPool::with_capacity(
            config.antidote.addresses.clone(),
            MAX_CONNECTIONS,
        ));
        let mut openfiles = Openfiles::new(config.clone(), pool.clone());

        let mut connection = pool.acquire().await?;
        Self::create_root(&config, &mut openfiles, &mut connection).await
    }

    #[tracing::instrument(skip(connection))]
    async fn create_root(
        config: &Config,
        openfiles: &mut Openfiles,
        connection: &mut Connection,
    ) -> Result<()> {
        let ts = time::now();

        let mut tx =
            transaction!(config, connection, { exclusive: [inode::key(Ino::root())] }).await?;

        match Self::attr_of(config, openfiles, &mut tx, Ino::root()).await {
            Ok(_) => return Err(EEXIST),
            Err(DriverError::Sys(Errno::ENOENT)) => {}
            Err(error) => return Err(error),
        };

        let links = [
            inode::Link::new(Ino::root(), Name::new(".", View::root())),
            inode::Link::new(Ino::root(), Name::new("..", View::root())),
        ];
        let root = inode::CreateDesc {
            ino: Ino::root(),
            owner: Owner { uid: 0, gid: 0 },
            links: links.iter().cloned(),
            size: config.driver.page_size_b,
            mode: 0o755,
            dotdot: Some(Ino::root()),
        };
        tx.update(
            config.bucket(),
            updates!(
                inode::create(ts, root),
                dentries::create(View { uid: 0 }, Ino::root(), Ino::root())
            ),
        )
        .await?;

        tx.commit().await?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn getattr(&self, view: View, ino: Ino) -> Result<FileAttr> {
        let mut connection = self.pool.acquire().await?;

        let mut tx = transaction!(self.config, connection, { shared: [inode::key(ino)] }).await?;

        let mut openfiles = self.openfiles.lock().await;
        let attrs = Self::attr_of(&self.config, &mut openfiles, &mut tx, ino).await?;

        let result = tx.commit().await;

        result?;
        Ok(attrs)
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn setattr(
        &self,
        view: View,
        ino: Ino,
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

        if inode::kind(ino) == InodeKind::Regular {
            let mut openfiles = self.openfiles.lock().await;
            let openfile = openfiles.open(ino).await?;

            let result = openfile
                .write_attrs(Box::new(WriteAttrsDesc {
                    mode,
                    uid,
                    gid,
                    size,
                    atime,
                    mtime,
                }))
                .await;

            openfiles.close(ino).await?;
            return result;
        }

        let mut connection = self.pool.acquire().await?;
        let mut tx =
            transaction!(self.config, connection, { exclusive: [inode::key(ino)] }).await?;

        let (inode, updates) = {
            let mut reply = tx
                .read(self.config.bucket(), vec![inode::read(ino)])
                .await?;
            let mut inode = inode::decode(ino, &mut reply, 0).ok_or(ENOENT)?;

            update!(inode.mode, mode);
            update!(inode.owner.uid, uid);
            update!(inode.owner.gid, gid);
            update!(inode.atime, atime);
            update!(inode.mtime, mtime);

            let updates = inode::UpdateAttrsDesc {
                mode,
                size: None,
                atime,
                mtime,
                owner: Some(inode.owner),
            };
            (inode, inode::update_attrs(ino, updates))
        };

        tx.update(self.config.bucket(), updates!(updates)).await?;
        tx.commit().await?;

        Ok(inode.attr(ino))
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn lookup(
        &self,
        view: View,
        parent_ino: Ino,
        name: &NameRef,
    ) -> Result<FileAttr> {
        let mut connection = self.pool.acquire().await?;
        let mut tx = transaction!(self.config, connection, {
            shared: [dentries::key(parent_ino)]
        })
        .await?;

        let mut reply = tx
            .read(self.config.bucket(), reads!(dentries::read(parent_ino)))
            .await?;

        let entries = self
            .dirs
            .decode_view(view, &mut tx, parent_ino, &mut reply, 0)
            .await?;

        let attr = match entries.get(&name) {
            Some(entry) => {
                let mut openfiles = self.openfiles.lock().await;
                Self::attr_of(&self.config, &mut *openfiles, &mut tx, entry.ino).await
            }
            None => Err(ENOENT),
        };

        tx.commit().await?;
        attr
    }

    async fn attr_of(
        config: &Config,
        openfiles: &mut Openfiles,
        tx: &mut Transaction<'_>,
        ino: Ino,
    ) -> Result<FileAttr> {
        if inode::kind(ino) == InodeKind::Regular {
            let openfile = openfiles.open(ino).await?;

            let result = openfile.read_attrs().await;

            openfiles.close(ino).await?;
            return result;
        }

        let mut reply = tx.read(config.bucket(), vec![inode::read(ino)]).await?;

        let inode = inode::decode(ino, &mut reply, 0).ok_or(ENOENT)?;
        return Ok(inode.attr(ino));
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn opendir(&self, view: View, ino: Ino) -> Result<()> {
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn releasedir(&self, view: View, ino: Ino) -> Result<()> {
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn readdir(
        &self,
        view: View,
        ino: Ino,
        offset: i64,
    ) -> Result<Vec<ReadDirEntry>> {
        assert!(offset >= 0);
        let mut connection = self.pool.acquire().await?;
        let mut tx =
            transaction!(self.config, connection, { shared: [dentries::key(ino)] }).await?;

        let mut reply = tx
            .read(
                self.config.bucket(),
                reads!(inode::read(ino), dentries::read(ino)),
            )
            .await?;

        let inode = inode::decode(ino, &mut reply, 0).ok_or(ENOENT)?;
        let entries = self
            .dirs
            .decode_view(view, &mut tx, ino, &mut reply, 1)
            .await?;

        let mut mapped_entries = Vec::with_capacity((offset < 2) as usize * 2 + entries.len());

        if offset < 1 {
            mapped_entries.push(ReadDirEntry {
                name: ".".into(),
                ino,
                kind: inode::file_type(ino),
            });
        }

        if offset < 2 {
            let dotdot = inode.dotdot.unwrap();
            mapped_entries.push(ReadDirEntry {
                name: "..".into(),
                ino: dotdot,
                kind: inode::file_type(dotdot),
            });
        }

        let offset = offset.saturating_sub(2);
        for entry in entries.iter_from(self.config.driver.listing_flavor, offset as usize) {
            let entry = entry?;

            mapped_entries.push(ReadDirEntry {
                name: entry.name.into_owned(),
                ino: entry.ino,
                kind: inode::file_type(entry.ino),
            });
        }

        tx.commit().await?;
        Ok(mapped_entries)
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn mkdir(
        &self,
        view: View,
        owner: Owner,
        mode: u32,
        parent_ino: Ino,
        name: &NameRef,
    ) -> Result<FileAttr> {
        let ts = time::now();

        let mut connection = self.pool.acquire().await?;
        let ino = self
            .ino_counter
            .next(inode::Directory, &mut connection)
            .await?;

        let mut tx = transaction!(self.config, connection, {
            exclusive: [
                inode::key(parent_ino),
                dentries::key(parent_ino)
            ]
        })
        .await?;

        let mut reply = tx
            .read(self.config.bucket(), reads!(dentries::read(parent_ino)))
            .await?;

        /* 1. Check if the entry doesn't already exists. */
        let entries = self
            .dirs
            .decode_view(view, &mut tx, parent_ino, &mut reply, 0)
            .await?;
        if entries.contains_key(&name) {
            return Err(EEXIST);
        }

        /* 2. We are good from our point of view, create the entry.
        It doesn't say that someone is also trying to create
        the same entry, but we have exclusivness with our view_id. */
        let name = name.clone().canonicalize(view);
        let links = [
            inode::Link::new(parent_ino, name.clone()),
            inode::Link::new(ino, Name::new(".", view)),
        ];
        let dotdot_link = inode::Link::new(ino, Name::new("..", view));
        let desc = inode::CreateDesc {
            ino,
            links: links.iter().cloned(),
            mode,
            size: self.config.driver.page_size_b,
            owner,
            dotdot: Some(parent_ino),
        };
        let dentry = dentries::Entry::new(name, ino);

        tx.update(
            self.config.bucket(),
            updates!(
                inode::create(ts, desc),
                dentries::add_entry(parent_ino, &dentry),
                inode::add_link(ts, parent_ino, dotdot_link),
                dentries::create(view, parent_ino, ino)
            ),
        )
        .await?;

        tx.commit().await?;

        Ok(FileAttr {
            atime: time::timespec(ts),
            ctime: time::timespec(ts),
            mtime: time::timespec(ts),
            crtime: time::timespec(ts),
            blocks: 1,
            rdev: 0,
            size: self.config.driver.page_size_b,
            ino: u64::from(ino),
            gid: owner.gid,
            uid: owner.uid,
            perm: mode as u16,
            kind: FileType::Directory,
            flags: 0,
            nlink: 2,
        })
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn rmdir(&self, view: View, parent_ino: Ino, name: &NameRef) -> Result<()> {
        let ts = time::now();

        let mut connection = self.pool.acquire().await?;
        let mut tx = transaction!(self.config, connection, {
            exclusive: [inode::key(parent_ino), dentries::key(parent_ino)]
        })
        .await?;

        let mut reply = tx
            .read(
                self.config.bucket(),
                reads!(inode::read(parent_ino), dentries::read(parent_ino)),
            )
            .await?;

        let parent_inode = inode::decode(parent_ino, &mut reply, 0).ok_or(ENOENT)?;
        let parent_entries = self
            .dirs
            .decode_view(view, &mut tx, parent_ino, &mut reply, 1)
            .await?;
        let entry = parent_entries.get(&name).ok_or(ENOENT)?;

        let mut reply = tx
            .read(self.config.bucket(), reads!(dentries::read(entry.ino)))
            .await?;

        let entries = self
            .dirs
            .decode_view(view, &mut tx, entry.ino, &mut reply, 0)
            .await?;

        if entries.len() > 0 {
            return Err(ENOTEMPTY);
        }

        let dentry_to_remove = entry.into_dentry();
        let dotdot = Name::new("..", dentry_to_remove.name.view);
        let old_dotdot_link = parent_inode.links.find(entry.ino, &dotdot).unwrap();

        tx.update(
            self.config.bucket(),
            updates!(
                dentries::remove_entry(parent_ino, &dentry_to_remove),
                inode::remove_link(ts, parent_ino, old_dotdot_link.clone()),
                inode::remove(entry.ino),
                dentries::remove(entry.ino)
            ),
        )
        .await?;

        tx.commit().await?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn mknod(
        &self,
        view: View,
        owner: Owner,
        mode: u32,
        parent_ino: Ino,
        name: &NameRef,
        _rdev: u32,
    ) -> Result<FileAttr> {
        let ts = time::now();

        let mut connection = self.pool.acquire().await?;
        let ino = self.ino_counter.next(inode::Regular, &mut connection).await?;

        let mut tx = transaction!(self.config, connection, {
            exclusive: [
                inode::key(parent_ino),
                dentries::key(parent_ino)
            ]
        })
        .await?;

        let mut reply = tx
            .read(self.config.bucket(), reads!(dentries::read(parent_ino)))
            .await?;

        let entries = self
            .dirs
            .decode_view(view, &mut tx, parent_ino, &mut reply, 0)
            .await?;
        if entries.contains_key(&name) {
            return Err(EEXIST);
        }

        let name = name.clone().canonicalize(view);
        let links = [inode::Link::new(parent_ino, name.clone())];
        let inode = inode::CreateDesc {
            ino,
            links: links.iter().cloned(),
            mode,
            owner,
            size: 0,
            dotdot: None,
        };
        tx.update(
            self.config.bucket(),
            vec![
                inode::create(ts, inode),
                inode::touch(ts, parent_ino),
                dentries::add_entry(parent_ino, &dentries::Entry::new(name, ino)),
            ],
        )
        .await?;

        tx.commit().await?;

        Ok(FileAttr {
            atime: time::timespec(ts),
            ctime: time::timespec(ts),
            mtime: time::timespec(ts),
            crtime: time::timespec(ts),
            blocks: 1,
            rdev: 0,
            size: 0,
            ino: u64::from(ino),
            gid: owner.gid,
            uid: owner.uid,
            perm: mode as u16,
            kind: FileType::RegularFile,
            flags: 0,
            nlink: 1,
        })
    }

    pub async fn unlink(&self, view: View, parent_ino: Ino, name: &NameRef) -> Result<()> {
        let ts = time::now();

        let mut connection = self.pool.acquire().await?;
        let mut tx = transaction!(self.config, connection, {
            exclusive: [
                inode::key(parent_ino),
                dentries::key(parent_ino)
            ]
        })
        .await?;

        /* 1. Get the entry to unlink. */
        let mut reply = tx
            .read(self.config.bucket(), reads!(dentries::read(parent_ino)))
            .await?;

        let entries = self
            .dirs
            .decode_view(view, &mut tx, parent_ino, &mut reply, 0)
            .await?;
        let entry = entries.get(&name).ok_or(ENOENT)?;
        let dentry_to_remove = entry.into_dentry();

        /* 2. Get the inode to remove the link with the view that it was
        created from. */
        let mut reply = tx
            .read(self.config.bucket(), reads!(inode::read(entry.ino)))
            .await?;

        let inode = inode::decode(entry.ino, &mut reply, 0).ok_or(ENOENT)?;
        let link = inode
            .links
            .find(parent_ino, &dentry_to_remove.name)
            .cloned()
            .ok_or(ENOENT)?;

        tx.update(
            self.config.bucket(),
            updates!(
                dentries::remove_entry(parent_ino, &dentry_to_remove),
                inode::remove_link(ts, entry.ino, link)
            ),
        )
        .await?;

        tx.commit().await?;

        /* Unfortunately, this is not atomic, the truncate is not part of the same transaction. On the other hand
        we have currently no means of preventing concurrent modification on the inode. So let's clear it on a best effort
        basis as a simpler implementation. */
        if inode.links.nlink() - 1 == 0 {
            let mut openfiles = self.openfiles.lock().await;
            let openfile = openfiles.open(entry.ino).await?;

            openfile.clear_on_exit().await;

            openfiles.close(openfile.fh()).await?;
        }

        Ok(())
    }

    async fn openfile<'c>(&self, fh: FileHandle) -> Result<OpenfileHandle> {
        let openfiles = self.openfiles.lock().await;
        openfiles.get(fh)
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn open(&self, ino: Ino) -> Result<FileHandle> {
        let mut openfiles = self.openfiles.lock().await;
        let handle = openfiles.open(ino).await?;

        Ok(handle.fh())
    }

    pub(crate) async fn flush(&self, fh: FileHandle) -> Result<()> {
        self.fsync(fh).await
    }

    pub(crate) async fn fsync(&self, fh: FileHandle) -> Result<()> {
        let openfile = self.openfile(fh).await?;
        openfile.sync().await
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn release(&self, view: View, fh: FileHandle) -> Result<()> {
        let mut openfiles = self.openfiles.lock().await;
        openfiles.close(fh).await
    }

    #[tracing::instrument(skip(self, buffer), fields(offset, len = buffer.len()))]
    pub(crate) async fn write(&self, fh: FileHandle, buffer: Bytes, offset: u64) -> Result<()> {
        let openfile = self.openfile(fh).await?;
        openfile.write(WriteSlice { buffer, offset }).await
    }

    pub(crate) async fn read(&self, fh: FileHandle, offset: u64, len: u32) -> Result<Bytes> {
        let openfile = self.openfile(fh).await?;
        openfile.read(offset, len as u64).await
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn rename(
        &self,
        view: View,
        parent_ino: Ino,
        name: &NameRef,
        new_parent_ino: Ino,
        new_name: &NameRef,
    ) -> Result<()> {
        let parents_to_lock = self
            .up_until_common_ancestor(parent_ino, new_parent_ino)
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

        /* 1. Fetch the source and destination dir entries, this is used
        to determine the kind of rename that we are dealing with. */
        let mut reply = tx
            .read(
                self.config.bucket(),
                reads!(dentries::read(parent_ino), dentries::read(new_parent_ino)),
            )
            .await?;

        let parent_entries = self
            .dirs
            .decode_view(view, &mut tx, parent_ino, &mut reply, 0)
            .await?;

        let new_parent_entries = self
            .dirs
            .decode_view(view, &mut tx, new_parent_ino, &mut reply, 1)
            .await?;

        let entry = parent_entries.get(&name).cloned().ok_or(ENOENT)?;

        let state = RenameState {
            entry: entry.clone(),
            parent_ino,
            new_parent_ino,
            new_name: new_name.clone(),
            new_parent_entries,
        };

        match inode::kind(entry.ino) {
            inode::Directory => {
                self.rename_dir(view, &mut tx, state).await?;
            }
            _ => {
                self.rename_file(view, &mut tx, state).await?;
            }
        }

        tx.commit().await?;
        Ok(())
    }

    async fn rename_file(
        &self,
        view: View,
        tx: &mut Transaction<'_>,
        state: RenameState,
    ) -> Result<()> {
        let target_entry = state.new_parent_entries.get(&state.new_name).cloned();

        match target_entry {
            Some(entry) => self.rename_file_to_occupied(view, tx, state, entry).await,
            None => self.rename_file_to_vacant(view, tx, state).await,
        }
    }

    async fn rename_file_to_vacant(
        &self,
        view: View,
        tx: &mut Transaction<'_>,
        state: RenameState,
    ) -> Result<()> {
        let ts = time::now();

        let ino = state.entry.ino;
        let mut reply = tx
            .read(self.config.bucket(), reads!(inode::read(ino)))
            .await?;

        let inode = inode::decode(ino, &mut reply, 0).ok_or(ENOENT)?;

        let new_name = state.new_name.canonicalize(view);
        let dentry_to_remove = state.entry.into_dentry();
        let dentry_to_add = dentries::Entry::new(new_name.clone(), state.entry.ino);

        let old_link = inode
            .links
            .find(state.parent_ino, &dentry_to_remove.name)
            .cloned()
            .unwrap();
        let new_link = inode::Link::new(state.new_parent_ino, dentry_to_add.name.clone());

        tx.update(
            self.config.bucket(),
            updates!(
                dentries::remove_entry(state.parent_ino, &dentry_to_remove),
                dentries::add_entry(state.new_parent_ino, &dentry_to_add),
                inode::remove_link(ts, ino, old_link),
                inode::add_link(ts, ino, new_link)
            ),
        )
        .await?;

        Ok(())
    }

    async fn rename_file_to_occupied(
        &self,
        view: View,
        tx: &mut Transaction<'_>,
        state: RenameState,
        target: dir::EntryView,
    ) -> Result<()> {
        let ts = time::now();

        /* Don't allow overwrite of directory. */
        if inode::kind(target.ino) == inode::Directory {
            return Err(EEXIST);
        }

        /* 1. Read the target inode to remove the link in the target directory. */
        let mut reply = tx
            .read(self.config.bucket(), reads!(inode::read(target.ino)))
            .await?;
        let target_inode = inode::decode(target.ino, &mut reply, 0).ok_or(ENOENT)?;

        let dentry_to_remove = target.into_dentry();
        let target_nlink = target_inode.links.nlink();
        if target_nlink > 1 {
            let link = target_inode
                .links
                .find(state.new_parent_ino, &dentry_to_remove.name)
                .cloned()
                .unwrap();

            tx.update(
                self.config.bucket(),
                updates!(
                    inode::remove_link(ts, target.ino, link),
                    dentries::remove_entry(state.new_parent_ino, &dentry_to_remove)
                ),
            )
            .await?;
        } else {
            tx.update(
                self.config.bucket(),
                updates!(
                    inode::remove(target.ino),
                    symlink::remove(target.ino),
                    dentries::remove_entry(state.new_parent_ino, &dentry_to_remove)
                ),
            )
            .await?;
        }

        self.rename_file_to_vacant(view, tx, state).await
    }

    async fn rename_dir(
        &self,
        view: View,
        tx: &mut Transaction<'_>,
        state: RenameState,
    ) -> Result<()> {
        let target_entry = state.new_parent_entries.get(&state.new_name).cloned();
        match target_entry {
            Some(entry) => self.rename_dir_to_occupied(view, tx, state, entry).await,
            None => self.rename_dir_to_vacant(view, tx, state).await,
        }
    }

    async fn rename_dir_to_occupied(
        &self,
        view: View,
        tx: &mut Transaction<'_>,
        state: RenameState,
        target: dir::EntryView,
    ) -> Result<()> {
        let ts = time::now();

        /* Only rename to empty directories are accepted. */
        if inode::kind(target.ino) != inode::Directory {
            return Err(EEXIST);
        }

        /* 1. Fetch target dentries to check if the destination is empty. */
        let mut reply = tx
            .read(
                self.config.bucket(),
                reads!(
                    inode::read(state.new_parent_ino),
                    dentries::read(target.ino)
                ),
            )
            .await?;

        let new_parent_inode = inode::decode(state.new_parent_ino, &mut reply, 0).ok_or(ENOENT)?;

        /* dotdot link must have the same view as the found dentry */
        let target_dentry = target.into_dentry();
        let dotdot = Name::new("..", target_dentry.name.view);
        let old_dotdot_link = new_parent_inode
            .links
            .find(target.ino, &dotdot)
            .unwrap()
            .clone();

        let target_entries = self
            .dirs
            .decode_view(view, tx, target.ino, &mut reply, 1)
            .await?;

        if target_entries.len() > 0 {
            return Err(ENOTEMPTY);
        }

        /* 2. The target directory is empty, remove it before putting,
        the new directory. */

        tx.update(
            self.config.bucket(),
            updates!(
                inode::remove(target.ino),
                dentries::remove(target.ino),
                inode::remove_link(ts, state.new_parent_ino, old_dotdot_link)
            ),
        )
        .await?;

        /* 3. Perform the rename as if the direcotry didn't exists. */
        self.rename_dir_to_vacant(view, tx, state).await
    }

    async fn rename_dir_to_vacant(
        &self,
        view: View,
        tx: &mut Transaction<'_>,
        state: RenameState,
    ) -> Result<()> {
        let ts = time::now();

        let ino = state.entry.ino;
        let mut reply = tx
            .read(
                self.config.bucket(),
                reads!(inode::read(state.entry.ino), inode::read(state.parent_ino)),
            )
            .await?;

        /*
        Moving a directory implies a few things:
            - We need to remove the .. link of the old parent.
            - We need to add the .. of the new parent.
            - We need to remove the dentry of from old parent.
            - We need to add the dentry of in the new parent.
            - We need to remove the old .. dentry of the old parent.
            - We need to add a new .. dentry of the new parent. */
        let inode = inode::decode(ino, &mut reply, 0).ok_or(ENOENT)?;
        let parent_inode = inode::decode(state.parent_ino, &mut reply, 1).ok_or(ENOENT)?;

        let old_dentry = state.entry.into_dentry();
        let old_dotdot = Name::new("..", old_dentry.name.view);
        let old_link = inode
            .links
            .find(state.parent_ino, &old_dentry.name)
            .unwrap();
        let old_dotdot_link = parent_inode.links.find(ino, &old_dotdot).unwrap();

        let new_name = state.new_name.canonicalize(view);
        let new_dotdot = Name::new("..", new_name.view);
        let new_dentry = dentries::Entry::new(new_name.clone(), state.entry.ino);
        let new_dotdot_link = inode::Link::new(ino, new_dotdot);
        let new_link = inode::Link::new(state.new_parent_ino, new_name);

        /* Antidote doesn't always like multiple updates of the same object
        inside an operation, let's split them in multiple calls: first
        we delete the old entries, then we create the new ones. We are
        still inside a unique transaction, we shoud be safe. */
        tx.update(
            self.config.bucket(),
            updates!(
                dentries::remove_entry(state.parent_ino, &old_dentry),
                inode::unlink_from_parent(ts, ino, old_link.clone()),
                inode::remove_link(ts, state.parent_ino, old_dotdot_link.clone())
            ),
        )
        .await?;

        tx.update(
            self.config.bucket(),
            updates!(
                dentries::add_entry(state.new_parent_ino, &new_dentry),
                inode::link_to_parent(ts, ino, state.new_parent_ino, new_link),
                inode::add_link(ts, state.new_parent_ino, new_dotdot_link)
            ),
        )
        .await?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn link(
        &self,
        view: View,
        ino: Ino,
        new_parent_ino: Ino,
        new_name: &NameRef,
    ) -> Result<FileAttr> {
        let ts = time::now();

        let mut connection = self.pool.acquire().await?;
        let mut tx = transaction!(self.config, connection, {
            exclusive: [
                inode::key(ino),
                inode::key(new_parent_ino),
                dentries::key(new_parent_ino)
            ]
        })
        .await?;

        /* 1. Check if an entry exist with the same name. */
        let mut reply = tx
            .read(
                self.config.bucket(),
                reads!(inode::read(ino), dentries::read(new_parent_ino)),
            )
            .await?;

        let inode = inode::decode(ino, &mut reply, 0).ok_or(ENOENT)?;
        let entries = self
            .dirs
            .decode_view(view, &mut tx, new_parent_ino, &mut reply, 1)
            .await?;
        if entries.get(&new_name).is_some() {
            return Err(EEXIST);
        }

        /* 2. We are good, create the entry */
        let new_name = new_name.clone().canonicalize(view);
        let new_dentry = dentries::Entry::new(new_name.clone(), ino);
        let new_link = inode::Link::new(new_parent_ino, new_name);

        tx.update(
            self.config.bucket(),
            updates!(
                inode::touch(ts, new_parent_ino),
                dentries::add_entry(new_parent_ino, &new_dentry),
                inode::add_link(ts, ino, new_link)
            ),
        )
        .await?;

        tx.commit().await?;

        let old_attr = inode.attr(ino);
        Ok(FileAttr {
            nlink: old_attr.nlink + 1, /* The link that we just created */
            atime: time::timespec(ts),
            ..old_attr
        })
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn read_link(&self, view: View, ino: Ino) -> Result<String> {
        let mut connection = self.pool.acquire().await?;
        let mut tx = transaction!(self.config, connection, { shared: [symlink::key(ino)] }).await?;

        let mut reply = tx
            .read(self.config.bucket(), reads!(symlink::read(ino)))
            .await?;

        let link = symlink::decode(&mut reply, 0).ok_or(ENOENT)?;

        tx.commit().await?;
        Ok(link)
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn symlink(
        &self,
        view: View,
        parent_ino: Ino,
        owner: Owner,
        name: &NameRef,
        link: &str,
    ) -> Result<FileAttr> {
        let ts = time::now();

        let mut connection = self.pool.acquire().await?;
        let ino = self.ino_counter.next(inode::Symlink, &mut connection).await?;

        let mut tx = transaction!(self.config, connection, {
            exclusive: [
                inode::key(parent_ino),
                dentries::key(parent_ino)
            ]
        })
        .await?;

        let symlink_size = link.len() as u64;

        /* 1. Check if the entry exists. */
        let mut reply = tx
            .read(self.config.bucket(), reads!(dentries::read(parent_ino)))
            .await?;

        let entries = self
            .dirs
            .decode_view(view, &mut tx, parent_ino, &mut reply, 0)
            .await?;

        if entries.contains_key(&name) {
            return Err(EEXIST);
        }

        /* 2. We are good, create the entry. */
        let name = name.clone().canonicalize(view);
        let links = [inode::Link::new(parent_ino, name.clone())];
        let dentry = dentries::Entry::new(name, ino);
        let inode = inode::CreateDesc {
            ino,
            links: links.iter().cloned(),
            owner,
            mode: 0o644,
            size: symlink_size,
            dotdot: None,
        };
        tx.update(
            self.config.bucket(),
            updates![
                inode::create(ts, inode),
                inode::touch(ts, parent_ino),
                dentries::add_entry(parent_ino, &dentry),
                symlink::create(ino, link.into())
            ],
        )
        .await?;

        tx.commit().await?;

        Ok(FileAttr {
            atime: time::timespec(ts),
            ctime: time::timespec(ts),
            mtime: time::timespec(ts),
            crtime: time::timespec(ts),
            blocks: ((symlink_size - 1) / 4096 + 1) as u64,
            rdev: 0,
            size: symlink_size,
            ino: u64::from(ino),
            gid: owner.gid,
            uid: owner.uid,
            perm: 0o644,
            kind: FileType::Symlink,
            flags: 0,
            nlink: 1,
        })
    }

    pub async fn up_until_common_ancestor(
        &self,
        mut lhs_parent: Ino,
        mut rhs_parent: Ino,
    ) -> Result<Vec<Ino>> {
        let mut connection = self.pool.acquire().await?;
        let mut tx = connection.transaction().await?;

        let mut parents = Vec::with_capacity(1);
        while lhs_parent != rhs_parent {
            parents.push(lhs_parent);
            parents.push(rhs_parent);

            let mut reply = tx
                .read(
                    self.config.bucket(),
                    reads!(inode::read(lhs_parent), inode::read(rhs_parent)),
                )
                .await?;

            let lhs_inode = inode::decode(lhs_parent, &mut reply, 0).ok_or(ENOENT)?;
            let rhs_inode = inode::decode(rhs_parent, &mut reply, 1).ok_or(ENOENT)?;

            lhs_parent = lhs_inode.dotdot.ok_or(ENOENT)?;
            rhs_parent = rhs_inode.dotdot.ok_or(ENOENT)?;
        }
        assert_eq!(lhs_parent, rhs_parent);
        parents.push(lhs_parent);

        tx.commit().await?;
        Ok(parents)
    }
}

#[derive(Debug)]
pub(crate) struct ReadDirEntry {
    pub(crate) ino: Ino,
    pub(crate) kind: FileType,
    pub(crate) name: String,
}

struct RenameState {
    parent_ino: Ino,
    new_parent_ino: Ino,
    new_name: NameRef,
    entry: dir::EntryView,
    new_parent_entries: dir::DirView,
}
