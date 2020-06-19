use crate::inode::{self, Inode, Owner};
use crate::key::{Bucket, InoCounter};
use crate::page::PageDriver;
use antidotec::{self, counter, crdts, Connection, TransactionLocks};
use fuse::*;
use nix::errno::Errno;
use std::fmt::Debug;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tracing::debug;

const ROOT_INO: u64 = 1;
const INO_COUNTER: InoCounter = InoCounter::new(0);

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
    pub address: String,
}

#[derive(Debug)]
pub(crate) struct Driver {
    pub(crate) cfg: Config,
    pub(crate) pages: PageDriver,
}

impl Driver {
    #[tracing::instrument(skip(self))]
    pub(crate) async fn configure(&self) -> Result<()> {
        let mut connection = self.connect().await?;

        self.ensure_ino_counter(&mut connection).await?;
        self.ensure_root_dir(&mut connection).await?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn ensure_ino_counter(&self, connection: &mut Connection) -> Result<()> {
        let mut locks = TransactionLocks::with_capacity(1, 0);
        locks.push_exclusive(INO_COUNTER);

        let mut tx = connection.transaction_with_locks(locks).await?;
        {
            let mut reply = tx
                .read(self.cfg.bucket, vec![counter::get(INO_COUNTER)])
                .await?;
            if reply.counter(0) != 0 {
                return Ok(());
            }

            let inc = i32::min_value();
            tx.update(self.cfg.bucket, vec![counter::inc(INO_COUNTER, inc)])
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
        };

        let mut locks = TransactionLocks::with_capacity(1, 0);
        locks.push_exclusive(inode::Key::new(ROOT_INO));

        let mut tx = connection.transaction_with_locks(locks).await?;
        {
            tx.update(self.cfg.bucket, vec![inode::update(&root_inode)])
                .await?;
        }
        tx.commit().await?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn getattr(&self, ino: u64) -> Result<FileAttr> {
        let mut connection = self.connect().await?;

        let mut tx = connection.transaction().await?;
        let inode = {
            let mut reply = tx.read(self.cfg.bucket, vec![inode::read(ino)]).await?;

            match reply.rrmap(0) {
                Some(map) => inode::decode(ino, map),
                None => return Err(Error::Sys(Errno::ENOENT)),
            }
        };
        tx.commit().await?;

        Ok(inode.attr())
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

        let mut connection = self.connect().await?;

        let mut tx = connection.transaction().await?;
        let inode = {
            let mut reply = tx.read(self.cfg.bucket, vec![inode::read(ino)]).await?;

            let mut inode = match reply.rrmap(0) {
                Some(map) => inode::decode(ino, map),
                None => return Err(Error::Sys(Errno::ENOENT)),
            };

            update!(inode.mode, mode);
            update!(inode.owner.uid, uid);
            update!(inode.owner.gid, gid);
            update!(inode.size, size);
            update!(inode.atime, atime);
            update!(inode.mtime, mtime);

            tx.update(self.cfg.bucket, vec![inode::update(&inode)])
                .await?;

            inode
        };
        tx.commit().await?;

        Ok(inode.attr())
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn lookup(&self, parent_ino: u64, name: &str) -> Result<FileAttr> {
        let mut connection = self.connect().await?;

        let mut locks = TransactionLocks::with_capacity(0, 1);
        locks.push_shared(inode::Key::dir_entries(parent_ino));

        let mut tx = connection.transaction_with_locks(locks).await?;
        let entries = {
            let mut reply = tx
                .read(self.cfg.bucket, vec![inode::read_dir(parent_ino)])
                .await?;
            tx.commit().await?;

            match reply.rrmap(0) {
                Some(map) => inode::decode_dir(map),
                None => {
                    return Err(Error::Sys(Errno::ENOENT));
                }
            }
        };

        match entries.get(name) {
            Some(ino) => self.getattr(*ino).await,
            None => Err(Error::Sys(Errno::ENOENT)),
        }
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
        let mut connection = self.connect().await?;

        let mut locks = TransactionLocks::with_capacity(0, 1);
        locks.push_shared(inode::Key::dir_entries(ino));

        let mut tx = connection.transaction_with_locks(locks).await?;
        let entries = {
            let entries = {
                let mut reply = tx.read(self.cfg.bucket, vec![inode::read_dir(ino)]).await?;

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

            let mut reply = tx.read(self.cfg.bucket, attr_reads).await?;

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
        let mut connection = self.connect().await?;
        let ino = self.generate_ino(&mut connection).await?;

        let mut locks = TransactionLocks::with_capacity(2, 0);
        locks.push_exclusive(inode::Key::new(parent_ino));
        locks.push_exclusive(inode::Key::dir_entries(parent_ino));

        let mut tx = connection.transaction_with_locks(locks).await?;
        let attr = {
            let mut reply = tx
                .read(
                    self.cfg.bucket,
                    vec![inode::read(parent_ino), inode::read_dir(parent_ino)],
                )
                .await?;

            let mut parent_inode = match reply.rrmap(0) {
                Some(inode) => inode::decode(parent_ino, inode),
                None => {
                    return Err(Error::Sys(Errno::ENOENT));
                }
            };

            let mut entries = inode::decode_dir(reply.rrmap(1).unwrap_or(crdts::RrMap::new()));
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
            };
            parent_inode.size = entries.len() as u64;
            parent_inode.mtime = t;
            parent_inode.atime = t;

            let attr = inode.attr();

            tx.update(
                self.cfg.bucket,
                vec![
                    inode::update(&parent_inode),
                    inode::update_dir(parent_ino, &entries),
                    inode::update(&inode),
                ],
            )
            .await?;

            attr
        };
        tx.commit().await?;

        Ok(attr)
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn rmdir(&self, parent_ino: u64, name: String) -> Result<()> {
        let mut connection = self.connect().await?;

        let mut locks = TransactionLocks::with_capacity(2, 0);
        locks.push_exclusive(inode::Key::new(parent_ino));
        locks.push_exclusive(inode::Key::dir_entries(parent_ino));

        let mut tx = connection.transaction_with_locks(locks).await?;
        {
            let mut reply = tx
                .read(
                    self.cfg.bucket,
                    vec![inode::read(parent_ino), inode::read_dir(parent_ino)],
                )
                .await?;

            let mut parent_inode = match reply.rrmap(0) {
                Some(inode) => inode::decode(parent_ino, inode),
                None => {
                    return Err(Error::Sys(Errno::ENOENT));
                }
            };

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
                self.cfg.bucket,
                vec![
                    inode::remove(ino),
                    inode::remove_dir(ino),
                    inode::remove_dir_entry(parent_ino, name),
                ],
            )
            .await?;
        }
        tx.commit().await?;

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
        let mut connection = self.connect().await?;
        let ino = self.generate_ino(&mut connection).await?;

        let mut locks = TransactionLocks::with_capacity(2, 0);
        locks.push_exclusive(inode::Key::new(parent_ino));
        locks.push_exclusive(inode::Key::dir_entries(parent_ino));

        let mut tx = connection.transaction_with_locks(locks).await?;
        let attr = {
            let mut reply = tx
                .read(
                    self.cfg.bucket,
                    vec![inode::read(parent_ino), inode::read_dir(parent_ino)],
                )
                .await?;

            let mut parent_inode = match reply.rrmap(0) {
                Some(inode) => inode::decode(parent_ino, inode),
                None => {
                    return Err(Error::Sys(Errno::ENOENT));
                }
            };

            let mut entries = inode::decode_dir(reply.rrmap(1).unwrap_or(crdts::GMap::new()));
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
            };
            parent_inode.size = entries.len() as u64;
            parent_inode.mtime = t;
            parent_inode.atime = t;

            let attr = inode.attr();

            tx.update(
                self.cfg.bucket,
                vec![
                    inode::update(&parent_inode),
                    inode::update_dir(parent_ino, &entries),
                    inode::update(&inode),
                ],
            )
            .await?;

            attr
        };
        tx.commit().await?;

        Ok(attr)
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn unlink(&self, parent_ino: u64, name: String) -> Result<()> {
        // FIXME: We do not track the link count of file. meaning that an
        // unlink op is always deleting the inner file.
        let mut connection = self.connect().await?;

        let mut locks = TransactionLocks::with_capacity(2, 0);
        locks.push_exclusive(inode::Key::new(parent_ino));
        locks.push_exclusive(inode::Key::dir_entries(parent_ino));

        let mut tx = connection.transaction_with_locks(locks).await?;
        {
            let mut reply = tx
                .read(
                    self.cfg.bucket,
                    vec![inode::read(parent_ino), inode::read_dir(parent_ino)],
                )
                .await?;

            let mut parent_inode = match reply.rrmap(0) {
                Some(inode) => inode::decode(parent_ino, inode),
                None => {
                    return Err(Error::Sys(Errno::ENOENT));
                }
            };

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
                self.cfg.bucket,
                vec![
                    inode::remove(ino),
                    inode::remove_dir_entry(parent_ino, name),
                ],
            )
            .await?;
        }
        tx.commit().await?;
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
        let mut connection = self.connect().await?;
        let mut tx = connection.transaction().await?;

        self.pages
            .write(&mut tx, ino, offset as usize, bytes)
            .await?;

        let mut reply = tx.read(self.cfg.bucket, vec![inode::read(ino)]).await?;

        let mut inode = match reply.rrmap(0) {
            Some(map) => inode::decode(ino, map),
            None => return Err(Error::Sys(Errno::ENOENT)),
        };

        let wrote = (offset + bytes.len() as u64) - inode.size;
        let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        inode.atime = t;
        inode.mtime = t;
        inode.size += wrote as u64;

        tx.update(self.cfg.bucket, vec![inode::update(&inode)])
            .await?;

        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn read(&self, ino: u64, offset: u64, len: u32) -> Result<Vec<u8>> {
        // Manual trace to avoid priting content result.
        tracing::trace!(ino, offset, len);

        let len = len as usize;
        let mut connection = self.connect().await?;
        let mut tx = connection.transaction().await?;

        let mut reply = tx.read(self.cfg.bucket, vec![inode::read(ino)]).await?;
        let mut inode = match reply.rrmap(0) {
            Some(map) => inode::decode(ino, map),
            None => return Err(Error::Sys(Errno::ENOENT)),
        };

        let mut bytes = Vec::with_capacity(len as usize);
        self.pages
            .read(&mut tx, ino, offset as usize, len as usize, &mut bytes)
            .await?;

        let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        inode.atime = t;

        tx.update(self.cfg.bucket, vec![inode::update(&inode)])
            .await?;

        tx.commit().await?;

        debug!(len, read_len = bytes.len(), requested_len = len);
        Ok(bytes)
    }

    #[tracing::instrument(skip(self, connexion))]
    pub(crate) async fn generate_ino(&self, connexion: &mut Connection) -> Result<u64> {
        let mut locks = TransactionLocks::with_capacity(1, 0);
        locks.push_exclusive(INO_COUNTER);

        let mut tx = connexion.transaction_with_locks(locks).await?;
        let ino = {
            let mut reply = tx
                .read(self.cfg.bucket, vec![counter::get(INO_COUNTER)])
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

            tx.update(self.cfg.bucket, vec![counter::inc(INO_COUNTER, inc)])
                .await?;

            ino as u64
        };
        tx.commit().await?;

        Ok(ino)
    }

    async fn connect(&self) -> Result<Connection> {
        Ok(antidotec::Connection::new(&self.cfg.address).await?)
    }
}

#[derive(Debug)]
pub(crate) struct ReadDirEntry {
    pub(crate) ino: u64,
    pub(crate) kind: FileType,
    pub(crate) name: String,
}
