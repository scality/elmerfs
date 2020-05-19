mod inode;
mod key;
mod op;

use crate::inode::{Inode, Owner};
use crate::key::{Bucket, InoCounter};
use crate::op::{GetAttr, Lookup, MkDir, Op, OpenDir, ReadDir, ReleaseDir};
use antidotec::{counter, crdts, Connection, TransactionLocks};
use async_std::sync::Arc;
use async_std::task;
use clap::{App, Arg};
use fuse::{Filesystem, *};
use log::*;
use nix::{errno::Errno, libc};
use std::ffi::{OsStr, OsString};
use std::fmt::Debug;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

const ROOT_INO: u64 = 1;
const OP_BUFFERING_SIZE: usize = 1024;
const MAIN_BUCKET: Bucket = Bucket::new(0);
const INO_COUNTER: InoCounter = InoCounter::new(0);

/// There is two main thread of execution to follow:
///
/// The first one is dedicated to fuse whom sole purpose is to perform
/// argument format validation (e.g are name given valid utf8 strings ?) and
/// send those requests to whoever might be interested.
///
/// The second one, the dispatcher thread, it takes fuse request and dispatch
/// them into asynchronous tasks calling into the root of the filesystem,
/// the Rp driver.
fn main() {
    env_logger::init();

    let args = App::new("rpfs")
        .arg(
            Arg::with_name("mountpoint")
                .long("mount")
                .short("m")
                .value_name("MOUNTPOINT")
                .takes_value(true)
                .required(true),
        )
        .get_matches();

    let mountpoint = args.value_of_os("mountpoint").unwrap().into();
    info!("starting with mount path: {:?}", mountpoint);

    let (op_sender, op_receiver) = op::sync_channel(OP_BUFFERING_SIZE);
    thread::Builder::new()
        .name("fuse".into())
        .spawn(move || fuse(mountpoint, op_sender))
        .unwrap();

    let driver = Arc::new(RpDriver {
        cfg: Config {
            bucket: MAIN_BUCKET,
            address: String::from("127.0.0.1:8101"),
        },
    });

    let ttl = || time::Timespec::new(600, 0);

    task::block_on(driver.configure()).unwrap();
    while let Ok(op) = op_receiver.recv() {
        debug!("op task: {:#?}", op);
        let driver = driver.clone();
        let name = op.name();

        // FIXME: Reply are done in the asynchronous tasks but may be blocking
        // for a significant amount of time. We should ensure that the scheduler
        // is handling this gracefully or that we explicity
        // call them inside a `spawn_blocking` block.
        match op {
            Op::GetAttr(getattr) => {
                task::spawn(async move {
                    match handle_result(name, driver.getattr(getattr.ino).await) {
                        Ok(attrs) => {
                            getattr.reply.attr(&ttl(), &attrs);
                        }
                        Err(errno) => {
                            getattr.reply.error(errno as libc::c_int);
                        }
                    }
                });
            }
            Op::Lookup(lookup) => {
                task::spawn(async move {
                    match handle_result(name, driver.lookup(lookup.parent_ino, &lookup.name).await)
                    {
                        Ok(attrs) => {
                            let generation = 0;
                            lookup.reply.entry(&ttl(), &attrs, generation);
                        }
                        Err(errno) => {
                            lookup.reply.error(errno as libc::c_int);
                        }
                    }
                });
            }
            Op::OpenDir(opendir) => {
                task::spawn(async move {
                    match handle_result(name, driver.opendir(opendir.ino).await) {
                        Ok(_) => {
                            let flags = 0;
                            opendir.reply.opened(opendir.ino, flags);
                        }
                        Err(errno) => {
                            opendir.reply.error(errno as libc::c_int);
                        }
                    }
                });
            }
            Op::ReleaseDir(releasedir) => {
                task::spawn(async move {
                    match handle_result(name, driver.releasedir(releasedir.ino).await) {
                        Ok(_) => releasedir.reply.ok(),
                        Err(errno) => {
                            releasedir.reply.error(errno as libc::c_int);
                        }
                    }
                });
            }
            Op::ReadDir(mut readdir) => {
                task::spawn(async move {
                    match handle_result(name, driver.readdir(readdir.ino, readdir.offset).await) {
                        Ok(entries) => {
                            for (i, entry) in entries.into_iter().enumerate() {
                                let offset = readdir.offset + i as i64 + 1;

                                let full = readdir.reply.add(entry.ino, offset, entry.kind, entry.name);
                                if full {
                                    break;
                                }
                            }

                            readdir.reply.ok();
                        }
                        Err(errno) => {
                            readdir.reply.error(errno as libc::c_int);
                        }
                    }
                });
            }
            Op::MkDir(mkdir) => {
                task::spawn(async move {
                    let owner = Owner {
                        gid: mkdir.gid,
                        uid: mkdir.uid,
                    };

                    let result = driver
                        .mkdir(owner, mkdir.mode, mkdir.parent_ino, mkdir.name)
                        .await;
                    match handle_result(name, result) {
                        Ok(attr) => {
                            let generation = 0;
                            mkdir.reply.entry(&ttl(), &attr, generation);
                        }
                        Err(errno) => {
                            mkdir.reply.error(errno as libc::c_int);
                        }
                    }
                });
            }
        }
    }
}

fn fuse(mountpoint: OsString, op_sender: op::Sender) {
    let options = ["-o", "fsname=rpfs"]
        .iter()
        .map(|o| o.as_ref())
        .collect::<Vec<&OsStr>>();

    let fs = Rpfs { op_sender };
    fuse::mount(fs, &mountpoint, &options).unwrap()
}
#[derive(Error, Debug)]
enum RpfsError {
    #[error("driver replied with: {0}")]
    Sys(Errno),
    #[error("io error with antidote: {0}")]
    Antidote(#[from] antidotec::Error),
    #[error("could not allocate a new inode number")]
    InoAllocFailed,
}

struct Rpfs {
    op_sender: op::Sender,
}

impl Filesystem for Rpfs {
    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        let _ = self.op_sender.send(Op::GetAttr(GetAttr { reply, ino }));
    }

    fn opendir(&mut self, _req: &Request, ino: u64, _flags: u32, reply: ReplyOpen) {
        let _ = self.op_sender.send(Op::OpenDir(OpenDir { reply, ino }));
    }

    fn releasedir(&mut self, _req: &Request, ino: u64, fh: u64, _flags: u32, reply: ReplyEmpty) {
        let _ = self
            .op_sender
            .send(Op::ReleaseDir(ReleaseDir { reply, fh, ino }));
    }

    fn readdir(&mut self, _req: &Request, ino: u64, fh: u64, offset: i64, reply: ReplyDirectory) {
        let _ = self.op_sender.send(Op::ReadDir(ReadDir {
            reply,
            fh,
            ino,
            offset,
        }));
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name = match name.to_str() {
            Some(name) => String::from(name),
            None => {
                reply.error(Errno::EINVAL as libc::c_int);
                return;
            }
        };

        let _ = self.op_sender.send(Op::Lookup(Lookup {
            reply,
            name,
            parent_ino: parent,
        }));
    }

    fn mkdir(
        &mut self,
        req: &Request,
        parent_ino: u64,
        name: &OsStr,
        mode: u32,
        reply: ReplyEntry,
    ) {
        let name = match name.to_str() {
            Some(name) => String::from(name),
            None => {
                reply.error(Errno::EINVAL as libc::c_int);
                return;
            }
        };

        let _ = self.op_sender.send(Op::MkDir(MkDir {
            reply,
            parent_ino,
            name,
            mode,
            uid: req.uid(),
            gid: req.gid(),
        }));
    }
}

struct Config {
    bucket: Bucket,
    address: String,
}

struct RpDriver {
    cfg: Config,
}

impl RpDriver {
    async fn configure(&self) -> Result<(), RpfsError> {
        let mut connection = self.connect().await?;

        self.ensure_ino_counter(&mut connection).await?;
        self.ensure_root_dir(&mut connection).await?;

        Ok(())
    }

    async fn ensure_ino_counter(&self, connection: &mut Connection) -> Result<(), RpfsError> {
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

    async fn ensure_root_dir(&self, connection: &mut Connection) -> Result<(), RpfsError> {
        match self.getattr(ROOT_INO).await {
            Ok(_) => return Ok(()),
            Err(RpfsError::Sys(Errno::ENOENT)) => {}
            Err(error) => return Err(error),
        };

        let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

        let root_inode = Inode {
            ino: 1,
            kind: inode::Kind::Directory,
            parent: 1,
            atime: t,
            ctime: t,
            mtime: t,
            owner: Owner { uid: 0, gid: 0 },
            mode: 0777,
            size: 0,
        };

        let mut tx = connection.transaction().await?;
        {
            tx.update(self.cfg.bucket, vec![inode::update(&root_inode)])
                .await?;
        }
        tx.commit().await?;

        Ok(())
    }

    async fn getattr(&self, ino: u64) -> Result<FileAttr, RpfsError> {
        let mut connection = self.connect().await?;

        let mut tx = connection.transaction().await?;
        let inode = {
            let mut reply = tx.read(self.cfg.bucket, vec![inode::read(ino)]).await?;

            match reply.gmap(0) {
                Some(gmap) => inode::decode(ino, gmap),
                None => return Err(RpfsError::Sys(Errno::ENOENT)),
            }
        };
        tx.commit().await?;

        Ok(inode.attr())
    }

    async fn lookup(&self, parent_ino: u64, name: &str) -> Result<FileAttr, RpfsError> {
        let mut connection = self.connect().await?;

        let mut tx = connection.transaction().await?;
        let entries = {
            let mut reply = tx
                .read(self.cfg.bucket, vec![inode::read_dir(parent_ino)])
                .await?;
            tx.commit().await?;

            match reply.gmap(0) {
                Some(gmap) => inode::decode_dir(gmap),
                None => {
                    return Err(RpfsError::Sys(Errno::ENOENT));
                }
            }
        };

        match entries.get(name) {
            Some(ino) => self.getattr(*ino).await,
            None => Err(RpfsError::Sys(Errno::ENOENT)),
        }
    }

    async fn opendir(&self, ino: u64) -> Result<(), RpfsError> {
        // FIXME: For now we are stateless, meaning that we do not track open
        // close calls. For now just perform a simple getattr as a dummy check.
        self.getattr(ino).await.map(|_| ())
    }

    async fn releasedir(&self, ino: u64) -> Result<(), RpfsError> {
        self.getattr(ino).await.map(|_| ())
    }

    async fn readdir(&self, ino: u64, offset: i64) -> Result<Vec<ReadDirEntry>, RpfsError> {
        let mut connection = self.connect().await?;
        let mut tx = connection.transaction().await?;
        let entries = {
            let entries = {
                let mut reply = tx.read(self.cfg.bucket, vec![inode::read_dir(ino)]).await?;

                match reply.gmap(0) {
                    Some(gmap) => inode::decode_dir(gmap),
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
                let inode = reply.gmap(index).unwrap();
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

    async fn mkdir(
        &self,
        owner: Owner,
        mode: u32,
        parent_ino: u64,
        name: String,
    ) -> Result<FileAttr, RpfsError> {
        let mut connection = self.connect().await?;
        let ino = self.generate_ino(&mut connection).await?;

        let mut tx = connection.transaction().await?;
        let attr = {
            let mut reply = tx
                .read(
                    self.cfg.bucket,
                    vec![inode::read(parent_ino), inode::read_dir(parent_ino)],
                )
                .await?;

            let mut parent_inode = match reply.gmap(0) {
                Some(inode) => inode::decode(parent_ino, inode),
                None => {
                    tx.abort().await?;
                    return Err(RpfsError::Sys(Errno::ENOENT));
                }
            };

            let mut entries = inode::decode_dir(reply.gmap(1).unwrap_or(crdts::GMap::new()));
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

    async fn generate_ino(&self, connexion: &mut Connection) -> Result<u64, RpfsError> {
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
                    tx.abort().await?;
                    return Err(RpfsError::InoAllocFailed);
                }
            };

            tx.update(self.cfg.bucket, vec![counter::inc(INO_COUNTER, inc)])
                .await?;

            ino as u64
        };
        tx.commit().await?;

        Ok(ino)
    }

    async fn connect(&self) -> Result<Connection, antidotec::Error> {
        antidotec::Connection::new(&self.cfg.address).await
    }
}

pub type ReadDirEntries = Vec<ReadDirEntry>;

#[derive(Debug)]
pub struct ReadDirEntry {
    pub ino: u64,
    pub kind: FileType,
    pub name: String,
}

fn handle_result<U: Debug + Send>(name: &str, result: Result<U, RpfsError>) -> Result<U, Errno> {
    match result {
        Ok(result) => {
            debug!("({}): success - {:#?}", name, result);
            Ok(result)
        }
        Err(RpfsError::Antidote(error)) => {
            debug!("({}): unexpected antidote error - {}", name, error);
            Err(Errno::EIO)
        }
        Err(RpfsError::Sys(errno)) => {
            debug!("({}): system error - {}", name, errno);
            Err(errno)
        }
        Err(RpfsError::InoAllocFailed) => {
            debug!("({}): ino alloc failed - ()", name);
            Err(Errno::ENOSPC)
        }
    }
}
