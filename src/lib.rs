mod dispatch;
mod driver;
mod ino;
mod inode;
mod key;
mod op;
mod page;
mod pool;
mod view;
mod dir;

use crate::driver::Driver;
use crate::op::*;
use crate::page::PageDriver;
use async_std::{sync::Arc, task};
use fuse::{Filesystem, *};
use nix::{errno::Errno, libc};
use std::ffi::{OsStr, OsString};
use std::io;
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use time::Timespec;
use tracing::*;

pub use crate::driver::Config;
pub use crate::key::Bucket;
pub use crate::pool::AddressBook;

const PAGE_SIZE: usize = 4 * 1024;
const OP_BUFFERING_SIZE: usize = 1024;

/// There is two main thread of execution to follow:
///
/// The first one is dedicated to fuse whom sole purpose is to perform
/// argument format validation (e.g are name given valid utf8 strings ?) and
/// send those requests to whoever might be interested.
///
/// The second one, the dispatcher thread, it takes fuse request and dispatch
/// them into asynchronous tasks calling into the root of the filesystem,
/// the Rp driver.
pub fn run(cfg: Config, mountpoint: &OsStr) {
    let mountpoint = OsString::from(mountpoint);
    let view = cfg.view;
    let (op_sender, op_receiver) = op::sync_channel(OP_BUFFERING_SIZE);
    thread::Builder::new()
        .name("fuse".into())
        .spawn(move || fuse(mountpoint, view, op_sender))
        .unwrap();

    let bucket = cfg.bucket;

    let driver =
        task::block_on(Driver::new(cfg, PageDriver::new(bucket, PAGE_SIZE))).expect("driver init");
    dispatch::drive(Arc::new(driver), op_receiver);
}

fn fuse(mountpoint: OsString, view: View, op_sender: op::Sender) {
    const RETRIES: u32 = 5;

    let options = ["-o", "fsname=rpfs"]
        .iter()
        .map(|o| o.as_ref())
        .collect::<Vec<&OsStr>>();

    for _ in 0..RETRIES {
        let _umount = UmountOnDrop(mountpoint.clone());

        let fs = Elmerfs {
            op_sender: op_sender.clone(),
        };
        match fuse::mount(fs, &mountpoint, &options) {
            Ok(()) => break,
            Err(error) if error.kind() == io::ErrorKind::NotConnected => {
                continue;
            }
            Err(error) => {
                error!("{:?}", error);
            }
        }
    }
}


macro_rules! check_utf8 {
    ($reply:expr, $arg:ident) => {
        match $arg.to_str() {
            Some($arg) => String::from($arg),
            None => {
                $reply.error(Errno::EINVAL as libc::c_int);
                return;
            }
        }
    };
}

macro_rules! check_name {
    ($reply:expr, $str:ident) => {{
        let n = check_utf8!($reply, $str);

        match n.parse() {
            Ok(name) => name,
            Err(_) => {
                $reply.error(Errno::EINVAL as libc::c_int);
                return;
            } 
        }
    }};
}

struct Elmerfs {
    op_sender: op::Sender,
}

impl Filesystem for Elmerfs {
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
        let name = check_name!(reply, name);

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
        let name = check_name!(reply, name);

        let _ = self.op_sender.send(Op::MkDir(MkDir {
            reply,
            parent_ino,
            name,
            mode,
            uid: req.uid(),
            gid: req.gid(),
        }));
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name = check_name!(reply, name);

        let _ = self.op_sender.send(Op::RmDir(RmDir {
            reply,
            parent_ino: parent,
            name,
        }));
    }

    fn mknod(
        &mut self,
        req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        let name = check_name!(reply, name);

        let _ = self.op_sender.send(Op::MkNod(MkNod {
            reply,
            parent_ino: parent,
            name,
            mode,
            uid: req.uid(),
            gid: req.gid(),
            rdev,
        }));
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name = check_name!(reply, name);

        let _ = self.op_sender.send(Op::Unlink(Unlink {
            reply,
            parent_ino: parent,
            name,
        }));
    }

    fn setattr(
        &mut self,
        _req: &Request,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<Timespec>,
        mtime: Option<Timespec>,
        fh: Option<u64>,
        _crtime: Option<Timespec>,
        _chgtime: Option<Timespec>,
        _bkuptime: Option<Timespec>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let _ = self.op_sender.send(Op::SetAttr(SetAttr {
            reply,
            ino,
            mode,
            uid,
            gid,
            size,
            atime,
            mtime,
            fh,
        }));
    }

    fn open(&mut self, _req: &Request, ino: u64, _flags: u32, reply: ReplyOpen) {
        let _ = self.op_sender.send(Op::Open(Open { reply, ino }));
    }

    fn release(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let _ = self.op_sender.send(Op::Release(Release { reply, fh, ino }));
    }

    fn write(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _flags: u32,
        reply: ReplyWrite,
    ) {
        if offset < 0 {
            reply.error(Errno::EINVAL as libc::c_int);
            return;
        }
        let offset = offset as u64;

        let _ = self.op_sender.send(Op::Write(Write {
            reply,
            ino,
            fh,
            offset,
            data: Vec::from(data),
        }));
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        reply: ReplyData,
    ) {
        if offset < 0 {
            reply.error(Errno::EINVAL as libc::c_int);
            return;
        }
        let offset = offset as u64;

        let _ = self.op_sender.send(Op::Read(Read {
            reply,
            ino,
            fh,
            offset,
            size,
        }));
    }

    fn rename(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        reply: ReplyEmpty,
    ) {
        let name = check_name!(reply, name);
        let new_name = check_name!(reply, newname);

        let _ = self.op_sender.send(Op::Rename(Rename {
            reply,
            parent_ino: parent,
            new_parent_ino: newparent,
            name,
            new_name,
        }));
    }

    fn link(
        &mut self,
        _req: &Request,
        ino: u64,
        newparent: u64,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        let new_name = check_name!(reply, newname);

        let _ = self.op_sender.send(Op::Link(Link {
            reply,
            ino,
            new_name,
            new_parent_ino: newparent,
        }));
    }

    fn symlink(
        &mut self,
        req: &Request,
        parent: u64,
        name: &OsStr,
        link: &Path,
        reply: ReplyEntry,
    ) {
        let link = link.as_os_str();

        let link = check_utf8!(reply, link);
        let name = check_name!(reply, name);

        let _ = self.op_sender.send(Op::Symlink(Symlink {
            reply,
            parent_ino: parent,
            name,
            link,
            uid: req.uid(),
            gid: req.gid(),
        }));
    }

    fn readlink(&mut self, _req: &Request, ino: u64, reply: ReplyData) {
        let _ = self.op_sender.send(Op::ReadLink(ReadLink { reply, ino }));
    }
}

struct UmountOnDrop(OsString);

impl Drop for UmountOnDrop {
    fn drop(&mut self) {
        Command::new("fusermount")
            .arg("-u")
            .arg(&self.0)
            .stderr(Stdio::piped())
            .stdout(Stdio::piped())
            .status()
            .expect("failed to umount test dir");
    }
}
