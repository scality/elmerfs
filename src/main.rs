mod inode;
mod key;
mod op;
mod dispatch;
mod driver;

use crate::key::Bucket;
use crate::op::*;
use async_std::sync::Arc;
use clap::{App, Arg};
use fuse::{Filesystem, *};
use nix::{errno::Errno, libc};
use std::ffi::{OsStr, OsString};
use std::thread;
use driver::Driver;
use tracing::{info, Level};
use tracing_subscriber;

const MAIN_BUCKET: Bucket = Bucket::new(0);
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
fn main() {
    let _trace_subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .init();

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

    let mountpoint: OsString = args.value_of_os("mountpoint").unwrap().into();
    let cfg = driver::Config {
        bucket: MAIN_BUCKET,
        address: String::from("127.0.0.1:8101"),
    };
    info!(cfg.mountpoint = ?mountpoint, cfg.address = &cfg.address as &str);

    let (op_sender, op_receiver) = op::sync_channel(OP_BUFFERING_SIZE);
    thread::Builder::new()
        .name("fuse".into())
        .spawn(move || fuse(mountpoint, op_sender))
        .unwrap();

    let driver = Arc::new(Driver { cfg });
    dispatch::drive(driver, op_receiver);
}

fn fuse(mountpoint: OsString, op_sender: op::Sender) {
    let options = ["-o", "fsname=rpfs"]
        .iter()
        .map(|o| o.as_ref())
        .collect::<Vec<&OsStr>>();

    let fs = Rpfs { op_sender };
    fuse::mount(fs, &mountpoint, &options).unwrap()
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

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name = match name.to_str() {
            Some(name) => String::from(name),
            None => {
                reply.error(Errno::EINVAL as libc::c_int);
                return;
            }
        };

        let _ = self.op_sender.send(Op::RmDir(RmDir {
            reply,
            parent_ino: parent,
            name,
        }));
    }
}


