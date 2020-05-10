mod inode;
mod op;
mod key;

use clap::{App, Arg};
use fuse::{Filesystem, *};
use nix::{errno::Errno, libc};
use std::ffi::{OsStr, OsString};
use std::thread;
use log::*;

const OP_BUFFERING_SIZE: usize = 1024;

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

    while let Ok(op) = op_receiver.recv() {
        match op {
            op::Op::MkNod(mknod) => {
                mknod.reply.error(Errno::EINVAL as libc::c_int);
            }
            _ => {}
        }
    }
}

fn fuse(mountpoint: OsString, op_sender: op::Sender) {
    let options = ["-o", "fsname=rpfs"]
        .iter()
        .map(|o| o.as_ref())
        .collect::<Vec<&OsStr>>();

    let fs = Rpfs { op_sender };
    fuse::mount(fs, &mountpoint, &options).unwrap();
}

struct Rpfs {
    op_sender: op::Sender,
}

impl Filesystem for Rpfs {}
