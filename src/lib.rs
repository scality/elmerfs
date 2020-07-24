mod dir;
mod driver;
mod ino;
mod inode;
mod key;
mod page;
mod pool;
mod fs;
mod view;

use crate::driver::Driver;
use crate::page::PageDriver;
use async_std::{sync::Arc, task};
use std::io;
use std::process::{Command, Stdio};
use tracing::*;
use std::ffi::{OsStr, OsString};
use crate::fs::Elmerfs;

pub use crate::driver::Config;
pub use crate::key::Bucket;
pub use crate::pool::AddressBook;
pub use crate::view::View;

const PAGE_SIZE: usize = 4 * 1024;

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
    const RETRIES: u32 = 5;

    let page_driver = PageDriver::new(cfg.bucket, PAGE_SIZE);
    let driver = task::block_on(Driver::new(cfg, page_driver)).expect("driver init");

    let driver = Arc::new(driver);
    let options = ["-o", "fsname=rpfs"]
        .iter()
        .map(|o| o.as_ref())
        .collect::<Vec<&OsStr>>();

    for _ in 0..RETRIES {
        let _umount = UmountOnDrop(mountpoint.to_os_string());

        let fs = Elmerfs {
            driver: driver.clone(),
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
