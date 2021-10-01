mod collections;
pub mod config;
mod driver;
mod fs;
mod key;
mod model;
mod time;
mod view;

use crate::driver::Driver;
use crate::fs::Elmerfs;
use async_std::{sync::Arc, task};
use std::ffi::{OsStr, OsString};
use std::io;
use std::process::{Command, Stdio};
use tracing::*;

pub use crate::driver::AddressBook;
pub use crate::driver::ListingFlavor;
pub use crate::key::Bucket;
pub use crate::view::View;
pub use config::Config;

/// There is two main thread of execution to follow:
///
/// The first one is dedicated to fuse whom sole purpose is to perform
/// argument format validation (e.g are name given valid utf8 strings ?) and
/// send those requests to whoever might be interested.
///
/// The second one, the dispatcher thread, it takes fuse request and dispatch
/// them into asynchronous tasks calling into the root of the filesystem,
/// the Rp driver.
pub fn run(config: Arc<Config>, forced_view: Option<View>, mountpoint: &OsStr) {
    const RETRIES: u32 = 5;

    let driver = task::block_on(Driver::new(config.clone())).expect("driver init");
    let driver = Arc::new(driver);

    let fuse_options: Vec<&OsStr> =
        config
            .fuse
            .options
            .iter()
            .fold(Vec::new(), |mut options, (name, value)| {
                options.push("-o".as_ref());
                options.push(name.as_ref());
                options.push(value.as_ref());
                options
            });

    for _ in 0..RETRIES {
        let _umount = UmountOnDrop::new(mountpoint);

        let fs = Elmerfs {
            forced_view,
            driver: driver.clone(),
        };
        match fuse::mount(fs, &mountpoint, &fuse_options) {
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

pub struct UmountOnDrop(OsString);

impl UmountOnDrop {
    pub fn new(mountpoint: &OsStr) -> Self {
        Self(mountpoint.to_os_string())
    }
}

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
