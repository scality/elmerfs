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
use driver::{DriverError, EIO};
use std::ffi::{OsStr, OsString};
use std::io;
use std::process::{Command, Stdio};
use tracing::*;

pub use crate::driver::AddressBook;
pub use crate::driver::ListingFlavor;
pub use crate::key::Bucket;
pub use crate::view::View;
pub use config::Config;

pub fn run(config: Arc<Config>, forced_view: Option<View>, mountpoint: &OsStr) -> Result<(), DriverError> {
    const RETRIES: u32 = 5;

    let driver = task::block_on(Driver::new(config.clone()))?;
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

    let mut mounted = false;
    for _ in 0..RETRIES {
        let _umount = UmountOnDrop::new(mountpoint);

        let fs = Elmerfs {
            forced_view,
            driver: driver.clone(),
        };
        match fuse::mount(fs, &mountpoint, &fuse_options) {
            Ok(()) => {
                mounted = true;
                break
            },
            Err(error) if error.kind() == io::ErrorKind::NotConnected => {
                continue;
            }
            Err(error) => {
                error!("{:?}", error);
            }
        }
    }

    if mounted {
        Ok(())
    } else {
        Err(EIO)
    }
}

pub fn bootstrap(config: Arc<Config>) -> Result<(), DriverError> {
    task::block_on(
        Driver::bootstrap(config)
    )
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
