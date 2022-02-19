mod collections;
pub mod config;
mod driver;
mod fs;
mod key;
mod metrics;
mod model;
mod time;
mod view;

use crate::driver::Driver;
use crate::fs::Elmerfs;
use driver::{DriverError, EIO};
use fuser::MountOption;
use std::ffi::{OsStr, OsString};
use std::process::{Command, Stdio};
use std::sync::Arc;
use tokio::runtime::Runtime;
use tracing::*;

pub use crate::driver::AddressBook;
pub use crate::driver::ListingFlavor;
pub use crate::key::Bucket;
pub use crate::view::View;
pub use config::Config;

pub fn run(
    runtime: Runtime,
    config: Arc<Config>,
    forced_view: Option<View>,
    mountpoint: &OsStr,
) -> Result<(), DriverError> {
    let driver = runtime.block_on(Driver::new(config.clone()))?;
    let driver = Arc::new(driver);

    let options = [
        MountOption::AllowOther,
        MountOption::DefaultPermissions,
        MountOption::AutoUnmount,
        MountOption::RW,
    ];

    let fs = Elmerfs {
        runtime: Arc::new(runtime),
        forced_view,
        driver: driver.clone(),
    };
    let _umount = UmountOnDrop::new(mountpoint);

    match fuser::mount2_mt(fs, &mountpoint, config.fuse.poll_threads as usize, &options) {
        Ok(()) => {}
        Err(error) => {
            error!("{:?}", error);
            return Err(EIO);
        }
    }

    Ok(())
}

pub fn bootstrap(runtime: Runtime, config: Arc<Config>) -> Result<(), DriverError> {
    runtime.block_on(Driver::bootstrap(config))
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
