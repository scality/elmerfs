mod collections;
pub mod config;
mod driver;
mod fs;
mod key;
mod model;
mod time;
mod view;
mod metrics;

use crate::driver::Driver;
use crate::fs::Elmerfs;
use std::sync::Arc;
use fuser::MountOption;
use tokio::runtime::{Runtime};
use driver::{DriverError, EIO};
use std::ffi::{OsStr, OsString};
use std::process::{Command, Stdio};
use tracing::*;


pub use crate::driver::AddressBook;
pub use crate::driver::ListingFlavor;
pub use crate::key::Bucket;
pub use crate::view::View;
pub use config::Config;

pub fn run(runtime: Runtime, config: Arc<Config>, forced_view: Option<View>, mountpoint: &OsStr) -> Result<(), DriverError> {
    let driver = runtime.block_on(Driver::new(config.clone()))?;
    let driver = Arc::new(driver);

    let options = [
        MountOption::AllowOther,
        MountOption::DefaultPermissions,
        MountOption::AutoUnmount,
        MountOption::RW
    ];

    let fs = Elmerfs {
        runtime,
        forced_view,
        driver: driver.clone(),
    };
    let _umount = UmountOnDrop::new(mountpoint);



    match fuser::mount2(fs, &mountpoint, &options) {
        Ok(()) => {},
        Err(error) => {
            error!("{:?}", error);
            return Err(EIO)
        }
    }

    let summaries = driver.metrics_summary();
    crate::metrics::fmt_timed_operations(&mut std::io::stderr(), summaries).unwrap();

    Ok(())
}

pub fn bootstrap(runtime: Runtime, config: Arc<Config>) -> Result<(), DriverError> {
    runtime.block_on(
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
