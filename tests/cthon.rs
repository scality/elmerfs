use elmerfs::{ UmountOnDrop, config, View};
use std::ffi::OsString;
use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile;
use tracing::info;
use tracing_subscriber::{self, filter::EnvFilter};

/* Run test as if they were on the behalf of the root user */
const TEST_VIEW: View = View { uid: 0 };
const CHTON_PATH: &str = "vendor/cthon04/";

fn setup_logging() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_default()
        .add_directive("async_std::task=warn".parse().unwrap())
        .add_directive("fuse=error".parse().unwrap())
        .add_directive("antidotec=trace".parse().unwrap())
        .add_directive("elmerfs=trace".parse().unwrap());

    tracing_subscriber::fmt().with_env_filter(filter).init();
}

#[test]
fn cthon_basic() {
    setup_logging();

    let tests_dir = tempfile::tempdir().expect("tmp mountpoint");
    let cfg = config::load("./tests/chton.elmerfs.toml").expect("config loaded");
    fs::create_dir_all(&tests_dir.path()).expect("test dirs created");
    info!(workdir = ?tests_dir.path().as_os_str());

    let tests_dir_path = OsString::from(tests_dir.path().as_os_str());
    let umount = UmountOnDrop::new(&tests_dir_path);
    let rpfs_thread = thread::spawn(move || elmerfs::run(Arc::new(cfg), Some(TEST_VIEW), &tests_dir_path));

    thread::sleep(Duration::from_secs(5));
    let bin_dir = Path::new(CHTON_PATH).join("basic");
    let chton_status = dbg!(Command::new("./runtests")
        .current_dir(bin_dir)
        .env("NFSTESTDIR", tests_dir.path().join("basic"))
        .stderr(Stdio::piped())
        .stdout(Stdio::piped()))
    .status()
    .expect("failed to run cthon basic test suite");
    assert_eq!(chton_status.code(), Some(0));

    tracing::info!("cleanup");
    drop(umount);
    assert!(rpfs_thread.join().is_ok());
}

#[test]
fn cthon_general() {
    setup_logging();

    let tests_dir = tempfile::tempdir().expect("tmp mountpoint");
    let cfg = config::load("./tests/chton.elmerfs.toml").expect("config loaded");
    fs::create_dir_all(&tests_dir.path()).expect("test dirs created");
    info!(workdir = ?tests_dir.path().as_os_str());

    let tests_dir_path = OsString::from(tests_dir.path().as_os_str());
    let umount = UmountOnDrop::new(&tests_dir_path);
    let rpfs_thread = thread::spawn(move || elmerfs::run(Arc::new(cfg), Some(TEST_VIEW), &tests_dir_path));

    thread::sleep(Duration::from_secs(5));
    let bin_dir = Path::new(CHTON_PATH).join("general");
    let chton_status = dbg!(Command::new("./runtests")
        .current_dir(bin_dir)
        .env("NFSTESTDIR", tests_dir.path().join("general"))
        .stderr(Stdio::piped())
        .stdout(Stdio::piped()))
    .status()
    .expect("failed to run cthon basic test suite");
    assert_eq!(chton_status.code(), Some(0));

    tracing::info!("cleanup");

    drop(umount);
    assert!(rpfs_thread.join().is_ok());
}
