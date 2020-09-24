use elmerfs::{AddressBook, Bucket, Config, View};
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

const TEST_VIEW: View = 0;
const CHTON_PATH: &str = "vendor/cthon04/";
const CTHON_BASIC_BUCKET: Bucket = Bucket::new(0);
const ANTIDOTE_URL: &str = "127.0.0.1:8101";

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

    let tests_dir = tempfile::tempdir().expect("failed to create mountpoint tmpdir");
    let cfg = Config {
        view: TEST_VIEW,
        bucket: CTHON_BASIC_BUCKET,
        addresses: Arc::new(AddressBook::with_addresses(vec![String::from(
            ANTIDOTE_URL,
        )])),
        locks: true,
    };

    fs::create_dir_all(&tests_dir.path()).expect("failed ot create test mountpoint");
    info!(workdir = ?tests_dir.path().as_os_str());

    let tests_dir_path = OsString::from(tests_dir.path().as_os_str());
    let rpfs_thread = thread::spawn(move || elmerfs::run(cfg, &tests_dir_path));

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
    assert!(rpfs_thread.join().is_ok());
}

#[test]
fn cthon_general() {
    setup_logging();

    let tests_dir = tempfile::tempdir().expect("failed to create mountpoint tmpdir");
    let cfg = Config {
        view: TEST_VIEW,
        bucket: CTHON_BASIC_BUCKET,
        addresses: Arc::new(AddressBook::with_addresses(vec![String::from(
            ANTIDOTE_URL,
        )])),
        locks: true,
    };

    fs::create_dir_all(&tests_dir.path()).expect("failed ot create test mountpoint");
    info!(workdir = ?tests_dir.path().as_os_str());

    let tests_dir_path = OsString::from(tests_dir.path().as_os_str());
    let rpfs_thread = thread::spawn(move || elmerfs::run(cfg, &tests_dir_path));

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
    assert!(rpfs_thread.join().is_ok());
}
