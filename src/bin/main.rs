use clap::{App, Arg};
use elmerfs::{self, AddressBook, Bucket, Config};
use tracing_subscriber::{self, filter::EnvFilter};

const MAIN_BUCKET: Bucket = Bucket::new(0);

fn main() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_default()
        .add_directive("async_std::task=warn".parse().unwrap())
        .add_directive("fuse=error".parse().unwrap());

    let (non_blocking_appender, _guard) = tracing_appender::non_blocking(std::io::stdout());
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(non_blocking_appender)
        .init();

    let args = App::new("elmerfs")
        .arg(
            Arg::with_name("mountpoint")
                .long("mount")
                .short("m")
                .value_name("MOUNTPOINT")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("antidote")
                .long("antidote")
                .short("s")
                .value_name("URL")
                .default_value("127.0.0.1:8101")
                .multiple(true)
        )
        .arg(
            Arg::with_name("no_distributed_locks")
                .long("no-distributed-locks")
                .short("nl")
                .takes_value(false),
        )
        .get_matches();

    let mountpoint = args.value_of_os("mountpoint").unwrap();
    let addresses = args.values_of("antidote").unwrap().map(String::from).collect();
    let use_distributed_locks = !args.is_present("no_distributed_locks");

    let cfg = Config {
        bucket: MAIN_BUCKET,
        addresses: AddressBook::with_addresses(addresses),
        use_distributed_locks,
    };

    elmerfs::run(cfg, mountpoint);
}
