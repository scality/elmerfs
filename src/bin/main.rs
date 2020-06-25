use clap::{App, Arg};
use elmerfs::{self, Bucket, Config};
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
            Arg::with_name("antidote_url")
                .long("antidote_url")
                .short("s")
                .value_name("URL")
                .default_value("127.0.0.1:8101"),
        )
        .arg(
            Arg::with_name("no_distributed_locks")
                .long("no-distributed-locks")
                .short("nl")
                .takes_value(false),
        )
        .get_matches();

    let mountpoint = args.value_of_os("mountpoint").unwrap();
    let address = args.value_of("antidote_url").unwrap();
    let use_distributed_locks = !args.is_present("no_distributed_locks");

    let cfg = Config {
        bucket: MAIN_BUCKET,
        address: String::from(address),
        use_distributed_locks,
    };

    elmerfs::run(cfg, mountpoint);
}
