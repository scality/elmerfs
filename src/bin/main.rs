use clap::{App, Arg};
use elmerfs::{self, AddressBook, Bucket, Config, View};
use std::sync::Arc;
use tracing_subscriber::{self, filter::EnvFilter};

const MAIN_BUCKET: Bucket = Bucket::new(0);

fn main() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_default();

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
                .multiple(true),
        )
        .arg(Arg::with_name("nlocks").long("no-locks").takes_value(false))
        .arg(
            Arg::with_name("lf")
                .long("listing-flavor")
                .possible_values(&["full", "partial"])
                .default_value("partial"),
        )
        .arg(
            Arg::with_name("fv")
                .long("force-view")
                .takes_value(true)
                .value_name("UID")
        )
        .get_matches();

    let mountpoint = args.value_of_os("mountpoint").unwrap();
    let addresses = args
        .values_of("antidote")
        .unwrap()
        .map(String::from)
        .collect();
    let locks = !args.is_present("nlocks");
    let listing_flavor = args.value_of("lf").unwrap().parse().unwrap();

    let forced_view = args.value_of("fv").map(|uid| View {
        uid: uid.parse().unwrap(),
    });

    let cfg = Config {
        bucket: MAIN_BUCKET,
        addresses: Arc::new(AddressBook::with_addresses(addresses)),
        locks,
        listing_flavor,
    };

    elmerfs::run(cfg, forced_view, mountpoint);
}
