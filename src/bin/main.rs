use clap::{App, Arg};
use elmerfs::{self, AddressBook, Bucket, Config, View};
use std::sync::Arc;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

const MAIN_BUCKET: Bucket = Bucket::new(0);

fn main() {
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
                .value_name("UID"),
        )
        .arg(
            Arg::with_name("flamegraph")
                .long("flamegraph")
                .takes_value(true)
                .default_value("./elmerfs.flamegraph"),
        )
        .get_matches();

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_default()
        .add_directive("async_io::reactor=error".parse().unwrap())
        .add_directive("polling=error".parse().unwrap());

    let (non_blocking_appender, _guard) = tracing_appender::non_blocking(std::io::stdout());
    let fmt_layer = fmt::Layer::new()
        .with_target(false)
        .with_writer(non_blocking_appender);

    let registry = tracing_subscriber::registry().with(filter).with(fmt_layer);

    let _flame_layer_guard = match args.value_of("flamegraph") {
        Some(path) => {
            let (flame_layer, guard) = tracing_flame::FlameLayer::with_file(path).unwrap();
            registry.with(flame_layer).init();
            Some(guard)
        }
        None => None,
    };

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
