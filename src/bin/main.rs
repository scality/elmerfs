#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

use clap::{App, Arg};
use elmerfs::{self, View};
use std::{path::Path, sync::Arc};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

fn main() -> Result<(), anyhow::Error> {
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
            Arg::with_name("config")
                .long("config")
                .short("c")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("fv")
                .long("force-view")
                .takes_value(true)
                .value_name("UID"),
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

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .init();

    let mountpoint = args.value_of_os("mountpoint").unwrap();
    let config_path = args.value_of_os("config").unwrap();
    let forced_view = args.value_of("fv").map(|uid| View {
        uid: uid.parse().unwrap(),
    });

    let config = Arc::new(elmerfs::config::load(&Path::new(config_path))?);
    tracing::info!(?config, "Config loaded.");

    elmerfs::run(config, forced_view, mountpoint);
    Ok(())
}
