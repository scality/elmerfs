use crate::{AddressBook, Bucket, ListingFlavor};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(
        "invalid '{category}' option for '{name}': \
             expected {expected} found {found}"
    )]
    BadFormat {
        category: String,
        name: String,
        expected: String,
        found: String,
    },
    #[error("invalid toml: {0}")]
    InvalidToml(#[from] toml::de::Error),
    #[error("io error loading config: {0}")]
    Load(#[from] std::io::Error),
}

mod raw {
    use serde::Deserialize;
    use std::collections::HashMap;

    #[derive(Deserialize)]
    pub struct Config {
        pub cluster_id: u8,
        pub node_id: u8,
        pub antidote: Antidote,
        pub driver: Driver,
        pub fuse: Fuse,
    }

    #[derive(Deserialize)]
    pub struct Antidote {
        pub addresses: Vec<String>,
    }

    #[derive(Deserialize)]
    pub struct Driver {
        pub bucket: u32,
        pub page_size_b: u64,
        pub gather_capacity_b: u64,
        pub page_cache_capacity_b: u64,
        pub listing_flavor: String,
    }

    #[derive(Deserialize)]
    pub struct Fuse {
        #[serde(flatten)]
        pub options: HashMap<String, String>,

        pub poll_threads: u32,
    }
}

#[derive(Debug)]
pub struct Config {
    pub cluster_id: u8,
    pub node_id: u8,
    pub antidote: Antidote,
    pub driver: Driver,
    pub fuse: Fuse,
}

impl Config {
    pub fn bucket(&self) -> Bucket {
        self.driver.bucket
    }
}

#[derive(Debug)]
pub struct Antidote {
    pub addresses: Arc<AddressBook>,
}

#[derive(Debug)]
pub struct Driver {
    pub bucket: Bucket,
    pub use_locks: bool,
    pub page_size_b: u64,
    pub gather_capacity_b: u64,
    pub page_cache_capacity_b: u64,
    pub listing_flavor: ListingFlavor,
}

#[derive(Debug)]
pub struct Fuse {
    pub options: HashMap<String, String>,
    pub poll_threads: u32,
}

pub fn load(path: impl AsRef<Path>) -> Result<Config, Error> {
    let content = std::fs::read(path)?;
    let raw_config: raw::Config = toml::from_slice(&content)?;

    let bucket = Bucket::new(raw_config.driver.bucket);
    let addresses = Arc::new(AddressBook::with_addresses(raw_config.antidote.addresses));

    let raw_listing_flavor = raw_config.driver.listing_flavor.clone();
    let listing_flavor =
        raw_config
            .driver
            .listing_flavor
            .parse()
            .map_err(|_| Error::BadFormat {
                category: "driver".into(),
                name: "listing_flavor".into(),
                found: raw_listing_flavor,
                expected: "full or partial".into(),
            })?;

    Ok(Config {
        cluster_id: raw_config.cluster_id,
        node_id: raw_config.node_id,
        antidote: Antidote { addresses },
        fuse: Fuse {
            options: raw_config.fuse.options,
            poll_threads: raw_config.fuse.poll_threads,
        },
        driver: Driver {
            bucket,
            use_locks: false,
            listing_flavor,
            page_size_b: raw_config.driver.page_size_b,
            gather_capacity_b: raw_config.driver.gather_capacity_b,
            page_cache_capacity_b: raw_config.driver.page_cache_capacity_b,
        },
    })
}
