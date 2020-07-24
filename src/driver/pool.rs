use antidotec::{Connection, Error};
use crossbeam::queue::{ArrayQueue, PushError};
use std::ops::{Deref, DerefMut};
use std::time::{Duration, Instant};
use tracing::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

const CONNECTION_TIMEOUT_S: u64 = 180;

#[derive(Debug)]
pub struct AddressBook {
    addresses: Vec<String>,
    next: AtomicUsize
}

impl AddressBook {
    pub fn with_addresses(addresses: Vec<String>) -> Self {
        assert_ne!(addresses.len(), 0);

        Self {
            addresses,
            next: AtomicUsize::new(0),
        }
    }

    pub fn next(&self) -> &str {
        let next = self.next.fetch_add(1, Ordering::Relaxed);
        &self.addresses[next % self.addresses.len()]
    }
}

#[derive(Debug)]
struct AvailableConnection {
    pushed_at: Instant,
    connection: Connection,
}

#[derive(Debug)]
pub struct ConnectionPool {
    addresses: Arc<AddressBook>,
    available: ArrayQueue<AvailableConnection>,
    timeout: Duration,
}

impl ConnectionPool {
    pub fn with_capacity(addresses: Arc<AddressBook>, capacity: usize) -> Self {
        ConnectionPool {
            addresses,
            available: ArrayQueue::new(capacity),
            timeout: Duration::from_secs(CONNECTION_TIMEOUT_S),
        }
    }

    #[instrument(skip(self))]
    pub async fn acquire(&self) -> Result<PoolGuard<'_>, Error> {
        debug!("try to acquire a connection");

        if let Ok(available) = self.available.pop() {
            let elasped = available.pushed_at.elapsed();
            if available.pushed_at.elapsed() < self.timeout {
                debug!(age = elasped.as_secs(), "reusing connection pushed at");
                return Ok(PoolGuard::new(self, available.connection));
            }
        }

        let connection = Connection::new(self.addresses.next()).await?;
        Ok(PoolGuard::new(self, connection))
    }

    #[instrument(skip(self))]
    fn push(&self, connection: Connection) {
        let pushed_at = Instant::now();
        let entry = AvailableConnection {
            pushed_at,
            connection,
        };

        if let Err(PushError(entry)) = self.available.push(entry) {
            debug!("pool is full");

            /* Drop the presumably an older connection */
            let _ = self.available.pop();
            let _ = self.available.push(entry);
        }
    }
}

pub struct PoolGuard<'p> {
    connection: Option<Connection>,
    pool: &'p ConnectionPool,
}

impl<'p> PoolGuard<'p> {
    pub fn new(pool: &'p ConnectionPool, connection: Connection) -> Self {
        Self {
            connection: Some(connection),
            pool,
        }
    }
}

impl Deref for PoolGuard<'_> {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        self.connection.as_ref().unwrap()
    }
}

impl DerefMut for PoolGuard<'_> {
    fn deref_mut(&mut self) -> &mut Connection {
        self.connection.as_mut().unwrap()
    }
}

impl Drop for PoolGuard<'_> {
    fn drop(&mut self) {
        let connection = self.connection.take().unwrap();
        self.pool.push(connection);
    }
}
