use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn now() -> Duration {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap()
}

pub fn timespec(d: Duration) -> time::Timespec {
    time::Timespec::new(d.as_secs() as i64, d.subsec_nanos() as i32)
}
