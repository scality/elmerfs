use std::time::{Duration, SystemTime};

pub fn now() -> SystemTime {
    SystemTime::now()
}

pub fn ts(st: SystemTime) -> Duration {
    st.duration_since(SystemTime::UNIX_EPOCH).unwrap()
}

pub fn from_ts(duration: Duration) -> SystemTime {
    SystemTime::UNIX_EPOCH.checked_add(duration).unwrap()
}