use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct TimedOperation {
    count: AtomicU64,
    total_us: AtomicU64,
    max: AtomicU64,
}

impl TimedOperation {
    pub fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            total_us: AtomicU64::new(0),
            max: AtomicU64::new(0),
        }
    }

    fn record(&self, elapsed: u64) {
        self.total_us.fetch_add(elapsed, Ordering::SeqCst);
        self.count.fetch_add(1, Ordering::SeqCst);
        self.max.fetch_max(elapsed, Ordering::SeqCst);
    }

    pub fn start(&self) -> TimedOperationState<'_> {
        TimedOperationState {
            op: self,
            start: Instant::now(),
        }
    }

    pub fn summary(&self) -> TimedOperationSummary {
        let count = self.count.load(Ordering::SeqCst);
        let total_us = self.total_us.load(Ordering::SeqCst);
        let total = std::time::Duration::from_micros(total_us);

        let max_us = self.max.load(Ordering::SeqCst);
        let max = std::time::Duration::from_micros(max_us);

        TimedOperationSummary { count, total, max }
    }
}

pub struct TimedOperationSummary {
    pub count: u64,
    pub total: Duration,
    pub max: Duration,
}

pub struct TimedOperationState<'a> {
    op: &'a TimedOperation,
    start: Instant,
}

impl Drop for TimedOperationState<'_> {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed().as_micros();
        self.op.record(elapsed as u64);
    }
}

pub fn fmt_timed_operations<W, I>(f: &mut W, summaries: I) -> std::io::Result<()>
where
    W: std::io::Write,
    I: IntoIterator<Item = (&'static str, TimedOperationSummary)>,
{
    write!(f, "{}, {}, {}, {}\n", "op", "count", "response time avg", "max")?;

    for (name, summary) in summaries {
        let avg = if summary.count > 0 {
            summary.total / summary.count as u32
        } else {
            Duration::from_secs(0)
        };

        write!(
            f, "{}, {}, {}ms {}ms\n",
            name,
            summary.count,
            avg.as_millis(),
            summary.max.as_millis()
        )?;
    }

    Ok(())
}
