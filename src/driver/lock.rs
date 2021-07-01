use async_std::sync::{Condvar, Mutex};
use std::collections::HashMap;
use std::future::Future;
use std::ops::Range;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum OpKind {
    Read,
    Write,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RangeOp {
    range: Range<u64>,
    kind: OpKind,
}

impl RangeOp {
    #[inline]
    fn conflicts_with(&self, other: &RangeOp) -> bool {
        if self.range.start < other.range.end && other.range.start < self.range.end {
            match (self.kind, other.kind) {
                (OpKind::Read, OpKind::Read) => false,
                (OpKind::Write, _) | (_, OpKind::Write) => true,
            }
        } else {
            false
        }
    }
}

#[derive(Debug)]
struct RangeLock {
    used_ranges: Vec<RangeOp>,
    range_signal: Arc<Condvar>,
}

#[derive(Debug)]
pub struct PageLocks {
    page_size: u64,
    by_ino: Mutex<HashMap<u64, RangeLock>>,
}

impl PageLocks {
    pub fn new(page_size: u64) -> Self {
        Self {
            page_size,
            by_ino: Mutex::new(HashMap::default()),
        }
    }


    pub fn rlock<'s>(&'s self, ino: u64, byte_range: Range<u64>) -> impl Future<Output=RangeGuard> + 's {
        self.lock(ino, OpKind::Read, byte_range)
    }

    pub fn wlock<'s>(&'s self, ino: u64, byte_range: Range<u64>) -> impl Future<Output=RangeGuard> + 's {
        self.lock(ino, OpKind::Write, byte_range)
    }

    #[inline]
    async fn lock(&self, ino: u64, kind: OpKind, byte_range: Range<u64>) -> RangeGuard {
        let requested_pages = self.page_range(&byte_range);
        let requested = RangeOp {
            kind,
            range: requested_pages,
        };

        let mut by_ino = self.by_ino.lock().await;
        by_ino.entry(ino).or_insert(RangeLock {
            used_ranges: Vec::with_capacity(1),
            range_signal: Arc::new(Condvar::new()),
        });

        while (&by_ino[&ino].used_ranges[..].iter().position(|used| used.conflicts_with(&requested))).is_some()
        {
            let range_lock = &by_ino[&ino];

            tracing::debug!(?range_lock, ?requested, "page contention");
            let cond = range_lock.range_signal.clone();
            by_ino = cond.wait(by_ino).await;
        }

        let range_lock = by_ino.get_mut(&ino).unwrap();

        range_lock.used_ranges.push(requested.clone());
        RangeGuard {
            ino,
            used: requested,
        }
    }

    pub async fn unlock(&self, guard: RangeGuard) {
        let ino = guard.ino;
        let released = guard.used.clone();
        std::mem::forget(guard);

        let mut by_ino = self.by_ino.lock().await;
        let range_lock = by_ino.get_mut(&ino).unwrap();

        let index = range_lock.used_ranges.iter().position(|used| used == &released).unwrap();
        range_lock.used_ranges.swap_remove(index);
        range_lock.range_signal.notify_all();

        if range_lock.used_ranges.is_empty()  && Arc::strong_count(&range_lock.range_signal) == 1 {
            by_ino.remove(&ino);
        }
    }

    fn page_range(&self, byte_range: &Range<u64>) -> Range<u64> {
        let first = byte_range.start / self.page_size;
        let last = byte_range.end / self.page_size;
        tracing::debug!(?byte_range, first, last);

        first..(last + 1)
    }
}

pub struct RangeGuard {
    ino: u64,
    used: RangeOp,
}

impl Drop for RangeGuard {
    fn drop(&mut self) {
        panic!("locked");
    }
}
