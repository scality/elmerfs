use async_std::sync::{Condvar, Mutex};
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

#[derive(Debug)]
struct RangeLock {
    used_ranges: Vec<Range<u64>>,
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

    pub async fn lock(&self, ino: u64, byte_range: Range<u64>) -> RangeGuard {
        let requested_pages = self.page_range(&byte_range);

        let mut by_ino = self.by_ino.lock().await;
        by_ino.entry(ino).or_insert(RangeLock {
            used_ranges: Vec::with_capacity(1),
            range_signal: Arc::new(Condvar::new()),
        });

        while Self::intersection_position(&by_ino[&ino].used_ranges[..], &requested_pages).is_some()
        {
            let range_lock = &by_ino[&ino];

            tracing::debug!(?range_lock, ?requested_pages, "page contention");
            let cond = range_lock.range_signal.clone();
            by_ino = cond.wait(by_ino).await;
        }

        let range_lock = by_ino.get_mut(&ino).unwrap();

        range_lock.used_ranges.push(requested_pages.clone());
        RangeGuard {
            ino,
            pages: requested_pages,
        }
    }

    pub async fn unlock(&self, guard: RangeGuard) {
        let ino = guard.ino;
        let pages = guard.pages.clone();
        std::mem::forget(guard);

        let mut by_ino = self.by_ino.lock().await;
        let range_lock = by_ino.get_mut(&ino).unwrap();

        let index = Self::intersection_position(&range_lock.used_ranges[..], &pages).unwrap();
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

    fn intersection_position(ranges: &[Range<u64>], range: &Range<u64>) -> Option<usize> {
        for (i, current) in ranges.iter().enumerate() {
            if current.start < range.end && range.start < current.end {
                return Some(i);
            }
        }

        None
    }
}

pub struct RangeGuard {
    ino: u64,
    pages: Range<u64>,
}

impl Drop for RangeGuard {
    fn drop(&mut self) {
        panic!("locked");
    }
}
