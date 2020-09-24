use crate::driver::Result;
use crate::key::{Bucket, KeyWriter, Ty};
use antidotec::{lwwreg, RawIdent, Transaction};
use async_std::sync::{Condvar, Mutex};
use std::collections::HashMap;
use std::mem;
use std::ops::Range;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct PageWriter {
    bucket: Bucket,
    page_size: u64,
    locks: PageLocks,
}

impl PageWriter {
    pub fn new(bucket: Bucket, page_size: u64) -> Self {
        Self {
            bucket,
            page_size,
            locks: PageLocks::new(page_size),
        }
    }

    #[tracing::instrument(skip(self, tx, content))]
    pub async fn write(
        &self,
        tx: &mut Transaction<'_>,
        ino: u64,
        offset: u64,
        content: &[u8],
    ) -> Result<()> {
        if content.len() == 0 {
            return Ok(());
        }

        let byte_range = offset..(offset + content.len() as u64);
        let guard = self.locks.lock(ino, byte_range).await;

        let result = self.write_nolock(tx, ino, offset, content).await;

        self.locks.unlock(guard).await;
        result
    }

    #[tracing::instrument(skip(self, tx, content))]
    pub async fn write_nolock(
        &self,
        tx: &mut Transaction<'_>,
        ino: u64,
        offset: u64,
        content: &[u8],
    ) -> Result<()> {
        let first_page = offset / self.page_size;
        let offset_in_page = offset - first_page * self.page_size;
        tracing::info!(first_page, offset_in_page);

        let unaligned_len = (self.page_size - offset_in_page).min(content.len() as u64);
        tracing::info!(?unaligned_len);

        let (content, remaining) = content.split_at(unaligned_len as usize);

        tracing::trace!(content_len = content.len(), remaining = remaining.len());
        self.write_page(tx, ino, first_page as u64, offset_in_page, content)
            .await?;

        let extent_start = first_page + 1;

        tracing::trace!(?extent_start);
        self.write_extent(tx, ino, extent_start as u64, remaining)
            .await?;

        tracing::trace!("done write_nolock");
        Ok(())
    }

    async fn write_page(
        &self,
        tx: &mut Transaction<'_>,
        ino: u64,
        page: u64,
        offset_in_page: u64,
        content: &[u8],
    ) -> Result<()> {
        let end = offset_in_page + content.len() as u64;
        assert!(end <= self.page_size);

        let page = Key::new(ino, page);
        let mut page_content = {
            let mut reply = tx.read(self.bucket, vec![lwwreg::get(page)]).await?;
            reply.lwwreg(0).unwrap_or_default()
        };

        if end > page_content.len() as u64 {
            page_content.resize(end as usize, 0);
        }
        page_content[offset_in_page as usize..end as usize].copy_from_slice(content);

        tx.update(self.bucket, vec![lwwreg::set(page, page_content)])
            .await?;

        Ok(())
    }

    async fn write_extent(
        &self,
        tx: &mut Transaction<'_>,
        ino: u64,
        extent_start: u64,
        content: &[u8],
    ) -> Result<()> {
        let mut page = extent_start;
        let writes = content.chunks_exact(self.page_size as usize).map(|chunk| {
            let write = lwwreg::set(Key::new(ino, page), chunk.into());
            page += 1;

            write
        });

        tracing::trace!("writing extent");
        tx.update(self.bucket, writes).await?;

        tracing::trace!("writing remaining page");
        self.write_page(
            tx,
            ino,
            page,
            0,
            content.chunks_exact(self.page_size as usize).remainder(),
        )
        .await?;

        tracing::trace!("done");
        Ok(())
    }

    #[tracing::instrument(skip(self, tx, output))]
    pub async fn read(
        &self,
        tx: &mut Transaction<'_>,
        ino: u64,
        offset: u64,
        len: u64,
        output: &mut Vec<u8>,
    ) -> Result<()> {
        if len == 0 {
            return Ok(());
        }

        let byte_range = offset..(offset + len);
        let guard = self.locks.lock(ino, byte_range).await;

        let result = self.read_nolock(tx, ino, offset, len, output).await;

        self.locks.unlock(guard).await;
        result
    }

    #[tracing::instrument(skip(self, tx, output))]
    pub async fn read_nolock(
        &self,
        tx: &mut Transaction<'_>,
        ino: u64,
        offset: u64,
        len: u64,
        output: &mut Vec<u8>,
    ) -> Result<()> {
        let first_page = offset / self.page_size;
        let offset_in_page = offset - first_page * self.page_size;
        tracing::info!(first_page, offset_in_page);

        let unaligned_len = (self.page_size - offset_in_page).min(len);
        tracing::info!(unaligned_len);

        self.read_page(
            tx,
            ino,
            first_page as u64,
            offset_in_page,
            unaligned_len,
            output,
        )
        .await?;

        let extent_start = first_page + 1;
        let remaining_len = len - unaligned_len;
        self.read_extent(tx, ino, extent_start, remaining_len, output)
            .await?;

        Ok(())
    }

    async fn read_page(
        &self,
        tx: &mut Transaction<'_>,
        ino: u64,
        page: u64,
        offset_in_page: u64,
        len: u64,
        output: &mut Vec<u8>,
    ) -> Result<()> {
        let end = offset_in_page + len;
        assert!(end <= self.page_size);

        let page = Key::new(ino, page);
        let page_content = {
            let mut reply = tx.read(self.bucket, vec![lwwreg::get(page)]).await?;
            reply.lwwreg(0).unwrap_or_default()
        };

        // we read a hole
        if page_content.is_empty() {
            output.resize(output.len() + len as usize, 0);
            return Ok(());
        }

        let overlapping = intersect_range(0..page_content.len() as u64, offset_in_page..end);
        output
            .extend_from_slice(&page_content[overlapping.start as usize..overlapping.end as usize]);
        Ok(())
    }

    async fn read_extent(
        &self,
        tx: &mut Transaction<'_>,
        ino: u64,
        extent_start: u64,
        len: u64,
        output: &mut Vec<u8>,
    ) -> Result<()> {
        if len == 0 {
            return Ok(());
        }

        let extent_len = len.checked_sub(1).unwrap() / self.page_size + 1;
        let pages = extent_start..(extent_start + extent_len as u64);
        tracing::info!(?pages);

        let reads = pages.clone().map(|page| lwwreg::get(Key::new(ino, page)));
        let mut reply = tx.read(self.bucket, reads).await?;

        let mut page_index = 0;
        let mut remaining = len;
        tracing::info!(?page_index, remaining);
        while remaining >= self.page_size {
            let content = reply.lwwreg(page_index).unwrap_or_default();

            // we read a hole
            if content.is_empty() {
                output.resize(output.len() + self.page_size as usize, 0);
                remaining -= self.page_size;
            } else {
                output.extend_from_slice(&content[..content.len()]);

                let padding = self.page_size - content.len() as u64;
                output.resize(output.len() + padding as usize, 0);
                remaining -= self.page_size;
            }

            page_index += 1;
            tracing::info!(?page_index, remaining);
        }

        if remaining > 0 {
            let content = reply.lwwreg(page_index).unwrap_or_default();
            output.extend_from_slice(&content[..remaining.min(content.len() as u64) as usize]);
        }

        Ok(())
    }

    #[tracing::instrument(skip(tx))]
    pub async fn remove(
        &self,
        tx: &mut Transaction<'_>,
        ino: u64,
        range: Range<u64>,
    ) -> Result<()> {
        let len = range.end.saturating_sub(range.start);
        if len == 0 {
            return Ok(());
        }

        let guard = self.locks.lock(ino, range.clone()).await;

        let result = self.remove_nolock(tx, ino, range).await;

        self.locks.unlock(guard).await;
        result
    }

    #[tracing::instrument(skip(tx))]
    pub async fn remove_nolock(
        &self,
        tx: &mut Transaction<'_>,
        ino: u64,
        range: Range<u64>,
    ) -> Result<()> {
        let len = range.end.saturating_sub(range.start);
        if len == 0 {
            return Ok(());
        }

        let first_page = range.start / self.page_size as u64;
        let offset_in_page = range.start - first_page * self.page_size as u64;
        tracing::info!(first_page, offset_in_page);

        let first_page_update = {
            let page_key = Key::new(ino, first_page);
            let mut reply = tx.read(self.bucket, vec![lwwreg::get(page_key)]).await?;
            let mut content = reply.lwwreg(0).unwrap_or_default();
            content.truncate(offset_in_page as usize);
            tracing::info!(old_len = content.len(), new_len = offset_in_page);

            lwwreg::set(page_key, content)
        };

        let remaining_pages = len.checked_sub(1).unwrap() / self.page_size as u64;
        let page_offset = first_page + 1;
        tracing::info!(remaining_pages, page_offset);

        let truncates = (page_offset..(page_offset + remaining_pages))
            .map(|i| lwwreg::set(Key::new(ino, i), Vec::new()));

        let truncates = std::iter::once(first_page_update)
            .chain(truncates)
            .collect::<Vec<_>>();
        tx.update(self.bucket, truncates).await?;

        Ok(())
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Key {
    ino: u64,
    page: u64,
}

impl Key {
    pub fn new(ino: u64, page: u64) -> Self {
        Key { ino, page }
    }

    const fn byte_len() -> usize {
        2 * mem::size_of::<u64>()
    }
}

impl Into<RawIdent> for Key {
    fn into(self) -> RawIdent {
        KeyWriter::with_capacity(Ty::Page, Self::byte_len())
            .write_u64(self.ino)
            .write_u64(self.page)
            .into()
    }
}

fn intersect_range(lhs: Range<u64>, rhs: Range<u64>) -> Range<u64> {
    if lhs.end < rhs.start || rhs.end < lhs.start {
        return 0..0;
    }

    lhs.start.max(rhs.start)..lhs.end.min(rhs.end)
}

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
        let requested_pages = self.page_range(byte_range);

        let mut by_ino = self.by_ino.lock().await;
        by_ino.entry(ino).or_insert(RangeLock {
            used_ranges: Vec::with_capacity(1),
            range_signal: Arc::new(Condvar::new()),
        });

        while let Some(_) =
            Self::intersection_position(&by_ino[&ino].used_ranges[..], &requested_pages)
        {
            let cond = by_ino[&ino].range_signal.clone();
            by_ino = cond.wait(by_ino).await;
        }

        let range_lock = by_ino.get_mut(&ino).unwrap();

        tracing::trace!(?requested_pages, "lock acquired");
        range_lock.used_ranges.push(requested_pages.clone());

        RangeGuard {
            ino,
            pages: requested_pages,
        }
    }

    pub async fn unlock(&self, guard: RangeGuard) {
        let ino = guard.ino;
        let pages = guard.pages.clone();
        mem::forget(guard);

        let mut by_ino = self.by_ino.lock().await;
        let range_lock = by_ino.get_mut(&ino).unwrap();

        let ranges = &range_lock.used_ranges[..];
        tracing::trace!(?ranges);

        let index = Self::intersection_position(&range_lock.used_ranges[..], &pages).unwrap();
        range_lock.used_ranges.swap_remove(index);
        range_lock.range_signal.notify_all();

        if range_lock.used_ranges.len() == 0 && Arc::strong_count(&range_lock.range_signal) == 1 {
            by_ino.remove(&ino);
        }
    }

    fn page_range(&self, byte_range: Range<u64>) -> Range<u64> {
        let len = byte_range.end.saturating_sub(byte_range.start);
        if len == 0 {
            return 0..0;
        }

        let page_count = (len - 1) / self.page_size + 1;
        let page_offset = byte_range.start / self.page_size;

        page_offset..(page_offset + page_count)
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
