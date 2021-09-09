use crate::collections::Lru;
use crate::driver::Result;
use crate::key::{Bucket, KeyWriter, Ty};
use antidotec::{lwwreg, Bytes, RawIdent, Transaction};
use std::fmt::Write;
use std::hash::Hash;
use std::ops::Range;

use super::PAGE_SIZE;
use super::buffer::WriteSlice;

#[derive(Debug, Copy, Clone)]
pub(crate) struct PageIoDriver {
    bucket: Bucket,
    page_size: u64,
}

impl PageIoDriver {
    pub fn new(bucket: Bucket, page_size: u64) -> Self {
        Self {
            bucket,
            cache,
            page_size
        }
    }

    pub fn page_size(&self) -> u64 {
        self.page_size
    }

    pub fn write(&self, tx: &mut Transaction<'_>, cache: &mut PageCache, ino: u64, slices: &[WriteSlice]) -> Result<()> {
        let pager = Pager::new(self.page_size, slices);

        while let Some(page_writes) = pager.next_page() {
            debug_assert!(page_writes.len() > 0);
            let id = page_id(page_writes[0].offset);
        }

        Ok(())
    }

    #[tracing::instrument(skip(self, tx, ino))]
    pub async fn remove(
        &self,
        tx: &mut Transaction<'_>,
        ino: u64,
        byte_range: Range<u64>,
    ) -> Result<()> {
        let pages = self.page_range(&byte_range);
        let remaining_pages = (pages.start + 1)..(pages.end);
        let offset = byte_range.start - pages.start * self.page_size;
        tracing::debug!(?byte_range, ?pages, ?remaining_pages, offset);

        let content_tail = {
            let page_key = Key::new(ino, pages.start);
            let mut reply = tx.read(self.bucket, vec![lwwreg::get(page_key)]).await?;
            let mut content = reply.lwwreg(0).unwrap_or_default();

            content.truncate(offset as usize);
            lwwreg::set(page_key, content)
        };

        let removes = remaining_pages.map(|p| lwwreg::set(Key::new(ino, p), Bytes::new()));

        let updates = std::iter::once(content_tail).chain(removes);
        tx.update(self.bucket, updates).await?;

        Ok(())
    }

    fn page_range(&self, byte_range: &Range<u64>) -> Range<u64> {
        let first = byte_range.start / self.page_size;
        let last = byte_range.end / self.page_size;
        tracing::debug!(first, last);

        first..(last + 1)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct Key {
    pub ino: u64,
    pub page: u64,
}

impl Key {
    pub fn new(ino: u64, page: u64) -> Self {
        Key { ino, page }
    }

    const fn byte_len() -> usize {
        2 * std::mem::size_of::<u64>()
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
pub(crate) struct PageCache {
    pages: Lru<u64, Bytes>,
    current: u64,
    limit: u64,
}

impl PageCache {
    pub(crate) fn new(limit: u64) -> Self {
        Self {
            pages: Lru::new(),
            current: 0,
            limit,
        }
    }

    pub fn clear(&mut self) {
        self.pages.clear();
    }

    async fn read(
        &mut self,
        bucket: Bucket,
        tx: &mut Transaction<'_>,
        key: Key,
    ) -> Result<Bytes> {
        let (hit, bytes) = match self.pages.lookup(&key.page) {
            Some(bytes) => (true, bytes.clone()),
            None => {
                let mut reply = tx.read(bucket, std::iter::once(lwwreg::get(key))).await?;
                let bytes = reply.lwwreg(0).unwrap_or_default();

                (false, bytes)
            }
        };

        if !hit {
            self.cache(key, bytes.clone())
        }
        Ok(bytes)
    }

    async fn write(
        &mut self,
        bucket: Bucket,
        tx: &mut Transaction<'_>,
        key: Key,
        bytes: Bytes,
    ) -> Result<()> {
        self.trim();

        tx.update(bucket, std::iter::once(lwwreg::set(key, bytes.clone())))
            .await?;
        self.cache(key, bytes);

        Ok(())
    }

    fn cache(&mut self, key: Key, bytes: Bytes) {
        self.current += bytes.len() as u64;
        self.pages.insert(key.page, bytes);
    }

    fn trim(&mut self) {
        while self.current > self.limit {
            match self.pages.evict() {
                Some((_, bytes)) => {
                    self.current -= bytes.len() as u64;
                }
                None => return,
            }
        }
    }
}

pub struct Pager<'a> {
    page_size: u64,
    slices: &'a [WriteSlice],
    current_idx: usize,
}


impl<'a> Pager<'a> {
    fn new(page_size: u64, slices: &'a [WriteSlice]) -> Self {
        Pager {
            page_size,
            slices,
            current_idx: 0
        }
    }

    fn next_page(&mut self) -> Option<&'_ [WriteSlice]> {
        if self.slices.len() == self.current_idx {
            return None;
        }

        let current_page_id = page_id(self.slices[self.current_idx].offset);
        let write_range = match self.slices[(self.current_idx + 1)..].iter().position(|w| page_id(w.offset) != current_page_id) {
            Some(position) => self.current_idx..position,
            None => self.current_idx..self.slices.len(),
        };

        self.current_idx = write_range.end;
        Some(&self.slices[write_range])
    }
}

fn page_id(offset: u64) -> u64 {
    (offset / PAGE_SIZE) * PAGE_SIZE
}
