use crate::collections::Lru;
use crate::driver::Result;
use crate::key::{Bucket, KeyWriter, Ty};
use antidotec::{lwwreg, Bytes, BytesMut, RawIdent, Transaction};
use std::hash::Hash;
use std::ops::Range;

use super::buffer::WriteSlice;
use super::PAGE_SIZE;

#[derive(Debug, Copy, Clone)]
pub(crate) struct PageDriver {
    ino: u64,
    bucket: Bucket,
    page_size: u64,
}

impl PageDriver {
    pub fn new(ino: u64, bucket: Bucket, page_size: u64) -> Self {
        Self {
            ino,
            bucket,
            page_size,
        }
    }

    pub fn page_size(&self) -> u64 {
        self.page_size
    }

    pub async fn write(
        &self,
        tx: &mut Transaction<'_>,
        cache: &mut PageCache,
        slices: &[WriteSlice],
    ) -> Result<()> {
        let mut pager = Pager::new(self.page_size, slices);

        while let Some(page_writes) = pager.next_page() {
            debug_assert!(page_writes.len() > 0);
            let id = page_id(page_writes[0].offset);
            let page_key = Key::new(self.ino, id);

            let mut page = cache.read(self.bucket, tx, page_key).await?.to_vec();

            let write_end = page_writes.last().map(|p| p.offset + p.len()).unwrap();
            let new_len = write_end % self.page_size;
            page.resize(new_len as usize, 0);

            for page_write in page_writes {
                let offset_in_page = page_write.offset % self.page_size;
                page[offset_in_page as usize..page_write.len() as usize]
                    .copy_from_slice(&page_write.buffer[..]);
            }

            cache
                .write(self.bucket, tx, page_key, Bytes::from(page))
                .await?;
        }

        Ok(())
    }

    pub async fn read(
        &self,
        tx: &mut Transaction<'_>,
        cache: &mut PageCache,
        offset: u64,
        len: u64,
        output: &mut BytesMut,
    ) -> Result<()> {
        let end = offset + len;
        let mut current_offset = offset;
        while current_offset < end {
            let id = page_id(current_offset);
            let page_key = Key::new(self.ino, id);

            let offset_in_page = current_offset % self.page_size;
            let chunk = (end - current_offset).min(self.page_size - offset_in_page);
            let end_in_page = offset_in_page + chunk;

            let page = cache.read(self.bucket, tx, page_key).await?;

            if offset_in_page >= page.len() as u64 {
                output.resize(output.len() + chunk as usize, 0);
            } else {
                let end = end_in_page.min(page.len() as u64) as usize;
                output.extend_from_slice(&page[offset_in_page as usize..end]);

                let padding = end_in_page.saturating_sub(page.len() as u64);
                output.resize(output.len() + padding as usize, 0);
            }

            current_offset += chunk;
        }

        Ok(())
    }

    pub async fn truncate(
        &self,
        tx: &mut Transaction<'_>,
        cache: &mut PageCache,
        new_size: u64,
        old_size: u64,
    ) -> Result<()> {
        let tail_page_id = page_id(new_size);
        let tail_page_key = Key::new(self.ino, tail_page_id);
        let mut tail_page = cache.read(self.bucket, tx, tail_page_key).await?;

        tail_page.truncate((new_size % self.page_size) as usize);
        cache
            .write(self.bucket, tx, tail_page_key, tail_page)
            .await?;

        let remove_start = tail_page_id + 1;
        let remove_end = page_id(new_size) + 1;
        let removed_keys = (remove_start..remove_end).map(|id| Key::new(self.ino, id));
        cache.remove_all(self.bucket, tx, removed_keys).await?;

        Ok(())
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

    pub fn cached_bytes(&self) -> u64 {
        self.current
    }

    pub fn clear(&mut self) {
        self.pages.clear();
        self.current = 0;
    }

    async fn read(&mut self, bucket: Bucket, tx: &mut Transaction<'_>, key: Key) -> Result<Bytes> {
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

    async fn remove_all<P>(
        &mut self,
        bucket: Bucket,
        tx: &mut Transaction<'_>,
        pages: P,
    ) -> Result<()>
    where
        P: IntoIterator<Item = Key>,
    {
        let removes = pages.into_iter().collect::<Vec<_>>();
        for removal in &removes {
            self.pages.remove(&removal.page);
        }

        tx.update(
            bucket,
            removes.into_iter().map(|p| lwwreg::set(p, Bytes::new())),
        )
        .await?;

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
            current_idx: 0,
        }
    }

    fn next_page(&mut self) -> Option<&[WriteSlice]> {
        if self.slices.len() == self.current_idx {
            return None;
        }

        let current_page_id = page_id(self.slices[self.current_idx].offset);
        let write_range = match self.slices[(self.current_idx + 1)..]
            .iter()
            .position(|w| page_id(w.offset) != current_page_id)
        {
            Some(position) => self.current_idx..position,
            None => self.current_idx..self.slices.len(),
        };

        self.current_idx = write_range.end;
        Some(&self.slices[write_range])
    }
}

fn page_id(offset: u64) -> u64 {
    offset / PAGE_SIZE
}
