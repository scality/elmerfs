use crate::collections::Lru;
use crate::driver::Result;
use crate::key::{Bucket, KeyWriter, Ty};
use antidotec::{lwwreg, Bytes, RawIdent, Transaction};
use std::hash::Hash;
use std::ops::Range;

#[derive(Debug, Copy, Clone)]
pub(crate) struct PageWriter {
    bucket: Bucket,
    page_size: u64,
}

impl PageWriter {
    pub fn new(bucket: Bucket, page_size: u64) -> Self {
        Self { bucket, page_size }
    }

    pub fn page_size(&self) -> u64 {
        self.page_size
    }

    pub async fn write(
        &self,
        tx: &mut Transaction<'_>,
        pages: &mut PageCache,
        ino: u64,
        offset: u64,
        content: Bytes,
    ) -> Result<()> {
        let byte_range = offset..(offset + content.len() as u64);

        let extent = self.page_range(&byte_range);
        let remaining_pages = (extent.start + 1)..extent.end;
        let offset = byte_range.start - extent.start * self.page_size;
        tracing::debug!(?byte_range, ?extent, ?remaining_pages, ?offset);

        let head_len = (self.page_size - offset).min(content.len() as u64);
        let (head, remaining) = content.split_at(head_len as usize);

        self.write_page(tx, pages, ino, extent.start, offset, head).await?;

        if !remaining.is_empty() {
            self.write_extent(tx, pages, ino, remaining_pages.start, remaining)
                .await?;
        }

        Ok(())
    }

    #[tracing::instrument(skip(self, tx, pages, content))]
    async fn write_page(
        &self,
        tx: &mut Transaction<'_>,
        pages: &mut PageCache,
        ino: u64,
        page: u64,
        offset: u64,
        content: &[u8],
    ) -> Result<()> {
        assert!(content.len() as u64 <= self.page_size);
        let write_range = offset..(offset + content.len() as u64);
        tracing::debug!(?write_range);

        let page = Key::new(ino, page);


        let page_content = pages.read(self.bucket, tx, page).await?;

        let mut new_page = page_content.to_vec();
        let previous_len = page_content.len();
        if write_range.end > page_content.len() as u64 {
            tracing::debug!(
                previous_len,
                extended = (write_range.end - previous_len as u64)
            );
            new_page.resize(write_range.end as usize, 0);
        }

        new_page[write_range.start as usize..write_range.end as usize].copy_from_slice(content);
        pages.write(self.bucket, tx, page, Bytes::from(new_page)).await
    }

    async fn write_extent(
        &self,
        tx: &mut Transaction<'_>,
        pages: &mut PageCache,
        ino: u64,
        extent_start: u64,
        content: &[u8],
    ) -> Result<()> {
        let content = Bytes::copy_from_slice(content);
        let mut page = extent_start;
        for chunk in content.chunks_exact(self.page_size as usize) {
            assert!(chunk.len() == self.page_size as usize);

            pages.write(self.bucket, tx, Key::new(ino, page), content.slice_ref(chunk)).await?;
            page += 1;
        };

        let remaining = content.chunks_exact(self.page_size as usize).remainder();

        if !remaining.is_empty() {
            self.write_page(tx, pages, ino, page, 0, remaining).await?;
        }

        Ok(())
    }

    #[tracing::instrument(skip(tx, pages, output))]
    pub async fn read(
        &self,
        tx: &mut Transaction<'_>,
        pages: &mut PageCache,
        ino: u64,
        offset: u64,
        len: u64,
        output: &mut Vec<u8>,
    ) -> Result<()> {
        let byte_range = offset..(offset + len);
        let extent = self.page_range(&byte_range);
        let remaining_pages = (extent.start + 1)..extent.end;
        let offset = byte_range.start - extent.start * self.page_size;
        tracing::debug!(?byte_range, ?extent, ?remaining_pages, ?offset);

        let head_len = (self.page_size - offset).min(len);
        self.read_page(tx, pages, ino, extent.start as u64, offset, head_len, output)
            .await?;
        assert_eq!(output.len(), head_len as usize);

        let remaining_len = len.saturating_sub(head_len);

        if remaining_len > 0 {
            self.read_extent(tx, pages, ino, remaining_pages, remaining_len, output)
                .await?;
        }

        Ok(())
    }

    #[tracing::instrument(skip(tx, pages, output))]
    async fn read_page(
        &self,
        tx: &mut Transaction<'_>,
        pages: &mut PageCache,
        ino: u64,
        page: u64,
        offset_in_page: u64,
        len: u64,
        output: &mut Vec<u8>,
    ) -> Result<()> {
        tracing::debug!(?page);
        let end = offset_in_page + len;
        assert!(end <= self.page_size);

        let page = Key::new(ino, page);
        let page_content = pages.read(self.bucket, tx, page).await?;

        if page_content.is_empty() {
            tracing::debug!("empty");
            output.resize(output.len() + len as usize, 0);
            return Ok(());
        }

        let page = 0..page_content.len();
        let read = offset_in_page..end;
        let overlapping = intersect_range(0..page_content.len() as u64, offset_in_page..end);
        tracing::debug!(page_len = ?page, ?read, ?overlapping);
        output
            .extend_from_slice(&page_content[overlapping.start as usize..overlapping.end as usize]);

        let padding = read.end.saturating_sub(page.end as u64).min(len);
        if padding > 0 {
            output.resize(output.len() + padding as usize, 0);
        }

        Ok(())
    }

    #[tracing::instrument(skip(tx, pages, output))]
    async fn read_extent(
        &self,
        tx: &mut Transaction<'_>,
        pages: &mut PageCache,
        ino: u64,
        extent: Range<u64>,
        len: u64,
        output: &mut Vec<u8>,
    ) -> Result<()> {
        tracing::debug!("extent");

        let mut page = extent.start;
        let mut remaining = len;
        while remaining >= self.page_size {
            let content = pages.read(self.bucket, tx, Key::new(ino, page)).await?;

            if content.is_empty() {
                output.resize(output.len() + self.page_size as usize, 0);
                remaining -= self.page_size;
            } else {
                output.extend_from_slice(&content[..content.len()]);

                let padding = self.page_size.checked_sub(content.len() as u64).unwrap();
                output.resize(output.len() + padding as usize, 0);
                remaining -= self.page_size;
            }

            page += 1;
        }

        if remaining > 0 {
            let content = pages.read(self.bucket, tx, Key::new(ino, page)).await?;
            output.extend_from_slice(&content[..remaining.min(content.len() as u64) as usize]);

//            let padding = remaining.saturating_sub(content.len() as u64);
//            output.resize(output.len() + padding as usize, 0);
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
