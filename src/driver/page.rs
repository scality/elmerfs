use crate::driver::Result;
use crate::key::{Bucket, KeyWriter, Ty};
use antidotec::{lwwreg, Bytes, RawIdent, Transaction};
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

    pub async fn write(
        &self,
        tx: &mut Transaction<'_>,
        ino: u64,
        offset: u64,
        content: &[u8],
    ) -> Result<()> {
        let byte_range = offset..(offset + content.len() as u64);

        let pages = self.page_range(&byte_range);
        let remaining_pages = (pages.start + 1)..pages.end;
        let offset = byte_range.start - pages.start * self.page_size;
        tracing::debug!(?byte_range, ?pages, ?remaining_pages, ?offset);

        let head_len = (self.page_size - offset).min(content.len() as u64);
        let (head, remaining) = content.split_at(head_len as usize);

        self.write_page(tx, ino, pages.start, offset, head).await?;

        if !remaining.is_empty() {
            self.write_extent(tx, ino, remaining_pages.start, remaining)
                .await?;
        }

        Ok(())
    }

    #[tracing::instrument(skip(self, tx, content))]
    async fn write_page(
        &self,
        tx: &mut Transaction<'_>,
        ino: u64,
        page: u64,
        offset: u64,
        content: &[u8],
    ) -> Result<()> {
        assert!(content.len() as u64 <= self.page_size);
        let write_range = offset..(offset + content.len() as u64);
        tracing::debug!(?write_range);

        let page = Key::new(ino, page);
        let mut page_content = {
            let mut reply = tx.read(self.bucket, vec![lwwreg::get(page)]).await?;
            reply.lwwreg(0).unwrap_or_default().to_vec()
        };

        let previous_len = page_content.len();
        if write_range.end > page_content.len() as u64 {
            tracing::debug!(
                previous_len,
                extended = (write_range.end - previous_len as u64)
            );
            page_content.resize(write_range.end as usize, 0);
        }

        page_content[write_range.start as usize..write_range.end as usize].copy_from_slice(content);
        tx.update(
            self.bucket,
            vec![lwwreg::set(page, Bytes::from(page_content))],
        )
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
            assert!(chunk.len() == self.page_size as usize);
            let write = lwwreg::set(Key::new(ino, page), Bytes::copy_from_slice(chunk));
            page += 1;

            write
        });

        tx.update(self.bucket, writes).await?;

        let remaining = content.chunks_exact(self.page_size as usize).remainder();

        if !remaining.is_empty() {
            self.write_page(tx, ino, page, 0, remaining).await?;
        }

        Ok(())
    }

    pub async fn read(
        &self,
        tx: &mut Transaction<'_>,
        ino: u64,
        offset: u64,
        len: u64,
        output: &mut Vec<u8>,
    ) -> Result<()> {
        let byte_range = offset..(offset + len);
        let pages = self.page_range(&byte_range);
        let remaining_pages = (pages.start + 1)..pages.end;
        let offset = byte_range.start - pages.start * self.page_size;
        tracing::debug!(?byte_range, ?pages, ?remaining_pages, ?offset);

        let head_len = (self.page_size - offset).min(len);
        self.read_page(tx, ino, pages.start as u64, offset, head_len, output)
            .await?;
        assert_eq!(output.len(), head_len as usize);

        let remaining_len = len.saturating_sub(head_len);

        if remaining_len > 0 {
            self.read_extent(tx, ino, remaining_pages, remaining_len, output)
                .await?;
        }

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

        if page_content.is_empty() {
            output.resize(output.len() + len as usize, 0);
            return Ok(());
        }

        let page = 0..page_content.len();
        let read = offset_in_page..end;
        let overlapping = intersect_range(0..page_content.len() as u64, offset_in_page..end);
        output
            .extend_from_slice(&page_content[overlapping.start as usize..overlapping.end as usize]);

        let padding = read.end.saturating_sub(page.end as u64).min(len);
        if padding > 0 {
            output.resize(output.len() + padding as usize, 0);
        }

        Ok(())
    }

    async fn read_extent(
        &self,
        tx: &mut Transaction<'_>,
        ino: u64,
        pages: Range<u64>,
        len: u64,
        output: &mut Vec<u8>,
    ) -> Result<()> {
        let reads = pages.clone().map(|page| lwwreg::get(Key::new(ino, page)));
        let mut reply = tx.read(self.bucket, reads).await?;

        let mut page_index = 0;
        let mut remaining = len;
        while remaining >= self.page_size {
            let content = reply.lwwreg(page_index as usize).unwrap_or_default();
            if content.is_empty() {
                output.resize(output.len() + self.page_size as usize, 0);
                remaining -= self.page_size;
            } else {
                output.extend_from_slice(&content[..content.len()]);

                let padding = self.page_size.checked_sub(content.len() as u64).unwrap();
                output.resize(output.len() + padding as usize, 0);
                remaining -= self.page_size;
            }

            page_index += 1;
        }

        if remaining > 0 {
            let content = reply.lwwreg(page_index as usize).unwrap_or_default();
            output.extend_from_slice(&content[..remaining.min(content.len() as u64) as usize]);
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
