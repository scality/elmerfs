use crate::driver::Result;
use crate::key::{Bucket, KeyWriter, Ty};
use antidotec::{lwwreg, RawIdent, Transaction};
use std::ops::Range;

#[derive(Debug)]
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
        let first_page = offset / self.page_size;
        let offset_in_page = offset - first_page * self.page_size;
        let unaligned_len = (self.page_size - offset_in_page).min(content.len() as u64);
        let (content, remaining) = content.split_at(unaligned_len as usize);

        self.write_page(tx, ino, first_page as u64, offset_in_page, content)
            .await?;

        let extent_start = first_page + 1;

        self.write_extent(tx, ino, extent_start as u64, remaining)
            .await?;

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

        tx.update(self.bucket, writes).await?;
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

    pub async fn read(
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

        let reads = pages.clone().map(|page| lwwreg::get(Key::new(ino, page)));
        let mut reply = tx.read(self.bucket, reads).await?;

        let mut page_index = 0;
        let mut remaining = len;
        while remaining >= self.page_size {
            let content = reply.lwwreg(page_index).unwrap_or_default();

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
        }

        if remaining > 0 {
            let content = reply.lwwreg(page_index).unwrap_or_default();
            output.extend_from_slice(&content[..remaining.min(content.len() as u64) as usize]);
        }

        Ok(())
    }

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

        let first_page = range.start / self.page_size as u64;
        let offset_in_page = range.start - first_page * self.page_size as u64;

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
