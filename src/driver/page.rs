use crate::driver::Result;
use crate::key::{Bucket, KeyWriter, Ty};
use antidotec::{lwwreg, RawIdent, Transaction};
use std::mem;
use std::ops::Range;
use tracing;

#[derive(Debug)]
pub(crate) struct PageWriter {
    bucket: Bucket,
    page_size: usize,
}

impl PageWriter {
    pub fn new(bucket: Bucket, page_size: usize) -> Self {
        Self { bucket, page_size }
    }

    #[tracing::instrument(skip(self, tx, content))]
    pub async fn write(
        &self,
        tx: &mut Transaction<'_>,
        ino: u64,
        offset: usize,
        content: &[u8],
    ) -> Result<()> {
        let first_page = offset / self.page_size;
        let offset_in_page = offset - first_page * self.page_size;

        let unaligned_len = (self.page_size - offset_in_page).min(content.len());
        let (content, remaining) = content.split_at(unaligned_len);
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
        offset_in_page: usize,
        content: &[u8],
    ) -> Result<()> {
        let end = offset_in_page + content.len();
        assert!(end <= self.page_size);

        let page = Key::new(ino, page);
        let mut page_content = {
            let mut reply = tx.read(self.bucket, vec![lwwreg::get(page)]).await?;
            reply.lwwreg(0).unwrap_or_default()
        };

        if end > page_content.len() {
            page_content.resize(end, 0);
        }
        page_content[offset_in_page..end].copy_from_slice(content);

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
        let writes = content.chunks_exact(self.page_size).map(|chunk| {
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
            content.chunks_exact(self.page_size).remainder(),
        )
        .await?;

        Ok(())
    }

    #[tracing::instrument(skip(self, tx, output))]
    pub async fn read(
        &self,
        tx: &mut Transaction<'_>,
        ino: u64,
        offset: usize,
        len: usize,
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
        self.read_extent(tx, ino, extent_start as u64, remaining_len, output)
            .await?;

        Ok(())
    }

    async fn read_page(
        &self,
        tx: &mut Transaction<'_>,
        ino: u64,
        page: u64,
        offset_in_page: usize,
        len: usize,
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
            output.resize(output.len() + len, 0);
            return Ok(())
        }

        let overlapping = intersect_range(0..page_content.len(), offset_in_page..end);
        output.extend_from_slice(&page_content[overlapping]);
        Ok(())
    }

    async fn read_extent(
        &self,
        tx: &mut Transaction<'_>,
        ino: u64,
        extent_start: u64,
        len: usize,
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
                output.resize(output.len() + self.page_size, 0);
                remaining -= self.page_size;
            } else {
                output.extend_from_slice(&content[..content.len()]);
                remaining -= content.len();
            }

            page_index += 1;
            tracing::info!(?page_index, remaining);
        }

        if remaining > 0 {
            let content = reply.lwwreg(page_index).unwrap_or_default();
            output.extend_from_slice(&content[..remaining.min(content.len())]);
        }

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

fn intersect_range(lhs: Range<usize>, rhs: Range<usize>) -> Range<usize> {
    if lhs.end < rhs.start || rhs.end < lhs.start {
        return 0..0;
    }

    lhs.start.max(rhs.start)..lhs.end.min(rhs.end)
}
