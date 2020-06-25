use crate::driver::Result;
use crate::key::{self, Bucket, Key};
use antidotec::{lwwreg, RawIdent, Transaction};
use std::mem;
use std::ops::Range;
use tracing::{self, debug};

#[derive(Debug)]
pub(crate) struct PageDriver {
    bucket: Bucket,
    page_size: usize,
}

impl PageDriver {
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

        debug!(first_page, offset_in_page, len = unaligned_len);
        self.write_page(tx, ino, first_page as u64, offset_in_page, content)
            .await?;

        let extent_start = first_page + 1;
        debug!(extent_start, len = remaining.len());
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

        let page = PageKey::new(ino, page);
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
            let write = lwwreg::set(PageKey::new(ino, page), chunk.into());
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

        let unaligned_len = (self.page_size - offset_in_page).min(len);
        debug!(first_page, offset_in_page, len = unaligned_len);
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
        debug!(extent_start, len = remaining_len);
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

        let page = PageKey::new(ino, page);
        let page_content = {
            let mut reply = tx.read(self.bucket, vec![lwwreg::get(page)]).await?;
            reply.lwwreg(0).unwrap_or_default()
        };

        let overlapping =
            intersect_range(0..page_content.len(), offset_in_page..end);
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
        let extent_len = len.saturating_sub(1) / self.page_size + 1;
        let pages = extent_start..(extent_start + extent_len as u64);

        let reads = pages
            .clone()
            .map(|page| lwwreg::get(PageKey::new(ino, page)));
        let mut reply = tx.read(self.bucket, reads).await?;

        let mut page_index = 0;
        let mut remaining = len;
        while remaining > self.page_size {
            let content = reply.lwwreg(page_index).unwrap_or_default();
            output.extend_from_slice(&content[..self.page_size.min(content.len())]);

            remaining -= self.page_size;
            page_index += 1;
        }

        if remaining > 0 {
            let content = reply.lwwreg(page_index).unwrap_or_default();
            output.extend_from_slice(&content[..remaining.min(content.len())]);
        }

        Ok(())
    }
}

#[derive(Debug, Copy, Clone)]
pub struct PageKey(Key<(u64, u64)>);

impl PageKey {
    pub fn new(ino: u64, page: u64) -> Self {
        Self(Key {
            kind: key::Kind::Page,
            payload: (ino, page),
        })
    }
}

impl Into<RawIdent> for PageKey {
    fn into(self) -> RawIdent {
        let Self(Key {
            payload: (ino, page),
            kind,
        }) = self;

        let mut ident = RawIdent::with_capacity(1 + 2 * mem::size_of::<u64>());
        ident.push(kind as u8);
        ident.extend_from_slice(&ino.to_le_bytes()[..]);
        ident.extend_from_slice(&page.to_le_bytes()[..]);

        ident
    }
}

fn intersect_range(lhs: Range<usize>, rhs: Range<usize>) -> Range<usize> {
    if lhs.end < rhs.start || rhs.end < lhs.start {
        return 0..0;
    }

    lhs.start.max(rhs.start)..lhs.end.min(rhs.end)
}
