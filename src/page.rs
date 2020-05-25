use crate::driver::Result;
use crate::key::{self, Bucket, Key};
use antidotec::{lwwreg, RawIdent, Transaction};
use std::mem;
use tracing;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
struct ExtentId {
    ino: u64,
    offset: u64,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
struct PageKey(Key<ExtentId>);

impl PageKey {
    fn with_offset(ino: u64, offset: u64) -> Self {
        Self(Key {
            kind: key::Kind::Page,
            payload: ExtentId { ino, offset },
        })
    }
}

impl Into<RawIdent> for PageKey {
    fn into(self) -> RawIdent {
        let mut ident = RawIdent::with_capacity(1 + 2 * mem::size_of::<u64>());
        ident.push(self.0.kind as u8);

        ident.extend_from_slice(&self.0.payload.ino.to_le_bytes()[..]);
        ident.extend_from_slice(&self.0.payload.offset.to_le_bytes()[..]);

        ident
    }
}

#[derive(Debug)]
pub(crate) struct PageDriver {
    bucket: Bucket,
    page_size: usize,
}

impl PageDriver {
    pub(crate) fn new(bucket: Bucket, page_size: usize) -> Self {
        Self { bucket, page_size }
    }

    #[tracing::instrument(skip(self, tx))]
    pub(crate) async fn write(
        &self,
        tx: &mut Transaction<'_>,
        ino: u64,
        bytes: &[u8],
        byte_offset: usize,
    ) -> Result<usize> {
        let page_offset = byte_offset / self.page_size as usize;
        let offset_in_page = byte_offset - page_offset * self.page_size;

        let mut remaining = &bytes[..];
        {
            let end = self.page_size.min(remaining.len());
            self.write_page(tx, ino, page_offset, offset_in_page, &remaining[..end])
                .await?;
            remaining = &remaining[end..];
        }

        let mut page_offset = page_offset + 1;
        while remaining.len() > 0 {
            let offset_in_page = 0;
            let end = self.page_size.min(remaining.len());

            self.write_page(tx, ino, page_offset, offset_in_page, &remaining[..end])
                .await?;
            remaining = &remaining[end..];
            page_offset += 1;
        }

        let new_len = (page_offset as i32 - 2).max(0) as usize * self.page_size
            + bytes.len() % self.page_size;
        Ok(new_len)
    }

    #[tracing::instrument(skip(self, tx))]
    pub(crate) async fn read(
        &self,
        tx: &mut Transaction<'_>,
        ino: u64,
        bytes: &mut Vec<u8>,
        byte_offset: usize,
        len: usize,
    ) -> Result<()> {
        let page_offset = byte_offset / self.page_size as usize;
        let offset_in_page = byte_offset - page_offset * self.page_size;

        let mut remaining = len;
        {
            let len = self.page_size.min(remaining);
            self.read_page(tx, ino, page_offset, offset_in_page, bytes, len)
                .await?;
            remaining -= len;
        }

        let mut page_offset = page_offset + 1;
        while remaining > 0 {
            let offset_in_page = 0;
            let len = self.page_size.min(remaining);

            self.read_page(tx, ino, page_offset, offset_in_page, bytes, len)
                .await?;
            remaining -= len;
            page_offset += 1;
        }

        Ok(())
    }

    #[tracing::instrument(skip(self, tx))]
    async fn write_page(
        &self,
        tx: &mut Transaction<'_>,
        ino: u64,
        page_offset: usize,
        offset_in_page: usize,
        bytes: &[u8],
    ) -> Result<()> {
        assert!(bytes.len() <= self.page_size);

        let key = PageKey::with_offset(ino, page_offset as u64);

        // If we overwrite everything don't bother reading previous value
        if bytes.len() == self.page_size {
            assert_eq!(offset_in_page, 0);

            tx.update(self.bucket, vec![lwwreg::set(key, bytes.into())])
                .await?;

            return Ok(());
        }

        let mut reply = tx.read(self.bucket, vec![lwwreg::get(key)]).await?;
        let mut reg = match reply.lwwreg(0) {
            Some(reg) => reg,
            None => vec![0; self.page_size],
        };

        reg[offset_in_page..offset_in_page + bytes.len()].copy_from_slice(bytes);

        tx.update(self.bucket, vec![lwwreg::set(key, bytes.into())])
            .await?;

        Ok(())
    }

    #[tracing::instrument(skip(self, tx))]
    async fn read_page(
        &self,
        tx: &mut Transaction<'_>,
        ino: u64,
        page_offset: usize,
        offset_in_page: usize,
        bytes: &mut Vec<u8>,
        len: usize,
    ) -> Result<()> {
        assert!(len <= self.page_size);

        let key = PageKey::with_offset(ino, page_offset as u64);

        let mut reply = tx.read(self.bucket, vec![lwwreg::get(key)]).await?;

        match reply.lwwreg(0) {
            Some(reg) => {
                let len = len.min(reg.len());
                bytes.extend_from_slice(&reg[..len]);
            }
            None => {
                bytes.resize(bytes.len() + len, 0);
            }
        }

        Ok(())
    }
}
