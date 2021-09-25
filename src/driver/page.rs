use crate::collections::Lru;
use crate::driver::Result;
use crate::key::{Bucket, KeyWriter, Ty};
use antidotec::{lwwreg, Bytes, BytesMut, RawIdent, Transaction};
use async_trait::async_trait;
use std::hash::Hash;

use super::buffer::WriteSlice;

/// A handle to a transaction. This is useful for when a PageStore doesn't have any
/// notion of transaction. Having this as an associated type would be a better model
/// but would require GAT.
pub enum TransactionHandle<'a, 'conn> {
    #[cfg(test)]
    Dangling,
    Active(&'a mut Transaction<'conn>),
}

impl<'a, 'conn> TransactionHandle<'a, 'conn> {
    fn get(&mut self) -> &mut Transaction<'conn> {
        match self {
            #[cfg(test)]
            Self::Dangling => panic!("dangling"),
            Self::Active(tx) => tx,
        }
    }

    #[cfg(test)]
    fn dangling() -> Self {
        TransactionHandle::Dangling
    }
}

impl<'a, 'conn> From<&'a mut Transaction<'conn>> for TransactionHandle<'a, 'conn> {
    fn from(tx: &'a mut Transaction<'conn>) -> Self {
        TransactionHandle::Active(tx)
    }
}

pub(crate) struct PageDriver<S: PageStore> {
    ino: u64,
    bucket: Bucket,
    page_size: u64,
    store: S,
}

impl<S: PageStore> PageDriver<S> {
    pub fn new(ino: u64, bucket: Bucket, page_size: u64, store: S) -> Self {
        Self {
            ino,
            bucket,
            page_size,
            store,
        }
    }

    pub async fn write(
        &mut self,
        mut tx: TransactionHandle<'_, '_>,
        slices: &[WriteSlice],
    ) -> Result<()> {
        let normalized_slices = normalize_slices(slices, self.page_size);

        let mut slices_range = 0..0;
        while slices_range.start < normalized_slices.len() {
            let page_id = self.page_id(normalized_slices[slices_range.start].offset);

            /* Get the number of slices sharing the same page id. */
            slices_range.end = slices_range.start + normalized_slices[slices_range.start..]
                .iter()
                .take_while(|s| self.page_id(s.offset) == page_id)
                .count();

            let page_key = Key::new(self.ino, page_id);
            let mut page = self
                .store
                .read(self.bucket, &mut tx, page_key)
                .await?
                .to_vec();

            let write_end = {
                let slice = &normalized_slices[slices_range.end - 1];
                slice.offset + slice.len()
            };

            let page_offset = page_id * self.page_size;
            let new_len = (write_end - page_offset).max(page.len() as u64);
            page.resize(new_len as usize, 0);

            for slice in &normalized_slices[slices_range.clone()] {
                let offset_in_page = slice.offset % self.page_size;
                let end_in_page = offset_in_page+ slice.len();
                page[offset_in_page as usize..end_in_page as usize]
                    .copy_from_slice(&slice.buffer[..]);
            }

            self.store
                .write(self.bucket, &mut tx, page_key, Bytes::from(page))
                .await?;

            slices_range.start = slices_range.end;
        }

        Ok(())
    }

    pub async fn read(
        &mut self,
        mut tx: TransactionHandle<'_, '_>,
        offset: u64,
        len: u64,
        output: &mut BytesMut,
    ) -> Result<()> {
        let end = offset + len;
        let mut current_offset = offset;
        while current_offset < end {
            let id = self.page_id(current_offset);
            let page_key = Key::new(self.ino, id);

            let offset_in_page = current_offset % self.page_size;
            let chunk = (end - current_offset).min(self.page_size - offset_in_page);
            let end_in_page = offset_in_page + chunk;

            let page = self.store.read(self.bucket, &mut tx, page_key).await?;

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
        &mut self,
        mut tx: TransactionHandle<'_, '_>,
        new_size: u64,
        old_size: u64,
    ) -> Result<()> {
        assert!(new_size <= old_size);
        let tail_page_id = self.page_id(new_size);
        let tail_page_key = Key::new(self.ino, tail_page_id);
        let mut tail_page = self.store.read(self.bucket, &mut tx, tail_page_key).await?;

        tail_page.truncate((new_size % self.page_size) as usize);
        self.store
            .write(self.bucket, &mut tx, tail_page_key, tail_page)
            .await?;

        let remove_start = tail_page_id + 1;
        let remove_end = self.page_id(old_size) + 1;
        let removed_keys = (remove_start..remove_end)
            .map(|id| Key::new(self.ino, id))
            .collect::<Vec<_>>();
        self.store
            .remove_all(self.bucket, &mut tx, removed_keys)
            .await?;

        Ok(())
    }

    pub fn invalidate_cache(&mut self) -> Result<()> {
        self.store.invalidate_cache()
    }

    fn page_id(&self, offset: u64) -> u64 {
        offset / self.page_size
    }
}

/// Creates a new buffer of slices with the guarantee that no slice
/// span more than one page.
fn normalize_slices(slices: &[WriteSlice], page_size: u64) -> Vec<WriteSlice> {
    /* Assumes that slices rarely span more than two pages. Pages are
    already big, this should be quite rare. */
    let mut normalized_slices = Vec::with_capacity(2 * slices.len());

    for slice in slices {
        let mut offset_in_slice = 0;

        while offset_in_slice < slice.buffer.len() as u64 {
            let current_offset = slice.offset + offset_in_slice;
            let offset_in_page = current_offset % page_size;

            let remaining_in_page =
                (page_size - offset_in_page).min(slice.buffer.len() as u64 - offset_in_slice);

            let end_in_slice = offset_in_slice + remaining_in_page;
            normalized_slices.push(WriteSlice {
                buffer: slice
                    .buffer
                    .slice(offset_in_slice as usize..end_in_slice as usize),
                offset: current_offset,
            });
            offset_in_slice += remaining_in_page;
        }
    }

    normalized_slices
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

#[async_trait]
pub(crate) trait PageStore: Send {
    async fn read(
        &mut self,
        bucket: Bucket,
        tx: &mut TransactionHandle<'_, '_>,
        key: Key,
    ) -> Result<Bytes>;

    async fn write(
        &mut self,
        bucket: Bucket,
        tx: &mut TransactionHandle<'_, '_>,
        key: Key,
        bytes: Bytes,
    ) -> Result<()>;

    async fn remove_all(
        &mut self,
        bucket: Bucket,
        tx: &mut TransactionHandle<'_, '_>,
        pages: Vec<Key>,
    ) -> Result<()>;

    fn invalidate_cache(&mut self) -> Result<()> {
        Ok(())
    }
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
        self.current = 0;
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

#[async_trait]
impl PageStore for PageCache {
    async fn read(
        &mut self,
        bucket: Bucket,
        tx: &mut TransactionHandle<'_, '_>,
        key: Key,
    ) -> Result<Bytes> {
        let tx = tx.get();

        let (hit, bytes) = match self.pages.lookup(&key.page) {
            Some(bytes) => (true, bytes.clone()),
            None => {
                let mut reply = tx.read(bucket, std::iter::once(lwwreg::get(key))).await?;
                let bytes = reply.lwwreg(0).unwrap_or_default();

                (false, bytes)
            }
        };

        tracing::debug!(hit, "cache lookup");
        if !hit {
            self.cache(key, bytes.clone())
        }
        Ok(bytes)
    }

    async fn write(
        &mut self,
        bucket: Bucket,
        tx: &mut TransactionHandle<'_, '_>,
        key: Key,
        bytes: Bytes,
    ) -> Result<()> {
        let tx = tx.get();

        self.trim();

        tx.update(bucket, std::iter::once(lwwreg::set(key, bytes.clone())))
            .await?;

        self.cache(key, bytes);

        Ok(())
    }

    async fn remove_all(
        &mut self,
        bucket: Bucket,
        tx: &mut TransactionHandle<'_, '_>,
        pages: Vec<Key>,
    ) -> Result<()> {
        let tx = tx.get();

        for removal in &pages {
            self.pages.remove(&removal.page);
        }

        tx.update(
            bucket,
            pages.into_iter().map(|p| lwwreg::set(p, Bytes::new())),
        )
        .await?;

        Ok(())
    }

    fn invalidate_cache(&mut self) -> Result<()> {
        self.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    const INO: u64 = 0x1;
    const PAGE_SIZE: u64 = 4;

    struct HashPageStore {
        pages: HashMap<Key, Bytes>,
    }

    impl HashPageStore {
        fn new() -> Self {
            HashPageStore {
                pages: HashMap::new(),
            }
        }

        fn page(&self, id: u64) -> &[u8] {
            &self.pages[&Key::new(INO, id)][..]
        }
    }

    #[async_trait]
    impl PageStore for HashPageStore {
        async fn read(
            &mut self,
            _bucket: Bucket,
            _tx: &mut TransactionHandle<'_, '_>,
            key: Key,
        ) -> Result<Bytes> {
            Ok(self.pages.get(&key).cloned().unwrap_or_default())
        }

        async fn write(
            &mut self,
            _bucket: Bucket,
            _tx: &mut TransactionHandle<'_, '_>,
            key: Key,
            bytes: Bytes,
        ) -> Result<()> {
            self.pages.insert(key, bytes);
            Ok(())
        }

        async fn remove_all(
            &mut self,
            _bucket: Bucket,
            _tx: &mut TransactionHandle<'_, '_>,
            pages: Vec<Key>,
        ) -> Result<()> {
            for page_key in pages {
                self.pages.remove(&page_key);
            }

            Ok(())
        }
    }

    fn new_driver() -> PageDriver<HashPageStore> {
        let store = HashPageStore::new();
        PageDriver::new(INO, Bucket::new(0), PAGE_SIZE, store)
    }

    fn new_slice(offset: u64, buffer: impl Into<Bytes>) -> WriteSlice {
        WriteSlice {
            offset,
            buffer: buffer.into(),
        }
    }

    fn tx() -> TransactionHandle<'static, 'static> {
        TransactionHandle::dangling()
    }

    #[test]
    fn test_normalize_already_normalized() {
        let slices = vec![
            new_slice(0, vec![0]),
            new_slice(1, vec![1]),
            new_slice(2, vec![2]),
            new_slice(3, vec![3]),
        ];

        let expected = slices.clone();
        let normalized = normalize_slices(&slices, PAGE_SIZE);
        assert_eq!(expected, normalized);
    }

    #[test]
    fn test_normalize_spanning_two() {
        let slices = vec![new_slice(0, vec![0, 1, 2, 3, 4, 5])];

        let expected = vec![
            new_slice(0, vec![0, 1, 2, 3]),
            new_slice(4, vec![4, 5])
        ];
        let normalized = normalize_slices(&slices, PAGE_SIZE);
        assert_eq!(expected, normalized);
    }

    #[test]
    fn test_normalize_gapped() {
        let slices = vec![
            new_slice(0, vec![0, 1, 2, 3, 4]),
            new_slice(16, vec![0, 1, 2, 3, 4]),
        ];

        let expected = vec![
            new_slice(0, vec![0, 1, 2, 3]),
            new_slice(4, vec![4]),

            new_slice(16, vec![0, 1, 2, 3]),
            new_slice(20, vec![4]),
        ];
        let normalized = normalize_slices(&slices, PAGE_SIZE);
        assert_eq!(expected, normalized);
    }

    #[test]
    fn test_normalize_empty() {
        let empty = vec![];
        let expected = empty.clone();
        assert_eq!(expected, normalize_slices(&empty, PAGE_SIZE));
    }

    #[async_std::test]
    async fn test_write_seq() -> Result<()> {
        let mut driver = new_driver();

        let writes = vec![
            new_slice(0, vec![0, 1, 2]),
            new_slice(3, vec![3, 4, 5]),
        ];
        driver.write(tx(), &writes).await?;


        assert_eq!(driver.store.page(0), vec![0, 1, 2, 3]);
        assert_eq!(driver.store.page(1), vec![4, 5]);

        Ok(())
    }

    #[async_std::test]
    async fn test_write_gapped() -> Result<()> {
        let mut driver = new_driver();

        let writes = vec![
            new_slice(0, vec![0, 1, 2]),
            new_slice(14, vec![3, 4, 5]),
        ];
        driver.write(tx(), &writes).await?;

        assert_eq!(driver.store.page(0), vec![0, 1, 2]);
        assert_eq!(driver.store.page(3), vec![0, 0, 3, 4]);
        assert_eq!(driver.store.page(4), vec![5]);

        Ok(())
    }

    #[async_std::test]
    async fn test_overwrite() -> Result<()> {
        let mut driver = new_driver();

        let writes = vec![
            new_slice(0, vec![0, 1, 2]),
            new_slice(14, vec![3, 4, 5]),
        ];
        driver.write(tx(), &writes).await?;

        let writes = vec![
            new_slice(3, vec![2, 3]),
            new_slice(12, vec![6, 7, 8]),
        ];
        driver.write(tx(), &writes).await?;

        assert_eq!(driver.store.page(0), vec![0, 1, 2, 2]);
        assert_eq!(driver.store.page(1), vec![3]);
        assert_eq!(driver.store.page(3), vec![6, 7, 8, 4]);
        assert_eq!(driver.store.page(4), vec![5]);

        Ok(())
    }

    #[async_std::test]
    async fn test_write_aligned() -> Result<()> {
        let mut driver = new_driver();

        let writes = vec![
            new_slice(0, vec![0, 1, 2, 3]),
            new_slice(4, vec![0, 1, 2, 3]),
        ];
        driver.write(tx(), &writes).await?;

        assert_eq!(driver.store.page(0), vec![0, 1, 2, 3]);
        assert_eq!(driver.store.page(1), vec![0, 1, 2, 3]);

        Ok(())
    }


    macro_rules! assert_read {
        ($driver:expr, $off:expr, $len:expr, $result:expr) => {
            let mut output = BytesMut::with_capacity(4);
            $driver.read(tx(), $off, $len, &mut output).await?;

            assert_eq!(&output[..], $result);
        };
    }


    #[async_std::test]
    async fn test_read_exact() -> Result<()> {
        let mut driver = new_driver();

        let writes = vec![
            new_slice(0, vec![0, 1, 2, 3]),
        ];
        driver.write(tx(), &writes).await?;

        assert_read!(driver, 0, 4, vec![0, 1, 2, 3]);

        Ok(())
    }

    #[async_std::test]
    async fn test_read_more() -> Result<()> {
        let mut driver = new_driver();

        let writes = vec![
            new_slice(0, vec![0, 1, 2, 3]),
        ];
        driver.write(tx(), &writes).await?;

        assert_read!(driver, 0, 6, vec![0, 1, 2, 3, 0, 0]);

        Ok(())
    }

    #[async_std::test]
    async fn test_read_unaligned() -> Result<()> {
        let mut driver = new_driver();

        let writes = vec![
            new_slice(2, vec![2, 3]),
            new_slice(4, vec![2, 3]),
        ];
        driver.write(tx(), &writes).await?;

        assert_read!(driver, 3, 4, vec![3, 2, 3, 0]);

        Ok(())
    }

    #[async_std::test]
    async fn test_read_not_written() -> Result<()> {
        let mut driver = new_driver();

        assert_read!(driver, 0, 4, vec![0, 0, 0, 0]);

        Ok(())
    }

    #[async_std::test]
    async fn test_truncate_down() -> Result<()> {
        let mut driver = new_driver();

        let new_size = 0;
        let old_size = 0;
        driver.truncate(tx(), new_size, old_size).await?;

        assert_read!(driver, 0, 4, vec![0, 0, 0, 0]);

        let writes = vec![
            new_slice(0, vec![0, 1, 2, 3]),
            new_slice(4, vec![0, 1, 2, 3]),
        ];
        driver.write(tx(), &writes).await?;

        let new_size = 4;
        let old_size = 8;
        driver.truncate(tx(), new_size, old_size).await?;

        assert_read!(driver, 2, 4, vec![2, 3, 0, 0]);
        assert_read!(driver, 0, 4, vec![0, 1, 2, 3]);

        let new_size = 2;
        let old_size = 4;
        driver.truncate(tx(), new_size, old_size).await?;
        assert_read!(driver, 0, 4, vec![0, 1, 0, 0]);

        Ok(())
    }

    #[async_std::test]
    async fn test_truncate_down_on_gapped() -> Result<()> {
        let mut driver = new_driver();

        let writes = vec![
            new_slice(0, vec![0, 1, 2, 3]),
            new_slice(8, vec![0, 1, 2, 3]),
        ];
        driver.write(tx(), &writes).await?;

        let new_size = 6;
        let old_size = 12;
        driver.truncate(tx(), new_size, old_size).await?;

        assert_read!(driver, 11, 4, vec![0, 0, 0, 0]);
        assert_read!(driver, 0, 5, vec![0, 1, 2, 3, 0]);

        Ok(())
    }
}
