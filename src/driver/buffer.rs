use crate::driver::FUSE_MAX_WRITE;
use antidotec::Bytes;
use std::ops::Range;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteSlice {
    pub offset: u64,
    pub buffer: Bytes,
}

impl WriteSlice {
    pub fn len(&self) -> u64 {
        self.buffer.len() as u64
    }
}

#[derive(Debug, Clone, Default)]
pub struct WriteBuffer {
    cost_threshold: u64,
    cost: u64,
    start_offset: u64,
    len: u64,
    writes: Vec<WriteSlice>,
}

impl WriteBuffer {
    pub fn new(bytes_threshold: u64) -> Self {
        Self {
            writes: Vec::with_capacity((bytes_threshold / FUSE_MAX_WRITE).min(1) as usize),
            cost: 0,
            cost_threshold: bytes_threshold,
            len: 0,
            start_offset: 0,
        }
    }

    pub fn push(&mut self, write: WriteSlice) -> Option<Flush<'_>> {
        let write_end = write.offset + write.len();

        if write.offset >= self.start_offset + self.len {
            self.writes.push(write);
        } else if write_end <= self.start_offset {
            self.writes.insert(0, write);
        } else {
            self.push_overlapping(write);
        }

        self.cost += write.len();
        self.start_offset = self.start_offset.min(write.offset);
        self.len = self.len.max(write_end);

        if self.cost > self.cost_threshold {
            Some(self.flush())
        } else {
            None
        }
    }

    fn push_overlapping(&mut self, write: WriteSlice) {
        let write_end = write.offset + write.len();

        /* Remove all buffered write that the new buffer covers */
        self.writes.retain(|other| {
            other.offset < write.offset || (other.offset + other.len()) > write_end
        });

        /* Insert the buffer at the correct position. Then adjust adjacent buffers
        to account for overlaps. */
        let insert_idx = match self
            .writes
            .binary_search_by(|other| other.offset.cmp(&write.offset))
        {
            Ok(index) => index,
            Err(index) => index,
        };

        let (write_offset, write_len) = (write.offset, write.len());
        self.writes.insert(insert_idx, write);

        /* Adjust the buffer that was before us (if any) */
        if insert_idx > 0 {
            let before = &mut self.writes[insert_idx - 1];
            let before_end = before.offset + before.len();

            if before_end > write_offset {
                let new_len = (before.len() - (before_end - write_offset)) as usize;
                before.buffer.truncate(new_len);
            }
        }

        /* Remove all buffered write that we overwrote. */
        if let Some(overlapping) = self.writes.get_mut(insert_idx + 1) {
            assert!(overlapping.offset < write_end);
            let split_off = (write_end - overlapping.offset) as usize;
            overlapping.buffer = overlapping.buffer.split_off(split_off as usize);
        }
    }

    pub fn flush(&mut self) -> Flush<'_> {
        Flush { inner: self }
    }

    fn clear(&mut self) {
        self.writes.clear();
        self.cost = 0;
        self.len = 0;
        self.start_offset = 0;
    }
}

pub struct Flush<'a> {
    inner: &'a mut WriteBuffer,
}

impl Flush<'_> {
    pub fn extent(&self) -> Option<Range<u64>> {
        if self.inner.len > 0 {
            Some(self.inner.start_offset..(self.inner.start_offset + self.inner.len))
        } else {
            None
        }
    }
}

impl std::ops::Deref for Flush<'_> {
    type Target = [WriteSlice];

    fn deref(&self) -> &Self::Target {
        &self.inner.writes
    }
}

impl Drop for Flush<'_> {
    fn drop(&mut self) {
        self.inner.clear();
    }
}
