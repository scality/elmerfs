use antidotec::{Bytes, BytesMut};

#[derive(Debug, PartialEq, Eq)]
pub struct WritePayload {
    pub start_offset: u64,
    pub bytes: Bytes,
}

#[derive(Debug, Clone, Default)]
pub struct WriteBuffer {
    bytes: BytesMut,
    limit: u64,
    start_offset: u64,
}

impl WriteBuffer {
    pub fn new(limit: u64) -> Self {
        Self {
            bytes: BytesMut::with_capacity(limit as usize),
            limit,
            start_offset: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    pub fn try_append(&mut self, offset: u64, payload: &[u8]) -> Option<WritePayload> {
        let was_empty = self.is_empty();
        if was_empty {
            self.start_offset = offset;
        }

        let buffered_end = self.start_offset + self.bytes.len() as u64;
        let write_end = offset + payload.len() as u64;

        if buffered_end == offset {
            self.bytes.extend_from_slice(&payload);

            if !was_empty && self.bytes.len() as u64 > self.limit {
                return Some(self.flush());
            } else {
                return None;
            }
        }

        /* If we are requested to write an already gathered part, simply
           overwrite */
        if offset >= self.start_offset && write_end <= buffered_end {
            self.bytes[offset as usize..write_end as usize].copy_from_slice(payload);
            return None;
        }

        /* Otherwise, we consider the part as being discontiguous and we won't
        gather it. */
        let previous_flush = self.flush();
        let gathered = self.try_append(offset, payload);
        assert_eq!(gathered, None);

        Some(previous_flush)
    }

    pub fn flush(&mut self) -> WritePayload {
        let written = self.bytes.split().freeze();
        self.bytes.reserve(self.limit as usize);

        let start_offset = self.start_offset;
        self.start_offset = 0;

        WritePayload {
            start_offset,
            bytes: written,
        }
    }
}
