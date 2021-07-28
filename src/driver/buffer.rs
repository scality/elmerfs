use antidotec::{Bytes, BytesMut};

#[derive(Debug, PartialEq, Eq)]
pub enum WriteCommand {
    Gathered,
    Flush { start_offset: u64, bytes: Bytes },
    None,
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

    pub fn try_append(&mut self, offset: u64, payload: &[u8]) -> WriteCommand {
        let was_empty = self.is_empty();
        if was_empty {
            self.start_offset = offset;
        }

        let buffered_end = self.start_offset + self.bytes.len() as u64;
        let write_end = offset + payload.len() as u64;

        if buffered_end == offset {
            self.bytes.extend_from_slice(&payload);

            if !was_empty && self.bytes.len() as u64 > self.limit {
                return self.flush();
            } else {
                return WriteCommand::Gathered;
            }
        }

        /* If we are requested to write an already gathered part, simply
           overwrite */
        if offset >= self.start_offset && write_end <= buffered_end {
            self.bytes[offset as usize..write_end as usize].copy_from_slice(payload);
            return WriteCommand::Gathered;
        }

        /* Otherwise, we consider the part as being discontiguous and we won't
        gather it. */
        let previous_flush = self.flush();
        let gathered = self.try_append(offset, payload);
        assert_eq!(gathered, WriteCommand::Gathered);

        previous_flush
    }

    pub fn flush(&mut self) -> WriteCommand {
        if self.bytes.is_empty() {
            return WriteCommand::None;
        }

        let written = self.bytes.split().freeze();
        let start_offset = self.start_offset;
        self.start_offset = 0;

        WriteCommand::Flush {
            bytes: written,
            start_offset,
        }
    }
}
