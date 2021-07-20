pub enum WriteCommand<'b> {
    Gathered,
    Discontiguous,
    Flush { start_offset: u64, bytes: &'b [u8] },
}

#[derive(Debug, Clone, Default)]
pub struct WriteBuffer {
    bytes: Vec<u8>,
    limit: u64,
    start_offset: u64,
}

impl WriteBuffer {
    pub fn new(limit: u64) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
            start_offset: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    pub fn try_append(&mut self, offset: u64, payload: &[u8]) -> WriteCommand<'_> {
        if self.is_empty() {
            self.start_offset = offset;
        }

        let buffered_end = self.start_offset + self.bytes.len() as u64;
        let write_end = offset + payload.len() as u64;

        if buffered_end == offset {
            self.bytes.extend_from_slice(payload);

            if self.bytes.len() as u64 > self.limit {
                return WriteCommand::Flush {
                    bytes: &self.bytes[..],
                    start_offset: self.start_offset,
                };
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
        WriteCommand::Discontiguous
    }

    pub fn gathered_so_far(&self) -> Option<(u64, &[u8])> {
        if self.is_empty() {
            None
        } else {
            Some((self.start_offset, &self.bytes[..]))
        }
    }

    pub fn reset(&mut self) {
        self.bytes.clear();
        self.start_offset = 0;
    }
}
