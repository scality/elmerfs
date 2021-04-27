use async_std::sync::Mutex;
use std::sync::Arc;

const SLOTS_CAPACITY: usize = 128;

pub enum WriteCommand<'b> {
    Gathered,
    Discontiguous,
    Flush { start_offset: u64, bytes: &'b [u8] },
}

#[derive(Debug, Copy, Clone)]
pub struct BufferHandle(pub u64);

#[derive(Debug, Clone, Default)]
pub struct WriteBuffer {
    bytes: Vec<u8>,
    max_len: usize,
    start_offset: u64,
}

impl WriteBuffer {
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

            if self.bytes.len() > self.max_len {
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

enum Slot {
    Free { next_free: Option<u64> },
    Occupied { buffer: Arc<Mutex<WriteBuffer>> },
}

impl Slot {
    fn reserve(&mut self, capacity: usize) -> Option<u64> {
        let bytes = Vec::with_capacity(capacity);
        let occupied = Slot::Occupied {
            buffer: Arc::new(Mutex::new(WriteBuffer {
                bytes,
                start_offset: 0,
                max_len: capacity,
            })),
        };

        match std::mem::replace(self, occupied) {
            Slot::Free { next_free } => next_free,
            _ => panic!("reserving twice"),
        }
    }

    fn release(&mut self, next_free: Option<u64>) -> Arc<Mutex<WriteBuffer>> {
        let free = Slot::Free { next_free };

        match std::mem::replace(self, free) {
            Slot::Occupied { buffer } => buffer,
            _ => panic!("double free"),
        }
    }

    fn as_buffer(&self) -> Arc<Mutex<WriteBuffer>> {
        match self {
            Slot::Occupied { buffer } => buffer.clone(),
            _ => panic!("free"),
        }
    }
}

pub struct WriteGatherer {
    slots: Vec<Slot>,
    next_free: Option<u64>,
}

impl WriteGatherer {
    pub fn new() -> Self {
        let slots = (0..SLOTS_CAPACITY)
            .map(|i| {
                if i == SLOTS_CAPACITY {
                    Slot::Free { next_free: None }
                } else {
                    Slot::Free {
                        next_free: Some((i + 1) as u64),
                    }
                }
            })
            .collect();

        Self {
            slots,
            next_free: Some(0),
        }
    }

    pub fn reserve(&mut self, capacity: usize) -> BufferHandle {
        let slot_idx = self.next_free.take().expect("full");

        self.next_free = self.slots[slot_idx as usize].reserve(capacity);
        BufferHandle(slot_idx)
    }

    pub fn release(&mut self, handle: BufferHandle) -> Arc<Mutex<WriteBuffer>> {
        let next_free = self.next_free.replace(handle.0);
        self.slots[handle.0 as usize].release(next_free)
    }

    pub fn lookup(&self, handle: BufferHandle) -> Arc<Mutex<WriteBuffer>> {
        self.slots[handle.0 as usize].as_buffer()
    }
}
