use antidotec::RawIdent;
use std::mem;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u8)]
pub enum Ty {
    Bucket = 0,
    Inode = 1,
    InoCounter = 2,
    Page = 3,
    Dir = 4,
    Symlink = 5,
}

pub struct KeyWriter {
    buffer: Vec<u8>,
}

impl KeyWriter {
    pub fn with_capacity(ty: Ty, capacity: usize) -> Self {
        let mut buffer = Vec::with_capacity(capacity + 1);
        buffer.push(ty as u8);

        KeyWriter { buffer }
    }

    #[inline]
    pub fn write_u8(mut self, x: u8) -> Self {
        self.buffer.extend_from_slice(&x.to_le_bytes()[..]);
        self
    }

    #[inline]
    pub fn write_u16(mut self, x: u16) -> Self {
        self.buffer.extend_from_slice(&x.to_le_bytes()[..]);
        self
    }

    #[inline]
    pub fn write_u32(mut self, x: u32) -> Self {
        self.buffer.extend_from_slice(&x.to_le_bytes()[..]);
        self
    }

    #[inline]
    pub fn write_u64(mut self, x: u64) -> Self {
        self.buffer.extend_from_slice(&x.to_le_bytes()[..]);
        self
    }
}

impl Into<RawIdent> for KeyWriter {
    fn into(self) -> RawIdent {
        self.buffer
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct Bucket(u32);

impl Bucket {
    pub const fn new(id: u32) -> Self {
        Self(id)
    }
}

impl Into<RawIdent> for Bucket {
    fn into(self) -> RawIdent {
        KeyWriter::with_capacity(Ty::Bucket, mem::size_of::<u32>())
            .write_u32(self.0)
            .into()
    }
}
