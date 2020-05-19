use std::mem;
use antidotec::RawIdent;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u8)]
pub enum Kind {
    Bucket = 0,
    Inode = 1,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct Key<P> {
    pub kind: Kind,
    pub payload: P
}

impl<P> Key<P> {
    pub const fn new(kind: Kind, payload: P) -> Self {
        Self {
            kind,
            payload,
        }
    }
}

type Id = u32;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct Bucket(Key<Id>);

impl Bucket {
    pub const fn new(id: Id) -> Self {
        Self(Key::new(Kind::Bucket, id))
    }
}

impl Into<RawIdent> for Bucket {
    fn into(self) -> RawIdent {
        let mut ident = RawIdent::with_capacity(mem::size_of::<Self>());
        ident.push(self.0.kind as u8);

        let id = self.0.payload.to_le_bytes();
        ident.extend_from_slice(&id);

        ident
    }
}