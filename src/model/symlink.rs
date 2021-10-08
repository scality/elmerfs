use crate::key::{KeyWriter, Ty};
use crate::model::inode::Ino;
use antidotec::RawIdent;
use std::mem;

#[derive(Debug, Copy, Clone)]
pub struct Key {
    ino: Ino,
}

impl Key {
    fn new(ino: Ino) -> Self {
        Self { ino }
    }
}

pub fn key(ino: Ino) -> Key {
    Key::new(ino)
}

impl Into<RawIdent> for Key {
    fn into(self) -> RawIdent {
        KeyWriter::with_capacity(Ty::Symlink, mem::size_of::<u64>())
            .write_u64(u64::from(self.ino))
            .into()
    }
}

pub use ops::*;
mod ops {
    use super::key;
    use crate::model::inode::Ino;
    use antidotec::{lwwreg, Bytes, ReadQuery, ReadReply, UpdateQuery};

    pub fn create(ino: Ino, content: String) -> UpdateQuery {
        lwwreg::set(key(ino), Bytes::from(content))
    }

    pub fn read(ino: Ino) -> ReadQuery {
        lwwreg::get(key(ino))
    }

    pub fn remove(ino: Ino) -> UpdateQuery {
        lwwreg::set(key(ino), Bytes::new())
    }

    pub fn decode(reply: &mut ReadReply, index: usize) -> Option<String> {
        reply
            .lwwreg(index)
            .map(|reg| String::from_utf8(reg.to_vec()).unwrap())
    }
}
