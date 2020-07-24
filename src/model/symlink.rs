use antidotec::RawIdent;
use crate::key::{Ty, KeyWriter};
use std::mem;
#[derive(Debug, Copy, Clone)]
pub struct Key {
    ino: u64,
}

impl Key {
    fn new(ino: u64) -> Self {
        Self { ino }
    }
}

pub fn key(ino: u64) -> Key {
    Key::new(ino)
}

impl Into<RawIdent> for Key {
    fn into(self) -> RawIdent {
        KeyWriter::with_capacity(Ty::Symlink, mem::size_of::<u64>())
            .write_u64(self.ino)
            .into()
    }
}

pub use ops::*;
mod ops {
    use super::key;
    use antidotec::{lwwreg, ReadReply, ReadQuery, UpdateQuery};

    pub fn create(ino: u64, content: String) -> UpdateQuery {
        lwwreg::set(key(ino), content.into_bytes())
    }

    pub fn read(ino: u64) -> ReadQuery {
        lwwreg::get(key(ino))
    }

    pub fn remove(ino: u64) -> UpdateQuery {
        lwwreg::set(key(ino), Vec::new())
    }

    pub fn decode(reply: &mut ReadReply, index: usize) -> Option<String> {
        reply.lwwreg(index).map(|reg| {
            String::from_utf8(reg).unwrap()
        })
    }
}