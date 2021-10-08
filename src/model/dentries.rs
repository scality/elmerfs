use crate::key::{KeyWriter, Ty};
use crate::view::{Name, View};
use crate::model::inode::Ino;
use antidotec::{Bytes, RawIdent};
use std::convert::TryInto;
use std::mem::size_of;

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
        KeyWriter::with_capacity(Ty::Dir, size_of::<u64>())
            .write_u64(u64::from(self.ino))
            .into()
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct Entry {
    pub ino: Ino,
    pub name: Name,
}

impl Entry {
    pub fn new(name: Name, ino: Ino) -> Self {
        Self { name, ino }
    }

    fn into_bytes(&self) -> Bytes {
        let mut buffer = Vec::new();
        self.to_bytes(&mut buffer);
        Bytes::from(buffer)
    }

    fn to_bytes(&self, content: &mut Vec<u8>) {
        content.reserve(self.byte_len());

        content.extend_from_slice(&self.ino.to_le_bytes()[..]);
        content.extend_from_slice(&self.name.view.uid.to_le_bytes()[..]);
        content.extend_from_slice(&self.name.prefix.as_bytes());
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        let (ino, view_prefix) = bytes.split_at(size_of::<u64>());
        let (view, prefix) = view_prefix.split_at(size_of::<View>());

        let mut ino_bytes = [0; size_of::<u64>()];
        ino_bytes.copy_from_slice(ino);

        let mut view_bytes = [0; size_of::<View>()];
        view_bytes.copy_from_slice(view);

        let prefix = String::from_utf8(prefix.into()).expect("valid utf8");

        Self {
            ino: Ino::from_le_bytes(ino_bytes),
            name: Name {
                view: View {
                    uid: u32::from_le_bytes(view.try_into().unwrap()),
                },
                prefix,
            },
        }
    }

    fn byte_len(&self) -> usize {
        self.name.prefix.len() + size_of::<View>() + size_of::<u64>()
    }
}

pub use ops::*;

mod ops {
    use super::{Entry, Key};
    use crate::view::View;
    use antidotec::{rwset, ReadQuery, ReadReply, UpdateQuery};
    use crate::model::inode::Ino;

    pub fn read(ino: Ino) -> ReadQuery {
        rwset::get(Key::new(ino))
    }

    pub fn decode(reply: &mut ReadReply, index: usize) -> Option<Vec<Entry>> {
        let set = reply.rwset(index)?;

        let mut entries = Vec::with_capacity(set.len());
        for encoded_entry in set {
            let entry = Entry::from_bytes(&encoded_entry);
            entries.push(entry);
        }
        entries.sort();
        Some(entries)
    }

    pub fn create(_view: View, _parent_ino: Ino, ino: Ino) -> UpdateQuery {
        rwset::insert(Key::new(ino)).build()
    }

    pub fn remove(ino: Ino) -> UpdateQuery {
        rwset::reset(Key::new(ino))
    }

    pub fn add_entry(ino: Ino, entry: &Entry) -> UpdateQuery {
        rwset::insert(Key::new(ino)).add(entry.into_bytes()).build()
    }

    pub fn remove_entry(ino: Ino, entry: &Entry) -> UpdateQuery {
        rwset::remove(Key::new(ino))
            .remove(entry.into_bytes())
            .build()
    }
}
