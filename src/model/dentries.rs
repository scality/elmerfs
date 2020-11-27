use crate::key::{KeyWriter, Ty};
use crate::model::inode::Kind;
use crate::view::{Name, View};
use antidotec::RawIdent;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::mem::size_of;

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
        KeyWriter::with_capacity(Ty::Dir, size_of::<u64>())
            .write_u64(self.ino)
            .into()
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct Entry {
    pub ino: u64,
    pub kind: Kind,
    pub name: Name,
}

impl Entry {
    pub fn new(name: Name, ino: u64, kind: Kind) -> Self {
        Self { name, ino, kind }
    }

    fn into_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        self.to_bytes(&mut buffer);
        buffer
    }

    fn to_bytes(&self, content: &mut Vec<u8>) {
        content.reserve(self.byte_len());

        content.extend_from_slice(&self.ino.to_le_bytes()[..]);
        content.push(self.kind as u8);
        content.extend_from_slice(&self.name.view.uid.to_le_bytes()[..]);
        content.extend_from_slice(&self.name.prefix.as_bytes());
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        let (ino, kind_view_prefix) = bytes.split_at(size_of::<u64>());
        let (kind, view_prefix) = kind_view_prefix.split_at(size_of::<Kind>());
        let (view, prefix) = view_prefix.split_at(size_of::<View>());

        let mut ino_bytes = [0; size_of::<u64>()];
        ino_bytes.copy_from_slice(ino);

        let mut view_bytes = [0; size_of::<View>()];
        view_bytes.copy_from_slice(view);

        let prefix = String::from_utf8(prefix.into()).expect("valid utf8");
        let kind = Kind::try_from(kind[0]).unwrap();

        Self {
            ino: u64::from_le_bytes(ino_bytes),
            kind,
            name: Name {
                view: View {
                    uid: u32::from_le_bytes(view.try_into().unwrap()),
                },
                prefix,
            },
        }
    }

    fn byte_len(&self) -> usize {
        self.name.prefix.len() + size_of::<View>() + size_of::<u64>() + size_of::<Kind>()
    }
}

pub use ops::*;

mod ops {
    use super::{Entry, Key};
    use crate::model::inode::Kind;
    use crate::view::{Name, View};
    use antidotec::{rwset, ReadQuery, ReadReply, UpdateQuery};

    pub fn read(ino: u64) -> ReadQuery {
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

    pub fn create(view: View, parent_ino: u64, ino: u64) -> UpdateQuery {
        let dot = Entry::new(Name::new(".", view), ino, Kind::Directory);
        let dotdot = Entry::new(Name::new("..", view), parent_ino, Kind::Directory);

        rwset::insert(Key::new(ino))
            .add(dot.into_bytes())
            .add(dotdot.into_bytes())
            .build()
    }

    pub fn remove(ino: u64) -> UpdateQuery {
        rwset::reset(Key::new(ino))
    }

    pub fn add_entry(ino: u64, entry: &Entry) -> UpdateQuery {
        rwset::insert(Key::new(ino)).add(entry.into_bytes()).build()
    }

    pub fn remove_entry(ino: u64, entry: &Entry) -> UpdateQuery {
        rwset::remove(Key::new(ino))
            .remove(entry.into_bytes())
            .build()
    }
}
