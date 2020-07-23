use crate::key::Kind;
use crate::view::{Name, NameRef, View};
use antidotec::RawIdent;
use std::fmt::{self, Display};
use std::mem::size_of;

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct Entry {
    pub name: Name,
    pub ino: u64,
}

impl Entry {
    pub fn new(name: Name, ino: u64) -> Self {
        Self { name, ino }
    }

    fn into_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        self.to_bytes(&mut buffer);
        buffer
    }

    fn to_bytes(&self, content: &mut Vec<u8>) {
        content.reserve(self.byte_len());

        content.extend_from_slice(&self.ino.to_le_bytes()[..]);
        content.extend_from_slice(&self.name.view.to_le_bytes()[..]);
        content.extend_from_slice(&self.name.prefix.as_bytes());
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        let (ino_and_view, prefix) = bytes.split_at(size_of::<u64>() + size_of::<View>());
        let (ino, view) = ino_and_view.split_at(size_of::<u64>());

        let mut ino_bytes = [0; size_of::<u64>()];
        ino_bytes.copy_from_slice(ino);

        let mut view_bytes = [0; size_of::<View>()];
        view_bytes.copy_from_slice(view);

        let prefix = String::from_utf8(prefix.into()).expect("valid utf8");

        Self {
            ino: u64::from_le_bytes(ino_bytes),
            name: Name {
                view: View::from_le_bytes(view_bytes),
                prefix,
            },
        }
    }

    fn byte_len(&self) -> usize {
        self.name.prefix.len() + size_of::<View>() + size_of::<u64>()
    }
}

impl Display for Name {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.prefix, self.view)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Key {
    ino: u64,
}

impl Key {
    pub fn new(ino: u64) -> Self {
        Self { ino }
    }
}

impl Into<RawIdent> for Key {
    fn into(self) -> RawIdent {
        let mut ident = RawIdent::with_capacity(size_of::<Kind>() + size_of::<u64>());
        ident.push(Kind::Dir as u8);
        ident.extend_from_slice(&self.ino.to_le_bytes()[..]);

        ident
    }
}

pub use ops::*;

mod ops {
    use super::{DirView, Entry, Key};
    use crate::view::{Name, View};
    use antidotec::{rwset, ReadQuery, ReadReply, UpdateQuery};

    pub fn read(ino: u64) -> ReadQuery {
        rwset::get(Key::new(ino))
    }

    pub fn decode(view: View, reply: &mut ReadReply, index: usize) -> Option<DirView> {
        let set = reply.rwset(index)?;

        let mut entries = Vec::with_capacity(set.len());
        for encoded_entry in set {
            let entry = Entry::from_bytes(&encoded_entry);
            entries.push(entry);
        }

        entries.sort();
        Some(DirView { view, entries })
    }

    pub fn create(view: View, parent_ino: u64, ino: u64) -> UpdateQuery {
        let dot = Entry::new(Name::new(".", view), ino);
        let dotdot = Entry::new(Name::new("..", view), parent_ino);

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

#[derive(Debug)]
pub struct DirView {
    view: View,
    entries: Vec<Entry>,
}

impl DirView {
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn get(&self, name: &NameRef) -> Option<&Entry> {
        self.position(name).map(|idx| &self.entries[idx])
    }

    pub fn contains_key(&self, name: &NameRef) -> bool {
        self.get(name).is_some()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Entry> {
        self.entries.iter()
    }

    fn position(&self, name: &NameRef) -> Option<usize> {
        match name {
            NameRef::Exact(name) => self.entries.iter().position(|e| e.name == *name),
            NameRef::Partial(prefix) => {
                /* This is simple algorithm to resolve conflicts (multiple entry with
                the same prefix). If there is only one entry for a given prefix
                there is no conflict so we can simply entry. Otherwise, try to
                fetch the exact entry by using our current view */

                let start = self.entries.iter().position(|e| &e.name.prefix == prefix)?;
                let end = start
                    + self.entries[start + 1..]
                        .iter()
                        .position(|e| &e.name.prefix != prefix)
                        .map(|i| i + 1)
                        .unwrap_or(1);

                match (start..end).len() {
                    1 => Some(start),
                    _ => self.entries[start..end]
                        .iter()
                        .position(|e| e.name.view == self.view),
                }
            }
        }
    }
}
