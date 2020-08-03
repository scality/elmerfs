use crate::key::{KeyWriter, Ty};
use crate::model::inode::Kind;
use crate::view::{Name, NameRef, View};
use antidotec::RawIdent;
use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::{self, Display};
use std::mem::size_of;
use std::sync::Arc;

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
    pub name: Name,
    pub ino: u64,
    pub kind: Kind,
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
        content.extend_from_slice(&self.name.view.to_le_bytes()[..]);
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
                view: View::from_le_bytes(view_bytes),
                prefix,
            },
        }
    }

    fn byte_len(&self) -> usize {
        self.name.prefix.len() + size_of::<View>() + size_of::<u64>() + size_of::<Kind>()
    }
}

impl Display for Name {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.prefix, self.view)
    }
}

pub use ops::*;

mod ops {
    use super::{DirView, Entry, EntryList, EntryView, Key};
    use crate::model::inode::Kind;
    use crate::view::{Name, View};
    use antidotec::{rwset, ReadQuery, ReadReply, UpdateQuery};
    use std::collections::HashMap;
    use std::sync::Arc;

    pub fn read(ino: u64) -> ReadQuery {
        rwset::get(Key::new(ino))
    }

    pub fn decode(view: View, reply: &mut ReadReply, index: usize) -> Option<DirView> {
        use std::collections::hash_map::Entry as HashEntry;

        let set = reply.rwset(index)?;

        let mut entries = Vec::with_capacity(set.len());
        let mut by_name: HashMap<_, EntryList> = HashMap::with_capacity(set.len());
        for encoded_entry in set {
            let entry = Entry::from_bytes(&encoded_entry);
            let prefix: Arc<str> = Arc::from(entry.name.prefix);

            entries.push(EntryView {
                ino: entry.ino,
                prefix: prefix.clone(),
                view: entry.name.view,
                kind: entry.kind,
                next: None,
            });
        }
        entries.sort();

        for idx in 0..entries.len() {
            let prefix = entries[idx].prefix.clone();

            match by_name.entry(prefix) {
                HashEntry::Occupied(mut entry) => {
                    let entry_list = entry.get_mut();
                    entries[entry_list.tail].next = Some(idx);
                    entry_list.tail = idx;
                }
                HashEntry::Vacant(entry) => {
                    entry.insert(EntryList {
                        head: idx,
                        tail: idx,
                    });
                }
            }
        }

        Some(DirView {
            view,
            entries,
            by_name,
        })
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

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct EntryView {
    pub ino: u64,
    pub view: View,
    pub kind: Kind,
    pub prefix: Arc<str>,
    next: Option<usize>,
}

impl EntryView {
    pub fn into_dentry(&self) -> Entry {
        Entry {
            ino: self.ino,
            name: Name {
                prefix: String::from(&*self.prefix as &str),
                view: self.view,
            },
            kind: self.kind,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
struct EntryList {
    head: usize,
    tail: usize,
}

#[derive(Debug)]
pub struct DirView {
    view: View,
    entries: Vec<EntryView>,
    by_name: HashMap<Arc<str>, EntryList>,
}

impl DirView {
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn get(&self, name: &NameRef) -> Option<&EntryView> {
        self.position(name).map(|idx| &self.entries[idx])
    }

    pub fn contains_key(&self, name: &NameRef) -> bool {
        self.get(name).is_some()
    }

    pub fn iter_from(&self, offset: usize) -> impl Iterator<Item = EntryRef<'_>> {
        let start = offset.min(self.entries.len());
        Iter {
            entries: self.entries[start..].iter(),
            by_name: &self.by_name,
            view: self.view,
        }
    }

    fn position(&self, name: &NameRef) -> Option<usize> {
        match name {
            NameRef::Exact(name) => {
                let entry_list = self.by_name.get(&name.prefix as &str)?;
                self.resolve_by_view(&entry_list, name.view)
            }
            NameRef::Partial(prefix) => {
                /* This is simple algorithm to resolve conflicts (multiple entry with
                the same prefix). If there is only one entry for a given prefix
                there is no conflict so we can simply entry. Otherwise, try to
                fetch the exact entry by using our current view */

                let entry_list = self.by_name.get(prefix as &str)?;
                if entry_list.head == entry_list.tail {
                    return Some(entry_list.head);
                }

                self.resolve_by_view(&entry_list, self.view)
            }
        }
    }

    fn resolve_by_view(&self, entry_list: &EntryList, view: View) -> Option<usize> {
        let mut current = Some(entry_list.head);
        while let Some(idx) = current {
            let entry = &self.entries[idx];
            if entry.view == view {
                return Some(idx);
            }

            current = entry.next;
        }

        None
    }
}

#[derive(Debug)]
pub struct EntryRef<'a> {
    pub name: Cow<'a, str>,
    pub ino: u64,
    pub kind: Kind,
}

pub struct Iter<'a> {
    entries: std::slice::Iter<'a, EntryView>,
    by_name: &'a HashMap<Arc<str>, EntryList>,
    view: View,
}

impl<'a> Iterator for Iter<'a> {
    type Item = EntryRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        use crate::view::REF_SEP;

        let entry = self.entries.next()?;
        let entry_list = self.by_name[&entry.prefix];

        let show_alias = entry_list.head == entry_list.tail || entry.view == self.view;

        let entry = if show_alias {
            EntryRef {
                name: Cow::Borrowed(&*entry.prefix as &str),
                ino: entry.ino,
                kind: entry.kind,
            }
        } else {
            let fully_qualified = format!(
                "{prefix}{sep}{view}",
                prefix = entry.prefix,
                sep = REF_SEP,
                view = entry.view
            );

            EntryRef {
                name: Cow::Owned(fully_qualified),
                ino: entry.ino,
                kind: entry.kind,
            }
        };

        Some(entry)
    }
}
