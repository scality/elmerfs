use crate::driver::ino;
use crate::key::{KeyWriter, Ty};
use crate::view::{Name, View};
use antidotec::{Bytes, RawIdent};
use fuse::FileAttr;
use std::collections::HashSet;
use std::convert::TryInto;
use std::mem;
use std::time::Duration;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct Owner {
    pub gid: u32,
    pub uid: u32,
}

impl From<u64> for Owner {
    fn from(x: u64) -> Self {
        let gid = (x >> 32) as u32;
        let uid = (x & 0x0000FFFF) as u32;

        Self { gid, uid }
    }
}

impl Into<u64> for Owner {
    fn into(self) -> u64 {
        let x = (self.gid as u64) << 32;
        x | (self.uid as u64)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Link {
    pub parent_ino: u64,
    pub name: Name,
}

impl Link {
    pub fn new(parent_ino: u64, name: Name) -> Self {
        Self { parent_ino, name }
    }

    fn into_bytes(self) -> Bytes {
        let mut buffer = Vec::new();
        self.to_bytes(&mut buffer);
        Bytes::from(buffer)
    }

    fn to_bytes(&self, content: &mut Vec<u8>) {
        content.reserve(self.byte_len());

        content.extend_from_slice(&self.parent_ino.to_le_bytes()[..]);
        content.extend_from_slice(&self.name.view.uid.to_le_bytes()[..]);
        content.extend_from_slice(&self.name.prefix.as_bytes());
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        let (parent_ino, bytes) = bytes.split_at(mem::size_of::<u64>());
        let (view, prefix) = bytes.split_at(mem::size_of::<u32>());

        let view = View {
            uid: u32::from_le_bytes(view.try_into().unwrap()),
        };
        let prefix = String::from_utf8(prefix.into()).unwrap();
        let parent_ino = u64::from_le_bytes(parent_ino.try_into().unwrap());

        Self {
            parent_ino,
            name: Name::new(prefix, view),
        }
    }

    fn byte_len(&self) -> usize {
        mem::size_of::<u64>() + mem::size_of::<View>() + self.name.prefix.len()
    }
}

#[derive(Debug, Clone)]
pub struct Links {
    links: HashSet<Link>,
}

impl Links {
    pub fn nlink(&self) -> u32 {
        self.links.len() as u32
    }

    pub fn find(&self, parent_ino: u64, name: &Name) -> Option<&Link> {
        self.links
            .iter()
            .find(|l| l.parent_ino == parent_ino && l.name == *name)
    }
}

#[derive(Debug, Clone)]
pub struct Inode {
    pub atime: Duration,
    pub ctime: Duration,
    pub mtime: Duration,
    pub owner: Owner,
    pub mode: u32,
    pub size: u64,
    pub links: Links,
    pub dotdot: Option<u64>,
}

impl Inode {
    pub fn attr(&self, ino: u64) -> FileAttr {
        let timespec_from_duration = |duration: Duration| {
            time::Timespec::new(duration.as_secs() as i64, duration.subsec_nanos() as i32)
        };

        FileAttr {
            ino,
            size: self.size,
            blocks: self.size / crate::driver::PAGE_SIZE,
            atime: timespec_from_duration(self.atime),
            mtime: timespec_from_duration(self.mtime),
            ctime: timespec_from_duration(self.ctime),
            crtime: timespec_from_duration(self.atime),
            kind: ino::file_type(ino),
            perm: self.mode as u16,
            nlink: self.links.nlink(),
            uid: self.owner.uid,
            gid: self.owner.gid,
            rdev: 0,
            flags: 0,
        }
    }
}

#[derive(Debug, Copy, Clone)]
#[repr(u8)]
enum Field {
    Atime = 1,
    Ctime = 2,
    Mtime = 3,
    Owner = 4,
    Mode = 5,
    Size = 6,
    Links = 7,
    DotDot = 8,
}

#[derive(Debug, Copy, Clone)]
pub struct Key {
    ino: u64,
    field: Option<Field>,
}

impl Key {
    fn new(ino: u64) -> Self {
        Key { ino, field: None }
    }

    fn field(self, field: Field) -> RawIdent {
        Key {
            ino: self.ino,
            field: Some(field),
        }
        .into()
    }

    const fn byte_len() -> usize {
        mem::size_of::<u8>() + mem::size_of::<u64>()
    }
}

pub fn key(ino: u64) -> Key {
    Key::new(ino)
}

impl Into<RawIdent> for Key {
    fn into(self) -> RawIdent {
        KeyWriter::with_capacity(Ty::Inode, Self::byte_len())
            .write_u64(self.ino)
            .write_u8(self.field.map(|f| f as u8).unwrap_or(0))
            .into()
    }
}

pub use ops::*;

mod ops {
    use super::{key, Field, Inode, Key, Link, Links, Owner};
    use antidotec::{lwwreg, rrmap, rwset, ReadQuery, ReadReply, UpdateQuery};
    use std::time::Duration;

    pub fn read(ino: u64) -> ReadQuery {
        rrmap::get(key(ino))
    }

    pub struct CreateDesc<T> {
        pub ino: u64,
        pub owner: Owner,
        pub mode: u32,
        pub size: u64,
        pub links: T,
        pub dotdot: Option<u64>,
    }

    pub fn create<T>(ts: Duration, desc: CreateDesc<T>) -> UpdateQuery
    where
        T: IntoIterator<Item = Link>,
    {
        let key = key(desc.ino);

        rrmap::update(key)
            .push(lwwreg::set_duration(key.field(Field::Atime), ts))
            .push(lwwreg::set_duration(key.field(Field::Ctime), ts))
            .push(lwwreg::set_duration(key.field(Field::Mtime), ts))
            .push(lwwreg::set_u64(key.field(Field::Owner), desc.owner.into()))
            .push(lwwreg::set_u32(key.field(Field::Mode), desc.mode))
            .push(lwwreg::set_u64(key.field(Field::Size), desc.size))
            .push(lwwreg::set_u64(
                key.field(Field::DotDot),
                desc.dotdot.unwrap_or(0),
            ))
            .push(links_add(key, desc.links.into_iter()))
            .build()
    }

    pub fn add_link(ts: Duration, ino: u64, link: Link) -> UpdateQuery {
        let key = key(ino);

        rrmap::update(key)
            .push(lwwreg::set_duration(key.field(Field::Atime), ts))
            .push(links_add(key, std::iter::once(link)))
            .build()
    }

    pub fn remove_link(ts: Duration, ino: u64, link: Link) -> UpdateQuery {
        let key = key(ino);

        rrmap::update(key)
            .push(lwwreg::set_duration(key.field(Field::Atime), ts))
            .push(links_remove(key, std::iter::once(link)))
            .build()
    }

    pub fn link_to_parent(ts: Duration, ino: u64, parent_ino: u64, link: Link) -> UpdateQuery {
        let key = key(ino);

        rrmap::update(key)
            .push(lwwreg::set_duration(key.field(Field::Atime), ts))
            .push(lwwreg::set_u64(key.field(Field::DotDot), parent_ino))
            .push(links_add(key, std::iter::once(link)))
            .build()
    }

    pub fn unlink_from_parent(ts: Duration, ino: u64, link: Link) -> UpdateQuery {
        let key = key(ino);

        rrmap::update(key)
            .push(lwwreg::set_duration(key.field(Field::Atime), ts))
            .push(lwwreg::set_u64(key.field(Field::DotDot), 0))
            .push(links_remove(key, std::iter::once(link)))
            .build()
    }

    pub fn touch(ts: Duration, ino: u64) -> UpdateQuery {
        let key = key(ino);

        rrmap::update(key)
            .push(lwwreg::set_duration(key.field(Field::Atime), ts))
            .push(lwwreg::set_duration(key.field(Field::Ctime), ts))
            .build()
    }

    pub fn update_size(ts: Duration, ino: u64, new_size: u64) -> UpdateQuery {
        let key = key(ino);

        rrmap::update(key)
            .push(lwwreg::set_duration(key.field(Field::Ctime), ts))
            .push(lwwreg::set_duration(key.field(Field::Mtime), ts))
            .push(lwwreg::set_u64(key.field(Field::Size), new_size))
            .build()
    }

    fn links_add(key: Key, links: impl IntoIterator<Item = Link>) -> UpdateQuery {
        let mut adds = rwset::insert(key.field(Field::Links));
        for link in links {
            adds = adds.add(link.into_bytes());
        }

        adds.build()
    }

    fn links_remove(key: Key, links: impl IntoIterator<Item = Link>) -> UpdateQuery {
        let mut removes = rwset::remove(key.field(Field::Links));
        for link in links {
            removes = removes.remove(link.into_bytes());
        }

        removes.build()
    }

    pub fn decode_size(ino: u64, reply: &mut ReadReply, index: usize) -> Option<u64> {
        let mut map = reply.rrmap(index)?;
        let key = key(ino);

        let size = map.remove(&key.field(Field::Size)).unwrap().into_lwwreg();
        Some(lwwreg::read_u64(&size))
    }

    pub fn decode(ino: u64, reply: &mut ReadReply, index: usize) -> Option<Inode> {
        let mut map = reply.rrmap(index)?;
        let key = key(ino);

        let atime = map.remove(&key.field(Field::Atime)).unwrap().into_lwwreg();
        let ctime = map.remove(&key.field(Field::Ctime)).unwrap().into_lwwreg();
        let mtime = map.remove(&key.field(Field::Mtime)).unwrap().into_lwwreg();
        let owner = map.remove(&key.field(Field::Owner)).unwrap().into_lwwreg();
        let mode = map.remove(&key.field(Field::Mode)).unwrap().into_lwwreg();
        let size = map.remove(&key.field(Field::Size)).unwrap().into_lwwreg();
        let dotdot = map.remove(&key.field(Field::DotDot)).unwrap().into_lwwreg();

        let encoded_links = map.remove(&key.field(Field::Links))?.into_rwset();
        let links = encoded_links
            .into_iter()
            .map(|encoded| Link::from_bytes(&encoded[..]))
            .collect();

        let owner = Owner::from(lwwreg::read_u64(&owner));

        let dotdot = lwwreg::read_u64(&dotdot);
        let dotdot = if dotdot != 0 { Some(dotdot) } else { None };

        Some(Inode {
            atime: lwwreg::read_duration(&atime),
            ctime: lwwreg::read_duration(&ctime),
            mtime: lwwreg::read_duration(&mtime),
            owner,
            mode: lwwreg::read_u32(&mode),
            size: lwwreg::read_u64(&size),
            links: Links { links },
            dotdot,
        })
    }

    pub fn remove(ino: u64) -> UpdateQuery {
        rrmap::reset(key(ino))
    }
}
