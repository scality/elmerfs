use crate::key::{KeyWriter, Ty};
use crate::view::{Name, View};
use antidotec::{Bytes, RawIdent};
use fuse::FileAttr;
use std::collections::HashSet;
use std::convert::TryInto;
use std::mem;
use std::time::Duration;
use std::fmt;

/* An ino number is defined as follow:
[CLUSTER_ID (8) | NODE_ID (8) | INO_KIND (4) | SEQUENCE (44)] */

const CLUSTER_ID_BITS: u64 = 8;
const CLUSTER_ID_MASK: u64 = 0xFF << (INO_KIND_BITS + SEQUENCE_BITS + NODE_ID_BITS);

const NODE_ID_BITS: u64 = 8;
const NODE_ID_MASK: u64 = 0xFF << (INO_KIND_BITS + SEQUENCE_BITS);

const INO_KIND_BITS: u64 = 4;
const INO_KIND_MASK: u64 = 0x0F << SEQUENCE_BITS;

const SEQUENCE_BITS: u64 = 64 - CLUSTER_ID_BITS - NODE_ID_BITS - INO_KIND_BITS;
const SEQUENCE_MASK: u64 = (0x01 << SEQUENCE_BITS) - 1;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum InodeKind {
    Regular = 0,
    Directory = 1,
    Symlink = 2,
}
pub use self::InodeKind::*;

impl From<u8> for InodeKind {
    fn from(kind: u8) -> Self {
        match kind {
            0 => InodeKind::Regular,
            1 => InodeKind::Directory,
            2 => InodeKind::Symlink,
            _ => panic!("invalid inode kind"),
        }
    }
}

impl From<InodeKind> for fuse::FileType {
    fn from(kind: InodeKind) -> fuse::FileType {
        match kind {
            InodeKind::Regular => fuse::FileType::RegularFile,
            InodeKind::Directory => fuse::FileType::Directory,
            InodeKind::Symlink => fuse::FileType::Symlink,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct InoDesc {
   pub cluster_id: u8,
   pub node_id: u8,
   pub kind: InodeKind,
   pub sequence: u64,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[derive(Hash)]
pub struct Ino(pub u64);

impl Ino {
    pub const fn encode(desc: InoDesc) -> Self {
        let mut ino = 0;
        ino |= (desc.cluster_id as u64) << (INO_KIND_BITS + SEQUENCE_BITS + NODE_ID_BITS);
        ino |= (desc.node_id as u64) << (INO_KIND_BITS + SEQUENCE_BITS);
        ino |= (desc.kind as u64) << SEQUENCE_BITS;
        ino |= desc.sequence;

        Ino(ino)
    }

    pub const fn root() -> Self {
        Ino(1)
    }

    pub fn decode(self) -> InoDesc {
        let ino = self.0;
        InoDesc {
            cluster_id: ((ino & CLUSTER_ID_MASK) >> (INO_KIND_BITS + SEQUENCE_BITS + NODE_ID_BITS)) as u8,
            node_id: ((ino & NODE_ID_MASK) >> (INO_KIND_BITS + SEQUENCE_BITS)) as u8,
            kind: InodeKind::from(((ino & INO_KIND_MASK) >> SEQUENCE_BITS) as u8),
            sequence: ino & SEQUENCE_MASK,
        }
    }

    pub fn to_le_bytes(self) -> [u8; 8] {
        self.0.to_le_bytes()
    }

    pub fn from_le_bytes(bytes: [u8; 8]) -> Self {
        Self(u64::from_le_bytes(bytes))
    }
}

impl fmt::Display for Ino {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x{:X}", self.0)
    }
}

impl From<Ino> for u64 {
    fn from(ino: Ino) -> u64 {
        ino.0
    }
}

pub fn kind(ino: Ino) -> InodeKind {
    ino.decode().kind
}

pub fn file_type(ino: Ino) -> fuse::FileType {
    if ino == Ino::root() {
        return fuse::FileType::Directory;
    }

    ino.decode().kind.into()
}

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
    pub parent_ino: Ino,
    pub name: Name,
}

impl Link {
    pub fn new(parent_ino: Ino, name: Name) -> Self {
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
            parent_ino: Ino(parent_ino),
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

    pub fn find(&self, parent_ino: Ino, name: &Name) -> Option<&Link> {
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
    pub dotdot: Option<Ino>,
}

impl Inode {
    pub fn attr(&self, ino: Ino) -> FileAttr {
        let timespec_from_duration = |duration: Duration| {
            time::Timespec::new(duration.as_secs() as i64, duration.subsec_nanos() as i32)
        };

        FileAttr {
            ino: ino.into(),
            size: self.size,
            blocks: self.size / crate::driver::PAGE_SIZE,
            atime: timespec_from_duration(self.atime),
            mtime: timespec_from_duration(self.mtime),
            ctime: timespec_from_duration(self.ctime),
            crtime: timespec_from_duration(self.atime),
            kind: file_type(ino),
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
    ino: Ino,
    field: Option<Field>,
}

impl Key {
    fn new(ino: Ino) -> Self {
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

pub fn key(ino: Ino) -> Key {
    Key::new(ino)
}

impl Into<RawIdent> for Key {
    fn into(self) -> RawIdent {
        KeyWriter::with_capacity(Ty::Inode, Self::byte_len())
            .write_u64(self.ino.into())
            .write_u8(self.field.map(|f| f as u8).unwrap_or(0))
            .into()
    }
}

pub use ops::*;

mod ops {
    use super::{Field, Ino, Inode, Key, Link, Links, Owner, key};
    use antidotec::{lwwreg, rrmap, rwset, ReadQuery, ReadReply, UpdateQuery};
    use std::time::Duration;

    pub fn read(ino: Ino) -> ReadQuery {
        rrmap::get(key(ino))
    }

    pub struct CreateDesc<T> {
        pub ino: Ino,
        pub owner: Owner,
        pub mode: u32,
        pub size: u64,
        pub links: T,
        pub dotdot: Option<Ino>,
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
                desc.dotdot.map(|ino| u64::from(ino)).unwrap_or(0),
            ))
            .push(links_add(key, desc.links.into_iter()))
            .build()
    }

    #[derive(Debug, Default)]
    pub struct UpdateAttrsDesc {
        pub mode: Option<u32>,
        pub owner: Option<Owner>,
        pub size: Option<u64>,
        pub atime: Option<Duration>,
        pub mtime: Option<Duration>,
    }

    pub fn update_attrs(ino: Ino, desc: UpdateAttrsDesc) -> UpdateQuery {
        let key = key(ino);

        let mut update = rrmap::update(key);

        if let Some(new_mode) = desc.mode {
            update = update.push(lwwreg::set_u32(key.field(Field::Mode), new_mode));
        }

        if let Some(new_owner) = desc.owner {
            update = update.push(lwwreg::set_u64(key.field(Field::Owner), new_owner.into()))
        }

        if let Some(new_size) = desc.size {
            update = update.push(lwwreg::set_u64(key.field(Field::Size), new_size));
        }

        if let Some(new_atime) = desc.atime {
            update = update.push(lwwreg::set_duration(key.field(Field::Atime), new_atime));
        }

        if let Some(new_mtime) = desc.mtime {
            update = update.push(lwwreg::set_duration(key.field(Field::Mtime), new_mtime));
        }

        update.build()
    }

    pub fn add_link(ts: Duration, ino: Ino, link: Link) -> UpdateQuery {
        let key = key(ino);

        rrmap::update(key)
            .push(lwwreg::set_duration(key.field(Field::Atime), ts))
            .push(links_add(key, std::iter::once(link)))
            .build()
    }

    pub fn remove_link(ts: Duration, ino: Ino, link: Link) -> UpdateQuery {
        let key = key(ino);

        rrmap::update(key)
            .push(lwwreg::set_duration(key.field(Field::Atime), ts))
            .push(links_remove(key, std::iter::once(link)))
            .build()
    }

    pub fn link_to_parent(ts: Duration, ino: Ino, parent_ino: Ino, link: Link) -> UpdateQuery {
        let key = key(ino);

        rrmap::update(key)
            .push(lwwreg::set_duration(key.field(Field::Atime), ts))
            .push(lwwreg::set_u64(key.field(Field::DotDot), parent_ino.into()))
            .push(links_add(key, std::iter::once(link)))
            .build()
    }

    pub fn unlink_from_parent(ts: Duration, ino: Ino, link: Link) -> UpdateQuery {
        let key = key(ino);

        rrmap::update(key)
            .push(lwwreg::set_duration(key.field(Field::Atime), ts))
            .push(lwwreg::set_u64(key.field(Field::DotDot), 0))
            .push(links_remove(key, std::iter::once(link)))
            .build()
    }

    pub fn touch(ts: Duration, ino: Ino) -> UpdateQuery {
        let key = key(ino);

        rrmap::update(key)
            .push(lwwreg::set_duration(key.field(Field::Atime), ts))
            .push(lwwreg::set_duration(key.field(Field::Ctime), ts))
            .build()
    }

    pub fn update_size(ts: Duration, ino: Ino, new_size: u64) -> UpdateQuery {
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

    pub fn decode_size(ino: Ino, reply: &mut ReadReply, index: usize) -> Option<u64> {
        let mut map = reply.rrmap(index)?;
        let key = key(ino);

        let size = map.remove(&key.field(Field::Size)).unwrap().into_lwwreg();
        Some(lwwreg::read_u64(&size))
    }

    pub fn decode(ino: Ino, reply: &mut ReadReply, index: usize) -> Option<Inode> {
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
        let dotdot = if dotdot != 0 { Some(Ino(dotdot)) } else { None };

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

    pub fn remove(ino: Ino) -> UpdateQuery {
        rrmap::reset(key(ino))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ino_encode_decode() {
        let ino_desc = InoDesc {
            cluster_id: 16,
            node_id: 4,
            kind: InodeKind::Directory,
            sequence: 0,
        };

        let encoded = Ino::encode(ino_desc);
        println!("{}", encoded);
        assert_eq!(encoded.decode(), ino_desc);
    }
}