use crate::key::{Ty, KeyWriter};
use fuse::{FileAttr, FileType};
use std::{convert::TryFrom, time::Duration};
use antidotec::RawIdent;
use std::mem;

#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub enum Kind {
    Regular = 0,
    Directory = 1,
    Symlink = 2,
}

impl Kind {
    pub fn to_file_type(self) -> FileType {
        match self {
            Kind::Regular => FileType::RegularFile,
            Kind::Directory => FileType::Directory,
            Kind::Symlink => FileType::Symlink,
        }
    }
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

#[derive(Debug)]
pub struct InvalidKindByte;

impl TryFrom<u8> for Kind {
    type Error = InvalidKindByte;

    fn try_from(x: u8) -> Result<Kind, Self::Error> {
        match x {
            0 => Ok(Kind::Regular),
            1 => Ok(Kind::Directory),
            2 => Ok(Kind::Symlink),
            _ => Err(InvalidKindByte),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Inode {
    pub ino: u64,
    pub kind: Kind,
    pub parent: u64,
    pub atime: Duration,
    pub ctime: Duration,
    pub mtime: Duration,
    pub owner: Owner,
    pub mode: u32,
    pub size: u64,
    pub nlink: u64,
}

impl Inode {
    pub fn attr(&self) -> FileAttr {
        let timespec_from_duration = |duration: Duration| {
            time::Timespec::new(duration.as_secs() as i64, duration.subsec_nanos() as i32)
        };

        FileAttr {
            ino: self.ino,
            size: self.size,
            blocks: 0,
            atime: timespec_from_duration(self.atime),
            mtime: timespec_from_duration(self.mtime),
            ctime: timespec_from_duration(self.ctime),
            crtime: timespec_from_duration(self.atime),
            kind: self.kind.to_file_type(),
            perm: self.mode as u16,
            nlink: self.nlink as u32,
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
    Struct = 0,
    Kind = 1,
    Parent = 2,
    Atime = 3,
    Ctime = 4,
    Mtime = 5,
    Owner = 6,
    Mode = 7,
    Size = 8,
    NLink = 9,
}

#[derive(Debug, Copy, Clone)]
pub struct Key {
    ino: u64,
    field: Field,
}

impl Key {
    fn new(ino: u64) -> Self {
        Key {
            ino,
            field: Field::Struct,
        }
    }

    fn field(self, field: Field) -> RawIdent {
        Key {
            ino: self.ino,
            field,
        }.into()
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
            .write_u8(self.field as u8)
            .into()
    }
}


pub use ops::*;

mod ops {
    use super::{key, Inode, Owner, Field};
    use std::convert::TryFrom;
    use antidotec::{rrmap, lwwreg, counter, ReadQuery, ReadReply, UpdateQuery};

    pub fn read(ino: u64) -> ReadQuery {
        rrmap::get(key(ino))
    }
    
    pub fn create(inode: &Inode) -> UpdateQuery {
        let key = key(inode.ino);
    
        rrmap::update(key)
            .push(lwwreg::set_u8(key.field(Field::Kind), inode.kind as u8))
            .push(lwwreg::set_u64(key.field(Field::Parent), inode.parent))
            .push(lwwreg::set_duration(key.field(Field::Atime), inode.atime))
            .push(lwwreg::set_duration(key.field(Field::Ctime), inode.ctime))
            .push(lwwreg::set_duration(key.field(Field::Mtime), inode.mtime))
            .push(lwwreg::set_u64(key.field(Field::Owner), inode.owner.into()))
            .push(lwwreg::set_u32(key.field(Field::Mode), inode.mode))
            .push(lwwreg::set_u64(key.field(Field::Size), inode.size))
            .push(counter::inc(key.field(Field::NLink), inode.nlink as i32))
            .build()
    }

    pub fn update_stats(inode: &Inode) -> UpdateQuery {
        let key = key(inode.ino);

        rrmap::update(key)
            .push(lwwreg::set_u64(key.field(Field::Parent), inode.parent))
            .push(lwwreg::set_duration(key.field(Field::Atime), inode.atime))
            .push(lwwreg::set_duration(key.field(Field::Ctime), inode.ctime))
            .push(lwwreg::set_duration(key.field(Field::Mtime), inode.mtime))
            .push(lwwreg::set_u64(key.field(Field::Owner), inode.owner.into()))
            .push(lwwreg::set_u32(key.field(Field::Mode), inode.mode))
            .build()
    }

    pub fn update_stats_and_size(inode: &Inode) -> UpdateQuery {
        let key = key(inode.ino);

        rrmap::update(key)
            .push(lwwreg::set_u64(key.field(Field::Parent), inode.parent))
            .push(lwwreg::set_duration(key.field(Field::Atime), inode.atime))
            .push(lwwreg::set_duration(key.field(Field::Ctime), inode.ctime))
            .push(lwwreg::set_duration(key.field(Field::Mtime), inode.mtime))
            .push(lwwreg::set_u64(key.field(Field::Owner), inode.owner.into()))
            .push(lwwreg::set_u32(key.field(Field::Mode), inode.mode))
            .push(lwwreg::set_u64(key.field(Field::Size), inode.size))
            .build()
    }

    pub fn incr_link_count(ino: u64, amount: u32) -> UpdateQuery {
        let key = key(ino);

        rrmap::update(key)
            .push(counter::inc(key.field(Field::NLink), amount as i32))
            .build()
    }

    pub fn decr_link_count(ino: u64, amount: u32) -> UpdateQuery {
        let key = key(ino);

        rrmap::update(key)
            .push(counter::inc(key.field(Field::NLink), -(amount as i32)))
            .build()
    }

    pub fn decode(ino: u64, reply: &mut ReadReply, index: usize) -> Option<Inode> {
        let mut map = reply.rrmap(index)?;
        let key = key(ino);

        let kind_byte =
            lwwreg::read_u8(&map.remove(&key.field(Field::Kind)).unwrap().into_lwwreg());
        let parent = map.remove(&key.field(Field::Parent)).unwrap().into_lwwreg();
        let atime = map.remove(&key.field(Field::Atime)).unwrap().into_lwwreg();
        let ctime = map.remove(&key.field(Field::Ctime)).unwrap().into_lwwreg();
        let mtime = map.remove(&key.field(Field::Mtime)).unwrap().into_lwwreg();
        let owner = map.remove(&key.field(Field::Owner)).unwrap().into_lwwreg();
        let mode = map.remove(&key.field(Field::Mode)).unwrap().into_lwwreg();
        let size = map.remove(&key.field(Field::Size)).unwrap().into_lwwreg();
        let nlink = map.remove(&key.field(Field::NLink)).unwrap().into_counter();

        let kind = TryFrom::try_from(kind_byte).expect("invalid code byte");
        let owner = Owner::from(lwwreg::read_u64(&owner));
        
        Some(Inode {
            ino,
            kind,
            parent: lwwreg::read_u64(&parent),
            atime: lwwreg::read_duration(&atime),
            ctime: lwwreg::read_duration(&ctime),
            mtime: lwwreg::read_duration(&mtime),
            owner,
            mode: lwwreg::read_u32(&mode),
            size: lwwreg::read_u64(&size),
            nlink: nlink as u64,
        })
    }

    pub fn remove(ino: u64) -> UpdateQuery {
        rrmap::reset(key(ino))
    }
}