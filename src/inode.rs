use fuse::{FileAttr, FileType};
use std::{convert::TryFrom, time::Duration};

#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
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

pub use self::mapping::{
    decode, decode_link, decr_link_count, incr_link_count, read, read_link,
    remove, set_link, update, update_stats, remove_link,
    InodeKey as Key, NLinkInc,
};

mod mapping {
    use super::{Inode, Owner};
    use crate::key::{Key, Kind};
    use antidotec::{counter, crdts, lwwreg, rrmap, RawIdent, ReadQuery, UpdateQuery};
    use std::convert::TryFrom;
    use std::mem;

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
        SymlinkPath = 10,
    }

    #[derive(Debug, Copy, Clone)]
    pub struct Id {
        field: Field,
        ino: u64,
    }

    #[derive(Debug, Copy, Clone)]
    pub struct InodeKey(Key<Id>);

    impl InodeKey {
        pub fn new(ino: u64) -> Self {
            InodeKey(Key::new(
                Kind::Inode,
                Id {
                    ino,
                    field: Field::Struct,
                },
            ))
        }

        pub fn symlink(ino: u64) -> RawIdent {
            Self::new(ino).field(Field::SymlinkPath)
        }

        fn field(self, field: Field) -> RawIdent {
            InodeKey(Key::new(
                Kind::Inode,
                Id {
                    ino: self.0.payload.ino,
                    field,
                },
            ))
            .into()
        }
    }

    impl Into<RawIdent> for InodeKey {
        fn into(self) -> RawIdent {
            let mut ident = RawIdent::with_capacity(mem::size_of::<Self>());
            ident.push(self.0.kind as u8);
            ident.push(self.0.payload.field as u8);

            let ino_bytes = self.0.payload.ino.to_le_bytes();
            ident.extend_from_slice(&ino_bytes);

            ident
        }
    }

    pub fn read(ino: u64) -> ReadQuery {
        rrmap::get(InodeKey::new(ino))
    }

    pub fn update_stats(inode: &Inode) -> UpdateQuery {
        let key = InodeKey::new(inode.ino);

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

    #[derive(Debug, Copy, Clone)]
    pub struct NLinkInc(pub i32);

    pub fn update(inode: &Inode, NLinkInc(nlink_inc): NLinkInc) -> UpdateQuery {
        let key = InodeKey::new(inode.ino);

        rrmap::update(key)
            .push(lwwreg::set_u8(key.field(Field::Kind), inode.kind as u8))
            .push(lwwreg::set_u64(key.field(Field::Parent), inode.parent))
            .push(lwwreg::set_duration(key.field(Field::Atime), inode.atime))
            .push(lwwreg::set_duration(key.field(Field::Ctime), inode.ctime))
            .push(lwwreg::set_duration(key.field(Field::Mtime), inode.mtime))
            .push(lwwreg::set_u64(key.field(Field::Owner), inode.owner.into()))
            .push(lwwreg::set_u32(key.field(Field::Mode), inode.mode))
            .push(lwwreg::set_u64(key.field(Field::Size), inode.size))
            .push(counter::inc(key.field(Field::NLink), nlink_inc))
            .build()
    }

    pub fn incr_link_count(ino: u64, amount: u32) -> UpdateQuery {
        let key = InodeKey::new(ino);

        rrmap::update(key)
            .push(counter::inc(key.field(Field::NLink), amount as i32))
            .build()
    }

    pub fn decr_link_count(ino: u64, amount: u32) -> UpdateQuery {
        let key = InodeKey::new(ino);

        rrmap::update(key)
            .push(counter::inc(key.field(Field::NLink), -(amount as i32)))
            .build()
    }

    pub fn decode(ino: u64, mut map: crdts::RrMap) -> Inode {
        let key = InodeKey::new(ino);

        // FIXME: Add helper for decoding from a map or update the API to make
        // it easier if it become a common pattern.
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
        Inode {
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
        }
    }

    pub fn remove(ino: u64) -> UpdateQuery {
        rrmap::reset(InodeKey::new(ino))
    }

    pub fn remove_link(ino: u64) -> UpdateQuery {
        let key = InodeKey::new(ino).field(Field::SymlinkPath);
        lwwreg::set(key, vec![])
    }

    pub fn read_link(ino: u64) -> ReadQuery {
        let key = InodeKey::new(ino).field(Field::SymlinkPath);
        lwwreg::get(key)
    }

    pub fn set_link(ino: u64, path: String) -> UpdateQuery {
        let key = InodeKey::new(ino).field(Field::SymlinkPath);
        lwwreg::set(key, path.into_bytes())
    }

    pub fn decode_link(reg: crdts::LwwReg) -> String {
        String::from_utf8(reg).unwrap()
    }
}
