use std::{convert::TryFrom, time::Duration, collections::BTreeMap};
use fuse::FileType;

#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Kind {
    Regular = 0,
    Directory = 1,
}

impl Kind {
    pub fn to_file_type(self) -> FileType {
        match self {
            Kind::Regular => FileType::RegularFile,
            Kind::Directory => FileType::Directory,
        }
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
            _ => Err(InvalidKindByte)
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
    pub size: u64,
}

pub type DirEntries = BTreeMap<String, u64>;

pub use self::mapping::{decode, read, update, read_dir, update_dir, decode_dir};

mod mapping {
    use super::{Inode, DirEntries};
    use crate::key::{Key, Kind};
    use antidotec::{crdts, gmap, lwwreg, RawIdent, ReadQuery, UpdateQuery};
    use std::mem;
    use std::convert::TryFrom;

    #[derive(Debug, Copy, Clone)]
    #[repr(u8)]
    enum Field {
        Struct = 0,
        Kind = 1,
        Parent = 2,
        Atime = 3,
        Ctime = 4,
        Mtime = 5,
        Size = 6,
        DirEntries = 7,
    }

    impl Field {
        const COUNT: usize = 5;
    }

    #[derive(Debug, Copy, Clone)]
    struct Id {
        field: Field,
        ino: u64,
    }

    #[derive(Debug, Copy, Clone)]
    struct InodeKey(Key<Id>);

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

        pub fn field(self, field: Field) -> RawIdent {
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
        gmap::get(InodeKey::new(ino))
    }

    pub fn update(inode: &Inode) -> UpdateQuery {
        let key = InodeKey::new(inode.ino);

        gmap::update(key, Field::COUNT)
            .push(lwwreg::set_u8(key.field(Field::Kind), inode.kind as u8))
            .push(lwwreg::set_u64(key.field(Field::Parent), inode.parent))
            .push(lwwreg::set_duration(key.field(Field::Atime), inode.atime))
            .push(lwwreg::set_duration(key.field(Field::Ctime), inode.ctime))
            .push(lwwreg::set_duration(key.field(Field::Mtime), inode.mtime))
            .push(lwwreg::set_u64(key.field(Field::Size), inode.size))
            .build()
    }

    pub fn decode(ino: u64, mut gmap: crdts::GMap) -> Inode {
        let key = InodeKey::new(ino);

        // FIXME: Add helper for decoding from a map or update the API to make
        // it easier if it become a common pattern.
        let kind_byte = lwwreg::read_u8(&gmap.remove(&key.field(Field::Kind)).unwrap().into_lwwreg());
        let parent = gmap
            .remove(&key.field(Field::Parent))
            .unwrap()
            .into_lwwreg();
        let atime = gmap.remove(&key.field(Field::Atime)).unwrap().into_lwwreg();
        let ctime = gmap.remove(&key.field(Field::Ctime)).unwrap().into_lwwreg();
        let mtime = gmap.remove(&key.field(Field::Mtime)).unwrap().into_lwwreg();
        let size = gmap.remove(&key.field(Field::Size)).unwrap().into_lwwreg();

        let kind = TryFrom::try_from(kind_byte).expect("invalid code byte");

        Inode {
            ino,
            kind,
            parent: lwwreg::read_u64(&parent),
            atime: lwwreg::read_duration(&atime),
            ctime: lwwreg::read_duration(&ctime),
            mtime: lwwreg::read_duration(&mtime),
            size: lwwreg::read_u64(&size),
        }
    }

    pub fn read_dir(ino: u64) -> ReadQuery {
        let key = InodeKey::new(ino).field(Field::DirEntries);
        gmap::get(key)
    }

    pub fn update_dir(ino: u64, entries: &DirEntries) -> UpdateQuery {
        let key = InodeKey::new(ino);

        let mut gmap = gmap::update(key.field(Field::DirEntries), entries.len());

        for (name, ino) in entries {
            let name_ident = Vec::from(name.as_bytes());
            gmap = gmap.push(lwwreg::set_u64(name_ident, *ino));
        }

        gmap.build()
    }

    pub fn decode_dir(gmap: crdts::GMap) -> DirEntries {
        let mut entries = DirEntries::new();
        for (name_ident, ino_reg) in gmap {
            let name = String::from_utf8(name_ident).unwrap();

            let ino = lwwreg::read_u64(&ino_reg.into_lwwreg());
            entries.insert(name, ino);
        }

        entries
    }
}
