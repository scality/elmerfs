use std::time::Duration;
pub struct Inode {
    pub ino: u64,
    pub parent: u64,
    pub atime: Duration,
    pub ctime: Duration,
    pub mtime: Duration,
    pub size: u64,
}

mod mapping {
    use super::Inode;
    use crate::key::{Key, Kind};
    use antidotec::{crdts, gmap, lwwreg, RawIdent, ReadQuery, UpdateQuery};
    use std::mem;

    #[derive(Debug, Copy, Clone)]
    #[repr(u8)]
    enum Field {
        Struct = 0,
        Parent = 1,
        Atime = 2,
        Ctime = 3,
        Mtime = 4,
        Size = 5,
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
            )).into()
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
        let parent = gmap
            .remove(&key.field(Field::Parent))
            .unwrap()
            .into_lwwreg();

        let atime = gmap
            .remove(&key.field(Field::Atime))
            .unwrap()
            .into_lwwreg();

        let ctime = gmap
            .remove(&key.field(Field::Ctime))
            .unwrap()
            .into_lwwreg();

        let mtime = gmap
            .remove(&key.field(Field::Mtime))
            .unwrap()
            .into_lwwreg();

        let size = gmap
            .remove(&key.field(Field::Size))
            .unwrap()
            .into_lwwreg();

        Inode {
            ino,
            parent: lwwreg::read_u64(&parent),
            atime: lwwreg::read_duration(&atime),
            ctime: lwwreg::read_duration(&ctime),
            mtime: lwwreg::read_duration(&mtime),
            size: lwwreg::read_u64(&size),
        }
    }
}
