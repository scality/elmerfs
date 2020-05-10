#[derive(Debug, Copy, Clone)]
#[repr(u8)]
pub enum Kind {
    Inode = 0,
}

#[derive(Debug, Copy, Clone)]
pub struct Key<P> {
    pub kind: Kind,
    pub payload: P
}

impl<P> Key<P> {
    pub fn new(kind: Kind, payload: P) -> Self {
        Self {
            kind,
            payload,
        }
    }
}
