use fuse::{ReplyEntry, ReplyAttr, ReplyEmpty, ReplyOpen, ReplyDirectory};
use std::sync::mpsc;

pub type Sender = mpsc::SyncSender<Op>;
pub type Receiver = mpsc::Receiver<Op>;

#[derive(Debug)]
pub enum Op {
    GetAttr(GetAttr),
    Lookup(Lookup),
    OpenDir(OpenDir),
    ReleaseDir(ReleaseDir),
    ReadDir(ReadDir),
    MkDir(MkDir),
    RmDir(RmDir),
    MkNod(MkNod),
}

impl Op {
    pub fn name(&self) -> &'static str {
        match self {
            Self::GetAttr(_) => "getattr",
            Self::Lookup(_) => "lookup",
            Self::OpenDir(_) => "opendir",
            Self::ReleaseDir(_) => "releasedir",
            Self::ReadDir(_) => "readdir",
            Self::MkDir(_) => "mkdir",
            Self::RmDir(_) => "rmdir",
            Self::MkNod(_) => "mknod",
        }
    }
}

#[derive(Debug)]
pub struct GetAttr {
    pub reply: ReplyAttr,
    pub ino: u64,
}

#[derive(Debug)]
pub struct Lookup {
    pub reply: ReplyEntry,
    pub name: String,
    pub parent_ino: u64,
}

#[derive(Debug)]
pub struct OpenDir {
    pub reply: ReplyOpen,
    pub ino: u64,
}

#[derive(Debug)]
pub struct ReleaseDir {
    pub reply: ReplyEmpty,
    pub ino: u64,
    pub fh: u64,
}

#[derive(Debug)]
pub struct ReadDir {
    pub reply: ReplyDirectory,
    pub ino: u64,
    pub fh: u64,
    pub offset: i64,
}

#[derive(Debug)]
pub struct MkDir {
    pub reply: ReplyEntry,
    pub parent_ino: u64,
    pub name: String,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
}

#[derive(Debug)]
pub struct RmDir {
    pub reply: ReplyEmpty,
    pub parent_ino: u64,
    pub name: String,
}


#[derive(Debug)]
pub struct MkNod {
    pub reply: ReplyEntry,
    pub parent_ino: u64,
    pub name: String,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
}

pub fn sync_channel(size: usize) -> (Sender, Receiver) {
    mpsc::sync_channel(size)
}
