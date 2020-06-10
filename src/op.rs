use fuse::{ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite};
use std::sync::mpsc;
use time::Timespec;

pub type Sender = mpsc::SyncSender<Op>;
pub type Receiver = mpsc::Receiver<Op>;

#[derive(Debug)]
pub enum Op {
    GetAttr(GetAttr),
    SetAttr(SetAttr),
    Lookup(Lookup),
    OpenDir(OpenDir),
    ReleaseDir(ReleaseDir),
    ReadDir(ReadDir),
    MkDir(MkDir),
    RmDir(RmDir),
    MkNod(MkNod),
    Unlink(Unlink),
    Open(Open),
    Release(Release),
    Write(Write),
    Read(Read),
}

impl Op {
    pub fn name(&self) -> &'static str {
        match self {
            Self::GetAttr(_) => "getattr",
            Self::SetAttr(_) => "setattr",
            Self::Lookup(_) => "lookup",
            Self::OpenDir(_) => "opendir",
            Self::ReleaseDir(_) => "releasedir",
            Self::ReadDir(_) => "readdir",
            Self::MkDir(_) => "mkdir",
            Self::RmDir(_) => "rmdir",
            Self::MkNod(_) => "mknod",
            Self::Unlink(_) => "unlink",
            Self::Open(_) => "open",
            Self::Release(_) => "release",
            Self::Write(_) => "write",
            Self::Read(_) => "read",
        }
    }
}

#[derive(Debug)]
pub struct GetAttr {
    pub reply: ReplyAttr,
    pub ino: u64,
}

#[derive(Debug)]
pub struct SetAttr {
    pub ino: u64,
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub size: Option<u64>,
    pub atime: Option<Timespec>,
    pub mtime: Option<Timespec>,
    pub fh: Option<u64>,
    pub reply: ReplyAttr,
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

#[derive(Debug)]
pub struct Unlink {
    pub reply: ReplyEmpty,
    pub parent_ino: u64,
    pub name: String,
}

#[derive(Debug)]
pub struct Write {
    pub reply: ReplyWrite,
    pub ino: u64,
    pub fh: u64,
    pub offset: u64,
    pub data: Vec<u8>,
}

#[derive(Debug)]
pub struct Read {
    pub reply: ReplyData,
    pub ino: u64,
    pub fh: u64,
    pub offset: u64,
    pub size: u32,
}

#[derive(Debug)]
pub struct Open {
    pub reply: ReplyOpen,
    pub ino: u64,
}

#[derive(Debug)]
pub struct Release {
    pub reply: ReplyEmpty,
    pub fh: u64,
    pub ino: u64,
}

pub fn sync_channel(size: usize) -> (Sender, Receiver) {
    mpsc::sync_channel(size)
}
