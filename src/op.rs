use fuse::ReplyEntry;
use std::sync::mpsc;

pub type Sender = mpsc::SyncSender<Op>;
pub type Receiver = mpsc::Receiver<Op>;

#[derive(Debug)]
pub enum Op {
    MkNod(MkNod),
}

#[derive(Debug)]
pub struct MkNod {
    pub reply: ReplyEntry,
    pub name: String,
    pub parent: u64,
    pub mode: u32,
    pub rdev: u32,
}

pub fn sync_channel(size: usize) -> (Sender, Receiver) {
    mpsc::sync_channel(size)
}
