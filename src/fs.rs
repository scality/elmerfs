use crate::driver::{Driver, DriverError};
use crate::model::inode::{Ino, Owner};
use crate::view::View;
use antidotec::Bytes;
use fuser::{Filesystem, *};
use nix::{errno::Errno, libc};
use std::ffi::OsStr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::runtime::Runtime;
use tokio::time as tokiot;
use tracing_futures::Instrument;

const ATTEMPTS_ON_ABORTED: u16 = 160;
const ATTEMPTS_WAIT_MS: u16 = 500;

macro_rules! function {
    () => {{
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let name = type_name_of(f);
        (&name[..name.len() - 3]).rsplit("::").next().unwrap()
    }};
}

macro_rules! check_utf8 {
    ($reply:expr, $arg:ident) => {
        match $arg.to_str() {
            Some($arg) => String::from($arg),
            None => {
                $reply.error(Errno::EINVAL as libc::c_int);
                return;
            }
        }
    };
}

macro_rules! check_name {
    ($reply:expr, $str:ident) => {{
        let n = check_utf8!($reply, $str);

        match n.parse() {
            Ok(name) => name,
            Err(_) => {
                $reply.error(Errno::EINVAL as libc::c_int);
                return;
            }
        }
    }};
}

fn ttl() -> std::time::Duration {
    Duration::from_secs(15)
}

macro_rules! session {
    ($runtime:expr, $req:expr, $reply:ident, $op:expr, $ok:ident => $resp:block) => {
        let unique = $req.unique();
        let (uid, gid) = ($req.uid(), $req.gid());

        let task = async move {
            let mut final_result = None;
            for _ in 0..ATTEMPTS_ON_ABORTED {
                let result = $op.await;

                match &result {
                    Err(DriverError::Sys(Errno::ENOENT)) => {
                        final_result = Some(result);
                        break;
                    }
                    Err(error) if error.should_retry() => {
                        tracing::debug!(?error, "retrying aborted.");
                    },
                    _ => {
                        final_result = Some(result);
                        break;
                    }
                }

                // Exponential backoff would be way better in term of system
                // stress. But this is good for now.
                tokiot::sleep(Duration::from_millis(ATTEMPTS_WAIT_MS as u64))
                    .await;
            };


            match final_result {
                Some(Ok($ok)) => {
                    tracing::debug!("ok");
                    $resp;
                }
                Some(Err(error)) => {
                    match error {
                        DriverError::Sys(errno) => {
                            $reply.error(errno as libc::c_int);
                        },
                        DriverError::Antidote(_) => {
                            $reply.error(Errno::EIO as libc::c_int);
                        }
                        DriverError::Nix(error) => {
                            match error {
                                nix::Error::Sys(errno) => {
                                    $reply.error(errno as libc::c_int);
                                },
                                _ => {
                                    $reply.error(Errno::EINVAL as libc::c_int);
                                }
                            }
                        }
                    }
                },
                None => {
                    tracing::error!("no more attempts. Dropping...");
                    $reply.error(Errno::EIO as libc::c_int);
                }
            }
        };
        let task = task.instrument(
            tracing::trace_span!("session", op = function!(), id = unique, uid, gid)
        );

        $runtime.spawn(task);
    };

    ($runtime:expr, $req:expr, $reply:ident, $op:expr, _ => $resp:block) => {
        session!($runtime, $req, $reply, $op, _r => $resp);
    };
}

#[derive(Clone)]
pub struct Elmerfs {
    pub(crate) runtime: Arc<Runtime>,
    pub(crate) forced_view: Option<View>,
    pub(crate) driver: Arc<Driver>,
}

impl Elmerfs {
    fn view(&self, req: &Request) -> View {
        self.forced_view.unwrap_or(View { uid: req.uid() })
    }
}

impl Filesystem for Elmerfs {
    fn getattr(&mut self, req: &Request, ino: u64, reply: ReplyAttr) {
        let driver = self.driver.clone();
        let view = self.view(req);

        session!(&mut self.runtime, req, reply, driver.getattr(view, Ino(ino)), attrs => {
            rayon::spawn_fifo(move || reply.attr(&ttl(), &attrs));
        });
    }

    fn opendir(&mut self, req: &Request, ino: u64, _flags: i32, reply: ReplyOpen) {
        let driver = self.driver.clone();
        let view = self.view(req);

        session!(&mut self.runtime, req, reply, driver.opendir(view, Ino(ino)), _ => {
            let flags = 0;
            reply.opened(ino, flags);
        });
    }

    fn releasedir(&mut self, req: &Request, ino: u64, _fh: u64, _flags: i32, reply: ReplyEmpty) {
        let driver = self.driver.clone();
        let view = self.view(req);

        session!(&mut self.runtime, req, reply, driver.opendir(view, Ino(ino)), _ => {
            rayon::spawn_fifo(move || reply.ok());
        });
    }

    fn readdir(
        &mut self,
        req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let driver = self.driver.clone();
        let view = self.view(req);

        session!(&mut self.runtime, req, reply, driver.readdir(view, Ino(ino), offset), entries => {
            rayon::spawn_fifo(move || {
                for (i, entry) in entries.into_iter().enumerate() {
                    let offset = offset + i as i64 + 1;
                    let full = reply.add(u64::from(entry.ino), offset, entry.kind, entry.name);
                    if full {
                        break;
                    }
                }
                reply.ok();
            })
        });
    }

    fn lookup(&mut self, req: &Request, parent_ino: u64, name: &OsStr, reply: ReplyEntry) {
        let name = check_name!(reply, name);
        let driver = self.driver.clone();
        let view = self.view(req);

        session!(&mut self.runtime, req, reply, driver.lookup(view, Ino(parent_ino), &name), attrs => {
            let generation = 0;
            rayon::spawn_fifo(move ||
                reply.entry(&ttl(), &attrs, generation));
        });
    }

    fn mkdir(
        &mut self,
        req: &Request,
        parent_ino: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let owner = Owner {
            gid: req.gid(),
            uid: req.uid(),
        };
        let name = check_name!(reply, name);
        let driver = self.driver.clone();
        let view = self.view(req);

        session!(&mut self.runtime, req, reply, driver.mkdir(view, owner, mode, Ino(parent_ino), &name), attrs => {
            let generation = 0;
            rayon::spawn_fifo(move || reply.entry(&ttl(), &attrs, generation));
        });
    }

    fn rmdir(&mut self, req: &Request, parent_ino: u64, name: &OsStr, reply: ReplyEmpty) {
        let name = check_name!(reply, name);
        let driver = self.driver.clone();
        let view = self.view(req);

        session!(&mut self.runtime, req, reply, driver.rmdir(view, Ino(parent_ino), &name), _ => {
            rayon::spawn_fifo(move || reply.ok());
        });
    }

    fn mknod(
        &mut self,
        req: &Request,
        parent_ino: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        let name = check_name!(reply, name);
        let owner = Owner {
            gid: req.gid(),
            uid: req.uid(),
        };
        let driver = self.driver.clone();
        let view = self.view(req);

        session!(&mut self.runtime, req, reply, driver.mknod(view, owner, mode, Ino(parent_ino), &name, rdev), attrs => {
            let generation = 0;

            rayon::spawn_fifo(move || reply.entry(&ttl(), &attrs, generation));
        });
    }

    fn unlink(&mut self, req: &Request, parent_ino: u64, name: &OsStr, reply: ReplyEmpty) {
        let name = check_name!(reply, name);
        let driver = self.driver.clone();
        let view = self.view(req);

        session!(&mut self.runtime, req, reply, driver.unlink(view, Ino(parent_ino), &name), _ => {
            rayon::spawn_fifo(move || reply.ok());
        });
    }

    fn setattr(
        &mut self,
        req: &Request,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let now = SystemTime::now();
        let t2d = |t: TimeOrNow| match t {
            TimeOrNow::SpecificTime(t) => t,
            TimeOrNow::Now => now,
        };
        let atime = atime.map(t2d);
        let mtime = mtime.map(t2d);
        let driver = self.driver.clone();
        let view = self.view(req);

        session!(&mut self.runtime,
            req,
            reply,
            driver.setattr(view, Ino(ino), mode, uid, gid, size, atime, mtime),
            attrs => {
                rayon::spawn_fifo(move || reply.attr(&ttl(), &attrs));
            }
        );
    }

    fn open(&mut self, req: &Request, ino: u64, _flags: i32, reply: ReplyOpen) {
        let driver = self.driver.clone();

        session!(&mut self.runtime, req, reply, driver.open(Ino(ino)), fh => {
            let flags = 0;
            rayon::spawn_fifo(move || reply.opened(u64::from(fh), flags));
        });
    }

    fn release(
        &mut self,
        req: &Request,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let driver = self.driver.clone();
        let view = self.view(req);

        session!(&mut self.runtime, req, reply, driver.release(view, Ino(fh)), _ => {
            rayon::spawn_fifo(move || reply.ok());
        });
    }

    fn flush(&mut self, req: &Request, _ino: u64, fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        let driver = self.driver.clone();

        session!(&mut self.runtime, req, reply, driver.flush(Ino(fh)), _ => {
            rayon::spawn_fifo(move || reply.ok());
        });
    }

    fn fsync(&mut self, req: &Request, _ino: u64, fh: u64, _datasync: bool, reply: ReplyEmpty) {
        let driver = self.driver.clone();

        session!(&mut self.runtime, req, reply, driver.fsync(Ino(fh)), _ => {
            rayon::spawn_fifo(move || reply.ok());
        });
    }

    fn write(
        &mut self,
        req: &Request,
        _ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        if offset < 0 {
            reply.error(Errno::EINVAL as libc::c_int);
            return;
        }
        let offset = offset as u64;
        let driver = self.driver.clone();
        let data = Bytes::copy_from_slice(data);

        session!(&mut self.runtime, req, reply, driver.write(Ino(fh), data.clone(), offset), _ => {
            rayon::spawn_fifo(move || reply.written(data.len() as u32));
        });
    }

    fn read(
        &mut self,
        req: &Request,
        _ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        if offset < 0 {
            reply.error(Errno::EINVAL as libc::c_int);
            return;
        }
        let offset = offset as u64;
        let driver = self.driver.clone();

        session!(&mut self.runtime, req, reply, driver.read(Ino(fh), offset, size), data => {
            rayon::spawn_fifo(move || reply.data(&data));
        });
    }

    fn rename(
        &mut self,
        req: &Request,
        parent_ino: u64,
        name: &OsStr,
        newparent_ino: u64,
        newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        let name = check_name!(reply, name);
        let newname = check_name!(reply, newname);
        let driver = self.driver.clone();
        let view = self.view(req);

        session!(&mut self.runtime, req, reply, driver.rename(view, Ino(parent_ino), &name, Ino(newparent_ino), &newname), _ => {
            rayon::spawn_fifo(move || reply.ok());
        });
    }

    fn link(
        &mut self,
        req: &Request,
        ino: u64,
        newparent_ino: u64,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        let newname = check_name!(reply, newname);
        let driver = self.driver.clone();
        let view = self.view(req);

        session!(&mut self.runtime, req, reply, driver.link(view, Ino(ino), Ino(newparent_ino), &newname), attrs => {
            let generation = 0;
            rayon::spawn_fifo(move || reply.entry(&ttl(), &attrs, generation));
        });
    }

    fn symlink(
        &mut self,
        req: &Request,
        parent_ino: u64,
        name: &OsStr,
        link: &Path,
        reply: ReplyEntry,
    ) {
        let link = link.as_os_str();
        let link = check_utf8!(reply, link);
        let name = check_name!(reply, name);
        let owner = Owner {
            gid: req.gid(),
            uid: req.uid(),
        };
        let driver = self.driver.clone();
        let view = self.view(req);

        session!(&mut self.runtime, req, reply, driver.symlink(view, Ino(parent_ino), owner, &name, &link), attrs => {
            let generation = 0;
            rayon::spawn_fifo(move || reply.entry(&ttl(), &attrs, generation));
        });
    }

    fn readlink(&mut self, req: &Request, ino: u64, reply: ReplyData) {
        let driver = self.driver.clone();
        let view = self.view(req);

        session!(&mut self.runtime, req, reply, driver.read_link(view, Ino(ino)), path => {
            rayon::spawn_fifo(move || reply.data(path.as_bytes()));
        });
    }

    fn destroy(&mut self) {
        let summaries = self.driver.metrics_summary();
        crate::metrics::fmt_timed_operations(&mut std::io::stderr(), summaries).unwrap();
    }
}
