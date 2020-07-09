use crate::driver::{Driver, Error};
use crate::inode::Owner;
use crate::op::{self, *};
use async_std::task;
use nix::{errno::Errno, libc};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use time;
use tracing::{self, debug, error, warn};

#[tracing::instrument(skip(driver, op_receiver))]
pub(super) fn drive(driver: Arc<Driver>, op_receiver: op::Receiver) {
    let ttl = || time::Timespec::new(600, 0);

    while let Ok(op) = op_receiver.recv() {
        let driver = driver.clone();
        let name = op.name();

        // FIXME: Reply are done in the asynchronous tasks but may be blocking
        // for a significant amount of time. We should ensure that the scheduler
        // is handling this gracefully or that we explicity
        // call them inside a `spawn_blocking` block.
        match op {
            Op::GetAttr(getattr) => {
                task::spawn(async move {
                    match handle_result(name, driver.getattr(getattr.ino).await) {
                        Ok(attrs) => {
                            getattr.reply.attr(&ttl(), &attrs);
                        }
                        Err(errno) => {
                            getattr.reply.error(errno as libc::c_int);
                        }
                    }
                });
            }
            Op::SetAttr(setattr) => {
                task::spawn(async move {
                    match handle_result(
                        name,
                        driver
                            .setattr(
                                setattr.ino,
                                setattr.mode,
                                setattr.uid,
                                setattr.gid,
                                setattr.size,
                                setattr.atime.map(duration_from_timespec),
                                setattr.mtime.map(duration_from_timespec),
                            )
                            .await,
                    ) {
                        Ok(attr) => {
                            setattr.reply.attr(&ttl(), &attr);
                        }
                        Err(errno) => {
                            setattr.reply.error(errno as libc::c_int);
                        }
                    }
                });
            }
            Op::Lookup(lookup) => {
                task::spawn(async move {
                    match handle_result(name, driver.lookup(lookup.parent_ino, &lookup.name).await)
                    {
                        Ok(attrs) => {
                            let generation = 0;
                            lookup.reply.entry(&ttl(), &attrs, generation);
                        }
                        Err(errno) => {
                            lookup.reply.error(errno as libc::c_int);
                        }
                    }
                });
            }
            Op::OpenDir(opendir) => {
                task::spawn(async move {
                    match handle_result(name, driver.opendir(opendir.ino).await) {
                        Ok(_) => {
                            let flags = 0;
                            opendir.reply.opened(opendir.ino, flags);
                        }
                        Err(errno) => {
                            opendir.reply.error(errno as libc::c_int);
                        }
                    }
                });
            }
            Op::ReleaseDir(releasedir) => {
                task::spawn(async move {
                    match handle_result(name, driver.releasedir(releasedir.ino).await) {
                        Ok(_) => releasedir.reply.ok(),
                        Err(errno) => {
                            releasedir.reply.error(errno as libc::c_int);
                        }
                    }
                });
            }
            Op::ReadDir(mut readdir) => {
                task::spawn(async move {
                    match handle_result(name, driver.readdir(readdir.ino, readdir.offset).await) {
                        Ok(entries) => {
                            for (i, entry) in entries.into_iter().enumerate() {
                                let offset = readdir.offset + i as i64 + 1;

                                let full =
                                    readdir.reply.add(entry.ino, offset, entry.kind, entry.name);
                                if full {
                                    break;
                                }
                            }

                            readdir.reply.ok();
                        }
                        Err(errno) => {
                            readdir.reply.error(errno as libc::c_int);
                        }
                    }
                });
            }
            Op::MkDir(mkdir) => {
                task::spawn(async move {
                    let owner = Owner {
                        gid: mkdir.gid,
                        uid: mkdir.uid,
                    };

                    let result = driver
                        .mkdir(owner, mkdir.mode, mkdir.parent_ino, mkdir.name)
                        .await;
                    match handle_result(name, result) {
                        Ok(attr) => {
                            let generation = 0;
                            mkdir.reply.entry(&ttl(), &attr, generation);
                        }
                        Err(errno) => {
                            mkdir.reply.error(errno as libc::c_int);
                        }
                    }
                });
            }
            Op::RmDir(rmdir) => {
                task::spawn(async move {
                    let result = driver.rmdir(rmdir.parent_ino, rmdir.name).await;

                    match handle_result(name, result) {
                        Ok(_) => {
                            rmdir.reply.ok();
                        }
                        Err(errno) => {
                            rmdir.reply.error(errno as libc::c_int);
                        }
                    }
                });
            }
            Op::MkNod(mknod) => {
                let owner = Owner {
                    gid: mknod.gid,
                    uid: mknod.uid,
                };

                task::spawn(async move {
                    let result = driver
                        .mknod(owner, mknod.mode, mknod.parent_ino, mknod.name, mknod.rdev)
                        .await;

                    match handle_result(name, result) {
                        Ok(attr) => {
                            let generation = 0;
                            mknod.reply.entry(&ttl(), &attr, generation);
                        }
                        Err(errno) => {
                            mknod.reply.error(errno as libc::c_int);
                        }
                    }
                });
            }
            Op::Unlink(unlink) => {
                task::spawn(async move {
                    match handle_result(name, driver.unlink(unlink.parent_ino, unlink.name).await) {
                        Ok(_) => {
                            unlink.reply.ok();
                        }
                        Err(errno) => {
                            unlink.reply.error(errno as libc::c_int);
                        }
                    }
                });
            }
            Op::Open(open) => {
                task::spawn(async move {
                    match handle_result(name, driver.open(open.ino).await) {
                        Ok(_) => {
                            let flags = 0;
                            open.reply.opened(open.ino, flags);
                        }
                        Err(errno) => {
                            open.reply.error(errno as libc::c_int);
                        }
                    }
                });
            }
            Op::Release(release) => {
                task::spawn(async move {
                    match handle_result(name, driver.open(release.ino).await) {
                        Ok(_) => {
                            release.reply.ok();
                        }
                        Err(errno) => {
                            release.reply.error(errno as libc::c_int);
                        }
                    }
                });
            }
            Op::Write(write) => {
                task::spawn(async move {
                    match handle_result(
                        name,
                        driver.write(write.ino, &write.data, write.offset).await,
                    ) {
                        Ok(_) => {
                            write.reply.written(write.data.len() as u32);
                        }
                        Err(errno) => {
                            write.reply.error(errno as libc::c_int);
                        }
                    }
                });
            }
            Op::Read(read) => {
                task::spawn(async move {
                    match handle_result_silent(
                        name,
                        driver.read(read.ino, read.offset, read.size).await,
                    ) {
                        Ok(data) => {
                            read.reply.data(&data);
                        }
                        Err(errno) => {
                            read.reply.error(errno as libc::c_int);
                        }
                    }
                });
            }
            Op::Rename(rename) => {
                task::spawn(async move {
                    match handle_result(
                        name,
                        driver
                            .rename(
                                rename.parent_ino,
                                rename.name,
                                rename.new_parent_ino,
                                rename.new_name,
                            )
                            .await,
                    ) {
                        Ok(()) => rename.reply.ok(),
                        Err(errno) => rename.reply.error(errno as libc::c_int),
                    }
                });
            }
            Op::Link(link) => {
                task::spawn(async move {
                    match handle_result(
                        name,
                        driver
                            .link(link.ino, link.new_parent_ino, link.new_name)
                            .await,
                    ) {
                        Ok(attr) => {
                            link.reply.entry(&ttl(), &attr, 0);
                        }
                        Err(errno) => {
                            link.reply.error(errno as libc::c_int);
                        }
                    }
                });
            }
            Op::Symlink(symlink) => {
                task::spawn(async move {
                    let owner = Owner {
                        gid: symlink.gid,
                        uid: symlink.uid,
                    };

                    match handle_result(
                        name,
                        driver
                            .symlink(symlink.parent_ino, owner, symlink.name, symlink.link)
                            .await,
                    ) {
                        Ok(attr) => {
                            symlink.reply.entry(&ttl(), &attr, 0);
                        }
                        Err(errno) => {
                            symlink.reply.error(errno as libc::c_int);
                        }
                    }
                });
            }
            Op::ReadLink(readlink) => {
                task::spawn(async move {
                    match handle_result(name, driver.read_link(readlink.ino).await) {
                        Ok(path) => {
                            readlink.reply.data(path.as_bytes());
                        }
                        Err(errno) => {
                            readlink.reply.error(errno as libc::c_int);
                        }
                    }
                });
            }
        }
    }
}

fn handle_result_silent<U: Send>(name: &str, result: Result<U, Error>) -> Result<U, Errno> {
    match result {
        Ok(result) => {
            debug!(name, "success");
            Ok(result)
        }
        Err(Error::Antidote(error)) => {
            error!(name, ?error, "antidote error");
            Err(Errno::EIO)
        }
        Err(Error::Sys(errno)) => {
            warn!(name, ?errno, "system error");
            Err(errno)
        }
    }
}

fn handle_result<U: Debug + Send>(name: &str, result: Result<U, Error>) -> Result<U, Errno> {
    match result {
        Ok(result) => {
            debug!(name, ?result, "success");
            Ok(result)
        }
        Err(Error::Antidote(error)) => {
            error!(name, ?error, "antidote error");
            Err(Errno::EIO)
        }
        Err(Error::Sys(Errno::ENOENT)) => {
            debug!(name, "enoent");
            Err(Errno::ENOENT)
        }
        Err(Error::Sys(errno)) => {
            warn!(name, ?errno, "system error");
            Err(errno)
        }
    }
}

fn duration_from_timespec(t: time::Timespec) -> std::time::Duration {
    Duration::new(t.sec as u64, t.nsec as u32)
}
