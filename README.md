elmerfs
-------

elmerfs is a filesystem that leverage **Conflict Free Replicated Data Type**
[CRDT](https://crdt.tech/) on top of [AntidoteDB](https://www.antidotedb.eu/)
to be eventually consistent in a active-active geo distributed scenario.

### Building the project

The project require a recent version of rustc (tested with 1.42):

```
git clone git@github.com:scality/elmerfs.git
cd elmerfs
git submodule update --init --recursive
cargo run --bin main -- [OPTIONS]
```

When running the main binary, you will have the following options:

```
elmerfs

USAGE:
    main [FLAGS] [OPTIONS] --mount <MOUNTPOINT>

FLAGS:
    -h, --help        Prints help information
        --no-locks
    -V, --version     Prints version information

OPTIONS:
    -s, --antidote <URL>...       [default: 127.0.0.1:8101]
        --force-view <UID>
        --listing-flavor <lf>     [default: partial]  [possible values: full, partial]
    -m, --mount <MOUNTPOINT>
```

A usual launch will also include logs and backtrace env variables, for example, to
run the fs on a local antidote cluster:

```
RUST_BACKTRACE=1 RUST_LOG=info cargo run --release --bin main -- --mount ../elmerfsmount/ --antidote=127.0.0.1:8101 --antidote=127.0.0.1:8102 --antidote=127.0.0.1:8103 --no-locks
```

Note that is is important that all antidote IPs address are from the same
datacenter !

### Specifics notions

#### The View

Each `elmerfs` process have one or multiple view attached to it.
For convenience the view is based on the user which perform the operations.
It is expected that the user cannot perform conflicting update by himself against
himself. This means that an user should always contact and perform operation
on the same process.

Each time you create file or a directory, the user that it was created from
will be saved alongside the metadata.

#### Naming

In relation to the view id. There is two way to refer to a file in `elmerfs`.
You can either refer to it by what you are using to, by its name or,
by its fully qualified name `name:view_id`.

For example if you are the root user:

```bash
echo "Hello" > file:root
echo " World!" >> file
cat file
Hello World!
```

Note that consequently `:` as a special meaning.

#### Lookup Resolution

As you saw above, when doing a lookup, you have two ways to refer a file.

Using the short alias, `elmerfs` will use a simple algorithm to resolve the lookup.
First it will check if there is only one entry in the parent dir with this name. If so,
the lookup is successful and the entry is returned **even if the entry as a different view id associated
to it**.

If they are multiple entries, this mean that there was a concurrent update when updating the parent directory.
In this case, we try to find if an entry with the same view id as the running process exists.

For example lets say you have **elmerfs0** and **elmerfs1**:

```
elmerfs0/dir> touch f
```

```
elmerfs1/dir> touch f
```

At this point there is two file in the same directory, if we perform a listing
on both site, you will see the following:

```
elmerfs0/dir> ls
f
f:toto
echo "Hello from elmerfs0' >> f
```

```
elmerfs1/dir> ls
f
f:root
cat f:root
Hello from elmerfs0
```

What is interesting here is that applications on both process will work
seamlessly on their file without interruptions. To resolve the conflict you can
simply rename the file.

This works the same on directories.

#### Locking

`elmerfs` should work without any distributed locking, you can specify `no-locks` to avoid them.
When locks are used, no conflicts can happen, but you lose latency and availability.

#### Atomicity

Every fs operation is synchronous and done inside a unique transaction,
meaning that if an operation fails, nothing will be committed.

### Running chton tests

Cthon04 test suite must be built
from the 3rdparty submodule and copied into a vendor directory at the root
of this project.

Then, a simple `cargo test` should work.

```
$ destdir=$(realpath ./vendor/chton04)
$ mkdir -p $destdir
$ (cd 3rdparty/cthon04 && make all && make copy DESTDIR=$destdir)
$ cargo test
```

### State of the project

**elmerfs** is still in its early stage, basic fs operation are implemented
and the aim is to provide a prototype that is valid and resilient
but not necessarily fast.

The project is able to pass basics and general connectathon test suites.
Fsx from the linux test project is also able to run for
an extended period of time without issues.

More tests will be added in the future to check concurrent update handling.

Note that **concurrent update on file content** is not well handled yet.
Conflicts resolution of such update is done at the register level,
not at the update boundaries.
