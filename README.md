elmerfs
-------

elmerfs is a filesystem that leverage **Conflict Free Replicated Data Type**
(CRDT) on top of AntidoteDB to be eventually consistent in a active-active
geo distributed scenario.

### Building the project

The project require a recent version of rustc (tested with 1.42):

```
git clone git@github.com:scality/elmerfs.git
cd elmerfs
git submodule update --init --recursive
cargo test # or cargo run --bin main
```

The tests need a local instance of AntidoteDB.

### State of the project

**elmerfs** is still in its early stage, basic fs operation are implemented
but the aim is to pass the connectathon test suite, regardless of performance.

Once this step will be accomplished, more work will be put on optimisation and
smart usage of CRDTs.

