You are free to use any storage implementation with `OmniPaxos`. The only requirement is that it implements the `Storage` trait. OmniPaxos includes the package `omnipaxos_storage` which provides two types of storage implementation that work out of the box: `MemoryStorage` and `PersistentStorage`.

## Importing `omnipaxos_storage`
To use the provided storage implementations, we need to add `omnipaxos_storage` to the dependencies in the cargo file. You can find the latest version on [crates](https://crates.io/crates/omnipaxos_storage).
```rust
[dependencies]
omnipaxos_storage = { version = "LATEST_VERSION", default-features = true } 
```

**If** you **do** decide to implement your own storage, we recommend taking a look at `MemoryStorage` as a reference for implementing the functions required by `Storage`.
Upon receiving a `StorageResult::Error(_)` from the storage implementation, Omnipaxos tries to roll back incomplete changes, to enable crash-recovery, and then panicks.

## MemoryStorage
`MemoryStorage` is an in-memory storage implementation and it will be used in our examples. For simplicity, we leave out some parts of the implementation for now (such as [Snapshots](../compaction)).
```rust
    // from the module omnipaxos_storage::memory_storage
    #[derive(Clone)]
    pub struct MemoryStorage<T>
    where
        T: Entry,
    {
        /// Vector which contains all the replicated entries in-memory.
        log: Vec<T>,
        /// Last promised round.
        n_prom: Ballot,
        /// Last accepted round.
        acc_round: Ballot,
        /// Length of the decided log.
        ld: u64,
        ...
    }

    impl<T> Storage<T> for MemoryStorage<T>
    where
        T: Entry,
    {
        fn append_entry(&mut self, entry: T) -> StorageResult<u64> {
            self.log.push(entry);
            self.get_log_len()
        }

        fn append_entries(&mut self, entries: Vec<T>) -> StorageResult<u64> {
            let mut e = entries;
            self.log.append(&mut e);
            self.get_log_len()
        }

        fn append_on_prefix(&mut self, from_idx: u64, entries: Vec<T>) -> StorageResult<u64> {
            self.log.truncate(from_idx as usize);
            self.append_entries(entries)
        }

        fn set_promise(&mut self, n_prom: Ballot) -> StorageResult<()> {
            self.n_prom = n_prom;
            Ok(())
        }

        fn set_decided_idx(&mut self, ld: u64) StorageResult<()> {
            self.ld = ld;
            Ok(())
        }

        fn get_decided_idx(&self) -> StorageResult<u64> {
            Ok(self.ld)
        }

        fn set_accepted_round(&mut self, na: Ballot) -> StorageResult<()> {
            self.acc_round = na;
            Ok(())
        }

        fn get_accepted_round(&self) -> StorageResult<Ballot> {
            Ok(self.acc_round)
        }

        fn get_entries(&self, from: u64, to: u64) -> StorageResult<Vec<T>> {
            Ok(self.log
                .get(from as usize..to as usize)
                .unwrap_or(&[])
                .to_vec())
        }

        fn get_log_len(&self) -> StorageResult<u64> {
            Ok(self.log.len() as u64)
        }

        fn get_suffix(&self, from: u64) -> StorageResult<Vec<T>> {
            match self.log.get(from as usize..) {
                Some(s) => Ok(s.to_vec()),
                None => Ok(vec![]),
            }
        }

        fn get_promise(&self) -> StorageResult<Ballot> {
            Ok(self.n_prom)
        }
        ...
    }
```

## PersistentStorage
`PersistentStorage` is a persistent storage implementation that stores the replicated log and the state of OmniPaxos. The struct uses [Commitlog](https://crates.io/crates/commitlog) to store the replicated log, and the state is stored on [sled](https://crates.io/crates/sled) by default. The state can be changed to be stored on [RocksDB](https://crates.io/crates/rocksdb) instead of sled by using the feature `rocksdb`. Users can configure the path to log entries and OmniPaxos state, and storage-related options through `PersistentStorageConfig`. The configuration struct features a `default()` constructor for generating default configuration, and the constructor `with()` that takes the storage path and options as arguments. 
```rust
use omnipaxos_storage::{
    persistent_storage::{PersistentStorage, PersistentStorageConfig},
};
use commitlog::LogOptions;
use sled::{Config};

// user-defined configuration
let my_path = "my_storage"
let my_log_opts = LogOptions::new(my_path);
let mut my_sled_opts = Config::new();
my_sled_opts.path(self, my_path);

// generate default configuration and set user-defined options
let mut my_config = PersistentStorageConfig::default();
my_config.set_path(my_path);
my_config.set_commitlog_options(my_logopts);
my_config.set_sled_options(my_sled_opts);
```
The same configuration can also be done with the constructor that takes arguments:
```rust
let my_path = "another_storage"
let my_logopts = LogOptions::new(my_path);
let mut my_sled_opts = Config::new();
my_sled_opts.path(my_path);
my_sled_opts.new(true);

// create configuration with given arguments
let my_config = PersistentStorageConfig::with(my_path, my_logopts, my_sled_opts);
```
## Batching
OmniPaxos supports batching to reduce the number of IO operations to storage. We enable it by specifying the `batch_size` in `OmniPaxosConfig`.

```rust
    let omnipaxos_config = OmniPaxosConfig {
        server_config: ServerConfig {
            batch_size: 100, // `batch_size = 1` by default
            ..Default::default()
        },
        cluster_config: ClusterConfig {
            configuration_id: 1,
            nodes: vec![1, 2, 3],
            ..Default::default()
        },
    };
    // build omnipaxos instance
```

> **Note** OmniPaxos will wait until the batch size is reached before the entries get decided. A larger batch size may therefore incur higher latency before an append operation is decided. 
