You are free to use any storage implementation with `OmniPaxos`. The only requirement is that it implements the `Storage` trait. OmniPaxos includes the package `omnipaxos_storage` which provides two types of storage implementation that work out of the box: `MemoryStorage` and `PersistentStorage`.

## Importing `omnipaxos_storage`
To use the provided storage implementations, we need to add `omnipaxos_storage` to the dependencies in the cargo file.
```rust,edition2018,no_run,noplaypen
[dependencies]
omnipaxos_storage = { git = "https://github.com/haraldng/omnipaxos", default-features = true } 
```

**If** you **do** decide to implement your own storage, we recommend taking a look at `MemoryStorage` as a reference for implementing the functions required by `Storage`.

## MemoryStorage
`MemoryStorage` is an in-memory storage implementation and it will be used in our examples. For simplicity, we leave out some parts of the implementation for now (such as [Snapshots](../compaction.md)).
```rust,edition2018,no_run,noplaypen
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
        fn append_entry(&mut self, entry: T) -> u64 {
            self.log.push(entry);
            self.get_log_len()
        }

        fn append_entries(&mut self, entries: Vec<T>) -> u64 {
            let mut e = entries;
            self.log.append(&mut e);
            self.get_log_len()
        }

        fn append_on_prefix(&mut self, from_idx: u64, entries: Vec<T>) -> u64 {
            self.log.truncate(from_idx as usize);
            self.append_entries(entries)
        }

        fn set_promise(&mut self, n_prom: Ballot) {
            self.n_prom = n_prom;
        }

        fn set_decided_idx(&mut self, ld: u64) {
            self.ld = ld;
        }

        fn get_decided_idx(&self) -> u64 {
            self.ld
        }

        fn set_accepted_round(&mut self, na: Ballot) {
            self.acc_round = na;
        }

        fn get_accepted_round(&self) -> Ballot {
            self.acc_round
        }

        fn get_entries(&self, from: u64, to: u64) -> Vec<T> {
            self.log
                .get(from as usize..to as usize)
                .unwrap_or(&[])
                .to_vec()
        }

        fn get_log_len(&self) -> u64 {
            self.log.len() as u64
        }

        fn get_suffix(&self, from: u64) -> Vec<T> {
            match self.log.get(from as usize..) {
                Some(s) => s.to_vec(),
                None => vec![],
            }
        }

        fn get_promise(&self) -> Ballot {
            self.n_prom
        }
        ...
    }
```

## PersistentStorage
`PersistentStorage` is a persistent storage implementation that stores the replicated log and the state of OmniPaxos. The struct uses [Commitlog](https://crates.io/crates/commitlog) to store the replicated log, and the state is stored on [sled](https://crates.io/crates/sled) by default. The state can be changed to be stored on [RocksDB](https://crates.io/crates/rocksdb) instead of sled by using the feature `rocksdb`. Users can configure the path to log entries and OmniPaxos state, and storage-related options through `PersistentStorageConfig`. The configuration struct features a `default()` constructor for generating default configuration, and the constructor `with()` that takes the storage path and options as arguments. 
```rust,edition2018,no_run,noplaypen
use omnipaxos_core::{
    sequence_paxos::{OmniPaxos, OmniPaxosConfig},
};
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
```rust,edition2018,no_run,noplaypen
let my_path = "another_storage"
let my_logopts = LogOptions::new(my_path);
let mut my_sled_opts = Config::new();
my_sled_opts.path(my_path);
my_sled_opts.new(true);

// create configuration with given arguments
let my_config = PersistentStorageConfig::with(my_path, my_logopts, my_sled_opts);
```
