You are free to use any storage implementation with `OmniPaxos`. The only requirement is that it implements the `Storage` trait. OmniPaxos includes the package `omnipaxos_storage` which provides two types of storage implementation that work out of the box: `MemoryStorage` and `PersistentStorage`.

## Importing `omnipaxos_storage`
To use the provided storage implementations, we need to add `omnipaxos_storage` to the dependencies in the cargo file. You can find the latest version on [crates](https://crates.io/crates/omnipaxos_storage).
```toml
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
    ld: usize,
    /// Garbage collected index.
    trimmed_idx: usize,
    ...
}

impl<T> Storage<T> for MemoryStorage<T>
    where
    T: Entry,
{
    fn write_atomically(&mut self, ops: Vec<StorageOp<T>>) -> StorageResult<()> {
        for op in ops {
            match op {
                StorageOp::AppendEntry(entry) => self.append_entry(entry)?,
                StorageOp::AppendEntries(entries) => self.append_entries(entries)?,
                StorageOp::AppendOnPrefix(from_idx, entries) => {
                    self.append_on_prefix(from_idx, entries)?
                }
                StorageOp::SetPromise(bal) => self.set_promise(bal)?,
                StorageOp::SetDecidedIndex(idx) => self.set_decided_idx(idx)?,
                StorageOp::SetAcceptedRound(bal) => self.set_accepted_round(bal)?,
                ...
            }
        }
        Ok(())
    }

    fn append_entry(&mut self, entry: T) -> StorageResult<()> {
        self.log.push(entry);
        Ok(())
    }

    fn append_entries(&mut self, entries: Vec<T>) -> StorageResult<()> {
        let mut e = entries;
        self.log.append(&mut e);
        Ok(())
    }

    fn append_on_prefix(&mut self, from_idx: usize, entries: Vec<T>) -> StorageResult<()> {
        self.log.truncate(from_idx - self.trimmed_idx);
        self.append_entries(entries)
    }

    fn set_promise(&mut self, n_prom: Ballot) -> StorageResult<()> {
        self.n_prom = Some(n_prom);
        Ok(())
    }

    fn set_decided_idx(&mut self, ld: usize) -> StorageResult<()> {
        self.ld = ld;
        Ok(())
    }

    fn get_decided_idx(&self) -> StorageResult<usize> {
        Ok(self.ld)
    }

    fn set_accepted_round(&mut self, na: Ballot) -> StorageResult<()> {
        self.acc_round = Some(na);
        Ok(())
    }

    fn get_accepted_round(&self) -> StorageResult<Option<Ballot>> {
        Ok(self.acc_round)
    }

    fn get_entries(&self, from: usize, to: usize) -> StorageResult<Vec<T>> {
        let from = from - self.trimmed_idx;
        let to = to - self.trimmed_idx;
        Ok(self.log.get(from..to).unwrap_or(&[]).to_vec())
    }

    fn get_log_len(&self) -> StorageResult<usize> {
        Ok(self.log.len())
    }

    fn get_suffix(&self, from: usize) -> StorageResult<Vec<T>> {
        Ok(match self.log.get((from - self.trimmed_idx)..) {
                Some(s) => s.to_vec(),
                None => vec![],
                })
    }

    fn get_promise(&self) -> StorageResult<Option<Ballot>> {
        Ok(self.n_prom)
    }

    fn trim(&mut self, trimmed_idx: usize) -> StorageResult<()> {
        let to_trim = (trimmed_idx - self.trimmed_idx).min(self.log.len());
        self.log.drain(0..to_trim);
        self.trimmed_idx = trimmed_idx;
        Ok(())
    }
    ...
    }
```

## PersistentStorage
`PersistentStorage` is a persistent storage implementation, built on top of [RocksDB](https://crates.io/crates/rocksdb), that stores the replicated log and the state of OmniPaxos. Users can configure the path to log entries and OmniPaxos state, and storage-related options through `PersistentStorageConfig`. The configuration struct features a `default()` constructor for generating default configuration, and the constructor `with()` that takes the storage path and options as arguments.

```toml
[dependencies]
omnipaxos_storage = { version = "LATEST_VERSION", features=["persistent_storage"] }
```
The persistent storage implementation must first be enabled via the "persistent_storage" feature flag.

```rust
use omnipaxos_storage::{
    persistent_storage::{PersistentStorage, PersistentStorageConfig},
};
use rocksdb;

// user-defined configuration
let my_path = "my_storage";
let log_store_options = rocksdb::Options::default();
let state_store_options = rocksdb::Options::default();
state_store_options.create_missing_column_families(true); // required
state_store_options.create_if_missing(true); // required

// generate default configuration and set user-defined options
let mut my_config = PersistentStorageConfig::default();
my_config.set_path(my_path.to_string());
my_config.set_database_options(state_store_options);
my_config.set_log_options(log_store_options);
```
## Batching
OmniPaxos supports batching to reduce the number of IO operations to storage. It is enabled by specifying the `batch_size` in `OmniPaxosConfig`.

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
