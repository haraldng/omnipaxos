# Storage
You are free to use any storage implementation with `SequencePaxos`. The only requirement is that it implements the `Storage` trait. OmniPaxos includes the package `omnipaxos_storage` which provides two types of storage implementation, depending on the users need for fast reads and writes or persistance: `MemoryStorage` and `PersistentStorage`

## MemoryStorage
An in-memory storage implementation, `MemoryStorage` offers fast reads and writes. This storage type will be used in our examples. For simplicity, we leave out some parts of the implementation for now (such as [Snapshots](../compaction.md)).
```rust,edition2018,no_run,noplaypen
    // from the module omnipaxos_storage::memory_storage
    #[derive(Clone)]
    pub struct MemoryStorage<T, S>
    where
        T: Entry,
        S: Snapshot<T>,
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

    impl<T, S> Storage<T, S> for MemoryStorage<T, S>
    where
        T: Entry,
        S: Snapshot<T>,
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

        fn get_entries(&self, from: u64, to: u64) -> &[T] {
            self.log.get(from as usize..to as usize).unwrap_or(&[])
        }

        fn get_log_len(&self) -> u64 {
            self.log.len() as u64
        }

        fn get_suffix(&self, from: u64) -> &[T] {
            match self.log.get(from as usize..) {
                Some(s) => s,
                None => &[],
            }
        }

        fn get_promise(&self) -> Ballot {
            self.n_prom
        }
        
        ...
    }
```

## PersistentStorage
An persistent disk-based storage implementation, `PersistentStorage` makes use of the [Commitlog](https://crates.io/crates/commitlog) and [rocksDB](https://crates.io/crates/rocksdb) libraries to store logged entries and replica state. Users can configure the path to store log entries, replica state and storage related options through the `PersistentStorageConfig` struct. 

```rust,edition2018,no_run,noplaypen
    // from the module omnipaxos_storage::persistent_storage
    pub struct PersistentStorage<T, S>
    where
        T: Entry,
        S: Snapshot<T>,
    {
        /// disk-based commit log for entries
        commitlog: CommitLog,
        /// the path to the directory containing a commitlog
        log_path: String,
        /// local RocksDB key-value store
        rocksdb: DB,
        /// A placeholder for the T: Entry
        t: PhantomData<T>,
        /// A placeholder for the S: Snapshot<T>
        s: PhantomData<S>,
    }

    impl<T, S> Storage<T, S> for PersistentStorage<T, S>
    where
        T: Entry + Serialize + for<'a> Deserialize<'a>,
        S: Snapshot<T> + Serialize + for<'a> Deserialize<'a>,
    {
        fn append_entry(&mut self, entry: T) -> u64 {
            let entry_bytes = bincode::serialize(&entry).expect("Failed to serialize");
            self.commitlog
                .append_msg(entry_bytes)
                .expect("Failed to append log entry")
                + 1
        }

        fn append_entries(&mut self, entries: Vec<T>) -> u64 {
            let serialized = entries
                .into_iter()
                .map(|entry| bincode::serialize(&entry).expect("Failed to serialize"));
            let offset = self
                .commitlog
                .append(&mut MessageBuf::from_iter(serialized))
                .expect("Falied to append entries");
            offset.first() + offset.len() as u64
        }

        fn append_on_prefix(&mut self, from_idx: u64, entries: Vec<T>) -> u64 {
            self.commitlog
                .truncate(from_idx)
                .expect("Failed to truncate log");
            self.append_entries(entries)
        }

        fn get_entries(&self, from: u64, to: u64) -> Vec<T> {
            let buffer = self
                .commitlog
                .read(from, ReadLimit::default())
                .expect("Failed to read from log");
            let mut entries = Vec::<T>::with_capacity((to - from) as usize);
            let mut iter = buffer.iter();
            for _ in from..to {
                let msg = iter.next().unwrap();
                entries.push(bincode::deserialize(msg.payload()).expect("Failed to deserialize"));
            }
            entries
        }

        fn get_log_len(&self) -> u64 {
            self.commitlog.next_offset()
        }

        fn get_suffix(&self, from: u64) -> Vec<T> {
            self.get_entries(from, self.commitlog.next_offset())
        }

        fn get_promise(&self) -> Ballot {
            let value = self.rocksdb.get(NPROM).expect("Failed to retrive 'NPROM'");
            match value {
                Some(prom_bytes) => {
                    let b_store: BallotStorage =
                        FromBytes::read_from(prom_bytes.as_slice()).expect("Failed to deserialize");
                    Ballot::with(b_store.n, b_store.priority, b_store.pid)
                }
                None => Ballot::default(),
            }
        }

        fn set_promise(&mut self, n_prom: Ballot) {
            let ballot_store = BallotStorage::with(n_prom);
            let prom_bytes = AsBytes::as_bytes(&ballot_store);
            self.rocksdb
                .put(NPROM, prom_bytes)
                .expect("Failed to set 'NPROM'")
        }

        fn get_decided_idx(&self) -> u64 {
            let value = self
                .rocksdb
                .get(DECIDE)
                .expect("Failed to retrive 'DECIDE'");
            match value {
                Some(ld_bytes) => {
                    FromBytes::read_from(ld_bytes.as_slice()).expect("Failed to deserialize")
                }
                None => 0,
            }
        }

        fn set_decided_idx(&mut self, ld: u64) {
            let ld_bytes = AsBytes::as_bytes(&ld);
            self.rocksdb
                .put(DECIDE, ld_bytes)
                .expect("Failed to set 'DECIDE'");
        }
            
        ...
    }
```


