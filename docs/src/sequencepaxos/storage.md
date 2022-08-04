# Storage
You are free to use any storage implementation with `SequencePaxos`. The only requirement is that it implements the `Storage` trait. OmniPaxos includes the package `omnipaxos_storage` which provides two types of storage implementations, depending on the users need for persistance or faster reads and writes: `PersistentStorage` and `MemoryStorage`

### MemoryStorage
An in-memory storage implementation, `MemoryStorage` offers fast reads and writes but with no persistence. This storage type will be used in our examples. For simplicity, we leave out some parts of the implementation for now (such as [Snapshots](../compaction.md)).
```rust,edition2018,no_run,noplaypen
    // from the module omnipaxos_storage::memory::memory_storage
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

### PersistentStorage
A persistent disk-based storage implementation, `PersistentStorage` makes use of the [Commitlog](https://crates.io/crates/commitlog) and [rocksDB](https://crates.io/crates/rocksdb) libraries to store logged entries and replica state used in crash-recovery. Users can configure the path to store log entries, fields and storage related options through the `PersistentStorageConfig` struct

```rust,edition2018,no_run,noplaypen
    // from the module omnipaxos_storage::memory::persistent_storage
    pub struct PersistentStorage<T, S>
    where
        T: Entry,
        S: Snapshot<T>,
    {
        /// disk-based commit log for entries
        c_log: CommitLog,
        /// the path to the directory containing a commitlog
        c_path: String,
        /// local RocksDB key-value store
        db: DB,
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
            let entry_bytes = bincode::serialize(&entry).unwrap();
            match self.c_log.append_msg(entry_bytes) {
                Ok(x) => x,
                Err(_e) => panic!(),
            }
        }

        fn append_entries(&mut self, entries: Vec<T>) -> u64 {
            for entry in entries {
                self.append_entry(entry);
            }
            self.get_log_len()
        }

        fn append_on_prefix(&mut self, from_idx: u64, entries: Vec<T>) -> u64 {
            self.c_log
                .truncate(from_idx)
                .expect("Failed to truncate commitlog");
            self.append_entries(entries)
        }

        fn get_entries(&self, from: u64, to: u64) -> Vec<T> {
            let buffer = match self.c_log.read(from, ReadLimit::default()) {
                Ok(buf) => buf,
                Err(_) => panic!(),
            };
            let mut entries = Vec::<T>::with_capacity((to - from) as usize);
            let mut iter = buffer.iter();
            for _ in from..to {
                let msg = iter.next().unwrap();
                entries.push(bincode::deserialize(msg.payload()).unwrap());
            }
            entries
        }

        fn get_log_len(&self) -> u64 {
            self.c_log.next_offset()
        }

        fn get_suffix(&self, from: u64) -> Vec<T> {
            self.get_entries(from, self.get_log_len())
        }

        fn get_promise(&self) -> Ballot {
            match self.db.get(NPROM) {
                Ok(Some(prom_bytes)) => {
                    let b_store: BallotStorage =
                        FromBytes::read_from(prom_bytes.as_slice()).unwrap();
                    Ballot {
                        n: b_store.n,
                        priority: b_store.priority,
                        pid: b_store.pid,
                    }
                }
                Ok(None) => Ballot {
                    n: 0,
                    priority: 0,
                    pid: 0,
                },
                Err(_e) => panic!(),
            }
        }

        fn set_promise(&mut self, n_prom: Ballot) {
            let ballot_store = BallotStorage::with(n_prom);
            let prom_bytes = AsBytes::as_bytes(&ballot_store);
            self.db.put(NPROM, prom_bytes).unwrap()
        }

        fn get_decided_idx(&self) -> u64 {
            match self.db.get(DECIDE) {
                Ok(Some(ld_bytes)) => bincode::deserialize(&ld_bytes).unwrap(),
                Ok(None) => 0,
                Err(_e) => panic!(),
            }
        }

        fn set_decided_idx(&mut self, ld: u64) {
            let ld_bytes = bincode::serialize(&ld).unwrap();
            self.db.put(DECIDE, ld_bytes).unwrap();
        }
        
        ...
    }
```


