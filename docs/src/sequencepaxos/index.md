# SequencePaxos
Each server in the cluster should have a local instance of the  `SequencePaxos` struct. `SequencePaxos` maintains a local state of the replicated log, handles incoming messages and produces outgoing messages that the user has to fetch and send using their network implementation. The users also accesses the replicated log via `SequencePaxos`.

## Example: Key-Value store
As a guide for this tutorial, we will use OmniPaxos to implement a replicated log for the purpose of a consistent Key-Value store. 

We begin by defining the type that we want our log entries to consist of:
```rust,edition2018,no_run,noplaypen
#[derive(Clone, Debug)] // Clone and Debug are required traits.
pub struct KeyValue {
    pub key: String,
    pub value: u64,
}
``` 

## Storage
You are free to use any storage implementation with `SequencePaxos`. The only requirement is that it implements the `Storage` trait. OmniPaxos provides an in-memory storage implementation `MemoryStorage` which we will use in our example.
For simplicity, we leave out some parts of the implementation for now (such as [Snapshots](../compaction.md)).
```rust,edition2018,no_run,noplaypen
    // from the module storage::memory_storage
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

## Creating a Node
With the structs for log entry and storage defined, we can now go ahead and create our `SequencePaxos` replica instance.  Let's assume we want our KV-store to be replicated on three servers. On, say node 2, we would do the following: 
```rust,edition2018,no_run,noplaypen
use omnipaxos_core::{
    sequence_paxos::{SequencePaxos, SequencePaxosConfig},
    storage::{memory_storage::MemoryStorage},
};

// configuration with id 1 and the following cluster
let configuration_id = 1;
let _cluster = vec![1, 2, 3];

// create the replica 2 in this cluster (other replica instances are created similarly with pid 1 and 3 on the other nodes)
let my_pid = 2;
let my_peers = vec![1, 3];

let mut sp_config = SequencePaxosConfig::default();
sp_config.set_configuration_id(configuration_id);
sp_config.set_pid(my_pid);
sp_config.set_peers(my_peers);

let storage = MemoryStorage::<KeyValue, ())>::default();
let mut sp = SequencePaxos::with(sp_config, storage);
```
For convenience, `SequencePaxosConfig` also features a constructor `SequencePaxosConfig::with_hocon()` that loads the values using [hocon](https://vleue.com/hocon.rs/hocon/index.html). One could then instead have the parameters in a file `config/node2.conf`

```json
{
    config_id: 1,
    pid: 2,
    log_file_path: "/sequencepaxos/logs"
}
```
This can then be loaded to construct `SequencePaxosConfig`:

```rust,edition2018,no_run,noplaypen
let raw_cfg = HoconLoader::new()
    .load_file("tests/config/node2.conf")
    .expect("Failed to load hocon file")
    .hocon()
    .unwrap();

let sp_config = SequencePaxosConfig::with_hocon(cfg);
```

## Crash-recovery
To support crash-recovery, we have to make sure that our storage implementation persisted the values. Then upon recovery, we have to make sure that our ``SequencePaxos`` will be started with the previously persisted state. To do so, create `SequencePaxos` as earlier described but use the persisted state as the `storage` argument. Then, call `fail_recovery()` to correctly initialize the volatile state.

```rust,edition2018,no_run,noplaypen
/* Restarting our node after a crash... */
let recovered_storage = ...;    // the recovered persistent state.
let mut recovered_paxos = SequencePaxos::with(sp_config, recovered_storage);
recovered_paxos.fail_recovery();
```
