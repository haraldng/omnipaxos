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

## Creating a Node
With the structs for log entry and storage defined, we can now go ahead and create our `SequencePaxos` replica instance.  Let's assume we want our KV-store to be replicated on three servers. On, say node 2, we would do the following: 
```rust,edition2018,no_run,noplaypen
use omnipaxos_core::{
    sequence_paxos::{SequencePaxos, SequencePaxosConfig},
};
use omnipaxos_storage::{
    memory_storage::MemoryStorage,
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

let storage = MemoryStorage::<KeyValue, ()>::default();
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
