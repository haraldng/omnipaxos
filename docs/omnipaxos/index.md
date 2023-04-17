# OmniPaxos
Each server in the cluster should have a local instance of the  `OmniPaxos` struct. `OmniPaxos` maintains a local state of the replicated log, handles incoming messages and produces outgoing messages that the user has to fetch and send using their network implementation. The users also accesses the replicated log via `OmniPaxos`.

## Example: Key-Value store
As a guide for this tutorial, we will use OmniPaxos to implement a replicated log for the purpose of building a consistent Key-Value store. 

We begin by defining the type that we want our log entries to consist of:
```rust,edition2018,no_run,noplaypen
#[derive(Clone, Debug)] // Clone and Debug are required traits.
pub struct KeyValue {
    pub key: String,
    pub value: u64,
}
``` 

## Creating a Node
With the structs for log entry and storage defined, we can now go ahead and create our `OmniPaxos` replica instance.  Let's assume we want our KV-store to be replicated on three servers. On, say node 2, we would do the following: 
```rust,edition2018,no_run,noplaypen
use omnipaxos_core::{
    omni_paxos::{OmniPaxos, OmniPaxosConfig},
};
use omnipaxos_storage::{
    memory_storage::MemoryStorage,
};

// configuration with id 1 and the following cluster
let configuration_id = 1;
let cluster = vec![1, 2, 3];

// create the replica 2 in this cluster (other replica instances are created similarly with pid 1 and 3 on the other nodes)
let my_pid = 2;
let my_peers = vec![1, 3];

let omnipaxos_config = OmniPaxosConfig {
    configuration_id,
    pid: my_pid,
    peers: my_peers,
    ..Default::default()
}

let storage = MemoryStorage::<KeyValue, ()>::default();
let mut omni_paxos = omni_paxos_config.build(storage);
```
For convenience, `OmniPaxosConfig` also features a constructor `OmniPaxosConfig::with_hocon()` that loads the values using [hocon](https://vleue.com/hocon.rs/hocon/index.html). One could then instead have the parameters in a file `config/node2.conf`

```json
{
    config_id: 1,
    pid: 2,
    log_file_path: "/omnipaxos/logs"
}
```
This can then be loaded to construct `OmniPaxosConfig`:

```rust,edition2018,no_run,noplaypen
let cfg = HoconLoader::new()
    .load_file("tests/config/node2.conf")
    .expect("Failed to load hocon file")
    .hocon()
    .unwrap();

let omni_paxos_config = OmniPaxosConfig::with_hocon(cfg);
```

## Fail-recovery
To support Fail-recovery, we must ensure that our storage implementation can persist both the log entries and storage state. Upon recovery, we have to make sure that our ``OmniPaxos`` will start with the previously persisted state. To do so, we first re-create our storage with the same storage path as the previous instance. Then we create a `OmniPaxos` instance but use the persisted state as the `storage` argument. Lastly, we call `fail_recovery()` to correctly initialize the volatile state. We show an example using [`PersistentStorage`](storage.md#persistentstorage).

```rust,edition2018,no_run,noplaypen
/* Re-creating our node after a crash... */

// Configuration from previous storage
let my_path = "/my_path_before_crash/";
let my_log_opts = LogOptions::new(my_path);
let persist_conf = PersistentStorageConfig::default();

persist_conf.set_path(my_path); // set the path to the persistent storage
my_config.set_commitlog_options(my_logopts);

// Re-create storage with previous state, then create `OmniPaxos`
let recovered_storage = PersistentStorage::open(persist_conf); 
let mut recovered_paxos = omni_paxos_config.build(recovered_storage);
recovered_paxos.fail_recovery();
```