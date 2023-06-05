Each server in the cluster should have a local instance of the `OmniPaxos` struct. `OmniPaxos` maintains a local state of the replicated log, handles incoming messages and produces outgoing messages that the user has to fetch and send using their network implementation. The users also accesses the replicated log via `OmniPaxos`. For the sake of this tutorial, we will use some convenient macros and storage implementations that work out-of-the-box. This requires us to modify our `Cargo.toml`:

```toml
[dependencies]
omnipaxos = { version = "LATEST_VERSION", features = ["macros"] }
omnipaxos_storage = "LATEST_VERSION"
``` 

## Example: Key-Value store
As a guide for this tutorial, we will use OmniPaxos to implement a replicated log for the purpose of building a consistent Key-Value store. 

We begin by defining the type that we want our log entries to consist of:
```rust
use omnipaxos::macros::Entry;

#[derive(Clone, Debug, Entry)] // Clone and Debug are required traits.
pub struct KeyValue {
    pub key: String,
    pub value: u64,
}
``` 

`Entry` is the trait for representing the entries stored in the replicated log of OmniPaxos. Here, we derive the implementation of it for our `KeyValue` using a macro. We will also show how to implement the trait manually when we discuss [`Snapshots`](../compaction/#snapshot).

> **Note** To use the #[derive(Entry)] macro, please make sure to enable the `macros` feature.

## Creating a Node
With the structs for log entry and storage defined, we can now go ahead and create our `OmniPaxos` replica instance. Let's assume we want our KV-store to be replicated on three servers. On, say node 2, we would do the following:
```rust
use omnipaxos::{
   {OmniPaxos, OmniPaxosConfig},
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
};

let storage = MemoryStorage::default();
let mut omni_paxos: OmniPaxos<KeyValue, MemoryStorage<KeyValue>> = omnipaxos_config.build(storage);
```
With the toml_config feature enabled, `OmniPaxosConfig` also features a constructor `OmniPaxosConfig::with_toml()` that loads the values using [TOML](https://toml.io). One could then instead have the parameters in a file `config/node1.toml`

```toml
configuration_id = 1
pid = 2
peers = [1, 3]
logger_file_path = "/omnipaxos/logs"
```
This can then be loaded to construct `OmniPaxosConfig`:

```rust
let config_file_path = "config/node1.toml"; 
let omnipaxos_config = OmniPaxosConfig::with_toml(config_file_path);
```

## Fail-recovery
To support Fail-recovery, we must ensure that our storage implementation can persist both the log entries and storage state. Upon recovery, we have to make sure that our ``OmniPaxos`` will start with the previously persisted state. To do so, we re-create our storage with the same storage path as the previous instance. Then we create a `OmniPaxos` instance but use the persisted state as the `storage` argument. We show an example using [`PersistentStorage`](../storage/#persistentstorage).

```rust
/* Re-creating our node after a crash... */

// Configuration from previous storage
let my_path = "/my_path_before_crash/";
let my_log_opts = LogOptions::new(my_path);
let persist_conf = PersistentStorageConfig::default();

persist_conf.set_path(my_path); // set the path to the persistent storage
my_config.set_commitlog_options(my_log_opts);

// Re-create storage with previous state, then create `OmniPaxos`
let recovered_storage = PersistentStorage::open(persist_conf); 
let mut recovered_paxos = omnipaxos_config.build(recovered_storage);
```
