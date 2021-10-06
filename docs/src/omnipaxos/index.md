# OmniPaxos

Each server in the cluster should have a local instance of the  `OmniPaxos` struct. `OmniPaxos` maintains a local state of the replicated log, handles incoming messages and produces outgoing messages that the user has to fetch and send using their network implementation. The user also has to fetch the decided entries that are guaranteed to be strongly consistent and linearizable, and therefore also safe to be used in a higher-level application.

> **Note:** `OmniPaxos` has a type parameter `R` that must implement the trait `Round`. This is the round numbers used in the log replication. It will be provided and furthered explained in [Ballot Leader Election](../ble/index.md), but for simplicity the reader can for now assume `R` to be `u64`.

## Storage
The replicated log can be stored using any underlying implementation provided by the user. OmniPaxos only requires a user to implement the traits `PaxosState<R>` and `Sequence<R>`. The `PaxosState<R>` maintains the internal state of the `OmniPaxos`, while `Sequence<R>` maintains the actual replicated log. The storage is split into these two traits since `PaxosState<R>` stores small values such as indexes, while `Sequence<R>` stores larger data (the log entries). Thus it is possible to tailor-fit the implementations respectively. 

### Example: In-memory storage
OmniPaxos provides an in-memory storage implementation in the traits `MemorySequence` and `MemoryState`.

```rust,edition2018,no_run,noplaypen
use omnipaxos::storage::memory_storage::{MemorySequence, MemoryState, Storage};

let storage = Storage::with(MemorySequence::<R>::new(), MemoryState::<R>::new());
``` 

## Creating an OmniPaxos instance
The constructor of `OmniPaxos` has the following parameters.

```rust,edition2018,no_run,noplaypen
OmniPaxos::with(
    config_id: u32,                                 // the configuration id. See the section "Reconfiguration" in `Communication`.
    pid: u64,                                       // the unique identifier of this server
    peers: Vec<u64>,                                // the PEERS of this server. I.e. this should not include `pid`
    storage: Storage<R, S, P>,
    skip_prepare_use_leader: Option<Leader<R>>,     // optimization to start with pre-determined leader after
                                                    // reconfiguration. See "Reconfiguration".
    logger: Option<Logger>,                         // custom logger
    log_file_path: Option<&str>,                    
)
```
With the initialized storage, we can go ahead and create our `OmniPaxos` replica instance:
```rust,edition2018,no_run,noplaypen
use omnipaxos::paxos::OmniPaxos;

// configuration with id 1 and the following cluster
let configuration_id = 1;
let _cluster = vec![1, 2, 3];

// create the replica 2 in this cluster (other replica instances are created similarly with pid 1 and 3 on other servers)
let my_pid = 2;
let my_peers= vec![1, 3];

let omni_paxos = OmniPaxos::with(
    configuration_id,
    my_pid,
    my_peers,
    storage,
    None,
    None,
    Some("omnipaxos/logs")
);
```
For convenience, OmniPaxos also features a constructor `OmniPaxos::with_hocon()` using [hocon](https://vleue.com/hocon.rs/hocon/index.html). One could then instead have some of the parameters in a file `config/server2.conf`

```json
{
    config_id: 1,
    pid: 2,
    log_file_path: "/omnipaxos/logs"
}
```
This can then be loaded to construct OmniPaxos:

```rust,edition2018,no_run,noplaypen
let cfg = HoconLoader::new()
    .load_file("config/server2.conf")?
    .hocon()?;

let omni_paxos = OmniPaxos::with_hocon(
    &cfg
    my_peers,
    storage,
    None,
    None,
);
```