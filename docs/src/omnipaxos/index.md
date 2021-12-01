# OmniPaxos

Each server in the cluster should have a local instance of the  `OmniPaxos` struct. `OmniPaxos` maintains a local state of the replicated log, handles incoming messages and produces outgoing messages that the user has to fetch and send using their network implementation. The user also has to fetch the decided entries that are guaranteed to be strongly consistent and linearizable, and therefore also safe to be used in a higher-level application.

## Example: Key-Value store
As a guide for this tutorial, we will use OmniPaxos to implement a replicated log for the purpose of a consistent Key-Value store. 

We begin by defining the type that we want our log entries to consist of:
```rust,edition2018,no_run,noplaypen
pub struct KeyValue {
    pub key: String,
    pub value: u64
}
``` 

## Storage
The replicated log can be stored using any underlying implementation provided by the user. OmniPaxos only requires the user to implement the traits `PaxosState` and `Sequence<T>`. The `PaxosState` maintains the internal state of the protocol, while `Sequence<T>` maintains the actual replicated log. In our example, `T` is `KeyValue`. The storage is split into these two traits since `PaxosState` stores small values such as indexes, while `Sequence<T>` stores larger data (the log entries). Thus it is possible to tailor-fit the implementations respectively. 

OmniPaxos provides an in-memory storage implementation in the structs `MemoryState` and `MemorySequence<T>`.

## Creating an OmniPaxos instance
The constructor of `OmniPaxos` has the following parameters.

```rust,edition2018,no_run,noplaypen
OmniPaxos::with(
    config_id: u32,                                 // the configuration id. See the section "Reconfiguration" in `Communication`.
    pid: u64,                                       // the unique identifier of this server
    peers: Vec<u64>,                                // the PEERS of this server. I.e. this should not include `pid`
    skip_prepare_use_leader: Option<Ballot>,        // optimization to start with pre-determined leader after
                                                    // reconfiguration. See "Reconfiguration".
    logger: Option<Logger>,                         // custom logger
    log_file_path: Option<&str>,                    
)
```
Let's go ahead and create our `OmniPaxos` replica instance:
```rust,edition2018,no_run,noplaypen
use omnipaxos::paxos::OmniPaxos;

// configuration with id 1 and the following cluster
let configuration_id = 1;
let _cluster = vec![1, 2, 3];

// create the replica 2 in this cluster (other replica instances are created similarly with pid 1 and 3 on other servers)
let my_pid = 2;
let my_peers= vec![1, 3];

let omni_paxos: OmniPaxos<KeyValue, MemorySequence, MemoryState> = OmniPaxos::with(
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