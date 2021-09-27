# OmniPaxos

In this section, we will introduce the features of OmniPaxos in detail.

In particular, we will talk about [communication](communication.md), [round](round.md), as well as how to [configure](configuration.md) it, before describing some of the advanced options for Omnipaxos, such as [garbage collection](garbage-collection.md), [logging](logging.md), and [storage](storage.md).

An OmniPaxos replica maintains a local state of the replicated log, handles incoming messages and produces outgoing messages that the user has to fetch periodically and send using a network implementation.
The user also has to periodically fetch the decided entries that are guaranteed to be strongly consistent and linearizable, and therefore also safe to be used in the higher-level application.

An OmniPaxos replica can be initialised by calling one of the two constructors. The second constructor uses a hocon configuration which will be discussed in further detail in [configuration](../omnipaxos/configuration.md).

```rust,edition2018,no_run,noplaypen
OmniPaxos::with(
    config_id: u32,
    pid: u64,
    peers: Vec<u64>,
    storage: Storage<R, S, P>,
    skip_prepare_use_leader: Option<Leader<R>>,
    logger: Option<Logger>,
    log_file_path: Option<&str>,
)

OmniPaxos::with_hocon(
    cfg: &Hocon,
    peers: Vec<u64>,
    storage: Storage<R, S, P>,
    skip_prepare_use_leader: Option<Leader<R>>,
    logger: Option<Logger>,
)
```
