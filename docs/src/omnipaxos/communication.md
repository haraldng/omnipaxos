# Communication
As previously mentioned, the user has to send/receive messages between servers themselves. In this section, we show how the user interacts with OmniPaxos and its incoming and outgoing messages. Furthermore, we show how new log entries are proposed and handled when they get decided. 

## Propose Entries
To propose a log entry to be replicated, the `OmniPaxos::propose_normal()` function is called (we reuse the `storage` and `omni_paxos` variables created in earlier [examples](../omnipaxos/index.md)):

```rust,edition2018,no_run,noplaypen
let kv = KeyValue { key: String::from("a"), value: "123" } ;   // log entry as raw bytes
omni_paxos.propose_normal(kv).expect("Failed to propose normal proposal");
```

Proposals can be pipelined without waiting for preceeding entries to be decided. Furthermore, `propose_normal` can be called on any server. If the calling server is not the leader, the proposal will be forwarded. 

## Incoming and Outgoing
By proposing a log entry, the `OmniPaxos` instance will produce outgoing messages to its peers. The outgoing messages can be accessed by calling `OmniPaxos::get_outgoing_msgs()`. These messages then need to be sent to the intended server by the user themselves on the network layer. An incoming message should then be handled with the function `OmniPaxos::handle()`. Handling the incoming message will change the state and produce outgoing messages of `OmniPaxos` according to the protocol.
```rust,edition2018,no_run,noplaypen
// send outgoing messages (this should be called periodically, e.g. every 500ms)
for out_msg: Message<KeyValue> in omni_paxos.get_outgoings_msgs() {
    let receiver = out_msg.to;
    // send out_msg to receiver on network layer
}

// handle incoming message on network layer
let msg: PaxosMsg:<KeyValue> = ...;    // message received to this server.
omni_paxos.handle(msg);
```

## Decided Entries
Once an entry has been appended to the log on a majority of the cluster, it has been **decided**. The decided entries can be fetched via `OmniPaxos::get_latest_decided_entries()` and are safe to be handled by a higher level application that requires strong consistency.


```rust,edition2018,no_run,noplaypen
// handle decided log entries periodically
for entry in omni_paxos.get_latest_decided_entries() {
    match entry {
        Entry::Normal(e) => {    // handle decided normal log entry
            // e is of type KeyValue that was proposed.
        }
        _ => { /* see next section about reconfiguration */ }
    }
}
```

> **Note** `get_latest_decided_entries()` returns all the decided elements **since the last time it was called** and is thus intended to be called periodically. To get all decided entries (since the start), use `get_decided_entries()`

## Reconfiguration
Reconfiguration can be used to add or remove servers from the cluster. To propose a reconfiguration, use `OmniPaxos::propose_reconfiguration()`:
```rust,edition2018,no_run,noplaypen
let new_cluster = vec![1, 2, 4];    // oops replica 3 appears to have crashed... let's replace it with a new replica 4
omni_paxos.propose_reconfiguration(new_cluster, None).expect("Failed to propose reconfiguration");

...

// handle decided log entries periodically
for entry in omni_paxos.get_latest_decided_entries() {
    match entry {
        Entry::Normal(_) => { /* See previous section */ }
    
        Entry::StopSign(ss) => {    // handle completed reconfiguration
            let next_configuration_id = ss.config_id;
            let next_cluster = ss.nodes;
            let next_leader = ss.skip_prepare_use_leader;   // if Some(), then the replica instances in the new
                                                            // configuration should use this in their constructor
        }
    }
}

```
Upon deciding a `StopSign`, the user should create a new replica instance with the new configuration id if the corresponding server is in `StopSign.nodes`. Furthermore, the user should also notify and migrate the log to any new servers joining the cluster. To stop the old instance and get the final log in the old configuration, one can call:

```rust,edition2018,no_run,noplaypen
let log: Vec<Entry<KeyValue>> = self.omni_paxos.stop_and_get_sequence();
// notify about reconfiguration and migrate log to new servers
```

> **Note** The user has to implement the log migration themselves before starting the new `OmniPaxos` instances.