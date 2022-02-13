# Communication
As previously mentioned, the user has to send/receive messages between servers themselves. In this section, we show how the user should interact with `SequencePaxos` and its incoming and outgoing messages.

## Incoming and Outgoing
When a message is received from the network layer intended for our node, we need to handle it in `SequencePaxos`.

```rust,edition2018,no_run,noplaypen
use omnipaxos_core::messages::Message;

// handle incoming message from network layer
let msg: Message<KeyValue, KVSnapshot> = ...;    // message to this node e.g. `msg.to = 2`
seq_paxos.handle(msg);
```

By handling incoming messages and local calls such as `append()`, our local `seq_paxos` will produce outgoing messages for its peers. Thus, we must periodically send the outgoing messages on the network layer.

```rust,edition2018,no_run,noplaypen
// send outgoing messages. This should be called periodically, e.g. every ms
for out_msg in seq_paxos.get_outgoings_msgs() {
    let receiver = out_msg.to;
    // send out_msg to receiver on network layer
}
```

> **Note:** The networking i.e. how to actually send and receive messages needs to be implemented by you, the user. Similarly, you have to periodically fetch these outgoing messages from `SequencePaxos`. 

## Handling Disconnections
One of the main advantages of Omni-Paxos is its resilience to partial connectivity. If one node loses connection to another and then reconnects (e.g. after a TCP-session drop), make sure to call ``reconnected(pid)`` before handling any incoming messages from that peer.

```rust,edition2018,no_run,noplaypen
// network layer notifies of reconnecting to peer with pid = 3
seq_paxos.reconnected(3);
...
```