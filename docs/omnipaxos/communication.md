As previously mentioned, the user has to send/receive messages between servers themselves. In this section, we show how the user should interact with `OmniPaxos` and its incoming and outgoing messages.

## Incoming and Outgoing
When a message is received from the network layer intended for our node, we need to handle it in `OmniPaxos`.

```rust
use omnipaxos::messages::Message;

// handle incoming message from network layer
let msg: Message<KeyValue> = in_msg;    // in_msg is a received message for this node e.g. `in_msg.get_receiver() == 2`
omni_paxos.handle_incoming(msg);
```

By handling incoming messages and local calls such as `append()`, our local `omni_paxos` will produce outgoing messages for its peers. Thus, we must periodically send the outgoing messages on the network layer.

```rust
// send outgoing messages. This should be called periodically, e.g. every ms
let mut msg_buffer = Vec::new();
omni_paxos.outgoing_messages(&mut msg_buffer);
for out_msg in msg_buffer.drain(..) {
    let receiver = out_msg.get_receiver();
    // send out_msg to receiver on network layer
}
```

> **Note:** The networking i.e. how to actually send and receive messages needs to be implemented by you, the user. You have to periodically fetch these outgoing messages from `OmniPaxos`.
