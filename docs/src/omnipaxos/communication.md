# Communication

By default, OmniPaxos does not implement any network dispatcher. It is up to the user to provide an implementation of the network.

Let us assume we have a network dispatcher courtesy of [Kompact](https://github.com/kompics/kompact).

```rust,edition2018,no_run,noplaypen
// Create a timer that triggers once every 1ms
// Get the outgoing messages and send them through the network to the receivers
timer = create_timer(1ms, {
    let msgs = paxos.get_outgoing_msgs();
    for out_msg in msgs {
        let receiver = out_msg.to;
        // send out_msg to receiver
    }
})
```

In the example above we schedule a timer once every 1ms to extract a vector of messages (outgoing queue) from inside of a Paxos replica, then we iterate through each message and send it through the network to the target replica.
