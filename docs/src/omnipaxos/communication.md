# Communication

By default, OmniPaxos does not implement any network dispatcher. It is up to the user to provide an implementation of the network.

Let us assume we have a network dispatcher courtesy of [Kompact](https://github.com/kompics/kompact).

```rust,edition2018,no_run,noplaypen
peers: HashMap<u64, ActorRef<BLEMessage>>,

fn send_outgoing_msgs(&mut self) {
    let outgoing = self.paxos.get_outgoing_msgs();

    for out in outgoing {
        let receiver = self.peers.get(&out.to).unwrap();

        receiver.tell(out);
    }
}

self.schedule_periodic(
    Duration::from_millis(1),
    Duration::from_millis(1),
    move |c, _| {
        c.send_outgoing_msgs();

        Handled::Ok
    },
)
```

In the example above we schedule a timer once every 1ms to extract a vector of messages (outgoing queue) from inside of a paxos replica, then we iterate through each message and send it through the network to the target replica.
