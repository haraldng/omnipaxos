# Ballot Leader Election
A unique feature of the Omni-Paxos protocol is guaranteed progress with one quorum-connected server using Ballot Leader Election (BLE). In this section, we will see how the leader election is used in OmniPaxos. Similar to `SequencePaxos`, `BallotLeaderElection` is represented as a Rust struct that should be started on every node in the cluster.

The BLE protocol is based on exchanging heartbeats that should be received within some expected time. To represent time, `BallotLeaderElection` uses an internal logical clock. It has a `tick()` function that progresses its logical clock. The expected time that heartbeats should be received in is specified by `hb_delay` of `BLEConfig` constructor:
```rust,edition2018,no_run,noplaypen
use omnipaxos_core::ballot_leader_election::{BallotLeaderElection, BLEConfig};

/* As before, we pretend we are node 2 */
// configuration with id 1 and the following cluster
let configuration_id = 1;
let _cluster = vec![1, 2, 3];

// create the replica 2 in this cluster (other replica instances are created similarly with pid 1 and 3 on other servers)
let my_pid = 2;
let my_peers = vec![1, 3];

let mut ble_config = BLEConfig::default();
ble_config.set_pid(my_pid);
ble_config.set_peers(my_peers);
ble_config.set_hb_delay(20);     // a leader timeout of 20 ticks

let mut ble = BallotLeaderElection::with(ble_conf);
```
> **Note:** The `priority` parameter allows user to give desired servers a higher priority to become the leader. This is a best-effort approach upon a leader change.

In this example, we created our `BallotLeaderElection` with a timeout of 20 ticks. Thus, if we call `ble.tick()` every 10ms, we will in practice be using a leader timeout of 200ms. Once a leader is elected/changed, `tick()` will return a `Ballot`. This should be passed by you, the user, to the corresponding local `SequencePaxos` instance using `handle_leader()`:

```rust,edition2018,no_run,noplaypen
let seq_paxos: SequencePaxos = ...; // create our node as in Chapter `SequencePaxos`

// every 10ms call the following
if let Some(leader) = ble.tick() {
    // a new leader is elected, pass it to SequencePaxos.
    seq_paxos.handle_leader(leader);
}
```

## Communication
Incoming and outgoing messages are handled in the same manner as in `SequencePaxos`. That is, the user, must fetch outgoing messages and send them over the network, and incoming messages must be passed to the `BallotLeaderElection` struct. 

```rust,edition2018,no_run,noplaypen
// handle incoming message from network layer
let ble_msg: BLEMessage = ...;    // message to this node e.g. `msg.to = 2`
ble.handle(ble_msg);

// send outgoing messages. This should be called periodically, e.g. every ms
for out_msg in ble.get_outgoings_msgs() {
    let receiver = out_msg.to;
    // send out_msg to receiver on network layer
}
``` 