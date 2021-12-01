# Ballot Leader Election
A unique feature of the Omni-Paxos protocol is guaranteed progress with one quorum-connected server using Ballot Leader Election (BLE). In this section, we will see how the leader election is used in OmniPaxos. Similar to `OmniPaxos`, `BallotLeaderElection` is represented as a Rust struct that should be started on every server in the cluster.

The BLE protocol is based on exchanging heartbeats that should be received within some expected time. To represent time, `BallotLeaderElection` uses an internal logical clock. It has a `tick()` function that progresses its logical clock. The expected time that heartbeats should be received in is specified by `hb_delay` of the `BallotLeaderElection` constructor:
```rust,edition2018,no_run,noplaypen
BallotLeaderElection::with(
    peers: Vec<u64>,        // the PEERS of this server. I.e. this should not include `pid`    
    pid: u64,               // the unique identifier of this server
    hb_delay: u64,          // the delay that is waited for heartbeat responses from peers. Measured in number of `tick()` calls.
    priority: Option<u64>,  // custom priority parameter.
    initial_leader: Option<Ballot>,     // initial leader
    initial_delay_factor: Option<u64>,  // allows for using a shorter delay when electing the initial leader.
    logger: Option<Logger>,
    log_file_path: Option<&str>,
)
```
> **Note** The `priority` parameter allows user to give desired servers a higher priority to become the leader. This is a best-effort approach upon a leader election or failure.

For instance, we can create a BLE instance with a timeout of 800ms that is ticked every 100ms. The user has to call `tick()` every 100ms themselves and check if a leader has been elected. Once a leader is elected, `tick()` will return an instance of the struct `Leader<Ballot>`. This should be passed to the corresponding local `OmniPaxos` instance using `OmniPaxos::handle_leader()`, which will in turn use the `Ballot` as a round number for log replication.

```rust,edition2018,no_run,noplaypen
use omnipaxos::paxos::OmniPaxos;
use omnipaxos::leader_election::ballot_leader_election::*;

let _cluster = vec![1, 2, 3];
let my_pid = 2;
let my_peers= vec![1, 3];

let hb_delay = 8;                     // 8 ticks delay 
let initial_delay_factor = Some(2);   // first hb_delay will be 8/2 = 4 ticks i.e. 400ms.

let omni_paxos = OmniPaxos::with(... , my_pid, my_peers, ...); // see `OmniPaxos` section

let ble = BallotLeaderElection::with(
    my_peers,
    my_pid,
    None,
    hb_delay,
    None,
    initial_delay_factor,
    None,
    None,
)

// every 100ms call the following
if let Some(leader) = ble.tick() {
    omni_paxos.handle_leader(leader);
}

for out_msg in ble.get_outgoings_msgs() {
    let receiver = out_msg.to;
    // send out_msg to receiver on network layer
}

// handle incoming message on network layer
let msg: BLEMessage = ...;    // message received to this server.
ble.handle(msg);
```

As seen in the example, incoming and outgoing messages are handled in the same manner as in `OmniPaxos`. Furthermore, BLE also has a constructor that uses hocon: `with_hocon()`. 