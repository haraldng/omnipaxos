A unique feature of Omni-Paxos is the guaranteed progress with one quorum-connected server using Ballot Leader Election (BLE). In this section, we will see how the leader election is used in OmniPaxos.

To detect any leader failures and elect a new leader, you must call the function ``tick()`` periodically. The number of ticks required before an election timeout is triggered can be configured with the ``election_tick_timeout`` field of ``OmniPaxosConfig``. For example, if ``tick()`` is called every 10ms and the ``election_tick_timeout`` is 10, then an election will be triggered every 100ms.

```rust
// Call this periodically
omni_paxos.tick();
```

If a leader has failed, it will be detected in one election timeout and a new leader will be elected in the next timeout (if possible).

> **Note:** The `leader_priority` field in `OmniPaxosConfig` allows user to give desired servers a higher priority to get elected upon a leader change.

In some cases, a network error will require the resending of messsages between leader and follower. The `tick()` function also drives this behavior. The number of ticks required before checking if a message needs to be resent can be configured with the ``resend_message_tick_timeout`` field of ``OmniPaxosConfig``.



## Flexible Quorums
OmniPaxos also offers support for flexible quorums. Normally a node needs to consult a majority of nodes to become a leader and then as a leader consult a majority of nodes to append to the log. Flexible quorums allow for a trade off between the number of nodes that need to be consulted in each of these two scenarios.

OmniPaxos can be configured to use a flexible quorum as follows:
```rust
use omnipaxos::{OmniPaxosConfig, util::FlexibleQuroum};

let flex_quorum = FlexibleQuorum {
    read_quorum_size: 5,
    write_quorum_size: 3,
};

let config = OmniPaxosConfig {
    configuration_id: 1,
    pid: 1,
    peers: vec![2, 3, 4, 5, 6, 7],
    flexible_quorum: Some(flex_quorum),
    ..Default::deafult()
};
```
In OmniPaxos, becoming a leader involves reading from the log and so `read_quorum_size` is the number of nodes to consult in order to become a leader. The `write_quorum_size` is the number of nodes to consult in order to append to the log.

Appending to the log is a much more common occurance than electing a leader so reducing the `write_quorum_size` can reduce the latency and sometimes even increase throughput. Note, however, that reducing the `write_quorum_size` also decreases the fault tolerance of the cluster to `write_quorum_size - 1`.

In order to guarantee safety the `read_quorum_size` and `write_quorum_size` must overlap (i.e. `read_quorum_size` + `write_quorum_size` > # of nodes in cluster). In practice this means that reducing the `write_quorum_size` requires increasing the `read_quorum_size`.

