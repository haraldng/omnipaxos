OmniPaxos
============

OmniPaxos is an in-development sequence consensus library implemented in the Rust programming language. 

Similar to Raft, the Omni-Paxos algorithm can be used to build abstractions such as a distributed log or state-machine replication. However, Omni-Paxos uses a modular design that makes it resilient to partial connectivity and provides an efficient reconfiguration that allows new server to catch up the log in parallel.

An OmniPaxos replica is implemented as a Rust ```struct```. This allows it to be used with any desired storage and network implementations. This should allow for convenient usage in general or on top of an actor framework such as [Kompact](https://github.com/kompics/kompact). For more detailed explanations and examples, check out the [tutorial](https://haraldng.github.io/omnipaxos/foreword.html).

## Example
```rust,edition2018,no_run,noplaypen
use omnipaxos::{leader_election::*, paxos::*, storage::*};

// configuration with id 1 and the following cluster
let configuration_id = 1;
let _cluster = vec![1, 2, 3];

// create the replica 2 in this cluster
let my_pid = 2;
let my_peers= vec![1, 3];

let sequence = S::new();        // S is a type that implements the storage::Sequence trait
let paxos_state = P::new();     // P is a type that implements the storage::PaxosState trait
let storage = Storage::with(sequence, paxos_state);

// create a replica in configuration 1 with process id 2
let omni_paxos = OmniPaxos::with(
    configuration_id,
    my_pid,
    my_peers,
    storage,
    ...
);

// create the corresponding Ballot Leader Election instance of this replica
let ble = BallotLeaderElection::with(
    my_pid, 
    my_peers,
    ...
)

...

if let Some(leader) = ble.tick() {
    // BLE indicates a leader has been elected
    omni_paxos.handle_leader(leader);
} 

...

// propose a client request
let data: Vec<u8> = ...; // request as raw bytes
omni_paxos.propose_normal(data).expect("Failed to propose normal proposal");

...

// propose a reconfiguration
let new_cluster = vec![1, 2, 4];    // oops replica 3 appears to have crashed... let's replace it with a new replica 4
omni_paxos.propose_reconfiguration(new_cluster, None).expect("Failed to propose reconfiguration");

...

// send outgoing messages. This should be called periodically by user
for out_msg in omni_paxos.get_outgoings_msgs() {
    let receiver = out_msg.to;
    // send out_msg to receiver
}
for out_msg in ble.get_outgoings_msgs() {
    let receiver = out_msg.to;
    // send out_msg to receiver
}

...

// handle decided client requests
for entry in omni_paxos.get_decided_entries() {
    match entry {
        Entry::Normal(data) => {    // handle normally decided entries
            // data is represented as raw bytes: Vec<u8>
        }
        Entry::StopSign(ss) => {    // handle completed reconfiguration
            let next_configuration_id = ss.config_id;
            let next_cluster = ss.nodes;
            // handle reconfiguration
        }
    }
}
```

## License

This project is licensed under the [Apache-2.0 license](LICENSE).

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in OmniPaxos by you shall be licensed as Apache-2.0, without any additional terms or conditions.
