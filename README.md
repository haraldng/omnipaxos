OmniPaxos
============

OmniPaxos is an in-development sequence consensus library implemented in the Rust programming language. 

Similar to Raft, the Omni-Paxos algorithm can be used to build abstractions such as a distributed log or state-machine replication. However, Omni-Paxos uses a modular design that makes it resilient to partial connectivity and provides an efficient reconfiguration that allows new server to catch up the log in parallel.

An OmniPaxos replica is implemented as a Rust ```struct```. This should allow for convenient usage in general or on top of an actor framework such as [Kompact](https://github.com/kompics/kompact).

## Example
```rust
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
let paxos = Paxos::with(
    config_id: configuration_id,
    pid: my_pid,
    peers: my_peers,
    storage,
    ...
);

...

// external leader election indicates we are the leader with the event l
paxos.handle_leader(l);

...

// propose a client request
let data: Vec<u8> = ...; // client request as raw bytes
paxos.propose_normal(data).expect("Failed to propose normal proposal");

...

// propose a reconfiguration
let new_cluster = vec![1, 2, 4];    // oops replica 3 appears to have crashed... let's replace it with a new replica 4
paxos.propose_reconfiguration(new_cluster).expect("Failed to propose reconfiguration");

...

// send outgoing messages
for out_msg in paxos.get_outgoings_msgs() {
    let receiver = out_msg.to;
    // send out_msg to receiver
}

...

// handle decided client requests
for entry in paxos.get_decided_entries() {
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
