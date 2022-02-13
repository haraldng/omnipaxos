# Runtime
Up to this point we have described how to work with the `SequencePaxos` and `BallotLeaderElection` structs from `omnipaxos_core`. They implement the core logic behind log replication and leader election, but require users to manually handle their interactions with each other with `tick()` and `handle_leader()` as described [earlier](../ble/index.md). To make it easier for users, we have prototyped a runtime called `omnipaxos_runtime` that handles this interaction with [tokio](https://tokio.rs/).

## Creating an OmniPaxos node
We will reuse the [``KeyValue``](../sequencepaxos/index.md) and [``KVSnapshot``](../sequencepaxos/compaction.md) from earlier chapters as our log entry and snapshot types. To create an `OmniPaxosNode`, we do the following:

```rust,edition2018,no_run,noplaypen
use omnipaxos_runtime::{
    sequence_paxos::{SequencePaxos, SequencePaxosConfig},
    storage::{memory_storage::MemoryStorage},
};

let _cluster = vec![1, 2, 3];

// create node 2 in this cluster (other instances are created similarly with pid 1 and 3 on the other nodes)
let my_pid = 2;
let my_peers = vec![1, 3];

let mut node_conf = NodeConfig::default();
node_conf.set_pid(my_pid);
node_conf.set_peers(my_peers);

let storage = MemoryStorage::<KeyValue, KVSnapshot>::default();
let mut op: OmniPaxosHandle<KeyValue, KVSnapshot> = OmniPaxosNode::new(node_conf, storage);

let OmniPaxosHandle {
    omni_paxos,
    seq_paxos_handle,
    ble_handle
} = op;
```

The `OmniPaxosHandle` has three fields with the types: `OmniPaxosNode`, `SequencePaxosHandle`, and `BLEHandle`. `OmniPaxosNode` exposes a similar, but async API as `SequencePaxos` for [reading/writing](../sequencepaxos/log.md), [compaction](../sequencepaxos/compaction.md), and [reconfiguration](../sequencepaxos/reconfiguration.md). The `SequencePaxosHandle`, and `BLEHandle` are used for communication. We will now go through how you should be using each of these.

## Communication
Both the `SequencePaxosHandle`, and `BLEHandle` consist of two [tokio mpsc channels](https://docs.rs/tokio/latest/tokio/sync/mpsc/index.html) each, one for incoming and one for outgoing messages. The user's task is to read messages from the outgoing channel and send them out over the network, and push any received message from the network layer into the incoming channel. As an example, we will use tokio to spawn a separate thread for each of these tasks:

```rust,edition2018,no_run,noplaypen
let mut sp_in: mpsc::Sender<Message<KeyValue, KVSnapshot>> = seq_paxos_handle.incoming; 
let mut sp_out: mpsc::Receiver<Message<KeyValue, KVSnapshot>> = seq_paxos_handle.outgoing; 

let mut ble_in: mpsc::Sender<BLEMessage> = ble_handle.incoming;
let mut ble_out: mpsc::Receiver<BLEMessage> = ble_handle.outgoing;

tokio::spawn(async move {
    // spawn thread to wait for any outgoing messages produced by SequencePaxos
    while let Some(message) = sp_out.recv().await {
        let receiver = message.to;
        // send Sequence Paxos message over network to the receiver
    }
});

tokio::spawn(async move {
    // spawn thread to wait for any outgoing messages produced by BallotLeaderElection
    while let Some(message) = ble_out.recv().await {
        let receiver = message.to;
        // send BLE message over network to the receiver
    }
});

tokio::spawn(async move {
    // spawn thread to wait for any incoming Sequence Paxos messages from the network layer
    loop {
        // pass message to SequencePaxos
        let sp_msg = todo!(); // received message from network layer;
        sp_in.send(sp_msg).await.expect("Failed to pass message to SequencePaxos");
    }
});

tokio::spawn(async move {
    // spawn thread to wait for any incoming BLE messages from the network layer
    loop {
        // pass message to BLE
        let ble_msg = todo!(); // received message from network layer;
        ble_in.send(ble_msg).await.expect("Failed to pass message to BallotLeaderElection");
    }
});
```

We leave the logic for receiving messages over the network as ``todo!()``, since this depends on your, the user's, network implementation.

## Log Interactions
Using the ``OmniPaxosNode``, we will be able to call the functions of `SequencePaxos` but in an async manner. With the code below, we show how to perform the corresponding functionalities described in the [SequencePaxos](../sequencepaxos) chapter.

```rust,edition2018,no_run,noplaypen
let _leader_pid = omni_paxos.get_current_leader().await;

let write_entry = KeyValue {
    key: String::from("a"),
    value: 123,
};
omni_paxos.append(write_entry).await.expect("Failed to append log");

let _entries: Vec<ReadEntry<KeyValue, KVSnapshot>> = omni_paxos.read_entries(10..).await.expect("Failed to read entries");
let _decided_entries: Vec<ReadEntry<KeyValue, KVSnapshot>> = omni_paxos.read_decided_suffix(0).await.expect("Failed to read decided suffix");

let local_only = true;
omni_paxos.snapshot(Some(100), local_only).await.expect("Failed to snapshot");

/* Reconfiguration */
// Node 3 seems to have crashed... let's replace it with node 4.
let new_configuration = vec![1, 2, 4];
let metadata = None;
let rc = ReconfigurationRequest::with(new_configuration, metadata);
omni_paxos
    .reconfigure(rc)
    .await
    .expect("Failed to propose reconfiguration");
```

Here, we blocked on every call, but since the functions in ``OmniPaxosNode`` are async, it is also possible to spawn a thread for each and wait for them concurrently.

>**Note:** The async functions in ``OmniPaxosNode`` currently returns when its local Sequence Paxos component has received the request. This implies that requests that require network communication is completed. e.g. when `append()` returns, it does NOT mean that the entry has been successfully decided yet. This functionality is planned to be added in the future.