use std::collections::HashMap;
use tokio::sync::mpsc;

use omnipaxos_core::ballot_leader_election::messages::BLEMessage;
// use std::sync::mpsc::Sender;
use omnipaxos_core::{
    messages::Message, sequence_paxos::ReconfigurationRequest, storage::Snapshot,
};
use omnipaxos_runtime::omnipaxos::{NodeConfig, OmniPaxosHandle, OmniPaxosNode, ReadEntry};
use omnipaxos_storage::memory::memory_storage::MemoryStorage;

#[derive(Clone, Debug)]
pub struct KeyValue {
    pub key: String,
    pub value: u64,
}

#[derive(Clone, Debug)]
pub struct KVSnapshot {
    snapshotted: HashMap<String, u64>,
}

impl Snapshot<KeyValue> for KVSnapshot {
    fn create(entries: &[KeyValue]) -> Self {
        let mut snapshotted = HashMap::new();
        for e in entries {
            let KeyValue { key, value } = e;
            snapshotted.insert(key.clone(), *value);
        }
        Self { snapshotted }
    }

    fn merge(&mut self, delta: Self) {
        for (k, v) in delta.snapshotted {
            self.snapshotted.insert(k, v);
        }
    }

    fn use_snapshots() -> bool {
        true
    }
}

#[allow(unused_variables, unused_mut)]
#[tokio::main]
async fn main() {
    let _cluster = vec![1, 2, 3];

    // create the replica 2 in this cluster (other replica instances are created similarly with pid 1 and 3 on other servers)
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
        ble_handle,
    } = op;

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
            sp_in
                .send(sp_msg)
                .await
                .expect("Failed to pass message to SequencePaxos");
        }
    });

    tokio::spawn(async move {
        // spawn thread to wait for any incoming BLE messages from the network layer
        loop {
            // pass message to BLE
            let ble_msg = todo!(); // received message from network layer;
            ble_in
                .send(ble_msg)
                .await
                .expect("Failed to pass message to BallotLeaderElection");
        }
    });

    let _leader_pid = omni_paxos.get_current_leader().await;

    let write_entry = KeyValue {
        key: String::from("a"),
        value: 123,
    };
    omni_paxos
        .append(write_entry)
        .await
        .expect("Failed to append log");

    let _entries: Vec<ReadEntry<KeyValue, KVSnapshot>> = omni_paxos
        .read_entries(10..)
        .await
        .expect("Failed to read entries");
    let _decided_entries: Vec<ReadEntry<KeyValue, KVSnapshot>> = omni_paxos
        .read_decided_suffix(0)
        .await
        .expect("Failed to read decided suffix");

    let local_only = true;
    omni_paxos
        .snapshot(Some(100), local_only)
        .await
        .expect("Failed to snapshot");

    /* Reconfiguration */
    // Node 3 seems to have crashed... let's replace it with node 4.
    let new_configuration = vec![1, 2, 4];
    let metadata = None;
    let rc = ReconfigurationRequest::with(new_configuration, metadata);
    omni_paxos
        .reconfigure(rc)
        .await
        .expect("Failed to propose reconfiguration");
}
