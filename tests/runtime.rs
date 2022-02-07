pub mod util;

use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::time::Duration;
use omnipaxos::core::storage::memory_storage::MemoryStorage;
use omnipaxos::omnipaxos::*;
use omnipaxos::runtime::{SequencePaxosHandle, BLEHandle};
use crate::util::{LatestValue, Value};
use serial_test::serial;
use tokio::runtime::Builder;

type EntryType = Value;
type SnapshotType = LatestValue;

const NUM_NODES: u64 = 3;
const NODES: RangeInclusive<u64> = 1..=NUM_NODES;

// #[serial]
#[test]
fn runtime_test() {
    let runtime = Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut outgoing_channels = HashMap::new();
    let mut incoming_channels = HashMap::new();
    let mut op_handles = vec![];
    for pid in NODES {
        let OmniPaxosHandle {
            omni_paxos: op_handle,
            seq_paxos_handle: sp_handle,
            ble_handle: ble,
        } = create_node(pid);
        outgoing_channels.insert(pid, (sp_handle.outgoing, ble.outgoing));
        incoming_channels.insert(pid, (sp_handle.incoming, ble.incoming));
        op_handles.push(op_handle);
    }

    for pid in NODES {
        let incoming = incoming_channels.clone();
        let (mut sp_outgoing, mut ble_outgoing) = outgoing_channels.remove(&pid).unwrap();
        runtime.spawn(async move {
            loop {
                tokio::select! {
                    Some(out_msg) = sp_outgoing.recv() => {
                        let receiver = out_msg.to;
                        let sp_channel = &incoming.get(&receiver).expect("No receiver Sequence Paxos channel").0;
                        sp_channel.send(out_msg).await.expect("Dropped Sequence Paxos channel");
                    },
                    Some(out_msg) = ble_outgoing.recv() => {
                        let receiver = out_msg.to;
                        let ble_channel = &incoming.get(&receiver).expect("No receiver BLE channel").1;
                        ble_channel.send(out_msg).await.expect("Dropped BLE channel");
                    },
                    else => break,
                }
            }
        });
    }


    std::thread::sleep(Duration::from_millis(5000));
    runtime.block_on(op_handles.get(2).unwrap().append(Value(100))).unwrap();
}

fn create_node(pid: u64) -> OmniPaxosHandle<EntryType, SnapshotType, MemoryStorage<EntryType, SnapshotType>> {
    let peers = NODES.filter(|x| x != &pid).collect();
    OmniPaxosNode::new(pid, peers, NodeConfig::default(), MemoryStorage::default())
}

