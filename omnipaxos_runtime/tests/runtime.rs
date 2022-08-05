use omnipaxos_core::storage::Snapshot;
use omnipaxos_runtime::omnipaxos::{NodeConfig, OmniPaxosHandle, OmniPaxosNode, ReadEntry};
use omnipaxos_storage::memory_storage::MemoryStorage;
use serial_test::serial;
use std::{collections::HashMap, ops::RangeInclusive, time::Duration};
use tokio::runtime::Builder;

type EntryType = Value;
type SnapshotType = LatestValue;

const NUM_NODES: u64 = 3;
const NODES: RangeInclusive<u64> = 1..=NUM_NODES;

#[test]
#[serial]
fn runtime_test() {
    let runtime = Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut outgoing_channels = HashMap::new();
    let mut incoming_channels = HashMap::new();
    let mut op_handles = HashMap::new();
    for pid in NODES {
        let OmniPaxosHandle {
            omni_paxos: op_handle,
            seq_paxos_handle: sp_handle,
            ble_handle: ble,
        } = create_node(pid);
        outgoing_channels.insert(pid, (sp_handle.outgoing, ble.outgoing));
        incoming_channels.insert(pid, (sp_handle.incoming, ble.incoming));
        op_handles.insert(pid, op_handle);
    }

    for pid in NODES {
        let incoming = incoming_channels.clone();
        let (mut sp_outgoing, mut ble_outgoing) = outgoing_channels.remove(&pid).unwrap();
        runtime.spawn(async move {
            loop {
                tokio::select! {
                    Some(out_msg) = sp_outgoing.recv() => {
                        // println!("Got SP out: {:?}", out_msg);
                        let receiver = out_msg.to;
                        let sp_channel = &incoming.get(&receiver).expect("No receiver Sequence Paxos channel").0;
                        sp_channel.send(out_msg).await.expect("Dropped Sequence Paxos channel");
                    },
                    Some(out_msg) = ble_outgoing.recv() => {
                        // println!("Got BLE out: {:?}", out_msg);
                        let receiver = out_msg.to;
                        let ble_channel = &incoming.get(&receiver).expect("No receiver BLE channel").1;
                        ble_channel.send(out_msg).await.expect("Dropped BLE channel");
                    },
                    else => {},
                }
            }
        });
    }

    std::thread::sleep(Duration::from_millis(3000));
    let leader = runtime.block_on(op_handles.get(&3u64).unwrap().get_current_leader());
    assert!(leader > 0);
    let num_proposals = 5;
    for i in 0..num_proposals {
        let entry = Value(i);
        let append_req = runtime.block_on(op_handles.get(&leader).unwrap().append(entry));
        assert!(append_req.is_ok());
    }
    std::thread::sleep(Duration::from_millis(3000)); // let it get decided...
    let decided_idx = runtime.block_on(op_handles.get(&1u64).unwrap().get_decided_idx());
    assert_eq!(decided_idx, num_proposals);
    let entries = runtime
        .block_on(op_handles.get(&leader).unwrap().read_entries(0..))
        .expect("Failed to read expected entries");

    if entries.len() == 1 {
        let snapshot = LatestValue {
            value: Value(num_proposals - 1),
        };
        match entries.first().unwrap() {
            ReadEntry::Snapshotted(s) => {
                assert_eq!(s.trimmed_idx, num_proposals);
                assert_eq!(s.snapshot, snapshot);
            }
            e => panic!("Unexpected read entry: {:?}. Expected SnapshottedEntry", e),
        }
    } else {
        assert_eq!(
            entries.len(),
            num_proposals as usize,
            "Unexpected read entries {:?}",
            entries
        );
        for (idx, entry) in entries.iter().enumerate() {
            match entry {
                ReadEntry::Decided(v) => {
                    assert_eq!(v, &Value(idx as u64))
                }
                e => panic!("unexpected read entry: {:?}", e),
            }
        }
    }
}

fn create_node(pid: u64) -> OmniPaxosHandle<EntryType, SnapshotType> {
    let mut node_conf = NodeConfig::default();
    node_conf.set_pid(pid);
    node_conf.set_peers(NODES.filter(|x| x != &pid).collect());
    OmniPaxosNode::new(node_conf, MemoryStorage::default())
}

#[derive(Clone, Copy, Debug, Default, PartialOrd, PartialEq)]
pub struct Value(pub u64);

#[derive(Clone, Copy, Debug, Default, PartialOrd, PartialEq)]
pub struct LatestValue {
    value: Value,
}

impl Snapshot<Value> for LatestValue {
    fn create(entries: &[Value]) -> Self {
        Self {
            value: *entries.last().unwrap_or(&Value(0)),
        }
    }

    fn merge(&mut self, delta: Self) {
        self.value = delta.value;
    }

    fn use_snapshots() -> bool {
        true
    }
}
