use omnipaxos_core::{
    ballot_leader_election::{BLEConfig, BallotLeaderElection},
    sequence_paxos::{CompactionErr, ReconfigurationRequest, SequencePaxos, SequencePaxosConfig},
    storage::{memory_storage::MemoryStorage, Snapshot},
    util::LogEntry,
};
use std::collections::HashMap;

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
fn main() {
    // configuration with id 1 and the following cluster
    let configuration_id = 1;
    let _cluster = vec![1, 2, 3];

    // create the replica 2 in this cluster (other replica instances are created similarly with pid 1 and 3 on other servers)
    let my_pid = 2;
    let my_peers = vec![1, 3];

    let mut sp_config = SequencePaxosConfig::default();
    sp_config.set_configuration_id(configuration_id);
    sp_config.set_pid(my_pid);
    sp_config.set_peers(my_peers.clone());

    //let storage = MemoryStorage::<KeyValue, KVSnapshot>::default();
    let storage = MemoryStorage::<KeyValue, ()>::default();

    let mut seq_paxos = SequencePaxos::with(sp_config, storage);
    let write_entry = KeyValue {
        key: String::from("a"),
        value: 123,
    };
    seq_paxos.append(write_entry).expect("Failed to append");

    /* Fail-recovery */
    /*
    let recovered_storage = ...;    // some persistent storage
    let mut recovered_paxos = SequencePaxos::with(sp_config, recovered_storage);
     */

    /* Reconfiguration */
    /*
    // Node 3 seems to have crashed... let's replace it with node 4.
    let new_configuration = vec![1, 2, 4];
    let metadata = None;
    let rc = ReconfigurationRequest::with(new_configuration, metadata);
    seq_paxos
        .reconfigure(rc)
        .expect("Failed to propose reconfiguration");

    let idx: u64 = 0; // some index we have read already
    // let decided_entries: Option<Vec<LogEntry<KeyValue, KVSnapshot>>> =
    //     seq_paxos.read_decided_suffix(idx);
    let decided_entries: Option<Vec<LogEntry<KeyValue, ()>>> =
        seq_paxos.read_decided_suffix(idx);

    //如果de有值
    if let Some(de) = decided_entries {
        for d in de {
            match d {
                LogEntry::StopSign(stopsign) => {
                    let new_configuration = stopsign.nodes;
                    if new_configuration.contains(&my_pid) {
                        // we are in new configuration, start new instance
                        let mut new_sp_conf = SequencePaxosConfig::default();
                        new_sp_conf.set_configuration_id(stopsign.config_id);
                        let new_storage = MemoryStorage::<KeyValue, KVSnapshot>::default();
                        let mut new_sp = SequencePaxos::with(new_sp_conf, new_storage);
                        todo!()
                    }
                }
                LogEntry::Snapshotted(s) => {
                    // read an entry that is snapshotted
                    let snapshotted_idx = s.trimmed_idx;
                    //let snapshot: KVSnapshot = s.snapshot;
                    let snapshot: () = s.snapshot;
                    // ...can query the latest value for a key in snapshot
                }
                _ => {
                    todo!()
                }
            }
        }
    }
    */
    match seq_paxos.trim(Some(100)) {
        Ok(_) => {
            // later, we can see that the trim succeeded with `seq_paxos.get_compacted_idx()`
        }
        Err(e) => {
            match e {
                CompactionErr::NotAllDecided(idx) => {
                    // Our provided trim index was not decided by all servers yet. All servers have currently only decided up to `idx`.
                }
                CompactionErr::UndecidedIndex(idx) => {
                    // Our provided snapshot index is not decided yet. The currently decided index is `idx`.
                }
            }
        }
    }
    
    let mut ble_conf = BLEConfig::default();
    let mut ble_config = BLEConfig::default();
    ble_config.set_pid(my_pid);
    ble_config.set_peers(my_peers);
    ble_config.set_hb_delay(100); // a leader timeout of 100 ticks

    let mut ble = BallotLeaderElection::with(ble_conf);

    // send outgoing messages. This should be called periodically, e.g. every ms
    for out_msg in ble.get_outgoing_msgs() {
        let receiver = out_msg.to;
        // send out_msg to receiver on network layer
    }
    
}
