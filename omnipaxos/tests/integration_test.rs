
// To run: cargo test --test integration_test --features "toml_config macros"
#![cfg(feature = "macros")]
use omnipaxos::macros::Entry;
use serde::{Deserialize, Serialize};
use commitlog::LogOptions;
use omnipaxos::{ClusterConfig, OmniPaxosConfig, ServerConfig};
use omnipaxos::messages::ballot_leader_election::BLEMessage;
use omnipaxos::messages::Message;
use omnipaxos_storage::persistent_storage::{PersistentStorage, PersistentStorageConfig};
use omnipaxos::storage::Snapshot;
#[cfg(not(feature = "macros"))]
use omnipaxos::storage::Entry;
use std::collections::HashMap;
use omnipaxos::util::LogEntry;


// Original: https://omnipaxos.com/docs/omnipaxos/omnipaxos/
#[cfg_attr(feature = "macros", derive(Entry))]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KeyValue {
    pub key: String,
    pub value: u64,
}


#[cfg(not(feature = "macros"))]
impl Entry for KeyValue {
    type Snapshot = KVSnapshot;
}

// Original: https://omnipaxos.com/docs/omnipaxos/compaction/
#[derive(Clone, Debug)]
pub struct KVSnapshot {
    snapshotted: HashMap<String, u64>
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


// Original: https://omnipaxos.com/docs/omnipaxos/omnipaxos/
#[test]
fn omnipaxos() {
    // Use case of creating a node
    // https://omnipaxos.com/docs/omnipaxos/omnipaxos/
    use omnipaxos::{
        {OmniPaxos, OmniPaxosConfig, ServerConfig, ClusterConfig},
    };
    use omnipaxos_storage::{
        memory_storage::MemoryStorage,
    };

    // Original: https://omnipaxos.com/docs/omnipaxos/omnipaxos/
    // configuration with id 1 and a cluster with 3 nodes
    let cluster_config = ClusterConfig {
        configuration_id: 1,
        nodes: vec![1, 2, 3],
        ..Default::default()
    };

    // create the replica 2 in this cluster (other replica instances are created similarly with pid 1 and 3 on the other nodes)
    let server_config = ServerConfig {
        pid: 2,
        ..Default::default()
    };

    // Combined OmniPaxos config with both clsuter-wide and server specific configurations
    let omnipaxos_config = OmniPaxosConfig {
        cluster_config,
        server_config,
    };

    let storage = MemoryStorage::default();
    let mut omni_paxos: OmniPaxos<KeyValue, MemoryStorage<KeyValue>> = omnipaxos_config.build(storage).unwrap();


    // Original: https://omnipaxos.com/docs/omnipaxos/omnipaxos/
    // Use case of creating a node with TOML config
    let config_file_path = "tests/config/node1.toml";
    let omnipaxos_config = OmniPaxosConfig::with_toml(config_file_path).unwrap();


    // Original: https://omnipaxos.com/docs/omnipaxos/omnipaxos/
    /* Re-creating our node after a crash... */

    // Use case of creating a node from fail-recovery
    // Configuration from previous storage
    let my_path = "tests/config/storage/node1";
    let my_log_opts = LogOptions::new(my_path);
    let mut persist_conf = PersistentStorageConfig::default();

    persist_conf.set_path(my_path.to_string()); // set the path to the persistent storage
    persist_conf.set_commitlog_options(my_log_opts);

    // Re-create storage with previous state, then create `OmniPaxos`
    let recovered_storage: PersistentStorage<KeyValue> = PersistentStorage::open(persist_conf);
    let mut recovered_paxos = omnipaxos_config.build(recovered_storage);


    // Original: https://omnipaxos.com/docs/omnipaxos/communication/
    // send outgoing messages. This should be called periodically, e.g. every ms
    for out_msg in omni_paxos.outgoing_messages() {
        let receiver = out_msg.get_receiver();
        // send out_msg to receiver on network layer
    }

    // handle incoming message from network layer
    // let msg: Message<KeyValue> = ...;   // message to this node e.g. `msg.get_receiver() == 2`
    // omni_paxos.handle_incoming(msg);


    // Original: https://omnipaxos.com/docs/omnipaxos/reading-and-writing/
    let write_entry = KeyValue { key: String::from("a"), value: 123 };
    omni_paxos.append(write_entry).expect("Failed to append");


    // Original: https://omnipaxos.com/docs/omnipaxos/reading-and-writing/
    /*** Read a single entry ***/
    let idx = 5;
    let read_entry = omni_paxos.read(idx);

    /*** Read a range ***/
    let read_entries = omni_paxos.read_entries(2..5);


    // Original: https://omnipaxos.com/docs/omnipaxos/leader-election/
    // Call this periodically
    omni_paxos.tick();


    // Original: https://omnipaxos.com/docs/omnipaxos/compaction/
    use omnipaxos::CompactionErr;

    // we will try trimming the first 100 entries of the log.
    let trim_idx = Some(100);  // using `None` will use the highest trimmable index
    match omni_paxos.trim(trim_idx) {
        Ok(_) => {
            // later, we can see that the trim succeeded with `omni_paxos.get_compacted_idx()`
        }
        Err(e) => {
            match e {
                CompactionErr::NotAllDecided(idx) => {
                    // Our provided trim index was not decided by all servers yet. All servers have currently only decided up to `idx`.
                    // If desired, users can retry with omni_paxos.trim(Some(idx)) which will then succeed.
                }
                _ => {}
            }
        }
    }


    // Original: https://omnipaxos.com/docs/omnipaxos/compaction/
    // we will try snapshotting the first 100 entries of the log.
    let snapshot_idx = Some(100);  // using `None` will use the highest snapshottable index
    let local_only = false; // snapshots will be taken by all nodes.
    match omni_paxos.snapshot(snapshot_idx, local_only) {
        Ok(_) => {
            // later, we can see that the snapshot succeeded with `omni_paxos.get_compacted_idx()`
        }
        Err(e) => {
            match e {
                CompactionErr::UndecidedIndex(idx) => {
                    // Our provided snapshot index is not decided yet. The currently decided index is `idx`.
                }
                _ => {}
            }
        }
    }

    // reading a snapshotted entry
    if let Some(e) = omni_paxos.read(20) {
        match e {
            LogEntry::Snapshotted(s) => {
                // entry at idx 20 is snapshotted since we snapshotted idx 100
                let snapshotted_idx = s.trimmed_idx;
                let snapshot = s.snapshot;
                // ...can query the latest value for a key in snapshot
            }
            _ => {}
        }
    }


    // Original: https://omnipaxos.com/docs/omnipaxos/reconfiguration/
    // Node 3 seems to have crashed... let's replace it with a new node 4.
    let new_configuration = ClusterConfig {
        configuration_id: 2,
        nodes: vec![1, 2, 4],
        ..Default::default()
    };
    let metadata = None;
    omni_paxos.reconfigure(new_configuration, metadata).expect("Failed to propose reconfiguration");


    // the ServerConfig config for current node
    let current_config = ServerConfig {
        pid: 2,
        ..Default::default()
    };
    let my_pid = current_config.pid;

    let idx: u64 = 2;  // some index we last read from
    let decided_entries: Option<Vec<LogEntry<KeyValue>>> = omni_paxos.read_decided_suffix(idx);

    if let Some(de) = decided_entries {
        for d in de {
            match d {
                LogEntry::StopSign(stopsign, true) => {
                    let new_configuration = stopsign.next_config;
                    if new_configuration.nodes.contains(&my_pid) {
                        // current configuration has been safely stopped. Start new instance
                        let new_storage = MemoryStorage::default();
                        let mut new_omnipaxos :OmniPaxos<KeyValue, MemoryStorage<KeyValue>>  = new_configuration.build_for_server(current_config.clone(), new_storage).unwrap();

                        // use new_omnipaxos
                        // ...
                    }
                }
                _ => {}
            }
        }
    }
}

// Original: https://omnipaxos.com/docs/omnipaxos/storage/
#[test]
fn storage() {
    let omnipaxos_config = OmniPaxosConfig {
        server_config: ServerConfig {
            batch_size: 100, // `batch_size = 1` by default
            ..Default::default()
        },
        cluster_config: ClusterConfig {
            configuration_id: 1,
            nodes: vec![1, 2, 3],
            ..Default::default()
        },
    };
    // build omnipaxos instance
}

#[test]
fn flexible_quorums() {
    // Original: https://omnipaxos.com/docs/omnipaxos/flexible-quorums/
    use omnipaxos::{ClusterConfig, OmniPaxosConfig, ServerConfig, util::FlexibleQuorum};

    let flex_quorum = FlexibleQuorum {
        read_quorum_size: 5,
        write_quorum_size: 3,
    };

    let cluster_config = ClusterConfig {
        configuration_id: 1,
        nodes: vec![1, 2, 3, 4, 5, 6, 7],
        flexible_quorum: Some(flex_quorum),
    };
    let server_config = ServerConfig {
        pid: 1,
        ..Default::default()
    };
    let config = OmniPaxosConfig {
        cluster_config,
        server_config,
    };
}

