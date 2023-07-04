// This test is to test the all the code examples on the OmniPaxos website:
// https://omnipaxos.com/docs/omnipaxos/omnipaxos/
// Every code example on the website are separated into different functions with a link
// to the corresponding docs file. If you change any code here,
// please update the code example in the corresponding md file.
// To run: cargo test --test docs_integration_test --features "toml_config macros"
#![cfg(feature = "macros")]
#![cfg(feature = "toml_config")]

use commitlog::LogOptions;
use omnipaxos::{
    messages::Message, storage::Snapshot, util::LogEntry, ClusterConfig, OmniPaxos,
    OmniPaxosConfig, ServerConfig,
};
use omnipaxos_storage::{
    memory_storage::MemoryStorage,
    persistent_storage::{PersistentStorage, PersistentStorageConfig},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// https://github.com/haraldng/omnipaxos/blob/master/docs/omnipaxos/index.md#example-key-value-store
// CODE_EXAMPLE
use omnipaxos::macros::Entry;

#[cfg_attr(feature = "macros", derive(Entry))]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KeyValue {
    pub key: String,
    pub value: u64,
}
// END_CODE_EXAMPLE

// https://github.com/haraldng/omnipaxos/blob/master/docs/omnipaxos/compaction.md#snapshot
// CODE_EXAMPLE
#[cfg(not(feature = "macros"))]
use omnipaxos::storage::Entry;

#[cfg(not(feature = "macros"))]
#[derive(Clone, Debug)]
pub struct KeyValue {
    pub key: String,
    pub value: u64,
}

#[cfg(not(feature = "macros"))]
impl Entry for KeyValue {
    type Snapshot = KVSnapshot;
}
// END_CODE_EXAMPLE

// https://github.com/haraldng/omnipaxos/blob/master/docs/omnipaxos/compaction.md#snapshot
// CODE_EXAMPLE
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
// END_CODE_EXAMPLE

// https://github.com/haraldng/omnipaxos/blob/master/docs/omnipaxos/index.md#creating-a-node
fn creating_a_node() -> OmniPaxos<KeyValue, MemoryStorage<KeyValue>> {
    // CODE_EXAMPLE
    use omnipaxos::{ClusterConfig, OmniPaxos, OmniPaxosConfig, ServerConfig};
    use omnipaxos_storage::memory_storage::MemoryStorage;

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
    let mut omni_paxos: OmniPaxos<KeyValue, MemoryStorage<KeyValue>> =
        omnipaxos_config.build(storage).unwrap();
    // END_CODE_EXAMPLE

    return omni_paxos;
}

// https://github.com/haraldng/omnipaxos/blob/master/docs/omnipaxos/index.md#creating-a-node
fn toml_config() -> OmniPaxosConfig {
    // CODE_EXAMPLE
    let config_file_path = "tests/config/node1.toml";
    let omnipaxos_config = OmniPaxosConfig::with_toml(config_file_path).unwrap();
    // END_CODE_EXAMPLE

    omnipaxos_config
}

// https://github.com/haraldng/omnipaxos/blob/master/docs/omnipaxos/index.md#fail-recovery
#[test]
fn fail_recovery() {
    let omnipaxos_config = toml_config();

    // CODE_EXAMPLE
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
    // END_CODE_EXAMPLE
}

// https://github.com/haraldng/omnipaxos/blob/master/docs/omnipaxos/storage.md#batching
#[test]
fn batch_config() {
    // CODE_EXAMPLE
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
    // END_CODE_EXAMPLE
}

// https://github.com/haraldng/omnipaxos/blob/master/docs/omnipaxos/communication.md#incoming-and-outgoing
fn incoming_messages(in_msg: Message<KeyValue>) {
    let mut omni_paxos = creating_a_node();

    // CODE_EXAMPLE
    use omnipaxos::messages::Message;

    // handle incoming message from network layer
    let msg: Message<KeyValue> = in_msg; // message to this node e.g. `msg.get_receiver() == 2`
    omni_paxos.handle_incoming(msg);
    // END_CODE_EXAMPLE
}

#[test]
fn outgoing_messages() {
    let mut omni_paxos = creating_a_node();

    // CODE_EXAMPLE
    // send outgoing messages. This should be called periodically, e.g. every ms
    for out_msg in omni_paxos.outgoing_messages() {
        let receiver = out_msg.get_receiver();
        // send out_msg to receiver on network layer
    }
    // END_CODE_EXAMPLE
}

// https://github.com/haraldng/omnipaxos/blob/master/docs/omnipaxos/log.md
#[test]
fn append_log() {
    let mut omni_paxos = creating_a_node();

    // CODE_EXAMPLE
    let write_entry = KeyValue {
        key: String::from("a"),
        value: 123,
    };
    omni_paxos.append(write_entry).expect("Failed to append");
    // END_CODE_EXAMPLE
}

#[test]
fn read_log() {
    let mut omni_paxos = creating_a_node();

    // CODE_EXAMPLE
    /*** Read a single entry ***/
    let idx = 5;
    let read_entry = omni_paxos.read(idx);

    /*** Read a range ***/
    let read_entries = omni_paxos.read_entries(2..5);
    // END_CODE_EXAMPLE
}

// https://github.com/haraldng/omnipaxos/blob/master/docs/omnipaxos/leader_election.md
#[test]
fn tick() {
    let mut omni_paxos = creating_a_node();

    // CODE_EXAMPLE
    // Call this periodically
    omni_paxos.tick();
    // END_CODE_EXAMPLE
}

// https://github.com/haraldng/omnipaxos/blob/master/docs/omnipaxos/compaction.md#trim
#[test]
fn trim() {
    let mut omni_paxos = creating_a_node();

    // CODE_EXAMPLE
    use omnipaxos::CompactionErr;

    // we will try trimming the first 100 entries of the log.
    let trim_idx = Some(100); // using `None` will use the highest trimmable index
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
    // END_CODE_EXAMPLE
}

// https://github.com/haraldng/omnipaxos/blob/master/docs/omnipaxos/compaction.md#snapshot
#[test]
fn snapshot() {
    use omnipaxos::CompactionErr;
    let mut omni_paxos = creating_a_node();

    // CODE_EXAMPLE
    // we will try snapshotting the first 100 entries of the log.
    let snapshot_idx = Some(100); // using `None` will use the highest snapshottable index
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
    // END_CODE_EXAMPLE
}

// https://github.com/haraldng/omnipaxos/blob/master/docs/omnipaxos/reconfiguration.md
#[test]
fn reconfiguration() {
    let mut omni_paxos = creating_a_node();

    // CODE_EXAMPLE
    // Node 3 seems to have crashed... let's replace it with a new node 4.
    let new_configuration = ClusterConfig {
        configuration_id: 2,
        nodes: vec![1, 2, 4],
        ..Default::default()
    };
    let metadata = None;
    omni_paxos
        .reconfigure(new_configuration, metadata)
        .expect("Failed to propose reconfiguration");
    // END_CODE_EXAMPLE
}

// https://github.com/haraldng/omnipaxos/blob/master/docs/omnipaxos/reconfiguration.md
#[test]
fn get_stop_sign() {
    let mut omni_paxos = creating_a_node();

    // CODE_EXAMPLE
    // the ServerConfig config for current node
    let current_config = ServerConfig {
        pid: 2,
        ..Default::default()
    };
    let my_pid = current_config.pid;

    let idx: u64 = 2; // some index we last read from
    let decided_entries: Option<Vec<LogEntry<KeyValue>>> = omni_paxos.read_decided_suffix(idx);

    if let Some(de) = decided_entries {
        for d in de {
            match d {
                LogEntry::StopSign(stopsign, true) => {
                    let new_configuration = stopsign.next_config;
                    if new_configuration.nodes.contains(&my_pid) {
                        // current configuration has been safely stopped. Start new instance
                        let new_storage = MemoryStorage::default();
                        let mut new_omnipaxos: OmniPaxos<KeyValue, MemoryStorage<KeyValue>> =
                            new_configuration
                                .build_for_server(current_config.clone(), new_storage)
                                .unwrap();

                        // use new_omnipaxos
                        // ...
                    }
                }
                _ => {}
            }
        }
    }
    // END_CODE_EXAMPLE
}

// https://github.com/haraldng/omnipaxos/blob/master/docs/omnipaxos/flexible_quorums.md
#[test]
fn flexible_quorums() {
    // CODE_EXAMPLE
    use omnipaxos::{util::FlexibleQuorum, ClusterConfig, OmniPaxosConfig, ServerConfig};

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
    // END_CODE_EXAMPLE
}
