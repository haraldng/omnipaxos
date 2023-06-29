// To run: cargo test --test integration_test --features "toml_config macros"
mod kv_store;

use commitlog::LogOptions;
use omnipaxos::OmniPaxosConfig;
use omnipaxos_storage::persistent_storage::{PersistentStorage, PersistentStorageConfig};
use crate::kv_store::kv::KeyValue;

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

    // Use case of creating a node with TOML config
    let config_file_path = "tests/config/node1.toml";
    let omnipaxos_config = OmniPaxosConfig::with_toml(config_file_path).unwrap();

    // Use case of creating a node from fail-recovery
    // Configuration from previous storage
    let my_path = "tests/kv_store/storage/node1";
    let my_log_opts = LogOptions::new(my_path);
    let mut persist_conf = PersistentStorageConfig::default();

    persist_conf.set_path(my_path.to_string()); // set the path to the persistent storage
    persist_conf.set_commitlog_options(my_log_opts);

    // Re-create storage with previous state, then create `OmniPaxos`
    let recovered_storage: PersistentStorage<KeyValue> = PersistentStorage::open(persist_conf);
    let mut recovered_paxos = omnipaxos_config.build(recovered_storage);
}

// #[test]
// fn storage() {
//     let omnipaxos_config = OmniPaxosConfig {
//         batch_size: 100, // `batch_size = 1` by default
//         configuration_id,
//         pid: my_pid,
//         peers: my_peers,
//         ..Default::default()
//     }
// // build omnipaxos instance
// }


