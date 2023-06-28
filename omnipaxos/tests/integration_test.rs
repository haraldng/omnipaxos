// To run: cargo test --test integration_test --features "toml_config macros"
mod kv_store;

mod node_creating {
    use crate::kv_store::kv::KeyValue;

    #[test]
    fn creating_node() {
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
        // End: https://omnipaxos.com/docs/omnipaxos/omnipaxos/

        // https://omnipaxos.com/docs/omnipaxos/omnipaxos/
        let config_file_path = "tests/config/node1.toml";
        let omnipaxos_config = OmniPaxosConfig::with_toml(config_file_path).unwrap();
        // End: https://omnipaxos.com/docs/omnipaxos/omnipaxos/
    }
}

