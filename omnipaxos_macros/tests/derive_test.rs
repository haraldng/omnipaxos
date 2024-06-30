use omnipaxos::CompartmentalizationConfig;

#[test]
fn build_op_test() {
    use omnipaxos::{macros::Entry, ClusterConfig, OmniPaxos, OmniPaxosConfig, ServerConfig};
    use omnipaxos_storage::memory_storage::MemoryStorage;

    #[derive(Clone, Debug, Entry)]
    struct TestEntry {
        pub _field1: u64,
        pub _field2: String,
    }

    let cluster_config = ClusterConfig {
        configuration_id: 1,
        nodes: vec![1, 2, 3],
        ..Default::default()
    };
    let server_config = ServerConfig {
        pid: 1,
        ..Default::default()
    };
    let compartmentalization_config = CompartmentalizationConfig {
        proxy_leaders: false
    };
    let config = OmniPaxosConfig {
        cluster_config,
        server_config,
        compartmentalization_config
    };

    let _omnipaxos: OmniPaxos<TestEntry, MemoryStorage<TestEntry>> =
        config.build(MemoryStorage::default()).unwrap();
}
