#[test]
fn derive_entry_test() {
    use omnipaxos::macros::Entry;

    #[derive(Clone, Debug, Entry)]
    struct TestEntry {
        pub _field1: u64,
        pub _field2: String,
    }
}

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
    let config = OmniPaxosConfig {
        cluster_config,
        server_config,
    };

    let _omnipaxos: OmniPaxos<TestEntry, MemoryStorage<TestEntry>> =
        config.build(MemoryStorage::default());
}
