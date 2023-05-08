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
    use omnipaxos::{macros::Entry, OmniPaxos, OmniPaxosConfig};
    use omnipaxos_storage::memory_storage::MemoryStorage;

    #[derive(Clone, Debug, Entry)]
    struct TestEntry {
        pub _field1: u64,
        pub _field2: String,
    }

    let config = OmniPaxosConfig {
        configuration_id: 1,
        pid: 1,
        peers: vec![2, 3],
        ..Default::default()
    };

    let _omnipaxos: OmniPaxos<TestEntry, MemoryStorage<TestEntry>> =
        config.build(MemoryStorage::default());
}
