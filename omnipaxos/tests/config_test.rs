#![cfg(feature = "toml_config")]
pub mod utils;

use omnipaxos::{util::FlexibleQuorum, OmniPaxosConfig};
use omnipaxos_storage::memory_storage::MemoryStorage;
use serial_test::serial;
use utils::Value;

/// Tests that all the fields of OmniPaxosConfig can be deserialized
/// from a TOML file.
#[test]
#[serial]
fn config_all_fields_test() {
    let file_path = "tests/config/node1.toml";
    match OmniPaxosConfig::with_toml(file_path) {
        Err(e) => panic!("{e}"),
        Ok(config) => {
            assert_eq!(config.cluster_config.configuration_id, 1);
            assert_eq!(config.cluster_config.nodes, vec![1, 2, 3, 4, 5]);
            assert_eq!(
                config.cluster_config.flexible_quorum,
                Some(FlexibleQuorum {
                    read_quorum_size: 4,
                    write_quorum_size: 2
                })
            );
            assert_eq!(config.server_config.pid, 1);
            assert_eq!(config.server_config.election_tick_timeout, 10);
            assert_eq!(config.server_config.resend_message_tick_timeout, 100);
            assert_eq!(config.server_config.flush_batch_tick_timeout, 200);
            assert_eq!(config.server_config.buffer_size, 10000);
            assert_eq!(config.server_config.batch_size, 2);
            #[cfg(feature = "logging")]
            assert_eq!(
                config.server_config.logger_file_path,
                Some("logs/paxos_1.log".to_string())
            );
            assert_eq!(config.server_config.leader_priority, 2);

            // Make sure we pass asserts in build
            config.build(MemoryStorage::<Value>::default()).unwrap();
        }
    }
}
