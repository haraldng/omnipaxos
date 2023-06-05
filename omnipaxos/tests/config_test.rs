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
        Err(e) => panic!("Couldn't parse config file: {:?}", e),
        Ok(omnipaxos_config) => {
            assert_eq!(omnipaxos_config.configuration_id, 1);
            assert_eq!(omnipaxos_config.pid, 1);
            assert_eq!(omnipaxos_config.peers, vec![2, 3, 4, 5]);
            assert_eq!(omnipaxos_config.buffer_size, 10000);
            assert_eq!(omnipaxos_config.election_tick_timeout, 10);
            assert_eq!(omnipaxos_config.resend_message_tick_timeout, 100);
            #[cfg(feature = "logging")]
            assert_eq!(
                omnipaxos_config.logger_file_path,
                Some("logs/paxos_1.log".to_string())
            );
            assert_eq!(omnipaxos_config.leader_priority, 3);
            assert_eq!(
                omnipaxos_config.flexible_quorum,
                Some(FlexibleQuorum {
                    read_quorum_size: 4,
                    write_quorum_size: 2
                })
            );
            // Make sure we pass asserts in build
            omnipaxos_config.build(MemoryStorage::<Value>::default());
        }
    }
}
