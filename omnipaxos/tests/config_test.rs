#![cfg(feature = "toml_config")]

use omnipaxos::{ballot_leader_election::Ballot, OmniPaxosConfig};
use serial_test::serial;

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
            assert_eq!(config.cluster_config.nodes, vec![1, 2, 3]);
            assert_eq!(
                config.cluster_config.initial_leader,
                Some(Ballot {
                    n: 1,
                    priority: 2,
                    pid: 1,
                })
            );
            assert_eq!(config.server_config.pid, 1);
            assert_eq!(config.server_config.buffer_size, 100000);
            #[cfg(feature = "logging")]
            assert_eq!(
                config.server_config.logger_file_path,
                Some("/omnipaxos/logs".to_string())
            );
            assert_eq!(config.server_config.leader_priority, 2);
        }
    }
}
