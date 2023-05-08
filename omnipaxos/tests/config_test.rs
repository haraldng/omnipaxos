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
        Err(e) => panic!("Couldn't parse config file: {:?}", e),
        Ok(omnipaxos_config) => {
            assert_eq!(omnipaxos_config.configuration_id, 1);
            assert_eq!(omnipaxos_config.pid, 1);
            assert_eq!(omnipaxos_config.peers, vec![2, 3]);
            assert_eq!(omnipaxos_config.buffer_size, 100001);
            assert_eq!(omnipaxos_config.election_tick_timeout, 32);
            assert_eq!(omnipaxos_config.resend_message_tick_timeout, 6343);
            #[cfg(feature = "logging")]
            assert_eq!(
                omnipaxos_config.logger_file_path,
                Some("/omnipaxos/logs".to_string())
            );
            assert_eq!(omnipaxos_config.leader_priority, 2);
            assert_eq!(
                omnipaxos_config.skip_prepare_use_leader,
                Some(Ballot {
                    config_id: 3,
                    n: 5,
                    priority: 2,
                    pid: 2,
                })
            );
            assert_eq!(
                omnipaxos_config.initial_leader,
                Some(Ballot {
                    config_id: 3,
                    n: 1,
                    priority: 1,
                    pid: 1,
                })
            );
        }
    }
}

/// Tests that a deserialized OmniPaxosConfig has default values
/// for fields not specified its TOML file.
#[test]
#[serial]
fn config_some_fields_test() {
    let file_path = "tests/config/node2.toml";

    match OmniPaxosConfig::with_toml(file_path) {
        Err(e) => panic!("Couldn't parse config file: {:?}", e),
        Ok(omnipaxos_config) => {
            assert_eq!(omnipaxos_config.configuration_id, 2);
            assert_eq!(omnipaxos_config.pid, 0);
            assert_eq!(omnipaxos_config.peers, vec![]);
            assert_eq!(omnipaxos_config.buffer_size, 100000);
            assert_eq!(omnipaxos_config.election_tick_timeout, 1);
            assert_eq!(omnipaxos_config.resend_message_tick_timeout, 5);
            #[cfg(feature = "logging")]
            assert_eq!(omnipaxos_config.logger_file_path, None);
            assert_eq!(omnipaxos_config.leader_priority, 0);
            assert_eq!(omnipaxos_config.skip_prepare_use_leader, None);
            assert_eq!(omnipaxos_config.initial_leader, None);
        }
    }
}
