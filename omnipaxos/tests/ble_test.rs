pub mod utils;

use serial_test::serial;
use std::thread;
use utils::{TestConfig, TestSystem};

/// Test Ballot Election Leader module.
/// The test waits for [`num_elections`] elections.
/// After each election, the leader node is killed and the process repeats
/// until the number of elections is achieved.
#[test]
#[serial]
fn ble_test() {
    let cfg = TestConfig::load("ble_test").expect("Test config loaded");
    let mut sys = TestSystem::with(cfg);
    sys.start_all_nodes();

    let mut prev_leader = 0;
    let mut killed_nodes = vec![];
    let num_elections = cfg.num_nodes / 2;
    for _ in 0..num_elections {
        // Wait to ensure stabilized leader
        thread::sleep(10 * cfg.election_timeout);
        let alive_node = sys
            .nodes
            .keys()
            .find(|x| !killed_nodes.contains(*x))
            .unwrap();
        let elected_leader = sys.get_elected_leader(*alive_node, cfg.wait_timeout);
        assert_ne!(
            elected_leader, prev_leader,
            "Failed to elect new leader after timeout"
        );
        prev_leader = elected_leader;
        println!("elected: {:?}", elected_leader);
        sys.kill_node(elected_leader);
        killed_nodes.push(elected_leader);
    }
    println!("Pass ballot_leader_election");

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}
