pub mod utils;

use kompact::prelude::{promise, Ask};
use omnipaxos::ballot_leader_election::Ballot;
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

    let num_elections = cfg.num_nodes / 2;
    for _ in 0..num_elections {
        // Wait to ensure stabilized leader
        thread::sleep(6 * cfg.election_timeout);
        let elected_leader = sys.get_elected_leader(1, cfg.wait_timeout);
        println!("elected: {:?}", elected_leader);
        assert_ne!(elected_leader, 1, "Leader should never stabilize to 1");
        sys.kill_node(elected_leader)
    }
    println!("Pass ballot_leader_election");

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}
