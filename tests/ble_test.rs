pub mod test_config;
pub mod util;

use kompact::prelude::{promise, Ask};
use omnipaxos::leader_election::ballot_leader_election::Ballot;
use omnipaxos::leader_election::Leader;
use serial_test::serial;
use test_config::TestConfig;
use util::TestSystem;

/// Test Ballot Election Leader module.
/// The test waits for [`num_elections`] elections.
/// After each election, the leader node is killed and the process repeats
/// until the number of elections is achieved.
#[test]
#[serial]
fn ble_test() {
    let cfg = TestConfig::load("ble_test").expect("Test config loaded");

    let mut sys = TestSystem::with(
        cfg.num_nodes,
        cfg.ble_hb_delay,
        None,
        None,
        cfg.increment_delay,
        cfg.num_threads,
    );

    let (ble, _) = sys.ble_paxos_nodes().get(&1).unwrap();

    let mut futures = vec![];
    for _ in 0..cfg.num_elections {
        let (kprom, kfuture) = promise::<Leader<Ballot>>();
        ble.on_definition(|x| x.add_ask(Ask::new(kprom, ())));
        futures.push(kfuture);
    }

    sys.start_all_nodes();

    for fr in futures.into_iter() {
        let elected_leader = fr
            .wait_timeout(cfg.wait_timeout)
            .expect("No leader has been elected in the allocated time!");
        println!("elected: {} {}", elected_leader.pid, elected_leader.round.n);
        sys.kill_node(elected_leader.pid);
    }

    println!("Pass ballot_leader_election");

    match sys.kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}
