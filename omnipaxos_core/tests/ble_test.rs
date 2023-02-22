pub mod utils;

use kompact::prelude::{promise, Ask};
use omnipaxos_core::ballot_leader_election::Ballot;
use serial_test::serial;
use utils::{TestConfig, TestSystem};

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
        cfg.election_timeout,
        cfg.num_threads,
        cfg.storage_type,
    );

    let num_elections = cfg.num_nodes / 2;
    let mut promises = vec![];
    let mut futures = vec![];
    for _ in 0..num_elections {
        let (kprom, kfuture) = promise::<Ballot>();
        promises.push(Ask::new(kprom, ()));
        futures.push(kfuture);
    }

    let first_node = sys.nodes.get(&1).unwrap();
    first_node.on_definition(|x| x.election_futures.append(&mut promises));
    sys.start_all_nodes();

    futures.reverse(); // reverse as the future is being popped when replying

    for (i, fr) in futures.into_iter().enumerate() {
        let elected_leader = fr
            .wait_timeout(cfg.wait_timeout)
            .expect(format!("No leader in election {}", i + 1).as_str());
        println!("elected: {:?}", elected_leader);
        sys.kill_node(elected_leader.pid);
    }

    println!("Pass ballot_leader_election");

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}
