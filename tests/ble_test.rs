pub mod util;

use kompact::prelude::{promise, Ask};
use omnipaxos::leader_election::ballot_leader_election::Ballot;
use omnipaxos::leader_election::Leader;
use serial_test::serial;
use std::time::Duration;
use util::TestSystem;

const WAIT_TIMEOUT: Duration = Duration::from_secs(2);
const NUM_THREADS: usize = 8;
const NUM_NODES: usize = 9;
const BLE_HB_DELAY: u64 = 5;
const INCREMENT_DELAY: u64 = 2;
const NUM_ELECTIONS: u64 = 3;

/// Test Ballot Election Leader module.
/// The test waits for [`NUM_ELECTIONS`] elections.
/// After each election, the leader node is killed and the process repeats
/// until the number of elections is achieved.
#[test]
#[serial]
fn ble_test() {
    let mut sys = TestSystem::with(
        NUM_NODES,
        BLE_HB_DELAY,
        None,
        None,
        INCREMENT_DELAY,
        NUM_THREADS,
    );

    let (ble, _) = sys.ble_paxos_nodes().get(&1).unwrap();

    let mut futures = vec![];
    for _ in 0..NUM_ELECTIONS {
        let (kprom, kfuture) = promise::<Leader<Ballot>>();
        ble.on_definition(|x| x.add_ask(Ask::new(kprom, ())));
        futures.push(kfuture);
    }

    sys.start_all_nodes();

    for fr in futures.into_iter() {
        let elected_leader = fr
            .wait_timeout(WAIT_TIMEOUT)
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
