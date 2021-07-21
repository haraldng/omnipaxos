pub mod util;

use kompact::prelude::{promise, Ask};
use omnipaxos::leader_election::ballot_leader_election::Ballot;
use omnipaxos::leader_election::Leader;
use omnipaxos::storage::Entry;
use rand::Rng;
use serial_test::serial;
use std::time::Duration;
use util::TestSystem;

const WAIT_TIMEOUT: Duration = Duration::from_secs(2);
const NUM_THREADS: usize = 8;
const NUM_NODES: usize = 6;
const BLE_HB_DELAY: u64 = 10;
const INCREMENT_DELAY: u64 = 5;

#[test]
#[serial]
fn proposal_test() {
    let sys = TestSystem::with(
        NUM_NODES,
        BLE_HB_DELAY,
        None,
        None,
        false,
        INCREMENT_DELAY,
        NUM_THREADS,
    );

    let (ble, _) = sys.ble_paxos_nodes().get(&1).unwrap();

    let (kprom_ble, kfuture_ble) = promise::<Leader<Ballot>>();
    ble.on_definition(|x| x.add_ask(Ask::new(kprom_ble, ())));

    sys.start_all_nodes();

    let elected_leader = kfuture_ble
        .wait_timeout(WAIT_TIMEOUT)
        .expect("No leader has been elected in the allocated time!");
    println!("elected: {} {}", elected_leader.pid, elected_leader.round.n);

    let mut proposal_node: u64;
    loop {
        proposal_node = rand::thread_rng().gen_range(1..=NUM_NODES as u64);

        if proposal_node != elected_leader.pid {
            break;
        }
    }

    let (_, px) = sys.ble_paxos_nodes().get(&proposal_node).unwrap();

    let (kprom_px, kfuture_px) = promise::<Entry<Ballot>>();
    px.on_definition(|x| {
        x.add_ask(Ask::new(kprom_px, ()));
        x.propose("Decide Paxos".as_bytes().to_vec());
    });

    kfuture_px
        .wait_timeout(WAIT_TIMEOUT)
        .expect("The message was not proposed in the allocated time!");

    println!("Pass proposal");

    match sys.kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}
