pub mod test_config;
pub mod util;

use kompact::prelude::{promise, Ask};
use omnipaxos::leader_election::ballot_leader_election::Ballot;
use omnipaxos::leader_election::Leader;
use omnipaxos::storage::Entry;
use rand::Rng;
use serial_test::serial;
use test_config::TestConfig;
use util::TestSystem;

/// Verifies if the follower nodes forwards the proposal message to a leader
/// so it can get decided.
#[test]
#[serial]
fn forward_proposal_test() {
    let cfg = TestConfig::load("proposal_test").expect("Test config loaded");

    let sys = TestSystem::with(
        cfg.num_nodes,
        cfg.ble_hb_delay,
        None,
        None,
        cfg.increment_delay,
        cfg.num_threads,
    );

    let (ble, _) = sys.ble_paxos_nodes().get(&1).unwrap();

    let (kprom_ble, kfuture_ble) = promise::<Leader<Ballot>>();
    ble.on_definition(|x| x.add_ask(Ask::new(kprom_ble, ())));

    sys.start_all_nodes();

    let elected_leader = kfuture_ble
        .wait_timeout(cfg.wait_timeout)
        .expect("No leader has been elected in the allocated time!");
    println!("elected: {} {}", elected_leader.pid, elected_leader.round.n);

    let mut proposal_node: u64;
    loop {
        proposal_node = rand::thread_rng().gen_range(1..=cfg.num_nodes as u64);

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
        .wait_timeout(cfg.wait_timeout)
        .expect("The message was not proposed in the allocated time!");

    println!("Pass forward_proposal");

    match sys.kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}
