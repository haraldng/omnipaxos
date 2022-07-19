pub mod test_config;
pub mod util;

use crate::util::Value;
use kompact::prelude::{promise, Ask};
use omnipaxos_core::ballot_leader_election::Ballot;
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

    let sys = TestSystem::with(cfg.num_nodes, cfg.ble_hb_delay, cfg.num_threads);

    let (ble, _) = sys.ble_paxos_nodes().get(&1).unwrap();

    let (kprom_ble, kfuture_ble) = promise::<Ballot>();
    ble.on_definition(|x| x.add_ask(Ask::new(kprom_ble, ())));

    sys.start_all_nodes();

    let elected_leader = kfuture_ble
        .wait_timeout(cfg.wait_timeout)
        .expect("No leader has been elected in the allocated time!");
    println!("elected: {:?}", elected_leader);

    let mut proposal_node: u64;
    loop {
        proposal_node = rand::thread_rng().gen_range(1..=cfg.num_nodes as u64);

        if proposal_node != elected_leader.pid {
            break;
        }
    }

    let (_, px) = sys.ble_paxos_nodes().get(&proposal_node).unwrap();

    let (kprom_px, kfuture_px) = promise::<Value>();
    px.on_definition(|x| {
        x.add_ask(Ask::new(kprom_px, ()));
        x.paxos.append(Value(123)).expect("Failed to append");
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
