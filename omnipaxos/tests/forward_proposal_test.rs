pub mod utils;

use kompact::prelude::{promise, Ask};
use omnipaxos::ballot_leader_election::Ballot;
use rand::Rng;
use serial_test::serial;
use std::time::Duration;
use utils::{TestConfig, TestSystem, Value};

/// Verifies if the follower nodes forwards the proposal message to a leader
/// so it can get decided.
#[test]
#[serial]
fn forward_proposal_test() {
    let cfg = TestConfig::load("proposal_test").expect("Test config loaded");

    let mut sys = TestSystem::with(
        cfg.num_nodes,
        cfg.election_timeout_ms,
        cfg.resend_message_timeout_ms,
        cfg.num_threads,
        cfg.storage_type,
    );

    let first_node = sys.nodes.get(&1).unwrap();

    let (kprom_ble, kfuture_ble) = promise::<Ballot>();
    first_node.on_definition(|x| x.election_futures.push(Ask::new(kprom_ble, ())));

    sys.start_all_nodes();

    let elected_leader = kfuture_ble
        .wait_timeout(Duration::from_millis(cfg.wait_timeout_ms))
        .expect("No leader has been elected in the allocated time!");
    println!("elected: {:?}", elected_leader);

    let mut proposal_node: u64;
    loop {
        proposal_node = rand::thread_rng().gen_range(1..=cfg.num_nodes as u64);

        if proposal_node != elected_leader.pid {
            break;
        }
    }

    let px = sys.nodes.get(&proposal_node).unwrap();

    let v = Value(1);
    let (kprom_px, kfuture_px) = promise::<Value>();
    px.on_definition(|x| {
        x.decided_futures.push(Ask::new(kprom_px, ()));
        x.paxos.append(v).expect("Failed to call Append");
    });

    let decided = kfuture_px
        .wait_timeout(Duration::from_millis(cfg.wait_timeout_ms))
        .expect("The message was not proposed in the allocated time!");

    assert_eq!(
        v, decided,
        "The decided value is not the same as the forwarded proposal"
    );
    println!("Pass forward_proposal");

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}
