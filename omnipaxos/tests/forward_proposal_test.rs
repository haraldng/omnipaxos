pub mod utils;

use kompact::prelude::{promise, Ask};
use omnipaxos::ballot_leader_election::Ballot;
use omnipaxos::util::NodeId;
use rand::Rng;
use serial_test::serial;
use utils::{TestConfig, TestSystem, Value};

/// Verifies if the follower nodes forwards the proposal message to a leader
/// so it can get decided.
#[test]
#[serial]
fn forward_proposal_test() {
    let cfg = TestConfig::load("proposal_test").expect("Test config loaded");
    let mut sys = TestSystem::with(cfg);

    let first_node = sys.nodes.get(&1).unwrap();
    let (kprom_ble, kfuture_ble) = promise::<Ballot>();
    first_node.on_definition(|x| x.election_futures.push(Ask::new(kprom_ble, ())));

    sys.start_all_nodes();

    let elected_leader = kfuture_ble
        .wait_timeout(cfg.wait_timeout)
        .expect("No leader has been elected in the allocated time!");
    println!("elected: {:?}", elected_leader);

    let mut proposal_node: NodeId;
    loop {
        proposal_node = rand::thread_rng().gen_range(1..=cfg.num_nodes as NodeId);

        if proposal_node != elected_leader.pid {
            break;
        }
    }

    let px = sys.nodes.get(&proposal_node).unwrap();
    let v = Value::with_id(proposal_node);
    let (kprom, kfuture) = promise();
    px.on_definition(|x| {
        x.insert_decided_future(Ask::new(kprom, v.clone()));
        x.paxos.append(v.clone()).expect("Failed to call Append");
    });

    kfuture
        .wait_timeout(cfg.wait_timeout)
        .expect("The message was not proposed in the allocated time!");

    println!("Pass forward_proposal");

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}
