pub mod utils;

use omnipaxos::util::NodeId;
use serial_test::serial;
use std::thread;
use utils::{verification::verify_log, TestConfig, TestSystem, Value};

/// Verifies that an OmniPaxos cluster with a write quorum size of Q can still make
/// progress with Q-1 failures, including leader failure.
#[test]
#[serial]
fn flexible_quorum_prepare_phase_test() {
    // Start Kompact system
    let cfg = TestConfig::load("flexible_quorum_test").expect("Test config couldn't be loaded");
    let mut sys = TestSystem::with(cfg);
    sys.start_all_nodes();

    let initial_proposals = (0..cfg.num_proposals / 2).map(Value::with_id).collect();
    let last_proposals: Vec<Value> = ((cfg.num_proposals / 2)..cfg.num_proposals)
        .map(Value::with_id)
        .collect();
    let expected_log: Vec<Value> = (0..cfg.num_proposals).map(Value::with_id).collect();

    // Propose some initial values
    sys.make_proposals(2, initial_proposals, cfg.wait_timeout);
    let leader_id = sys.get_elected_leader(1, cfg.wait_timeout);

    // Kill maximum number of nodes (including leader) such that cluster can still function
    let maximum_tolerable_follower_failures = cfg.flexible_quorum.unwrap().1 - 1;
    let faulty_followers = (1..=cfg.num_nodes as NodeId)
        .filter(|id| *id != leader_id)
        .take(maximum_tolerable_follower_failures - 1);
    for node_id in faulty_followers {
        sys.kill_node(node_id);
    }
    sys.kill_node(leader_id);

    // Wait for next leader to get elected
    thread::sleep(8 * cfg.election_timeout);

    // Make some propsals
    let still_alive_node_id = sys.nodes.keys().next().unwrap();
    let still_alive_node = sys.nodes.get(still_alive_node_id).unwrap();
    sys.make_proposals(*still_alive_node_id, last_proposals, cfg.wait_timeout);

    // Verify log
    let nodes_log = still_alive_node.on_definition(|x| x.read_decided_log());
    verify_log(nodes_log, expected_log);
}

/// Verifies that an OmniPaxos cluster with N nodes and a write quorum size of Q can still make
/// progress with N - Q failures so long as nodes remain in the accept phase (leader doesn't fail).
#[test]
#[serial]
fn flexible_quorum_accept_phase_test() {
    // Start Kompact system
    let cfg = TestConfig::load("flexible_quorum_test").expect("Test config couldn't be loaded");
    let mut sys = TestSystem::with(cfg);
    sys.start_all_nodes();

    let initial_proposals = (0..cfg.num_proposals / 2).map(Value::with_id).collect();
    let last_proposals: Vec<Value> = ((cfg.num_proposals / 2)..cfg.num_proposals)
        .map(Value::with_id)
        .collect();
    let expected_log: Vec<Value> = (0..cfg.num_proposals).map(Value::with_id).collect();

    // Propose some values
    sys.make_proposals(2, initial_proposals, cfg.wait_timeout);
    let leader_id = sys.get_elected_leader(1, cfg.wait_timeout);

    // Kill maximum number of followers such that leader can still function
    let maximum_tolerable_follower_failures = (1..=cfg.num_nodes as NodeId)
        .filter(|id| *id != leader_id)
        .take(cfg.num_nodes - cfg.flexible_quorum.unwrap().1);
    for node_id in maximum_tolerable_follower_failures {
        sys.kill_node(node_id);
    }

    // Wait for next leader to get elected
    thread::sleep(8 * cfg.election_timeout);

    // Make some more propsals
    sys.make_proposals(leader_id, last_proposals, cfg.wait_timeout);

    // Verify log
    let leader = sys.nodes.get(&leader_id).unwrap();
    let leaders_log = leader.on_definition(|x| x.read_decided_log());
    verify_log(leaders_log, expected_log);
}
