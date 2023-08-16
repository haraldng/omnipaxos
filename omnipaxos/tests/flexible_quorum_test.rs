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

    let proposals: Vec<Value> = (0..cfg.num_proposals)
        .into_iter()
        .map(|v| Value(v))
        .collect();
    let expected_log = proposals.clone();

    // Kill maximum number of nodes (including leader) such that cluster can still function
    let leader_id = sys.get_elected_leader(1, cfg.wait_timeout);
    let maximum_tolerable_follower_failures = cfg.flexible_quorum.unwrap().1 - 1;
    let faulty_followers = (1..=cfg.num_nodes as NodeId)
        .into_iter()
        .filter(|id| *id != leader_id)
        .take(maximum_tolerable_follower_failures - 1);
    for node_id in faulty_followers {
        sys.kill_node(node_id);
    }
    sys.kill_node(leader_id);

    // Wait for next leader to get elected
    thread::sleep(4 * cfg.election_timeout);

    // Make some propsals
    let still_alive_node_id = sys.nodes.keys().next().unwrap();
    let still_alive_node = sys.nodes.get(still_alive_node_id).unwrap();
    sys.make_proposals(*still_alive_node_id, proposals, cfg.wait_timeout);

    // Verify log
    let nodes_log = still_alive_node.on_definition(|comp| {
        comp.paxos
            .read_decided_suffix(0)
            .expect("Cannot read decided log entry")
    });
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

    let proposals: Vec<Value> = (0..cfg.num_proposals / 2)
        .into_iter()
        .map(|v| Value(v))
        .collect();
    let expected_log = proposals.clone();

    // Kill maximum number of followers such that leader can still function
    let leader_id = sys.get_elected_leader(1, cfg.wait_timeout);
    let maximum_tolerable_follower_failures = (1..=cfg.num_nodes as NodeId)
        .into_iter()
        .filter(|id| *id != leader_id)
        .take(cfg.num_nodes - cfg.flexible_quorum.unwrap().1);
    for node_id in maximum_tolerable_follower_failures {
        sys.kill_node(node_id);
    }

    // Make some propsals
    sys.make_proposals(leader_id, proposals, cfg.wait_timeout);

    // Verify log
    let leader = sys.nodes.get(&leader_id).unwrap();
    let leaders_log = leader.on_definition(|comp| {
        comp.paxos
            .read_decided_suffix(0)
            .expect("Cannot read decided log entry")
    });
    verify_log(leaders_log, expected_log);
}
