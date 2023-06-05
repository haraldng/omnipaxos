pub mod utils;

use omnipaxos::{
    messages::{sequence_paxos::PaxosMsg, Message},
    storage::StopSign,
    util::{LogEntry, SequenceNumber},
    ClusterConfig,
};
use serial_test::serial;
use std::{thread, time::Duration};
use utils::{
    verification::{verify_entries, verify_log, verify_stopsign},
    TestConfig, TestSystem, Value,
};

const SLEEP_TIMEOUT: Duration = Duration::from_secs(1);
const INITIAL_PROPOSALS: u64 = 5;
const DROPPED_PROPOSALS: u64 = 5;
const SECOND_PROPOSALS: u64 = 5;

/// Verifies that a leader sends out AcceptSync messages
/// with increasing sequence numbers.
#[test]
#[serial]
fn increasing_accept_seq_num_test() {
    // Start Kompact system
    let cfg = TestConfig::load("reconnect_test").expect("Test config couldn't be loaded");
    let mut sys = TestSystem::with(cfg);
    sys.start_all_nodes();

    let initial_proposals: Vec<Value> = (0..INITIAL_PROPOSALS)
        .into_iter()
        .map(|v| Value(v))
        .collect();
    let leaders_proposals: Vec<Value> = (INITIAL_PROPOSALS..INITIAL_PROPOSALS + SECOND_PROPOSALS)
        .into_iter()
        .map(|v| Value(v))
        .collect();
    // We skip seq# 1 (AcceptSync), 2 (batched initial_proposals), and 3 (decide initial_proposals)
    let expected_seq_nums: Vec<SequenceNumber> = (4..4 + SECOND_PROPOSALS)
        .map(|counter| SequenceNumber {
            session: 1,
            counter,
        })
        .collect();

    // Propose some values so that a leader is elected
    sys.make_proposals(
        1,
        initial_proposals,
        Duration::from_millis(cfg.wait_timeout_ms),
    );
    let leader_id = sys.get_elected_leader(1, Duration::from_millis(cfg.wait_timeout_ms));
    let leader = sys.nodes.get(&leader_id).unwrap();
    let follower_id = (1..=cfg.num_nodes as u64)
        .into_iter()
        .find(|x| *x != leader_id)
        .expect("No followers found!");

    // Get leader to propose more values and then collect cooresponding AcceptDecide messages
    let mut accept_seq_nums = vec![];
    for val in leaders_proposals {
        let outgoing_messages = leader.on_definition(|x| {
            x.paxos.append(val).expect("Failed to append");
            x.paxos.outgoing_messages()
        });

        let seq_nums = outgoing_messages
            .iter()
            .filter_map(|msg| match msg {
                Message::SequencePaxos(m) => Some(m),
                _ => None,
            })
            .filter(|msg| msg.to == follower_id)
            .filter_map(|paxos_message| match &paxos_message.msg {
                PaxosMsg::AcceptSync(m) => Some(m.seq_num),
                PaxosMsg::AcceptDecide(m) => Some(m.seq_num),
                PaxosMsg::Decide(m) => Some(m.seq_num),
                _ => None,
            });
        accept_seq_nums.extend(seq_nums);
    }

    assert_eq!(accept_seq_nums, expected_seq_nums);
    println!("Passed ascending_accept_sequence_test!");

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}

/// Verifies that a follower detects a missed AcceptDecide message from the leader and re-syncs
/// with the same leader.
#[test]
#[serial]
fn reconnect_after_dropped_accepts_test() {
    // Start Kompact system
    let cfg = TestConfig::load("reconnect_test").expect("Test config couldn't be loaded");
    let mut sys = TestSystem::with(cfg);
    sys.start_all_nodes();

    let initial_proposals = (0..INITIAL_PROPOSALS)
        .into_iter()
        .map(|v| Value(v))
        .collect();
    let unseen_by_follower_proposals = (INITIAL_PROPOSALS..INITIAL_PROPOSALS + DROPPED_PROPOSALS)
        .into_iter()
        .map(|v| Value(v))
        .collect();
    let seen_by_follower_proposals = (INITIAL_PROPOSALS + DROPPED_PROPOSALS
        ..INITIAL_PROPOSALS + DROPPED_PROPOSALS + SECOND_PROPOSALS)
        .into_iter()
        .map(|v| Value(v))
        .collect();
    let expected_log = (0..INITIAL_PROPOSALS + DROPPED_PROPOSALS + SECOND_PROPOSALS)
        .into_iter()
        .map(|v| Value(v))
        .collect();

    // Propose some values so that a leader is elected
    sys.make_proposals(
        2,
        initial_proposals,
        Duration::from_millis(cfg.wait_timeout_ms),
    );
    let leader_id = sys.get_elected_leader(1, Duration::from_millis(cfg.wait_timeout_ms));
    let leader = sys.nodes.get(&leader_id).unwrap();
    let follower_id = (1..=cfg.num_nodes as u64)
        .into_iter()
        .find(|x| *x != leader_id)
        .expect("No followers found!");
    let follower = sys.nodes.get(&follower_id).unwrap();

    // Decide entries during omission period
    leader.on_definition(|comp| {
        comp.set_connection(follower_id, false);
    });
    sys.make_proposals(
        leader_id,
        unseen_by_follower_proposals,
        Duration::from_millis(cfg.wait_timeout_ms),
    );

    // Decide entries after omission period so follower finds seq break
    leader.on_definition(|comp| {
        comp.set_connection(follower_id, true);
    });
    sys.make_proposals(
        leader_id,
        seen_by_follower_proposals,
        Duration::from_millis(cfg.wait_timeout_ms),
    );

    // Wait for Re-sync with leader to finish
    thread::sleep(SLEEP_TIMEOUT);

    // Verify log
    let followers_log: Vec<LogEntry<Value>> = follower.on_definition(|comp| {
        comp.paxos
            .read_decided_suffix(0)
            .expect("Cannot read decided log entry")
    });
    verify_log(followers_log, expected_log);

    // Shutdown system
    println!("Passed reconnect_to_leader_test!");
    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}

/// Verifies that a follower that misses a prepare message from a leader change
/// eventually receives a prepare from the new leader.
#[test]
#[serial]
fn reconnect_after_dropped_prepare_test() {
    // Start Kompact system
    let cfg = TestConfig::load("reconnect_test").expect("Test config couldn't be loaded");
    let mut sys = TestSystem::with(cfg);
    sys.start_all_nodes();

    let initial_proposals = (0..INITIAL_PROPOSALS)
        .into_iter()
        .map(|v| Value(v))
        .collect();
    let unseen_by_follower_proposals = (INITIAL_PROPOSALS..INITIAL_PROPOSALS + DROPPED_PROPOSALS)
        .into_iter()
        .map(|v| Value(v))
        .collect();
    let expected_log = (0..INITIAL_PROPOSALS + DROPPED_PROPOSALS)
        .into_iter()
        .map(|v| Value(v))
        .collect();

    // Propose some values so that a leader is elected
    sys.make_proposals(
        2,
        initial_proposals,
        Duration::from_millis(cfg.wait_timeout_ms),
    );
    let leader_id = sys.get_elected_leader(2, Duration::from_millis(cfg.wait_timeout_ms));
    let follower_id = (1..=cfg.num_nodes as u64)
        .into_iter()
        .find(|x| *x != leader_id)
        .expect("No followers found!");
    let follower = sys.nodes.get(&follower_id).unwrap();

    // Disconnect everyone from follower and choose a new leader
    for node in sys.nodes.values() {
        node.on_definition(|comp| {
            comp.set_connection(follower_id, false);
        });
    }
    sys.stop_node(leader_id);
    thread::sleep(SLEEP_TIMEOUT);
    sys.start_node(leader_id);
    let new_leader_id = sys.get_elected_leader(2, Duration::from_millis(cfg.wait_timeout_ms));
    assert_ne!(
        leader_id, new_leader_id,
        "reconnect_after_dropped_prepare_test failed to elect a different leader"
    );

    // Decide new entries while follower is still disconnected
    sys.make_proposals(
        new_leader_id,
        unseen_by_follower_proposals,
        Duration::from_millis(cfg.wait_timeout_ms),
    );

    // Reconnect everyone to follower
    for node in sys.nodes.values() {
        node.on_definition(|comp| {
            comp.set_connection(follower_id, true);
        });
    }
    thread::sleep(SLEEP_TIMEOUT);

    let followers_log: Vec<LogEntry<Value>> = follower.on_definition(|comp| {
        comp.paxos
            .read_decided_suffix(0)
            .expect("Cannot read decided log entry")
    });
    verify_log(followers_log, expected_log);

    // Shutdown system
    println!("Passed reconnect_to_leader_test!");
    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}

/// Verifies that a leader that misses a Promise message from a follower
/// eventually receives a Promise from the follower.
#[test]
#[serial]
fn reconnect_after_dropped_promise_test() {
    // Start Kompact system
    let cfg = TestConfig::load("reconnect_test").expect("Test config couldn't be loaded");
    let mut sys = TestSystem::with(cfg);
    sys.start_all_nodes();

    let initial_proposals = (0..INITIAL_PROPOSALS)
        .into_iter()
        .map(|v| Value(v))
        .collect();
    let unseen_by_follower_proposals = (INITIAL_PROPOSALS..INITIAL_PROPOSALS + DROPPED_PROPOSALS)
        .into_iter()
        .map(|v| Value(v))
        .collect();
    let expected_log = (0..INITIAL_PROPOSALS + DROPPED_PROPOSALS)
        .into_iter()
        .map(|v| Value(v))
        .collect();

    // Propose some values so that a leader is elected
    sys.make_proposals(
        2,
        initial_proposals,
        Duration::from_millis(cfg.wait_timeout_ms),
    );
    let leader_id = sys.get_elected_leader(2, Duration::from_millis(cfg.wait_timeout_ms));
    let follower_id = (1..=cfg.num_nodes as u64)
        .into_iter()
        .find(|x| *x != leader_id)
        .expect("No followers found!");
    let follower = sys.nodes.get(&follower_id).unwrap();

    // Drop outgoing messages from follower so that a Promise is lost when next leader is chosen
    follower.on_definition(|comp| {
        for &node_id in sys.nodes.keys() {
            if node_id != follower_id {
                comp.set_connection(node_id, false);
            }
        }
    });

    sys.stop_node(leader_id);
    thread::sleep(SLEEP_TIMEOUT);
    sys.start_node(leader_id);
    let new_leader_id = sys.get_elected_leader(2, Duration::from_millis(cfg.wait_timeout_ms));
    assert_ne!(
        leader_id, new_leader_id,
        "reconnect_after_dropped_promise_test failed to elect a different leader"
    );

    // Decide new entries while follower is still disconnected
    sys.make_proposals(
        new_leader_id,
        unseen_by_follower_proposals,
        Duration::from_millis(cfg.wait_timeout_ms),
    );

    // Reconnect follower and wait for re-sync with leader
    follower.on_definition(|comp| {
        for &node_id in sys.nodes.keys() {
            if node_id != follower_id {
                comp.set_connection(node_id, true);
            }
        }
    });
    thread::sleep(SLEEP_TIMEOUT);

    // Verify log
    let followers_log: Vec<LogEntry<Value>> = follower.on_definition(|comp| {
        comp.paxos
            .read_decided_suffix(0)
            .expect("Cannot read decided log entry")
    });
    verify_log(followers_log, expected_log);

    // Shutdown system
    println!("Passed reconnect_to_leader_test!");
    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}

/// Verifies that a leader that misses a PrepareReq message from a follower eventually
/// receives a PrepareReq.
#[test]
#[serial]
fn reconnect_after_dropped_preparereq_test() {
    // Start Kompact system
    let cfg = TestConfig::load("reconnect_test").expect("Test config couldn't be loaded");
    let mut sys = TestSystem::with(cfg);
    sys.start_all_nodes();

    let initial_proposals = (0..INITIAL_PROPOSALS)
        .into_iter()
        .map(|v| Value(v))
        .collect();
    let unseen_by_follower_proposals = (INITIAL_PROPOSALS..INITIAL_PROPOSALS + DROPPED_PROPOSALS)
        .into_iter()
        .map(|v| Value(v))
        .collect();
    let seen_by_follower_proposals = (INITIAL_PROPOSALS + DROPPED_PROPOSALS
        ..INITIAL_PROPOSALS + DROPPED_PROPOSALS + SECOND_PROPOSALS)
        .into_iter()
        .map(|v| Value(v))
        .collect();
    let expected_log = (0..INITIAL_PROPOSALS + DROPPED_PROPOSALS + SECOND_PROPOSALS)
        .into_iter()
        .map(|v| Value(v))
        .collect();

    // Propose some values so that a leader is elected
    sys.make_proposals(
        2,
        initial_proposals,
        Duration::from_millis(cfg.wait_timeout_ms),
    );
    let leader_id = sys.get_elected_leader(2, Duration::from_millis(cfg.wait_timeout_ms));
    let leader = sys.nodes.get(&leader_id).unwrap();
    let follower_id = (1..=cfg.num_nodes as u64)
        .into_iter()
        .find(|x| *x != leader_id)
        .expect("No followers found!");
    let follower = sys.nodes.get(&follower_id).unwrap();

    // Disconnect leader from follower and decide new entries
    leader.on_definition(|comp| {
        comp.set_connection(follower_id, false);
    });
    sys.make_proposals(
        leader_id,
        unseen_by_follower_proposals,
        Duration::from_millis(cfg.wait_timeout_ms),
    );

    // Decide entries after omission period so follower finds seq break but drop PrepareReq
    leader.on_definition(|comp| {
        comp.set_connection(follower_id, true);
    });
    follower.on_definition(|comp| {
        comp.set_connection(leader_id, false);
    });
    sys.make_proposals(
        leader_id,
        seen_by_follower_proposals,
        Duration::from_millis(cfg.wait_timeout_ms),
    );

    // Reconnect follower to leader
    follower.on_definition(|comp| {
        comp.set_connection(leader_id, true);
    });
    // Wait for Re-Sync with leader to finish
    thread::sleep(SLEEP_TIMEOUT);

    let followers_log: Vec<LogEntry<Value>> = follower.on_definition(|comp| {
        comp.paxos
            .read_decided_suffix(0)
            .expect("Cannot read decided log entry")
    });
    verify_log(followers_log, expected_log);

    // Shutdown system
    println!("Passed reconnect_to_leader_test!");
    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}

/// Verifies that a follower that misses an AcceptStopSign message from their leader
/// eventually receives the missed AcceptStopSign. The test ensures that the follower
/// never sees a DecideStopSign, and thus can't use it to detect the dropped AcceptStopSign.
#[test]
#[serial]
// TODO: Re-visit once SS changes are finalized
#[ignore]
fn reconnect_after_dropped_acceptstopsign_test_old() {
    // Start Kompact system
    let cfg = TestConfig::load("reconnect_test").expect("Test config couldn't be loaded");
    let mut sys = TestSystem::with(cfg);
    sys.start_all_nodes();

    let initial_proposals: Vec<Value> = (0..INITIAL_PROPOSALS)
        .into_iter()
        .map(|v| Value(v))
        .collect();
    let expected_entries = initial_proposals.clone();

    // Propose some values so that a leader is elected
    sys.make_proposals(
        2,
        initial_proposals,
        Duration::from_millis(cfg.wait_timeout_ms),
    );
    let leader_id = sys.get_elected_leader(2, Duration::from_millis(cfg.wait_timeout_ms));
    let leader = sys.nodes.get(&leader_id).unwrap();
    let follower_id = (1..=cfg.num_nodes as u64)
        .into_iter()
        .find(|x| *x != leader_id)
        .expect("No followers found!");
    let follower = sys.nodes.get(&follower_id).unwrap();

    // Disconnect leader from follower and start reconfigure
    let next_config = ClusterConfig {
        configuration_id: 2,
        nodes: vec![1, 2],
        flexible_quorum: None,
    };
    leader.on_definition(|comp| {
        comp.set_connection(follower_id, false);
        comp.paxos
            .reconfigure(next_config.clone(), None)
            .expect("Couldn't reconfigure!")
    });
    // Wait for AcceptStopSign to be sent and dropped
    thread::sleep(SLEEP_TIMEOUT);

    // Reconnect leader to follower
    leader.on_definition(|comp| {
        comp.set_connection(follower_id, true);
    });
    // Wait for leader to resend AcceptStopSign
    thread::sleep(SLEEP_TIMEOUT);

    // Verify log
    let followers_log: Vec<LogEntry<Value>> = follower.on_definition(|comp| {
        comp.paxos
            .read_decided_suffix(0)
            .expect("Cannot read decided log entry")
    });
    let (followers_entries, followers_stopsign) = followers_log.split_at(followers_log.len() - 1);
    verify_entries(
        followers_entries,
        &expected_entries,
        0,
        followers_entries.len() as u64,
    );
    verify_stopsign(followers_stopsign, &StopSign::with(next_config, None));

    // Shutdown system
    println!("Passed reconnect_to_leader_test!");
    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}

/// Verifies that a follower that misses an AcceptStopSign message from their leader
/// eventually receives the missed AcceptStopSign. The test ensures that the StopSign in never
/// decided so the follower never sees a DecideStopSign, and thus can't use it to detect the dropped
/// AcceptStopSign.
#[test]
#[serial]
// TODO: unignore once we can read accepted StopSigns
#[ignore]
fn reconnect_after_dropped_acceptstopsign_test() {
    // Start Kompact system
    let cfg = TestConfig::load("reconnect_test").expect("Test config couldn't be loaded");
    let mut sys = TestSystem::with(cfg);
    sys.start_all_nodes();

    let leader_id = sys.get_elected_leader(1, Duration::from_millis(cfg.wait_timeout_ms));
    let mut followers = (1..=cfg.num_nodes as u64)
        .into_iter()
        .filter(|x| *x != leader_id);
    let follower_id = followers.next().expect("Couldn't find follower");

    let write_quorum_size = match cfg.flexible_quorum {
        Some((_, write_quorum_size)) => write_quorum_size,
        None => cfg.num_nodes / 2 + 1,
    };
    assert!(
        write_quorum_size > 2,
        "Test doesn't work if 2 nodes alone can decide a stopsign"
    );

    // Disconnect follower from leader, kill others, and then propose StopSign
    for other_follower in followers {
        sys.kill_node(other_follower);
    }
    let next_config = ClusterConfig {
        configuration_id: 2,
        nodes: vec![1, 2],
        flexible_quorum: None,
    };
    let leader = sys.nodes.get(&leader_id).unwrap();
    leader.on_definition(|comp| {
        comp.set_connection(follower_id, false);
        comp.paxos
            .reconfigure(next_config.clone(), Some(vec![1, 2, 3]))
            .expect("Couldn't reconfigure!")
    });
    // Wait for AcceptStopSign to be sent and dropped
    thread::sleep(SLEEP_TIMEOUT);

    // Reconnect leader to follower
    leader.on_definition(|comp| {
        comp.set_connection(follower_id, true);
    });
    // Wait for leader to resend AcceptStopSign
    thread::sleep(SLEEP_TIMEOUT);

    // Verify log
    let follower = sys.nodes.get(&follower_id).unwrap();
    let followers_log: Vec<LogEntry<Value>> = follower.on_definition(|comp| {
        comp.paxos
            .read_entries(0..1)
            .expect("Cannot read log entry")
    });
    verify_stopsign(
        &followers_log,
        &StopSign::with(next_config, Some(vec![1, 2, 3])),
    );

    // Shutdown system
    println!("Passed reconnect_to_leader_test!");
    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}
