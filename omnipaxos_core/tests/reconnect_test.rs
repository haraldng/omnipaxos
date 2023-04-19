pub mod utils;

use omnipaxos_core::{
    messages::{sequence_paxos::PaxosMsg, Message},
    util::LogEntry,
};
use serial_test::serial;
use std::{thread, time::Duration};
use utils::{util_functions::verify_log, LatestValue, TestConfig, TestSystem, Value};

const SLEEP_TIMEOUT: Duration = Duration::from_secs(1);

/// Verifies that a leader sends out AcceptSync messages
/// with increasing sequence numbers.
#[test]
#[serial]
fn increasing_accept_seq_num_test() {
    // Start Kompact system
    let cfg = TestConfig::load("reconnect_test").expect("Test config couldn't be loaded");
    let mut sys = TestSystem::with(
        cfg.num_nodes,
        cfg.election_timeout,
        cfg.num_threads,
        cfg.storage_type,
    );
    sys.start_all_nodes();

    let initial_proposals: Vec<Value> = (0..5).into_iter().map(|v| Value(v)).collect();
    let leaders_proposals: Vec<Value> = (5..10).into_iter().map(|v| Value(v)).collect();
    // We skip seq# 1 (AcceptSync), 2 (batched initial_proposals), and 3 (decide initial_proposals)
    let expected_seq_nums: Vec<u64> = (4..9).collect();

    // Propose some values so that a leader is elected
    sys.make_proposals(1, initial_proposals, cfg.wait_timeout);
    let leader_id = sys.get_elected_leader(1, cfg.wait_timeout);
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

/// Verifies that a follower detects a missed AcceptDecide message and re-syncs
/// with the leader.
#[test]
#[serial]
fn reconnect_to_leader_test() {
    // Start Kompact system
    let cfg = TestConfig::load("reconnect_test").expect("Test config couldn't be loaded");
    let mut sys = TestSystem::with(
        cfg.num_nodes,
        cfg.election_timeout,
        cfg.num_threads,
        cfg.storage_type,
    );
    sys.start_all_nodes();

    let initial_proposals = (1..5).into_iter().map(|v| Value(v)).collect();
    let unseen_by_follower_proposals = (5..10).into_iter().map(|v| Value(v)).collect();
    let seen_by_follower_proposals = (10..15).into_iter().map(|v| Value(v)).collect();
    let expected_log = (1..15).into_iter().map(|v| Value(v)).collect();

    // Propose some values so that a leader is elected
    sys.make_proposals(2, initial_proposals, cfg.wait_timeout);
    let leader_id = sys.get_elected_leader(1, cfg.wait_timeout);
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
    sys.make_proposals(leader_id, unseen_by_follower_proposals, cfg.wait_timeout);

    // Decide entries after omission period so follower finds seq break
    leader.on_definition(|comp| {
        comp.set_connection(follower_id, true);
    });
    sys.make_proposals(leader_id, seen_by_follower_proposals, cfg.wait_timeout);

    // Wait for Re-sync with leader to finish
    thread::sleep(SLEEP_TIMEOUT);

    // Verify log
    let followers_log: Vec<LogEntry<Value, LatestValue>> = follower.on_definition(|comp| {
        comp.paxos
            .read_decided_suffix(0)
            .expect("Cannot read decided log entry")
    });
    verify_log(followers_log, expected_log, 14);

    // Shutdown system
    println!("Passed reconnect_to_leader_test!");
    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}
