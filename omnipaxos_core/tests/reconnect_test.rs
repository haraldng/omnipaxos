pub mod utils;

use kompact::prelude::{promise, Ask, FutureCollection};
use omnipaxos_core::{ballot_leader_election::Ballot, storage::Snapshot, util::LogEntry, messages::{Message, sequence_paxos::PaxosMsg}};
use serial_test::serial;
use std::{thread, time::Duration};
use utils::{LatestValue, TestConfig, TestSystem, Value};

const SLEEP_TIMEOUT: Duration = Duration::from_secs(1);


/// Verifies that a leader sends out AcceptSync messages
/// with increasing sequence numbers.
#[test]
#[serial]
fn ascending_accept_sequence_test() {
    // Start Kompact system
    let cfg = TestConfig::load("reconnect_test").expect("Test config couldn't be loaded");
    let mut sys = TestSystem::with(
        cfg.num_nodes,
        cfg.election_timeout,
        cfg.num_threads,
        cfg.storage_type,
    );
    sys.start_all_nodes();

    // Propose some values so that a leader is elected
    make_proposals(&sys, &cfg, 1, 1, 5);
    let leader_id = get_elected_leader(&sys, cfg.wait_timeout);
    let leader = sys.nodes.get(&leader_id).unwrap();
    let follower_id = (1..=cfg.num_nodes as u64)
        .into_iter()
        .find(|x| *x != leader_id)
        .expect("No followers found!");
 
    // Get leader to propose more values and then collect cooresponding AcceptDecide messages
    let mut accept_seq_nums = vec![];
    for i in 5..10 {
        let outgoing_messages = leader.on_definition(|x| {
            x.paxos.append(Value(i)).expect("Failed to append");
            x.paxos.outgoing_messages()
        });

        let seq_nums = outgoing_messages
            .iter()
            .filter_map(|msg| match msg {
                Message::SequencePaxos(m) => Some(m),
                _ => None
            })
            .filter(|msg| msg.to == follower_id)
            .filter_map(|paxos_message| match &paxos_message.msg {
               PaxosMsg::AcceptDecide(m)  => Some(m.seq_num),
                _ => None
            });
        accept_seq_nums.extend(seq_nums);
    }
 
    // We skip seq# 0 and 1 due to AcceptSync and batched proposal 1..5
    let expected_seq_nums: Vec<u64> = (2..7).collect();
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

    // Propose some values so that a leader is elected
    make_proposals(&sys, &cfg, 2, 1, 5);
    let leader_id = get_elected_leader(&sys, cfg.wait_timeout);
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
    make_proposals(&sys, &cfg, leader_id, 5, 10);

    // Decide entries after omission period so follower finds seq break
    leader.on_definition(|comp| {
        comp.set_connection(follower_id, true);
    });
    make_proposals(&sys, &cfg, leader_id, 10, 15);

    // Wait for Re-sync with leader to finish
    thread::sleep(SLEEP_TIMEOUT);

    // Verify log
    let expected_log = (1..15)
        .into_iter()
        .map(|v| Value(v))
        .collect();
    let followers_log: Vec<LogEntry<Value, LatestValue>> = follower.on_definition(|comp| {
        comp.paxos
            .read_decided_suffix(0)
            .expect("Cannot read decided log entry")
    });
    verify_log_after_recovery(followers_log, expected_log, 14);
    
    // Shutdown system
    println!("Passed reconnect_to_leader_test!");
    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}


/// Return the elected leader. If there is no leader yet then
/// wait until a leader is elected in the allocated time.
fn get_elected_leader(sys: &TestSystem, wait_timeout: Duration) -> u64 {
    let first_node = sys.nodes.get(&1).expect("No BLE component found");

    first_node.on_definition(|x| {
        let leader_pid = x.paxos.get_current_leader();
        leader_pid.unwrap_or_else(|| {
            // Leader is not elected yet
            let (kprom, kfuture) = promise::<Ballot>();
            x.election_futures.push(Ask::new(kprom, ()));

            let ballot = kfuture
                .wait_timeout(wait_timeout)
                .expect("No leader has been elected in the allocated time!");
            ballot.pid
        })
    })
}

/// Use node `proposer` to propose values in the range `from..to`, then waits for the proposals
/// to be decided.
fn make_proposals(sys: &TestSystem, cfg: &TestConfig, proposer: u64, from: u64, to: u64) {
    let proposer = sys.nodes.get(&proposer).expect("No SequencePaxos component found");

    let mut proposal_futures = vec![];
    for i in from..to {
        let (kprom, kfuture) = promise::<Value>();
        proposer.on_definition(|x| {
            x.paxos.append(Value(i)).expect("Failed to append");
            x.decided_futures.push(Ask::new(kprom, ()));
        });
        proposal_futures.push(kfuture);
    }

    match FutureCollection::collect_with_timeout::<Vec<_>>(proposal_futures, cfg.wait_timeout) {
        Ok(_) => {}
        Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
    }
}

/// Verify that the log is correct after a fail recovery, Depending on
/// the timing the log should match one of the following cases.
/// * All entries are decided, verify the decided entries
/// * Only a snapshot was taken, verify the snapshot
/// * A snapshot was taken and entries decided on afterwards, verify both the snapshot and entries
fn verify_log_after_recovery(
    read_log: Vec<LogEntry<Value, LatestValue>>,
    proposals: Vec<Value>,
    num_proposals: u64,
) {
    match read_log[..] {
        [LogEntry::Decided(_), ..] => verify_entries(&read_log, &proposals, 0, num_proposals),
        [LogEntry::Snapshotted(_)] => {
            let exp_snapshot = LatestValue::create(proposals.as_slice());
            verify_snapshot(&read_log, num_proposals, &exp_snapshot);
        }
        [LogEntry::Snapshotted(_), LogEntry::Decided(_), ..] => {
            let (first_proposals, last_proposals) =
                proposals.split_at((num_proposals / 2) as usize);
            let (first_entry, decided_entries) = read_log.split_at(1); // separate the snapshot from the decided entries
            let exp_snapshot = LatestValue::create(first_proposals);
            verify_snapshot(first_entry, num_proposals / 2, &exp_snapshot);
            verify_entries(decided_entries, last_proposals, 0, num_proposals);
        }
        _ => panic!("Unexpected entries in the log: {:?} ", read_log),
    }
}

/// Verify that the log has a single snapshot of the latest entry.
fn verify_snapshot(
    read_entries: &[LogEntry<Value, LatestValue>],
    exp_compacted_idx: u64,
    exp_snapshot: &LatestValue,
) {
    assert_eq!(
        read_entries.len(),
        1,
        "Expected snapshot, got: {:?}",
        read_entries
    );
    match read_entries
        .first()
        .expect("Expected entry from first element")
    {
        LogEntry::Snapshotted(s) => {
            assert_eq!(s.trimmed_idx, exp_compacted_idx);
            assert_eq!(&s.snapshot, exp_snapshot);
        }
        e => {
            panic!("{}", format!("Not a snapshot: {:?}", e));
        }
    }
}

/// Verify that all log entries are decided and matches the proposed entries.
fn verify_entries(
    read_entries: &[LogEntry<Value, LatestValue>],
    exp_entries: &[Value],
    offset: u64,
    decided_idx: u64,
) {
    assert_eq!(
        read_entries.len(),
        exp_entries.len(),
        "read: {:?}, expected: {:?}",
        read_entries,
        exp_entries
    );
    for (idx, entry) in read_entries.iter().enumerate() {
        let log_idx = idx as u64 + offset;
        match entry {
            LogEntry::Decided(i) if log_idx <= decided_idx => assert_eq!(*i, exp_entries[idx]),
            LogEntry::Undecided(i) if log_idx > decided_idx => assert_eq!(*i, exp_entries[idx]),
            e => panic!(
                "{}",
                format!(
                    "Unexpected entry at idx {}: {:?}, decided_idx: {}",
                    idx, e, decided_idx
                )
            ),
        }
    }
}
