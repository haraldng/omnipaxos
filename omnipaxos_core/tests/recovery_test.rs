pub mod utils;

use crate::utils::StorageTypeSelector;
use kompact::prelude::{promise, Ask, FutureCollection, KFuture};
use omnipaxos_core::{ballot_leader_election::Ballot, storage::Snapshot, util::LogEntry};
use serial_test::serial;
use std::{thread, time::Duration};
use utils::{LatestValue, TestConfig, TestSystem, Value};

const PERSISTENT_STORAGE: StorageTypeSelector = StorageTypeSelector::Persistent;
const SLEEP_TIMEOUT: Duration = Duration::from_secs(1);

#[test]
#[serial]
#[ignore]
fn leader_fail_follower_propose_test() {
    let cfg = TestConfig::load("recovery_test").expect("Test config loaded");

    let mut sys = TestSystem::with(
        cfg.num_nodes,
        cfg.election_timeout,
        cfg.num_threads,
        PERSISTENT_STORAGE,
    );

    sys.start_all_nodes();

    let proposals: Vec<Value> = (1..=cfg.num_proposals)
        .into_iter()
        .map(|v| Value(v))
        .collect();
    check_initial_proposals(&sys, &cfg);
    let leader = get_elected_leader(&sys, cfg.wait_timeout);
    let follower = (1..=cfg.num_nodes as u64)
        .into_iter()
        .find(|x| *x != leader)
        .expect("No followers found!");

    kill_and_recover_node(&mut sys, &cfg, leader);
    check_last_proposals(follower, leader, &sys, &cfg);

    thread::sleep(SLEEP_TIMEOUT);

    let recovery_px = sys
        .nodes
        .get(&leader)
        .expect("No SequencePaxos component found");
    let read_log: Vec<LogEntry<Value, LatestValue>> = recovery_px.on_definition(|comp| {
        comp.paxos
            .read_decided_suffix(0)
            .expect("Cannot read decided log entry")
    });

    verify_log_after_recovery(read_log, proposals, cfg.num_proposals);

    println!("Pass leader_fail_follower_propose!");

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}

#[test]
#[serial]
#[ignore]
fn leader_fail_leader_propose_test() {
    let cfg = TestConfig::load("recovery_test").expect("Test config loaded");

    let mut sys = TestSystem::with(
        cfg.num_nodes,
        cfg.election_timeout,
        cfg.num_threads,
        PERSISTENT_STORAGE,
    );

    sys.start_all_nodes();

    let proposals: Vec<Value> = (1..=cfg.num_proposals)
        .into_iter()
        .map(|v| Value(v))
        .collect();
    check_initial_proposals(&sys, &cfg);
    let leader = get_elected_leader(&sys, cfg.wait_timeout);

    kill_and_recover_node(&mut sys, &cfg, leader);
    check_last_proposals(leader, leader, &sys, &cfg);

    thread::sleep(SLEEP_TIMEOUT);

    let recovery_px = sys
        .nodes
        .get(&leader)
        .expect("No SequencePaxos component found");
    let read_log: Vec<LogEntry<Value, LatestValue>> = recovery_px.on_definition(|comp| {
        comp.paxos
            .read_decided_suffix(0)
            .expect("Cannot read decided log entry")
    });

    verify_log_after_recovery(read_log, proposals, cfg.num_proposals);

    println!("Pass leader_fail_leader_propose!");

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}

#[test]
#[serial]
#[ignore]
fn follower_fail_leader_propose_test() {
    let cfg = TestConfig::load("recovery_test").expect("Test config loaded");

    let mut sys = TestSystem::with(
        cfg.num_nodes,
        cfg.election_timeout,
        cfg.num_threads,
        PERSISTENT_STORAGE,
    );

    sys.start_all_nodes();

    let proposals: Vec<Value> = (1..=cfg.num_proposals)
        .into_iter()
        .map(|v| Value(v))
        .collect();
    check_initial_proposals(&sys, &cfg);
    let leader = get_elected_leader(&sys, cfg.wait_timeout);
    let follower = (1..=cfg.num_nodes as u64)
        .into_iter()
        .find(|x| *x != leader)
        .expect("No followers found!");

    kill_and_recover_node(&mut sys, &cfg, follower);
    check_last_proposals(leader, follower, &sys, &cfg);

    thread::sleep(SLEEP_TIMEOUT);

    let recovery_px = sys
        .nodes
        .get(&leader)
        .expect("No SequencePaxos component found");
    let read_log: Vec<LogEntry<Value, LatestValue>> = recovery_px.on_definition(|comp| {
        comp.paxos
            .read_decided_suffix(0)
            .expect("Cannot read decided log entry")
    });

    verify_log_after_recovery(read_log, proposals, cfg.num_proposals);

    println!("Pass follower_fail_leader_propose");

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}

#[test]
#[serial]
#[ignore]
fn follower_fail_follower_propose_test() {
    let cfg = TestConfig::load("recovery_test").expect("Test config loaded");

    let mut sys = TestSystem::with(
        cfg.num_nodes,
        cfg.election_timeout,
        cfg.num_threads,
        PERSISTENT_STORAGE,
    );

    sys.start_all_nodes();

    let proposals: Vec<Value> = (1..=cfg.num_proposals)
        .into_iter()
        .map(|v| Value(v))
        .collect();
    check_initial_proposals(&sys, &cfg);
    let leader = get_elected_leader(&sys, cfg.wait_timeout);
    let follower = (1..=cfg.num_nodes as u64)
        .into_iter()
        .find(|x| *x != leader)
        .expect("No followers found!");

    kill_and_recover_node(&mut sys, &cfg, follower);
    check_last_proposals(follower, follower, &sys, &cfg);

    thread::sleep(SLEEP_TIMEOUT);

    let recovery_px = sys
        .nodes
        .get(&leader)
        .expect("No SequencePaxos component found");
    let read_log: Vec<LogEntry<Value, LatestValue>> = recovery_px.on_definition(|comp| {
        comp.paxos
            .read_decided_suffix(0)
            .expect("Cannot read decided log entry")
    });

    verify_log_after_recovery(read_log, proposals, cfg.num_proposals);

    println!("Pass follower_fail_follower_propose");

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
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

/// Propose and check that the first proposals before any node fails are decided.
fn check_initial_proposals(sys: &TestSystem, cfg: &TestConfig) {
    let px = sys.nodes.get(&1).expect("No SequencePaxos component found");

    let mut proposal_futures = vec![];
    for i in 1..=(cfg.num_proposals / 2) {
        let (kprom, kfuture) = promise::<Value>();
        px.on_definition(|x| {
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

/// Propose and check that the last proposals are decided by the
/// recovered node. The recovered node can also be the proposer
fn check_last_proposals(proposer: u64, recover: u64, sys: &TestSystem, cfg: &TestConfig) {
    let proposer_px = sys
        .nodes
        .get(&proposer)
        .expect("No SequencePaxos component found");
    let recover_px = sys
        .nodes
        .get(&recover)
        .expect("No SequencePaxos component found");

    let futures: Vec<KFuture<Value>> = ((cfg.num_proposals / 2) + 1..=cfg.num_proposals)
        .map(|_| {
            let (kprom, kfuture) = promise::<Value>();
            recover_px.on_definition(|x| {
                x.decided_futures.push(Ask::new(kprom, ()));
            });
            kfuture
        })
        .collect();

    for i in (cfg.num_proposals / 2) + 1..=cfg.num_proposals {
        proposer_px.on_definition(|x| {
            x.paxos.append(Value(i)).expect("Failed to append");
        });
    }

    match FutureCollection::collect_with_timeout::<Vec<_>>(futures, cfg.wait_timeout) {
        Ok(_) => {}
        Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
    }
}

/// Kill and recover node given its 'pid' after some time.
pub fn kill_and_recover_node(sys: &mut TestSystem, cfg: &TestConfig, pid: u64) {
    sys.kill_node(pid);
    thread::sleep(SLEEP_TIMEOUT);

    let storage_path = sys.temp_dir_path.clone();
    sys.create_node(
        pid,
        cfg.num_nodes,
        cfg.election_timeout,
        PERSISTENT_STORAGE,
        &storage_path,
    );
    sys.start_node(pid);
    let px = sys
        .nodes
        .get(&pid)
        .expect("No SequencePaxos component found");
    px.on_definition(|x| {
        x.paxos.fail_recovery();
    });
}
