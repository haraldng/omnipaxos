pub mod utils;

use kompact::prelude::{promise, Ask, FutureCollection, KFuture};
use omnipaxos_core::{ballot_leader_election::Ballot, storage::Snapshot, util::LogEntry};
use serial_test::serial;
use std::{thread, time::Duration};
use utils::{LatestValue, TestConfig, TestSystem, Value};

const RECOVERY_PATH: &str = "/recovery_test/";

#[test]
#[serial]
fn leader_fail_follower_propose_test() {
    let cfg = TestConfig::load("recovery_test").expect("Test config loaded");

    let mut sys = TestSystem::with(
        cfg.num_nodes,
        cfg.ble_hb_delay,
        cfg.num_threads,
        cfg.storage_type,
        RECOVERY_PATH,
    );

    let (mut vec_proposals, leader_future) = check_first_proposals(&sys, &cfg);
    let (leader, follower) = check_leader_is_elected(&cfg, leader_future);

    kill_and_recover_node(&mut sys, &cfg, leader, RECOVERY_PATH);

    let mut futures: Vec<KFuture<Value>> = vec![];
    let (_, recovery_px) = sys.ble_paxos_nodes().get(&leader).unwrap();
    let (_, follower_px) = sys.ble_paxos_nodes().get(&follower).unwrap();

    for i in (cfg.num_proposals / 2) + 1..=cfg.num_proposals {
        let (kprom, kfuture) = promise::<Value>();
        vec_proposals.push(Value(i));
        follower_px.on_definition(|x| {
            x.paxos.append(Value(i)).expect("Failed to append");
        });
        recovery_px.on_definition(|x| {
            x.add_ask(Ask::new(kprom, ()));
        });
        futures.push(kfuture);
    }

    thread::sleep(cfg.wait_timeout);
    match FutureCollection::collect_with_timeout::<Vec<_>>(futures, cfg.wait_timeout) {
        Ok(_) => {}
        Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
    }

    let read_log: Vec<LogEntry<Value, LatestValue>> = recovery_px.on_definition(|comp| {
        comp.paxos
            .read_decided_suffix(0)
            .expect("Cannot read decided log entry")
    });

    // Match the read log with the following scenarios
    match read_log[..] {
        [LogEntry::Decided(_), ..] => {
            verify_entries(&read_log, &vec_proposals, 0, cfg.num_proposals)
        }
        [LogEntry::Snapshotted(_)] => {
            let snapshot = LatestValue::create(vec_proposals.as_slice());
            verify_snapshot(&read_log, cfg.num_proposals, &snapshot)
        }
        [LogEntry::Snapshotted(_), LogEntry::Decided(_), ..] => {
            let (first_proposals, last_proposals) = vec_proposals.split_at(vec_proposals.len() / 2);
            let snapshot = LatestValue::create(first_proposals);
            verify_snapshot_and_entries(
                &read_log,
                cfg.num_proposals / 2,
                &snapshot,
                &last_proposals,
                0,
                cfg.num_proposals,
            );
        }
        _ => panic!("Unexpected entries in log: {:?} ", read_log),
    }

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
fn leader_fail_leader_propose_test() {
    let cfg = TestConfig::load("recovery_test").expect("Test config loaded");

    let mut sys = TestSystem::with(
        cfg.num_nodes,
        cfg.ble_hb_delay,
        cfg.num_threads,
        cfg.storage_type,
        RECOVERY_PATH,
    );

    let (mut vec_proposals, leader_future) = check_first_proposals(&sys, &cfg);
    let (leader, _) = check_leader_is_elected(&cfg, leader_future);

    kill_and_recover_node(&mut sys, &cfg, leader, RECOVERY_PATH);

    let mut futures: Vec<KFuture<Value>> = vec![];
    let (_, recovery_px) = sys.ble_paxos_nodes().get(&leader).unwrap();

    for i in (cfg.num_proposals / 2) + 1..=cfg.num_proposals {
        let (kprom, kfuture) = promise::<Value>();
        vec_proposals.push(Value(i));
        recovery_px.on_definition(|x| {
            x.add_ask(Ask::new(kprom, ()));
            x.paxos.append(Value(i)).expect("Failed to append");
        });
        futures.push(kfuture);
    }

    thread::sleep(cfg.wait_timeout);
    match FutureCollection::collect_with_timeout::<Vec<_>>(futures, cfg.wait_timeout) {
        Ok(_) => {}
        Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
    }

    let log: Vec<LogEntry<Value, LatestValue>> = recovery_px.on_definition(|comp| {
        comp.paxos
            .read_decided_suffix(0)
            .expect("Cannot read decided log entry")
    });

    match log[..] {
        [LogEntry::Decided(_), ..] => verify_entries(&log, &vec_proposals, 0, cfg.num_proposals),
        [LogEntry::Snapshotted(_)] => {
            let snapshot = LatestValue::create(vec_proposals.as_slice());
            verify_snapshot(&log, cfg.num_proposals, &snapshot)
        }
        [LogEntry::Snapshotted(_), LogEntry::Decided(_), ..] => {
            let (first_proposals, last_proposals) = vec_proposals.split_at(vec_proposals.len() / 2);
            let snapshot = LatestValue::create(first_proposals);
            verify_snapshot_and_entries(
                &log,
                cfg.num_proposals / 2,
                &snapshot,
                &last_proposals,
                0,
                cfg.num_proposals,
            );
        }
        _ => panic!("Unexpected entries in log: {:?} ", log),
    }

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
fn follower_fail_leader_propose_test() {
    let cfg = TestConfig::load("recovery_test").expect("Test config loaded");

    // create testsystem
    let mut sys = TestSystem::with(
        cfg.num_nodes,
        cfg.ble_hb_delay,
        cfg.num_threads,
        cfg.storage_type,
        RECOVERY_PATH,
    );

    // check the first proposals go through
    let (mut vec_proposals, leader_future) = check_first_proposals(&sys, &cfg);
    let (leader, follower) = check_leader_is_elected(&cfg, leader_future);

    // kill and recovery
    kill_and_recover_node(&mut sys, &cfg, follower, RECOVERY_PATH);

    let mut futures: Vec<KFuture<Value>> = vec![];
    let (_, recovery_px) = sys.ble_paxos_nodes().get(&follower).unwrap();
    let (_, leader_px) = sys.ble_paxos_nodes().get(&leader).unwrap();

    for i in (cfg.num_proposals / 2) + 1..=cfg.num_proposals {
        let (kprom, kfuture) = promise::<Value>();
        vec_proposals.push(Value(i));
        recovery_px.on_definition(|x| {
            x.add_ask(Ask::new(kprom, ()));
        });
        leader_px.on_definition(|x| {
            x.paxos.append(Value(i)).expect("Failed to append");
        });

        futures.push(kfuture);
    }

    thread::sleep(cfg.wait_timeout);
    match FutureCollection::collect_with_timeout::<Vec<_>>(futures, cfg.wait_timeout) {
        Ok(_) => {}
        Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
    }

    let log: Vec<LogEntry<Value, LatestValue>> = recovery_px.on_definition(|comp| {
        comp.paxos
            .read_decided_suffix(0)
            .expect("Cannot read decided log entry")
    });

    match log[..] {
        [LogEntry::Decided(_), ..] => verify_entries(&log, &vec_proposals, 0, cfg.num_proposals),
        [LogEntry::Snapshotted(_)] => {
            let snapshot = LatestValue::create(vec_proposals.as_slice());
            verify_snapshot(&log, cfg.num_proposals, &snapshot)
        }
        [LogEntry::Snapshotted(_), LogEntry::Decided(_), ..] => {
            let (first_proposals, last_proposals) = vec_proposals.split_at(vec_proposals.len() / 2);
            let snapshot = LatestValue::create(first_proposals);
            verify_snapshot_and_entries(
                &log,
                cfg.num_proposals / 2,
                &snapshot,
                &last_proposals,
                0,
                cfg.num_proposals,
            );
        }
        _ => panic!("Unexpected entries in log: {:?} ", log),
    }

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
fn follower_fail_follower_propose_test() {
    let cfg = TestConfig::load("recovery_test").expect("Test config loaded");

    // create testsystem
    let mut sys = TestSystem::with(
        cfg.num_nodes,
        cfg.ble_hb_delay,
        cfg.num_threads,
        cfg.storage_type,
        RECOVERY_PATH,
    );

    // check the first proposals go through
    let (mut vec_proposals, leader_future) = check_first_proposals(&sys, &cfg);
    let (_, follower) = check_leader_is_elected(&cfg, leader_future);

    // kill and recovery
    kill_and_recover_node(&mut sys, &cfg, follower, RECOVERY_PATH);

    let mut futures: Vec<KFuture<Value>> = vec![];
    let (_, recovery_px) = sys.ble_paxos_nodes().get(&follower).unwrap();

    for i in (cfg.num_proposals / 2) + 1..=cfg.num_proposals {
        let (kprom, kfuture) = promise::<Value>();
        vec_proposals.push(Value(i));
        recovery_px.on_definition(|x| {
            x.add_ask(Ask::new(kprom, ()));
            x.paxos.append(Value(i)).expect("Failed to append");
        });
        futures.push(kfuture);
    }

    thread::sleep(cfg.wait_timeout);
    match FutureCollection::collect_with_timeout::<Vec<_>>(futures, cfg.wait_timeout) {
        Ok(_) => {}
        Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
    }

    let log: Vec<LogEntry<Value, LatestValue>> = recovery_px.on_definition(|comp| {
        comp.paxos
            .read_decided_suffix(0)
            .expect("Cannot read decided log entry")
    });

    match log[..] {
        [LogEntry::Decided(_), ..] => verify_entries(&log, &vec_proposals, 0, cfg.num_proposals),
        [LogEntry::Snapshotted(_)] => {
            let snapshot = LatestValue::create(vec_proposals.as_slice());
            verify_snapshot(&log, cfg.num_proposals, &snapshot)
        }
        [LogEntry::Snapshotted(_), LogEntry::Decided(_), ..] => {
            let (first_proposals, last_proposals) = vec_proposals.split_at(vec_proposals.len() / 2);
            let snapshot = LatestValue::create(first_proposals);
            verify_snapshot_and_entries(
                &log,
                cfg.num_proposals / 2,
                &snapshot,
                &last_proposals,
                0,
                cfg.num_proposals,
            );
        }
        _ => panic!("Unexpected entries in log: {:?} ", log),
    }

    println!("Pass follower_fail_follower_propose");

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}

// Verify that the log has a single snapshot of the latest entry
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
    match read_entries.first().unwrap() {
        LogEntry::Snapshotted(s) => {
            assert_eq!(s.trimmed_idx, exp_compacted_idx);
            assert_eq!(&s.snapshot, exp_snapshot);
        }
        e => {
            panic!("{}", format!("Not a snapshot: {:?}", e))
        }
    }
}

// Verify that the log contains a snapshot of the first entries and the last decided entries
fn verify_snapshot_and_entries(
    read_entries: &[LogEntry<Value, LatestValue>],
    exp_compacted_idx: u64,
    exp_snapshot: &LatestValue,
    exp_entries: &[Value],
    offset: u64,
    decided_idx: u64,
) {
    let (first_entry, decided_entries) = read_entries.split_first().expect("No entries in log!");

    match first_entry {
        LogEntry::Snapshotted(s) => {
            assert_eq!(s.trimmed_idx, exp_compacted_idx);
            assert_eq!(&s.snapshot, exp_snapshot);
        }
        e => {
            panic!("{}", format!("Not a snapshot: {:?}", e))
        }
    }

    verify_entries(decided_entries, exp_entries, offset, decided_idx)
}

// Verify that all log entries are decided and matches the proposed entries
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

// Check that the leader is elected before any node fails
fn check_leader_is_elected(cfg: &TestConfig, leader_future: KFuture<Ballot>) -> (u64, u64) {
    let leader = leader_future
        .wait_timeout(cfg.wait_timeout)
        .expect("No leader has been elected in the allocated time!")
        .pid;
    let follower = (1..=cfg.num_nodes as u64)
        .into_iter()
        .find(|x| *x != leader)
        .expect("No followers found!");
    println!("leader: {:?}, follower: {:?}", leader, follower);
    (leader, follower)
}

// Propose and check that the first proposals before any node fails are decided
// returns expected entries and a promise that the leader is elected
fn check_first_proposals(sys: &TestSystem, cfg: &TestConfig) -> (Vec<Value>, KFuture<Ballot>) {
    let (ble, px) = sys.ble_paxos_nodes().get(&1).unwrap();

    let mut vec_proposals = vec![];
    let mut proposal_futures = vec![];
    for i in 1..=cfg.num_proposals / 2 {
        let (kprom, kfuture) = promise::<Value>();
        vec_proposals.push(Value(i));
        px.on_definition(|x| {
            x.paxos.append(Value(i)).expect("Failed to append");
            x.add_ask(Ask::new(kprom, ()));
        });
        proposal_futures.push(kfuture);
    }

    let leader_future = ble.on_definition(|x| {
        let (kprom, kfuture) = promise::<Ballot>();
        x.add_ask(Ask::new(kprom, ()));
        kfuture
    });

    sys.start_all_nodes();

    match FutureCollection::collect_with_timeout::<Vec<_>>(proposal_futures, cfg.wait_timeout) {
        Ok(_) => {}
        Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
    }

    (vec_proposals, leader_future)
}

// Kill and recover a node after some time
pub fn kill_and_recover_node(sys: &mut TestSystem, cfg: &TestConfig, pid: u64, path: &str) {
    sys.kill_node(pid);
    thread::sleep(Duration::from_secs(cfg.ble_hb_delay));

    sys.create_node(pid, cfg.num_nodes, cfg.ble_hb_delay, cfg.storage_type, path);
    sys.start_node(pid);
    let (_, px) = sys.ble_paxos_nodes().get(&pid).unwrap();
    px.on_definition(|x| {
        x.paxos.fail_recovery();
    });
}
