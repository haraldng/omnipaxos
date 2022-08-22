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

    let mut vec_proposals = check_initial_proposals(&sys, &cfg);
    let (leader, follower) = check_leader_is_elected(&sys, &cfg);

    kill_and_recover_node(&mut sys, &cfg, leader, RECOVERY_PATH);

    let mut futures: Vec<KFuture<Value>> = vec![];
    let (_, recovery_px) = sys.ble_paxos_nodes().get(&leader).unwrap();
    let (_, follower_px) = sys.ble_paxos_nodes().get(&follower).unwrap();

    for i in (cfg.num_proposals / 2) + 1..=cfg.num_proposals {
        let (kprom, kfuture) = promise::<Value>();
        vec_proposals.push(Value(i));
        recovery_px.on_definition(|x| {
            x.add_ask(Ask::new(kprom, ()));
        });
        follower_px.on_definition(|x| {
            x.paxos.append(Value(i)).expect("Failed to append");
        });

        futures.push(kfuture);
    }

    match FutureCollection::collect_with_timeout::<Vec<_>>(futures, cfg.wait_timeout) {
        Ok(_) => {}
        Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
    }

    thread::sleep(Duration::from_secs(1));

    let read_log: Vec<LogEntry<Value, LatestValue>> = recovery_px.on_definition(|comp| {
        comp.paxos
            .read_decided_suffix(0)
            .expect("Cannot read decided log entry")
    });

    verify_log_after_recovery(read_log, vec_proposals, cfg.num_proposals);

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

    let mut vec_proposals = check_initial_proposals(&sys, &cfg);
    let (leader, _) = check_leader_is_elected(&sys, &cfg);

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

    match FutureCollection::collect_with_timeout::<Vec<_>>(futures, cfg.wait_timeout) {
        Ok(_) => {}
        Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
    }

    thread::sleep(Duration::from_secs(1));

    let read_log: Vec<LogEntry<Value, LatestValue>> = recovery_px.on_definition(|comp| {
        comp.paxos
            .read_decided_suffix(0)
            .expect("Cannot read decided log entry")
    });

    verify_log_after_recovery(read_log, vec_proposals, cfg.num_proposals);

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

    let mut sys = TestSystem::with(
        cfg.num_nodes,
        cfg.ble_hb_delay,
        cfg.num_threads,
        cfg.storage_type,
        RECOVERY_PATH,
    );

    let mut vec_proposals = check_initial_proposals(&sys, &cfg);
    let (leader, follower) = check_leader_is_elected(&sys, &cfg);

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

    match FutureCollection::collect_with_timeout::<Vec<_>>(futures, cfg.wait_timeout) {
        Ok(_) => {}
        Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
    }

    thread::sleep(Duration::from_secs(1));

    let read_log: Vec<LogEntry<Value, LatestValue>> = recovery_px.on_definition(|comp| {
        comp.paxos
            .read_decided_suffix(0)
            .expect("Cannot read decided log entry")
    });

    verify_log_after_recovery(read_log, vec_proposals, cfg.num_proposals);

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

    let mut sys = TestSystem::with(
        cfg.num_nodes,
        cfg.ble_hb_delay,
        cfg.num_threads,
        cfg.storage_type,
        RECOVERY_PATH,
    );

    let mut vec_proposals = check_initial_proposals(&sys, &cfg);
    let (_, follower) = check_leader_is_elected(&sys, &cfg);

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

    match FutureCollection::collect_with_timeout::<Vec<_>>(futures, cfg.wait_timeout) {
        Ok(_) => {}
        Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
    }

    thread::sleep(Duration::from_secs(1));

    let read_log: Vec<LogEntry<Value, LatestValue>> = recovery_px.on_definition(|comp| {
        comp.paxos
            .read_decided_suffix(0)
            .expect("Cannot read decided log entry")
    });

    verify_log_after_recovery(read_log, vec_proposals, cfg.num_proposals);

    println!("Pass follower_fail_follower_propose");

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}

/// Verify that the log is correct after a fail recovery, Depending on
/// the timing the log should match one of the following cases:
/// * All entries are decided, verify the decided entries
/// * Only a snapshot was taken, verify the snapshot
/// * A snapshot was taken and entries decided on afterwards, verify both the snapshot and entries
fn verify_log_after_recovery(
    read_log: Vec<LogEntry<Value, LatestValue>>,
    vec_proposals: Vec<Value>,
    num_proposals: u64,
) {
    match read_log[..] {
        [LogEntry::Decided(_), ..] => verify_entries(&read_log, &vec_proposals, 0, num_proposals),
        [LogEntry::Snapshotted(_)] => {
            let snapshot = LatestValue::create(vec_proposals.as_slice());
            verify_snapshot(&read_log, num_proposals, &snapshot)
        }
        [LogEntry::Snapshotted(_), LogEntry::Decided(_), ..] => {
            let (first_proposals, last_proposals) =
                vec_proposals.split_at((num_proposals / 2) as usize);
            let (first_entry, decided_entries) = read_log.split_at(1); // separate the snapshot from the decided entries
            let exp_snapshot = LatestValue::create(first_proposals);
            verify_snapshot(first_entry, num_proposals / 2, &exp_snapshot);
            verify_entries(decided_entries, last_proposals, 0, num_proposals);
        }
        _ => panic!("Unexpected entries in the log: {:?} ", read_log),
    }
}

/// Verify that the log has a single snapshot of the latest entry
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

/// Verify that all log entries are decided and matches the proposed entries
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

/// Check that the leader is elected before any node fails
fn check_leader_is_elected(sys: &TestSystem, cfg: &TestConfig) -> (u64, u64) {
    let (ble, _) = sys
        .ble_paxos_nodes()
        .get(&1)
        .expect("No BLE component found");

    let leader = ble.on_definition(|x| {
        if let Some(ballot) = x.ble.get_leader() {
            // leader is already elected
            return ballot.pid;
        } else {
            // leader is not yet elected
            let (kprom, kfuture) = promise::<Ballot>();
            x.add_ask(Ask::new(kprom, ()));

            let ballot = kfuture
                .wait_timeout(cfg.wait_timeout)
                .expect("No leader has been elected in the allocated time!");
            ballot.pid
        }
    });

    let follower = (1..=cfg.num_nodes as u64)
        .into_iter()
        .find(|x| *x != leader)
        .expect("No followers found!");
    println!("leader: {:?}, follower: {:?}", leader, follower);
    (leader, follower)
}

/// Propose and check that the first proposals before any node fails are decided
fn check_initial_proposals(sys: &TestSystem, cfg: &TestConfig) -> Vec<Value> {
    let (_, px) = sys
        .ble_paxos_nodes()
        .get(&1)
        .expect("No SequencePaxos component found");

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

    sys.start_all_nodes();

    match FutureCollection::collect_with_timeout::<Vec<_>>(proposal_futures, cfg.wait_timeout) {
        Ok(_) => {}
        Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
    }

    vec_proposals
}

/// Kill and recover a node after some time
pub fn kill_and_recover_node(sys: &mut TestSystem, cfg: &TestConfig, pid: u64, path: &str) {
    sys.kill_node(pid);

    sys.create_node(pid, cfg.num_nodes, cfg.ble_hb_delay, cfg.storage_type, path);
    sys.start_node(pid);
    let (_, px) = sys.ble_paxos_nodes().get(&pid).unwrap();
    px.on_definition(|x| {
        x.paxos.fail_recovery();
    });
}
