pub mod utils;

use kompact::prelude::{promise, Ask, FutureCollection, KFuture};
use omnipaxos_core::{storage::Snapshot, util::LogEntry};
use serial_test::serial;
use std::{
    thread,
    time::{self, Duration},
};
use utils::{LatestValue, TestConfig, TestSystem, Value};

const RECOVERY_TEST: &str = "recovery_test/";

/// Test that SequencePaxos can recover from failure and decided new entries
#[test]
#[serial]
fn leader_fail_follower_propose_test() {
    let cfg = TestConfig::load("consensus_test").expect("Test config loaded");

    // create testsystem
    let mut sys = TestSystem::with(
        cfg.num_nodes,
        cfg.ble_hb_delay,
        cfg.num_threads,
        cfg.storage_type,
        RECOVERY_TEST,
    );

    // check the first proposals go through
    let mut vec_proposals = check_first_proposals(&sys, cfg.num_proposals, cfg.wait_timeout);

    // kill and recovery
    kill_and_recreate_node(&mut sys, &cfg, 3);

    let mut new_futures: Vec<KFuture<Value>> = vec![];
    let (_, leader_px) = sys.ble_paxos_nodes().get(&3).unwrap();
    let (_, follower_px) = sys.ble_paxos_nodes().get(&1).unwrap();

    for i in (cfg.num_proposals / 2) + 1..=cfg.num_proposals {
        let (kprom, kfuture) = promise::<Value>();
        vec_proposals.push(Value(i));
        follower_px.on_definition(|x| {
            x.paxos.append(Value(i)).expect("Failed to append");
        });
        leader_px.on_definition(|x| {
            x.add_ask(Ask::new(kprom, ()));
        });
        new_futures.push(kfuture);
    }

    thread::sleep(time::Duration::from_secs(cfg.ble_hb_delay));
    match FutureCollection::collect_with_timeout::<Vec<_>>(new_futures, cfg.wait_timeout) {
        Ok(_) => {}
        Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
    }

    let log: Vec<LogEntry<Value, LatestValue>> =
        leader_px.on_definition(|comp| comp.paxos.read_decided_suffix(0).expect("Cannot read log"));
    let snapshot = LatestValue::create(vec_proposals.as_slice());
    verify_recovery_entries(&log, snapshot);

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}

fn leader_fail_leader_propose_test() {
    let cfg = TestConfig::load("consensus_test").expect("Test config loaded");

    // create testsystem
    let mut sys = TestSystem::with(
        cfg.num_nodes,
        cfg.ble_hb_delay,
        cfg.num_threads,
        cfg.storage_type,
        RECOVERY_TEST,
    );

    // check the first proposals go through
    let mut vec_proposals = check_first_proposals(&sys, cfg.num_proposals, cfg.wait_timeout);

    // kill and recovery
    kill_and_recreate_node(&mut sys, &cfg, 3);

    let mut new_futures: Vec<KFuture<Value>> = vec![];
    let (_, leader_px) = sys.ble_paxos_nodes().get(&3).unwrap();
    let (_, follower_px) = sys.ble_paxos_nodes().get(&1).unwrap();

    for i in (cfg.num_proposals / 2) + 1..=cfg.num_proposals {
        let (kprom, kfuture) = promise::<Value>();
        vec_proposals.push(Value(i));
        follower_px.on_definition(|x| {
            x.paxos.append(Value(i)).expect("Failed to append");
            x.add_ask(Ask::new(kprom, ()));
        });
        new_futures.push(kfuture);
    }

    thread::sleep(time::Duration::from_secs(cfg.ble_hb_delay));
    match FutureCollection::collect_with_timeout::<Vec<_>>(new_futures, cfg.wait_timeout) {
        Ok(_) => {}
        Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
    }

    let log: Vec<LogEntry<Value, LatestValue>> =
        leader_px.on_definition(|comp| comp.paxos.read_decided_suffix(0).expect("Cannot read log"));
    let snapshot = LatestValue::create(vec_proposals.as_slice());
    verify_recovery_entries(&log, snapshot);

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}

fn follower_fail_leader_propose_test_() {}

fn follower_fail_follower_propose_test_() {}

fn verify_recovery_entries(read_entries: &[LogEntry<Value, LatestValue>], exp_entry: LatestValue) {
    assert_eq!(read_entries.len(), 1, "Not a snapshot!");

    let snapshot = read_entries.first().expect("Error when taking logentry");
    let verify_val = match snapshot {
        LogEntry::Snapshotted(x) => x.snapshot,
        _ => panic!(),
    };

    assert_eq!(
        verify_val, exp_entry,
        "read: {:?}, expected: {:?}",
        verify_val, exp_entry
    );
}

// Check that the first 10 proposals are decided
fn check_first_proposals(
    sys: &TestSystem,
    num_proposals: u64,
    wait_timeout: Duration,
) -> Vec<Value> {
    let (_, follower_px) = sys.ble_paxos_nodes().get(&1).unwrap();

    let mut vec_proposals = vec![];
    let mut futures = vec![];
    for i in 1..=num_proposals / 2 {
        let (kprom, kfuture) = promise::<Value>();
        vec_proposals.push(Value(i));
        follower_px.on_definition(|x| {
            x.paxos.append(Value(i)).expect("Failed to append");
            x.add_ask(Ask::new(kprom, ()))
        });
        futures.push(kfuture);
    }

    sys.start_all_nodes();

    match FutureCollection::collect_with_timeout::<Vec<_>>(futures, wait_timeout) {
        Ok(_) => {}
        Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
    }

    vec_proposals
}

// kill and recovers node
pub fn kill_and_recreate_node(sys: &mut TestSystem, cfg: &TestConfig, pid: u64) {
    sys.kill_node(pid);

    sys.create_node(
        pid,
        cfg.num_nodes,
        cfg.ble_hb_delay,
        cfg.storage_type,
        RECOVERY_TEST,
    );
    let (_, px) = sys.ble_paxos_nodes().get(&pid).unwrap();
    px.on_definition(|x| {
        x.paxos.fail_recovery();
    });
    sys.start_node(pid);
}
