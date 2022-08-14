pub mod utils;

use kompact::prelude::{promise, Ask, FutureCollection, KFuture};
use omnipaxos_core::{storage::Snapshot, util::LogEntry};
use serial_test::serial;
use std::{
    thread,
    time::{self, Duration},
};
use utils::{LatestValue, TestConfig, TestSystem, Value};

const RECOVERY_PATH: &str = "recovery_test/";

#[test]
#[serial]
fn leader_fail_follower_propose_test() {
    let cfg = TestConfig::load("recovery_test").expect("Test config loaded");

    // create testsystem
    let mut sys = TestSystem::with(
        cfg.num_nodes,
        cfg.ble_hb_delay,
        cfg.num_threads,
        cfg.storage_type,
        RECOVERY_PATH,
    );

    // check that the first proposals are decided
    let mut vec_proposals = check_first_proposals(&sys, cfg.num_proposals, cfg.wait_timeout);

    // kill and recover node
    kill_and_recreate_node(&mut sys, &cfg, 3, RECOVERY_PATH);

    let mut futures: Vec<KFuture<Value>> = vec![];
    let (_, recovered_px) = sys.ble_paxos_nodes().get(&3).unwrap();
    let (_, follower_px) = sys.ble_paxos_nodes().get(&1).unwrap();

    for i in (cfg.num_proposals / 2) + 1..=cfg.num_proposals {
        let (kprom, kfuture) = promise::<Value>();
        vec_proposals.push(Value(i));
        follower_px.on_definition(|x| {
            x.paxos.append(Value(i)).expect("Failed to append");
        });
        recovered_px.on_definition(|x| {
            x.add_ask(Ask::new(kprom, ()));
        });
        futures.push(kfuture);
    }

    thread::sleep(time::Duration::from_secs(cfg.ble_hb_delay));
    match FutureCollection::collect_with_timeout::<Vec<_>>(futures, cfg.wait_timeout) {
        Ok(_) => {}
        Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
    }

    let log: Vec<LogEntry<Value, LatestValue>> = recovered_px
        .on_definition(|comp| comp.paxos.read_decided_suffix(0).expect("Cannot read log"));
    let snapshot = LatestValue::create(vec_proposals.as_slice());
    verify_snapshot(&log, vec_proposals.len() as u64, &snapshot);

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

    // create testsystem
    let mut sys = TestSystem::with(
        cfg.num_nodes,
        cfg.ble_hb_delay,
        cfg.num_threads,
        cfg.storage_type,
        RECOVERY_PATH,
    );

    // check the first proposals go through
    let mut vec_proposals = check_first_proposals(&sys, cfg.num_proposals, cfg.wait_timeout);

    // kill and recovery
    kill_and_recreate_node(&mut sys, &cfg, 3, RECOVERY_PATH);

    let mut futures: Vec<KFuture<Value>> = vec![];
    let (_, recovered_px) = sys.ble_paxos_nodes().get(&3).unwrap();
    let (_, follower_px) = sys.ble_paxos_nodes().get(&1).unwrap();

    for i in (cfg.num_proposals / 2) + 1..=cfg.num_proposals {
        let (kprom, kfuture) = promise::<Value>();
        vec_proposals.push(Value(i));
        follower_px.on_definition(|x| {
            x.add_ask(Ask::new(kprom, ()));
            x.paxos.append(Value(i)).expect("Failed to append");
        });
        futures.push(kfuture);
    }

    thread::sleep(time::Duration::from_secs(cfg.ble_hb_delay));
    match FutureCollection::collect_with_timeout::<Vec<_>>(futures, cfg.wait_timeout) {
        Ok(_) => {}
        Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
    }

    let log: Vec<LogEntry<Value, LatestValue>> = recovered_px
        .on_definition(|comp| comp.paxos.read_decided_suffix(0).expect("Cannot read log"));
    let snapshot = LatestValue::create(vec_proposals.as_slice());

    verify_snapshot(&log, vec_proposals.len() as u64, &snapshot);

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}

#[test]
#[serial]
#[ignore = "reason"]

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
    let mut vec_proposals = check_first_proposals(&sys, cfg.num_proposals, cfg.wait_timeout);

    // kill and recovery
    kill_and_recreate_node(&mut sys, &cfg, 1, RECOVERY_PATH);

    //let (ble, _) = sys.ble_paxos_nodes().get(&3).unwrap();
    // let leader = ble.on_definition(|x| {
    //     x.ble.get_leader().expect("Found no leader!").pid
    // });

    //println!("{:?}",leader);
    let mut futures: Vec<KFuture<Value>> = vec![];
    let (_, recovered_px) = sys.ble_paxos_nodes().get(&1).unwrap();
    let (_, leader_px) = sys.ble_paxos_nodes().get(&3).unwrap();

    for i in (cfg.num_proposals / 2) + 1..=cfg.num_proposals {
        let (kprom, kfuture) = promise::<Value>();
        vec_proposals.push(Value(i));
        recovered_px.on_definition(|x| {
            x.add_ask(Ask::new(kprom, ()));
        });
        leader_px.on_definition(|x| {
            x.paxos.append(Value(i)).expect("Failed to append");
        });
        
        futures.push(kfuture);
    }

    thread::sleep(time::Duration::from_secs(cfg.ble_hb_delay));
    match FutureCollection::collect_with_timeout::<Vec<_>>(futures, cfg.wait_timeout) {
        Ok(_) => {}
        Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
    }

    let log: Vec<LogEntry<Value, LatestValue>> = recovered_px
        .on_definition(|comp| comp.paxos.read_decided_suffix(0).expect("Cannot read log"));
    let snapshot = LatestValue::create(vec_proposals.as_slice());

    verify_snapshot_and_entries(&log, vec_proposals.len() as u64, &snapshot);

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}

#[test]
#[serial]
#[ignore = "reason"]
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
    let mut vec_proposals = check_first_proposals(&sys, cfg.num_proposals, cfg.wait_timeout);

    // kill and recovery
    kill_and_recreate_node(&mut sys, &cfg, 1, RECOVERY_PATH);

    //let (ble, _) = sys.ble_paxos_nodes().get(&2).unwrap();
    // let leader = ble.on_definition(|x| {
    //     x.ble.get_leader()
    // });

    let mut futures: Vec<KFuture<Value>> = vec![];
    let (_, recovered_px) = sys.ble_paxos_nodes().get(&1).unwrap();

    for i in (cfg.num_proposals / 2) + 1..=cfg.num_proposals {
        let (kprom, kfuture) = promise::<Value>();
        vec_proposals.push(Value(i));
        recovered_px.on_definition(|x| {
            x.add_ask(Ask::new(kprom, ()));
            x.paxos.append(Value(i)).expect("Failed to append");
        });
        futures.push(kfuture);
    }

    thread::sleep(time::Duration::from_secs(cfg.ble_hb_delay));
    match FutureCollection::collect_with_timeout::<Vec<_>>(futures, cfg.wait_timeout) {
        Ok(_) => {}
        Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
    }

    let log: Vec<LogEntry<Value, LatestValue>> = recovered_px
        .on_definition(|comp| comp.paxos.read_decided_suffix(0).expect("Cannot read log"));
    let snapshot = LatestValue::create(vec_proposals.as_slice());

    verify_snapshot_and_entries(&log, vec_proposals.len() as u64, &snapshot);

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}

fn verify_snapshot(
    read_entries: &[LogEntry<Value, LatestValue>],
    exp_compacted_idx: u64,
    exp_snapshot: &LatestValue,
) {
    assert_eq!(
        read_entries.len(),
        1,
        "Expected only snapshot, got: {:?}",
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

fn verify_snapshot_and_entries(
    read_entries: &[LogEntry<Value, LatestValue>],
    exp_compacted_idx: u64,
    exp_snapshot: &LatestValue,
) {
    assert_eq!(
        read_entries.len(),
        11,
        "Expected both snapshot and entries, got: {:?}",
        read_entries
    );
    match read_entries.last().unwrap() {
        LogEntry::Snapshotted(s) => {
            assert_eq!(s.trimmed_idx, exp_compacted_idx);
            assert_eq!(&s.snapshot, exp_snapshot);
        }
        e => {
            panic!("{}", format!("Not a snapshot: {:?}", e))
        }
    }
}

// Check that the first 10 proposals are decided
fn check_first_proposals(
    sys: &TestSystem,
    num_proposals: u64,
    wait_timeout: Duration,
) -> Vec<Value> {
    let (_, px) = sys.ble_paxos_nodes().get(&1).unwrap();

    let mut vec_proposals = vec![];
    let mut futures = vec![];
    for i in 1..=num_proposals / 2 {
        let (kprom, kfuture) = promise::<Value>();
        vec_proposals.push(Value(i));
        px.on_definition(|x| {
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
pub fn kill_and_recreate_node(sys: &mut TestSystem, cfg: &TestConfig, pid: u64, path: &str) {
    sys.kill_node(pid);

    sys.create_node(pid, cfg.num_nodes, cfg.ble_hb_delay, cfg.storage_type, path);
    let (_, px) = sys.ble_paxos_nodes().get(&pid).unwrap();
    px.on_definition(|x| {
        x.paxos.fail_recovery();
    });
    sys.start_node(pid);
}
