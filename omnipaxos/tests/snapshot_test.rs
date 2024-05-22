pub mod utils;

use crate::utils::{omnireplica::OmniPaxosComponent, ValueSnapshot};
use kompact::prelude::{promise, Ask, Component, FutureCollection};
use omnipaxos::{storage::Snapshot, util::LogEntry};
use serial_test::serial;
use std::{sync::Arc, thread};
use utils::{TestConfig, TestSystem, Value};

const SNAPSHOT_INDEX_INCREMENT: usize = 10;

/// Test trimming the log.
/// At the end the log is retrieved from each replica and verified
/// if the first [`gc_index`] are removed.
#[test]
#[serial]
fn snapshot_test() {
    let cfg = TestConfig::load("trim_test").expect("Test config loaded");
    let mut sys = TestSystem::with(cfg);
    sys.start_all_nodes();
    let elected_pid = sys.get_elected_leader(1, cfg.wait_timeout);
    let elected_leader = sys.nodes.get(&elected_pid).unwrap();

    thread::sleep(cfg.wait_timeout); // wait a little longer so that ALL nodes get prepared with empty logs

    let vec_proposals = utils::create_proposals(1, cfg.num_proposals);
    let mut futures = vec![];
    for v in &vec_proposals {
        let (kprom, kfuture) = promise::<()>();
        elected_leader.on_definition(|x| {
            x.insert_decided_future(Ask::new(kprom, v.clone()));
            x.paxos.append(v.clone()).expect("Failed to append");
        });
        futures.push(kfuture);
    }

    match FutureCollection::collect_with_timeout::<Vec<_>>(futures, cfg.wait_timeout) {
        Ok(_) => {}
        Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
    }

    elected_leader.on_definition(|x| {
        x.paxos
            .snapshot(Some(cfg.trim_idx), false)
            .expect("Failed to trim");
    });

    thread::sleep(cfg.wait_timeout);

    for (_pid, node) in sys.nodes {
        check_snapshot(&vec_proposals, cfg.trim_idx, node);
    }

    println!("Pass snapshot");

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem found in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}

/// Test trimming the log twice.
/// At the end the log is retrieved from each replica and verified
/// if the first [`gc_index`] + an increment are removed.
#[test]
#[serial]
fn double_snapshot_test() {
    let cfg = TestConfig::load("trim_test").expect("Test config loaded");
    let mut sys = TestSystem::with(cfg);
    sys.start_all_nodes();
    let elected_pid = sys.get_elected_leader(1, cfg.wait_timeout);
    let elected_leader = sys.nodes.get(&elected_pid).unwrap();

    thread::sleep(cfg.wait_timeout); // wait a little longer so that ALL nodes get prepared with empty logs

    let vec_proposals = utils::create_proposals(1, cfg.num_proposals);
    let mut futures = vec![];
    for v in &vec_proposals {
        let (kprom, kfuture) = promise::<()>();
        elected_leader.on_definition(|x| {
            x.insert_decided_future(Ask::new(kprom, v.clone()));
            x.paxos.append(v.clone()).expect("Failed to append");
        });
        futures.push(kfuture);
    }

    match FutureCollection::collect_with_timeout::<Vec<_>>(futures, cfg.wait_timeout) {
        Ok(_) => {}
        Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
    }

    elected_leader.on_definition(|x| {
        x.paxos
            .snapshot(Some(cfg.trim_idx), false)
            .expect("Failed to trim");
    });

    thread::sleep(cfg.wait_timeout);

    elected_leader.on_definition(|x| {
        x.paxos
            .snapshot(Some(cfg.trim_idx + SNAPSHOT_INDEX_INCREMENT), false)
            .expect("Failed to trim");
    });

    thread::sleep(cfg.wait_timeout);

    for (_pid, node) in sys.nodes {
        check_snapshot(
            &vec_proposals,
            cfg.trim_idx + SNAPSHOT_INDEX_INCREMENT,
            node,
        );
    }

    println!("Pass double snapshot");

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem found in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}

fn check_snapshot(
    vec_proposals: &[Value],
    snapshot_idx: usize,
    node: Arc<Component<OmniPaxosComponent>>,
) {
    let exp_snapshot = ValueSnapshot::create(&vec_proposals[0..snapshot_idx]);
    let num_proposals = vec_proposals.len();
    node.on_definition(|x| {
        let op = &x.paxos;
        for snapshotted_idx in 0..snapshot_idx {
            match op.read(snapshotted_idx).unwrap() {
                LogEntry::Snapshotted(s)
                    if s.snapshot == exp_snapshot && s.trimmed_idx == snapshot_idx => {}
                e => panic!(
                    "Unexpected entry at {}. Should be snapshot with trimmed index {} and latest value: {:?}, but got {:?}",
                    snapshotted_idx, snapshot_idx, exp_snapshot.latest_value, e
                ),
            }
        }
        for idx in snapshot_idx..num_proposals {
            let expected_value = vec_proposals.get(idx).unwrap();
            match op.read(idx).unwrap() {
                LogEntry::Decided(v) if &v == expected_value => {}
                e => panic!(
                    "Entry {} must be decided with {:?}, but was {:?}",
                    idx, expected_value, e
                ),
            }
        }
        let decided_sfx = op.read_decided_suffix(0).unwrap();
        assert_eq!(decided_sfx.len(), num_proposals - snapshot_idx + 1); // +1 as all snapshotted entries are represented by LogEntry::Snapshotted
    });
}
