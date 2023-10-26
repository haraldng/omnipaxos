pub mod utils;

use crate::utils::omnireplica::OmniPaxosComponent;
use kompact::prelude::{promise, Ask, Component, FutureCollection};
use omnipaxos::util::LogEntry;
use serial_test::serial;
use std::{sync::Arc, thread};
use utils::{TestConfig, TestSystem, Value};

const TRIM_INDEX_INCREMENT: usize = 10;

/// Test trimming the log.
/// At the end the log is retrieved from each replica and verified
/// if the first [`gc_index`] are removed.
#[test]
#[serial]
fn trim_test() {
    let cfg = TestConfig::load("trim_test").expect("Test config loaded");
    assert_ne!(cfg.trim_idx, 0, "trim_idx must be greater than 0");
    let mut sys = TestSystem::with(cfg);
    sys.start_all_nodes();
    let elected_pid = sys.get_elected_leader(1, cfg.wait_timeout);
    let elected_leader = sys.nodes.get(&elected_pid).unwrap();

    thread::sleep(cfg.wait_timeout); // wait a little longer so that ALL nodes get prepared with empty logs

    let vec_proposals = utils::create_proposals(1, cfg.num_proposals);
    let mut futures = vec![];
    let last = vec_proposals.last().unwrap();
    for node in sys.nodes.values() {
        let (kprom, kfuture) = promise::<()>();
        node.on_definition(|x| {
            x.insert_decided_future(Ask::new(kprom, last.clone()));
        });
        futures.push(kfuture);
    }
    for v in &vec_proposals {
        elected_leader.on_definition(|x| {
            x.paxos.append(v.clone()).expect("Failed to append");
        });
    }

    // wait until all nodes have decided last entry
    match FutureCollection::collect_with_timeout::<Vec<_>>(futures, cfg.wait_timeout) {
        Ok(_) => {}
        Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
    }

    elected_leader.on_definition(|x| {
        x.paxos.trim(Some(cfg.trim_idx)).expect("Failed to trim");
    });

    thread::sleep(cfg.wait_timeout); // wait a little longer so that ALL nodes get trim

    for (_pid, node) in sys.nodes {
        check_trim(&vec_proposals, cfg.trim_idx, node);
    }

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
fn double_trim_test() {
    let cfg = TestConfig::load("trim_test").expect("Test config loaded");
    assert_ne!(cfg.trim_idx, 0, "trim_idx must be greater than 0");
    assert!(
        cfg.num_proposals as usize >= cfg.trim_idx + TRIM_INDEX_INCREMENT,
        "Not enough proposals to test double trim"
    );
    let mut sys = TestSystem::with(cfg);
    sys.start_all_nodes();
    let elected_pid = sys.get_elected_leader(1, cfg.wait_timeout);
    let elected_leader = sys.nodes.get(&elected_pid).unwrap();

    thread::sleep(cfg.wait_timeout); // wait a little longer so that ALL nodes get prepared with empty logs

    let vec_proposals = utils::create_proposals(1, cfg.num_proposals);
    let mut futures = vec![];
    let last = vec_proposals.last().unwrap();
    for node in sys.nodes.values() {
        let (kprom, kfuture) = promise::<()>();
        node.on_definition(|x| {
            x.insert_decided_future(Ask::new(kprom, last.clone()));
        });
        futures.push(kfuture);
    }
    for v in &vec_proposals {
        elected_leader.on_definition(|x| {
            x.paxos.append(v.clone()).expect("Failed to append");
        });
    }

    // wait until all nodes have decided last entry
    match FutureCollection::collect_with_timeout::<Vec<_>>(futures, cfg.wait_timeout) {
        Ok(_) => {}
        Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
    }

    let second_trim_idx = cfg.trim_idx + TRIM_INDEX_INCREMENT;
    elected_leader.on_definition(|x| {
        x.paxos
            .trim(Some(cfg.trim_idx))
            .expect(format!("Failed to trim {}", cfg.trim_idx).as_str());
        x.paxos
            .trim(Some(second_trim_idx))
            .expect(format!("Failed to trim {}", second_trim_idx).as_str());
    });

    thread::sleep(cfg.wait_timeout); // wait a little longer so that ALL nodes trim

    for (_pid, node) in sys.nodes {
        check_trim(&vec_proposals, cfg.trim_idx + TRIM_INDEX_INCREMENT, node);
    }

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem found in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}

fn check_trim(vec_proposals: &Vec<Value>, trim_idx: usize, node: Arc<Component<OmniPaxosComponent>>) {
    let num_proposals = vec_proposals.len();
    node.on_definition(|x| {
        let op = &x.paxos;
        for trimmed_idx in 0..trim_idx {
            match op.read(trimmed_idx).unwrap() {
                LogEntry::Trimmed(idx) if idx == trim_idx => {}
                e => panic!(
                    "Entry {} must be Trimmed({}), but was {:?}",
                    trimmed_idx, trim_idx, e
                ),
            }
        }
        for idx in trim_idx..num_proposals {
            let expected_value = vec_proposals.get(idx).unwrap();
            match op.read(idx).unwrap() {
                LogEntry::Decided(v) if &v == expected_value => {}
                e => panic!(
                    "Entry must be decided with {:?} at idx {}, but was {:?}",
                    expected_value, idx, e
                ),
            }
        }
        let decided_sfx = op.read_decided_suffix(0).unwrap();
        assert_eq!(decided_sfx.len(), num_proposals - trim_idx + 1); // +1 as all trimmed entries are represented by LogEntry::Trimmed
    });
}
