pub mod utils;

use crate::utils::ValueSnapshot;
use kompact::prelude::{promise, Ask, FutureCollection};
use omnipaxos::{
    ballot_leader_election::Ballot,
    storage::Snapshot,
    util::{LogEntry, NodeId},
};
use serial_test::serial;
use std::thread;
use utils::{TestConfig, TestSystem, Value};

const TRIM_INDEX_INCREMENT: u64 = 10;

/// Test trimming the log.
/// At the end the log is retrieved from each replica and verified
/// if the first [`gc_index`] are removed.
#[test]
#[serial]
fn snapshot_test() {
    let cfg = TestConfig::load("trim_test").expect("Test config loaded");
    let mut sys = TestSystem::with(cfg);
    let first_node = sys.nodes.get(&1).unwrap();
    let (kprom, kfuture) = promise::<Ballot>();
    first_node.on_definition(|x| x.election_futures.push(Ask::new(kprom, ())));
    sys.start_all_nodes();

    let elected_pid = kfuture
        .wait_timeout(cfg.wait_timeout)
        .expect("No elected leader in election")
        .pid;
    let elected_leader = sys.nodes.get(&elected_pid).unwrap();
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

    let mut seqs_after = vec![];
    for (i, px) in sys.nodes {
        seqs_after.push(px.on_definition(|comp| {
            let seq = comp.paxos.read_entries(0..).expect("No log in paxos");
            (i, seq)
        }));
    }

    check_snapshot(&vec_proposals, &seqs_after, cfg.trim_idx, elected_pid);

    println!("Pass trim");

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
    let first_node = sys.nodes.get(&1).unwrap();
    let (kprom, kfuture) = promise::<Ballot>();
    first_node.on_definition(|x| x.election_futures.push(Ask::new(kprom, ())));
    sys.start_all_nodes();

    let elected_pid = kfuture
        .wait_timeout(cfg.wait_timeout)
        .expect("No elected leader in election")
        .pid;
    let elected_leader = sys.nodes.get(&elected_pid).unwrap();
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
            .snapshot(Some(cfg.trim_idx + TRIM_INDEX_INCREMENT), false)
            .expect("Failed to trim");
    });

    thread::sleep(cfg.wait_timeout);

    let mut seq_after_double = vec![];
    for (i, px) in sys.nodes {
        seq_after_double.push(px.on_definition(|comp| {
            let seq = comp.paxos.read_entries(0..).expect("No log in paxos");
            (i, seq)
        }));
    }

    check_snapshot(
        &vec_proposals,
        &seq_after_double,
        cfg.trim_idx + TRIM_INDEX_INCREMENT,
        elected_pid,
    );

    println!("Pass double trim");

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem found in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}

fn check_snapshot(
    vec_proposals: &Vec<Value>,
    seq_after: &Vec<(u64, Vec<LogEntry<Value>>)>,
    trim_idx: u64,
    leader: NodeId,
) {
    let exp_snapshot = ValueSnapshot::create(&vec_proposals[0..trim_idx as usize]);
    for (pid, after) in seq_after {
        if *pid == leader {
            let snapshot = after.first().unwrap();
            match snapshot {
                LogEntry::Snapshotted(s) => {
                    assert_eq!(s.trimmed_idx, trim_idx);
                    assert_eq!(&s.snapshot, &exp_snapshot);
                }
                l => panic!(
                    "Leader's first entry is not snapshot. Node {}, {:?}",
                    pid, l
                ),
            }
            // leader must have successfully trimmed
            // -1 as snapshot is one entry
            assert_eq!(vec_proposals.len(), (after.len() - 1 + trim_idx as usize));
        } else if (after.len() - 1 + trim_idx as usize) == vec_proposals.len() {
            let snapshot = after.first().unwrap();
            match snapshot {
                LogEntry::Snapshotted(s) => {
                    assert_eq!(s.trimmed_idx, trim_idx);
                    assert_eq!(&s.snapshot, &exp_snapshot);
                }
                _ => panic!("First entry is not a snapshot"),
            }
        } else {
            // must be prefix
            for (entry_idx, entry) in after.iter().enumerate() {
                match entry {
                    LogEntry::Decided(d) => {
                        assert_eq!(d, &vec_proposals[entry_idx]);
                    }
                    l => panic!(
                        "Unexpected entry for node {}, idx: {}: {:?}",
                        pid, entry_idx, l
                    ),
                }
            }
        }
    }
}
