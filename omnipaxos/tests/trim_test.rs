pub mod utils;

use kompact::prelude::{promise, Ask, FutureCollection};
use omnipaxos::{ballot_leader_election::Ballot, util::NodeId};
use serial_test::serial;
use std::thread;
use utils::{TestConfig, TestSystem, Value};

const TRIM_INDEX_INCREMENT: u64 = 10;

/// Test trimming the log.
/// At the end the log is retrieved from each replica and verified
/// if the first [`gc_index`] are removed.
#[test]
#[serial]
fn trim_test() {
    let cfg = TestConfig::load("trim_test").expect("Test config loaded");
    assert_ne!(cfg.trim_idx, 0, "trim_idx must be greater than 0");
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

    thread::sleep(cfg.wait_timeout); // wait a little longer so that ALL nodes decide

    elected_leader.on_definition(|x| {
        x.paxos.trim(Some(cfg.trim_idx)).expect("Failed to trim");
    });

    let mut seqs_after = vec![];
    for (i, px) in sys.nodes {
        seqs_after.push(px.on_definition(|x| {
            let seq = x.get_trimmed_suffix();
            (i, seq.to_vec())
        }));
    }

    check_trim(&vec_proposals, &seqs_after, cfg.trim_idx, elected_pid);

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
        cfg.num_proposals >= cfg.trim_idx + TRIM_INDEX_INCREMENT,
        "Not enough proposals to test double trim"
    );
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

    let mut seq_after_double = vec![];
    for (i, px) in sys.nodes {
        seq_after_double.push(px.on_definition(|x| {
            let seq = x.get_trimmed_suffix();
            (i, seq.to_vec())
        }));
    }

    check_trim(
        &vec_proposals,
        &seq_after_double,
        cfg.trim_idx + TRIM_INDEX_INCREMENT,
        elected_pid,
    );

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem found in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}

fn check_trim(
    vec_proposals: &Vec<Value>,
    seq_after: &Vec<(u64, Vec<Value>)>,
    trim_idx: u64,
    leader: NodeId,
) {
    for (pid, after) in seq_after {
        if *pid == leader {
            // leader must have successfully trimmed
            assert_eq!(vec_proposals.len(), (after.len() + trim_idx as usize));
            assert_eq!(vec_proposals.get(trim_idx as usize), after.get(0));
        } else if after.len() + trim_idx as usize == vec_proposals.len() {
            // successful trim
            assert_eq!(vec_proposals.get(trim_idx as usize), after.get(0));
        } else {
            // must be prefix
            for (entry_idx, entry) in after.iter().enumerate() {
                assert_eq!(Some(entry), vec_proposals.get(entry_idx));
            }
        }
    }
}
