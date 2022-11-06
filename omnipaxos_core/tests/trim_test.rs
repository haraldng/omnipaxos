pub mod utils;

use kompact::prelude::{promise, Ask, FutureCollection};
use serial_test::serial;
use std::thread;
use utils::{TestConfig, TestSystem, Value};

const GC_INDEX_INCREMENT: u64 = 10;

/// Test Garbage Collection.
/// At the end the log is retrieved from each replica and verified
/// if the first [`gc_index`] are removed.
#[test]
#[serial]
fn trim_test() {
    let cfg = TestConfig::load("gc_test").expect("Test config loaded");

    let mut sys = TestSystem::with(
        cfg.num_nodes,
        cfg.ble_hb_delay,
        cfg.num_threads,
        cfg.storage_type,
    );

    let first_node = sys.nodes.get(&1).unwrap();

    let mut vec_proposals = vec![];
    let mut futures = vec![];
    for i in 1..=cfg.num_proposals {
        let (kprom, kfuture) = promise::<Value>();
        vec_proposals.push(Value(i));
        first_node.on_definition(|x| {
            x.paxos.append(Value(i)).expect("Failed to append");
            x.decided_futures.push(Ask::new(kprom, ()));
        });
        futures.push(kfuture);
    }

    sys.start_all_nodes();

    match FutureCollection::collect_with_timeout::<Vec<_>>(futures, cfg.wait_timeout) {
        Ok(_) => {}
        Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
    }

    first_node.on_definition(|x| {
        x.paxos.trim(Some(cfg.gc_idx)).expect("Failed to trim");
    });

    thread::sleep(cfg.wait_timeout);

    let mut seqs_after = vec![];
    for (i, px) in sys.nodes {
        seqs_after.push(px.on_definition(|comp| {
            let seq = comp.get_trimmed_suffix();
            (i, seq.to_vec())
        }));
    }

    check_trim(vec_proposals, seqs_after, cfg.gc_idx);

    println!("Pass gc");

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem found in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}

/// Test double Garbage Collection.
/// At the end the log is retrieved from each replica and verified
/// if the first [`gc_index`] + an increment are removed.
#[test]
#[serial]
fn double_trim_test() {
    let cfg = TestConfig::load("gc_test").expect("Test config loaded");

    let mut sys = TestSystem::with(
        cfg.num_nodes,
        cfg.ble_hb_delay,
        cfg.num_threads,
        cfg.storage_type,
    );

    let first_node = sys.nodes.get(&1).unwrap();

    let mut vec_proposals = vec![];
    let mut futures = vec![];
    for i in 1..=cfg.num_proposals {
        let (kprom, kfuture) = promise::<Value>();

        vec_proposals.push(Value(i));
        first_node.on_definition(|x| {
            x.paxos.append(Value(i)).expect("Failed to append");
            x.decided_futures.push(Ask::new(kprom, ()))
        });
        futures.push(kfuture);
    }

    sys.start_all_nodes();

    match FutureCollection::collect_with_timeout::<Vec<_>>(futures, cfg.wait_timeout) {
        Ok(_) => {}
        Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
    }

    first_node.on_definition(|x| {
        x.paxos.trim(Some(cfg.gc_idx)).expect("Failed to trim");
    });

    thread::sleep(cfg.wait_timeout);

    first_node.on_definition(|x| {
        x.paxos
            .trim(Some(cfg.gc_idx + GC_INDEX_INCREMENT))
            .expect("Failed to trim");
    });

    thread::sleep(cfg.wait_timeout);

    let mut seq_after_double = vec![];
    for (i, px) in sys.nodes {
        seq_after_double.push(px.on_definition(|comp| {
            let seq = comp.get_trimmed_suffix();
            (i, seq.to_vec())
        }));
    }

    check_trim(
        vec_proposals,
        seq_after_double,
        cfg.gc_idx + GC_INDEX_INCREMENT,
    );

    println!("Pass double_gc");

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem found in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}

fn check_trim(vec_proposals: Vec<Value>, seq_after: Vec<(u64, Vec<Value>)>, gc_idx: u64) {
    for (_, after) in seq_after {
        assert_eq!(vec_proposals.len(), (after.len() + gc_idx as usize));
        assert_eq!(vec_proposals.get(gc_idx as usize), after.get(0));
    }
}
