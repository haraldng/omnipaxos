pub mod test_config;
pub mod util;

use crate::util::Value;
use kompact::prelude::{promise, Ask, FutureCollection};
use serial_test::serial;
use std::thread;
use test_config::TestConfig;
use util::TestSystem;

const GC_INDEX_INCREMENT: u64 = 10;

/// Test Garbage Collection.
/// At the end the log is retrieved from each replica and verified
/// if the first [`gc_index`] are removed.
#[test]
#[serial]
fn gc_test() {
    let cfg = TestConfig::load("gc_test").expect("Test config loaded");

    let sys = TestSystem::with(cfg.num_nodes, cfg.ble_hb_delay, cfg.num_threads);

    let (_, px) = sys.ble_paxos_nodes().get(&1).unwrap();

    let mut vec_proposals = vec![];
    let mut futures = vec![];
    for i in 0..cfg.num_proposals {
        let (kprom, kfuture) = promise::<Value>();
        vec_proposals.push(Value(i));
        px.on_definition(|x| {
            x.propose(Value(i));
            x.add_ask(Ask::new(kprom, ()))
        });
        futures.push(kfuture);
    }

    sys.start_all_nodes();

    match FutureCollection::collect_with_timeout::<Vec<_>>(futures, cfg.wait_timeout) {
        Ok(_) => {}
        Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
    }

    px.on_definition(|x| {
        x.trim(Some(cfg.gc_idx));
    });

    thread::sleep(cfg.wait_timeout);

    let mut seq_after: Vec<(&u64, Vec<Value>)> = vec![];
    for (i, (_, px)) in sys.ble_paxos_nodes() {
        seq_after.push(px.on_definition(|comp| {
            let seq = comp.get_trimmed_suffix();
            (i, seq.to_vec())
        }));
    }

    check_gc(vec_proposals, seq_after, cfg.gc_idx);

    println!("Pass gc");

    match sys.kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}

/// Test double Garbage Collection.
/// At the end the log is retrieved from each replica and verified
/// if the first [`gc_index`] + an increment are removed.
#[test]
#[serial]
fn double_gc_test() {
    let cfg = TestConfig::load("gc_test").expect("Test config loaded");

    let sys = TestSystem::with(cfg.num_nodes, cfg.ble_hb_delay, cfg.num_threads);

    let (_, px) = sys.ble_paxos_nodes().get(&1).unwrap();

    let mut vec_proposals = vec![];
    let mut futures = vec![];
    for i in 0..cfg.num_proposals {
        let (kprom, kfuture) = promise::<Value>();

        vec_proposals.push(Value(i));
        px.on_definition(|x| {
            x.propose(Value(i));
            x.add_ask(Ask::new(kprom, ()))
        });
        futures.push(kfuture);
    }

    sys.start_all_nodes();

    match FutureCollection::collect_with_timeout::<Vec<_>>(futures, cfg.wait_timeout) {
        Ok(_) => {}
        Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
    }

    px.on_definition(|x| {
        x.trim(Some(cfg.gc_idx));
    });

    thread::sleep(cfg.wait_timeout);

    px.on_definition(|x| {
        x.trim(Some(cfg.gc_idx + GC_INDEX_INCREMENT));
    });

    thread::sleep(cfg.wait_timeout);

    let mut seq_after_double: Vec<(&u64, Vec<Value>)> = vec![];
    for (i, (_, px)) in sys.ble_paxos_nodes() {
        seq_after_double.push(px.on_definition(|comp| {
            let seq = comp.get_trimmed_suffix();
            (i, seq.to_vec())
        }));
    }

    check_gc(
        vec_proposals,
        seq_after_double,
        cfg.gc_idx + GC_INDEX_INCREMENT,
    );

    println!("Pass double_gc");

    match sys.kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}

fn check_gc(vec_proposals: Vec<Value>, seq_after: Vec<(&u64, Vec<Value>)>, gc_idx: u64) {
    for i in 0..seq_after.len() {
        let (_, after) = seq_after.get(i).expect("After log");

        assert_eq!(vec_proposals.len(), (after.len() + gc_idx as usize));
        assert_eq!(vec_proposals.get(gc_idx as usize), after.get(0));
    }
}
