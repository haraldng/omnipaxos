pub mod test_config;
pub mod util;

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

    let sys = TestSystem::with(cfg.num_nodes, cfg.ble_hb_delay, None, None, cfg.num_threads);

    let (_, px) = sys.ble_paxos_nodes().get(&1).unwrap();

    let mut vec_proposals: Vec<u64> = vec![];
    let mut futures = vec![];
    for i in 0..cfg.num_proposals {
        let (kprom, kfuture) = promise::<u64>();
        vec_proposals.push(i);
        px.on_definition(|x| {
            x.propose(i);
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
        x.garbage_collect(Some(cfg.gc_idx));
    });

    thread::sleep(cfg.wait_timeout);

    let mut seq_after: Vec<(&u64, Vec<u64>)> = vec![];
    for (i, (_, px)) in sys.ble_paxos_nodes() {
        seq_after.push(px.on_definition(|comp| {
            let seq = comp.get_log();
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

    let sys = TestSystem::with(cfg.num_nodes, cfg.ble_hb_delay, None, None, cfg.num_threads);

    let (_, px) = sys.ble_paxos_nodes().get(&1).unwrap();

    let mut vec_proposals: Vec<u64> = vec![];
    let mut futures = vec![];
    for i in 0..cfg.num_proposals {
        let (kprom, kfuture) = promise::<u64>();

        vec_proposals.push(i);
        px.on_definition(|x| {
            x.propose(i);
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
        x.garbage_collect(Some(cfg.gc_idx));
    });

    thread::sleep(cfg.wait_timeout);

    px.on_definition(|x| {
        x.garbage_collect(Some(cfg.gc_idx + GC_INDEX_INCREMENT));
    });

    thread::sleep(cfg.wait_timeout);

    let mut seq_after_double: Vec<(&u64, Vec<u64>)> = vec![];
    for (i, (_, px)) in sys.ble_paxos_nodes() {
        seq_after_double.push(px.on_definition(|comp| {
            let seq = comp.get_log();
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

fn check_gc(vec_proposals: Vec<u64>, seq_after: Vec<(&u64, Vec<u64>)>, gc_idx: u64) {
    for i in 0..seq_after.len() {
        let (_, after) = seq_after.get(i).expect("After log");

        assert_eq!(vec_proposals.len(), (after.len() + gc_idx as usize));
        assert_eq!(vec_proposals.get(gc_idx as usize), after.get(0));
    }
}
