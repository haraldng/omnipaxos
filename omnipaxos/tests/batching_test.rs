pub mod utils;

use std::thread;
use kompact::prelude::{promise, Ask, FutureCollection};
use omnipaxos::{
    storage::{Snapshot, StopSign, StopSignEntry, Storage},
    OmniPaxosConfig,
};
use serial_test::serial;
use std::time::Duration;
use utils::{
    create_temp_dir,
    verification::{verify_entries, verify_snapshot, verify_stopsign},
    LatestValue, StorageType, TestConfig, TestSystem, Value,
};

/// Test case for batching.
#[test]
#[serial]
fn batching_test() {
    let wait_time_between_propose = Duration::from_millis(2);

    let cfg = TestConfig::load("batching_test").expect("Test config loaded");
    let mut sys = TestSystem::with(
        cfg.num_nodes,
        cfg.election_timeout_ms,
        cfg.resend_message_timeout_ms,
        cfg.num_threads,
        cfg.storage_type,
        cfg.batch_size,
    );
    let first_node = sys.nodes.get(&1).unwrap();
    sys.start_all_nodes();

    let mut futures = vec![];
    let mut vec_proposals = vec![];
    let mut last_decided_idx = 0;
    for i in 1..=cfg.num_proposals {
        let (kprom, kfuture) = promise::<Value>();
        vec_proposals.push(Value(i));
        first_node.on_definition(|x| {
            x.paxos.append(Value(i)).expect("Failed to append");
            x.decided_futures.push(Ask::new(kprom, ()))
        });
        futures.push(kfuture);
        thread::sleep(wait_time_between_propose);
        // check batching
        first_node.on_definition(|x| {
            let decided_idx = x.paxos.get_decided_idx();
            check_batching(decided_idx, last_decided_idx, cfg.batch_size as u64);
            last_decided_idx = decided_idx;
        });

    }

    match FutureCollection::collect_with_timeout::<Vec<_>>(futures, Duration::from_millis(cfg.wait_timeout_ms)) {
        Ok(_) => {}
        Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
    }

    let mut log = vec![];
    for (pid, node) in sys.nodes {
        log.push(node.on_definition(|comp| {
            let log = comp.paxos.get_decided_idx();
            (pid, log)
        }));
    }

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}

fn check_batching(decided_idx: u64, last_decided_idx: u64, batch_size: u64) {
    let idx_diff = decided_idx - last_decided_idx;
    if idx_diff != 0 {
        assert!(decided_idx - last_decided_idx >= batch_size);
    }
}
