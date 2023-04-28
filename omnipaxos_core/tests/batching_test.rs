pub mod utils;

use std::thread;
use std::time::Duration;
use kompact::prelude::{promise, Ask, FutureCollection};
use omnipaxos_core::{
    omni_paxos::OmniPaxosConfig,
    storage::{Snapshot, StopSign, StopSignEntry, Storage},
};
use serial_test::serial;
use utils::{
    create_temp_dir,
    verification::{verify_entries, verify_snapshot, verify_stopsign},
    LatestValue, StorageType, TestConfig, TestSystem, Value,
};

/// Test case for batching.
#[test]
#[serial]
fn batching_test() {
    let cfg = TestConfig::load("batching_test").expect("Test config loaded");
    let mut sys = TestSystem::with(
        cfg.num_nodes,
        cfg.election_timeout,
        cfg.num_threads,
        cfg.storage_type,
        cfg.batch_size,
    );

    let first_node = sys.nodes.get(&1).unwrap();
    sys.start_all_nodes();

    let mut futures = vec![];
    let mut vec_proposals = vec![];
    for i in 1..=cfg.num_proposals {
        let (kprom, kfuture) = promise::<Value>();
        vec_proposals.push(Value(i));
        first_node.on_definition(|x| {
            x.paxos.append(Value(i)).expect("Failed to append");
            x.decided_futures.push(Ask::new(kprom, ()))
        });
        futures.push(kfuture);

        thread::sleep(Duration::from_millis(50));
    }

    match FutureCollection::collect_with_timeout::<Vec<_>>(futures, cfg.wait_timeout) {
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

    check_uniform_agreement(log);

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}

fn check_uniform_agreement(log_responses: Vec<(u64, u64)>) {
    println!("{:?}",log_responses);
}
