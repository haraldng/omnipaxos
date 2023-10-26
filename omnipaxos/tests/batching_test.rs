pub mod utils;

use kompact::prelude::{promise, Ask, FutureCollection};
use serial_test::serial;
use std::{thread, time::Duration};
use utils::{TestConfig, TestSystem};

/// Test case for batching.
#[test]
#[serial]
fn batching_test() {
    let wait_time_between_propose = Duration::from_millis(2);

    let cfg = TestConfig::load("batching_test").expect("Test config loaded");
    let mut sys = TestSystem::with(cfg);
    let first_node = sys.nodes.get(&1).unwrap();
    sys.start_all_nodes();

    let mut futures = vec![];
    let mut last_decided_idx = 0;
    let proposals = utils::create_proposals(1, cfg.num_proposals);
    for v in proposals {
        let (kprom, kfuture) = promise::<()>();
        first_node.on_definition(|x| {
            x.insert_decided_future(Ask::new(kprom, v.clone()));
            x.paxos.append(v.clone()).expect("Failed to append");
        });
        futures.push(kfuture);
        thread::sleep(wait_time_between_propose);
        // check batching
        first_node.on_definition(|x| {
            let decided_idx = x.paxos.get_decided_idx();
            check_batching(decided_idx, last_decided_idx, cfg.batch_size);
            last_decided_idx = decided_idx;
        });
    }

    match FutureCollection::collect_with_timeout::<Vec<_>>(futures, cfg.wait_timeout) {
        Ok(_) => {}
        Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
    }

    let mut log = vec![];
    for (pid, node) in sys.nodes {
        log.push(node.on_definition(|x| {
            let log = x.paxos.get_decided_idx();
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

fn check_batching(decided_idx: usize, last_decided_idx: usize, batch_size: usize) {
    let idx_diff = decided_idx - last_decided_idx;
    if idx_diff != 0 {
        assert!(decided_idx - last_decided_idx >= batch_size);
    }
}
