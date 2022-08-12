pub mod utils;

use kompact::prelude::{promise, Ask, FutureCollection, KFuture};
use utils::{TestConfig, TestSystem, Value};
use std::{thread, time};

const RECOVERY_TEST: &str = "recovery_test/";

/// Test that SequencePaxos can recover from failure and decided new entries
#[test]
#[ignore = "not finished"]
fn leader_fail_recovery_test() {
    let cfg = TestConfig::load("consensus_test").expect("Test config loaded");
    let (leader, follower) = (3,1);

    //create testsystem
    let mut sys = TestSystem::with(
        cfg.num_nodes,
        cfg.ble_hb_delay,
        cfg.num_threads,
        cfg.storage_type,
        RECOVERY_TEST,
    );

    let (_, follower_px) = sys.ble_paxos_nodes().get(&follower).unwrap();

    //let follower propose the first 10
    let mut vec_proposals = vec![];
    let mut futures = vec![];
    for i in 1..=cfg.num_proposals / 2 {
        let (kprom, kfuture) = promise::<Value>();
        vec_proposals.push(Value(i));
        follower_px.on_definition(|x| {
            x.paxos.append(Value(i)).expect("Failed to append");
            x.add_ask(Ask::new(kprom, ()))
        });
        futures.push(kfuture);
    }

    sys.start_all_nodes();

    match FutureCollection::collect_with_timeout::<Vec<_>>(futures, cfg.wait_timeout) {
        Ok(_) => {}
        Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
    }

    //kill leader node
    sys.kill_node(leader);
    thread::sleep(time::Duration::from_secs(cfg.ble_hb_delay)); // make current thread sleep so all nodes can elect a new leader in time

    sys.create_node(
        leader,
        cfg.num_nodes,
        cfg.ble_hb_delay,
        cfg.storage_type,
        RECOVERY_TEST,
    );
    let (_, leader_px) = sys.ble_paxos_nodes().get(&leader).unwrap();
    leader_px.on_definition(|x| {
        x.paxos.fail_recovery();
    });
    sys.start_node(leader);

    let mut new_futures: Vec<KFuture<Value>> = vec![];
    let (_, leader_px) = sys.ble_paxos_nodes().get(&leader).unwrap();
    let (_, follower_px) = sys.ble_paxos_nodes().get(&follower).unwrap();

    for i in (cfg.num_proposals / 2) + 1..=cfg.num_proposals {
        let (kprom, kfuture) = promise::<Value>();
        vec_proposals.push(Value(i));
        follower_px.on_definition(|x| {
            x.paxos.append(Value(i)).expect("Failed to append");
        });
        leader_px.on_definition(|x| {
            x.add_ask(Ask::new(kprom, ()));
        });

        new_futures.push(kfuture);
    }

    thread::sleep(time::Duration::from_secs(cfg.ble_hb_delay));
    match FutureCollection::collect_with_timeout::<Vec<_>>(new_futures, cfg.wait_timeout) {
        Ok(_) => {}
        Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
    }

    let log: Vec<Value> = follower_px.on_definition(|comp| 
        comp.get_trimmed_suffix()
    );
    println!("{:?}", log);

    verify_entries(&log, &vec_proposals);

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}

fn verify_entries(read_entries: &[Value], exp_entries: &[Value]) {
    assert_eq!(
        read_entries.len(),
        exp_entries.len(),
        "read: {:?}, expected: {:?}",
        read_entries,
        exp_entries
    );
    for (idx, entry) in read_entries.iter().enumerate() {
        assert_eq!(*entry, exp_entries[idx])
    }
}
