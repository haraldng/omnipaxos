pub mod utils;

use kompact::prelude::{promise, Ask, FutureCollection};
use utils::{TestConfig, TestSystem, Value};

const RECOVERY_TEST: &str = "recovery_test/";

/// Test Ballot Election Leader module.
/// The test waits for [`num_elections`] elections.
/// After each election, the leader node is killed and the process repeats
/// until the number of elections is achieved.
#[test]
fn recovery_test() {
    let cfg = TestConfig::load("consensus_test").expect("Test config loaded");

    let mut sys = TestSystem::with(
        cfg.num_nodes,
        cfg.ble_hb_delay,
        cfg.num_threads,
        cfg.storage_type,
        RECOVERY_TEST,
    );

    let (_, px) = sys.ble_paxos_nodes().get(&1).unwrap();

    let mut vec_proposals = vec![];
    let mut futures = vec![];
    for i in 1..=cfg.num_proposals/2 {
        let (kprom, kfuture) = promise::<Value>();
        vec_proposals.push(Value(i));
        px.on_definition(|x| {
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

    sys.kill_node(1);
    
    let mut futures = vec![];
    let (_, another_px) = sys.ble_paxos_nodes().get(&2).unwrap();
    for i in cfg.num_proposals/2..=cfg.num_proposals {
        let (kprom, kfuture) = promise::<Value>();
        vec_proposals.push(Value(i));
        another_px.on_definition(|x| {
            x.paxos.append(Value(i)).expect("Failed to append");
            x.add_ask(Ask::new(kprom, ()))
        });
        futures.push(kfuture);
    }

    sys.create_node(1, cfg.num_nodes, cfg.ble_hb_delay, cfg.storage_type, RECOVERY_TEST);
    let (_, recovery_px) = sys.ble_paxos_nodes().get(&1).unwrap();
    recovery_px.on_definition(|x| {
        x.paxos.fail_recovery();
    });

    match FutureCollection::collect_with_timeout::<Vec<_>>(futures, cfg.wait_timeout) {
        Ok(_) => {}
        Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
    } 

    let (_, px) = sys.ble_paxos_nodes().get(&1).unwrap();
    let log: Vec<Value> = px.on_definition(|comp| {
            comp.get_trimmed_suffix()
    });

    verify_entries(&log, &vec_proposals, 0, cfg.num_nodes as u64)
}

fn verify_entries(
    read_entries: &[Value],
    exp_entries: &[Value],
    offset: u64,
    decided_idx: u64,
) {
    assert_eq!(
        read_entries.len() -1,
        exp_entries.len() +1,
        "read: {:?}, expected: {:?}",
        read_entries,
        exp_entries
    );
    for (idx, entry) in read_entries.iter().enumerate() {
        assert_eq!(*entry, exp_entries[idx])
    }
}