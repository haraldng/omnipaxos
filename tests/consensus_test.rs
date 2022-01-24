pub mod test_config;
pub mod util;

use kompact::prelude::{promise, Ask, FutureCollection};
use serial_test::serial;
use test_config::TestConfig;
use util::TestSystem;

/// Verifies the 3 properties that the Paxos algorithm offers
/// Quorum, Validity, Uniform Agreement
#[test]
#[serial]
fn consensus_test() {
    let cfg = TestConfig::load("consensus_test").expect("Test config loaded");

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

    let mut log: Vec<(&u64, Vec<u64>)> = vec![];
    for (i, (_, px)) in sys.ble_paxos_nodes() {
        log.push(px.on_definition(|comp| {
            let log = comp.get_log();
            (i, log.to_vec())
        }));
    }

    let quorum_size = cfg.num_nodes as usize / 2 + 1;
    check_quorum(log.clone(), quorum_size, vec_proposals.clone());
    check_validity(log.clone(), vec_proposals);
    check_uniform_agreement(log);

    match sys.kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}

/// Verifies that there is a majority when an entry is proposed.
fn check_quorum(log_responses: Vec<(&u64, Vec<u64>)>, quorum_size: usize, num_proposals: Vec<u64>) {
    for i in num_proposals {
        let num_nodes: usize = log_responses
            .iter()
            .filter(|(_, sr)| sr.contains(&i))
            .map(|sr| sr.0)
            .count();
        let timed_out_proposal = num_nodes == 0;
        if !timed_out_proposal {
            assert!(
                num_nodes >= quorum_size,
                "Decided value did NOT have majority quorum! contained: {:?}",
                num_nodes
            );
        }
    }

    println!("Pass check_quorum");
}

/// Verifies that only proposed values are decided.
fn check_validity(log_responses: Vec<(&u64, Vec<u64>)>, num_proposals: Vec<u64>) {
    let invalid_nodes: Vec<_> = log_responses
        .iter()
        .filter(|(_, sr)| {
            sr.iter()
                .filter(|ent| !num_proposals.contains(*ent))
                .count()
                != 0
        })
        .collect();
    assert!(
        invalid_nodes.len() < 1,
        "Nodes decided unproposed values. invalid_nodes: {:?}",
        invalid_nodes
    );

    println!("Pass check_validity");
}

/// Verifies if one correct node receives a message, then everyone will eventually receive it.
fn check_uniform_agreement(log_responses: Vec<(&u64, Vec<u64>)>) {
    let (_, longest_log) = log_responses
        .iter()
        .max_by(|(_, sr), (_, other_sr)| sr.len().cmp(&other_sr.len()))
        .expect("Empty SequenceResp from nodes!");
    for (_, sr) in &log_responses {
        assert!(longest_log.starts_with(sr.as_slice()));
    }

    println!("Pass check_uniform_agreement");
}
