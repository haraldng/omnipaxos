pub mod utils;

#[cfg(feature = "hocon_config")]
use kompact::prelude::{promise, Ask, FutureCollection};
use omnipaxos_core::omni_paxos::ReconfigurationRequest;
#[cfg(feature = "hocon_config")]
use serial_test::serial;
#[cfg(feature = "hocon_config")]
use utils::{TestConfig, TestSystem, Value, SS_METADATA};

#[cfg(feature = "hocon_config")]
/// Verifies that the decided StopSign is correct and error is returned when trying to append after decided StopSign.
#[test]
#[serial]
fn reconfig_test() {
    let cfg = TestConfig::load("consensus_test").expect("Test config loaded");

    let mut sys = TestSystem::with(
        cfg.num_nodes,
        cfg.election_timeout,
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

    let new_config: Vec<u64> = (cfg.num_nodes as u64..(cfg.num_nodes as u64 + 3)).collect();
    let rc = ReconfigurationRequest::with(new_config.clone(), Some(vec![SS_METADATA]));

    let reconfig_f = first_node.on_definition(|x| {
        let (kprom, kfuture) = promise::<Value>();
        x.paxos.reconfigure(rc).expect("Failed to reconfigure");
        x.decided_futures.push(Ask::new(kprom, ()));
        kfuture
    });

    let decided_ss_metadata = reconfig_f
        .wait_timeout(cfg.wait_timeout)
        .expect("Failed to collect reconfiguration future");
    assert_eq!(decided_ss_metadata, Value(SS_METADATA as u64));

    let decided_nodes = sys.nodes.iter().fold(vec![], |mut x, (pid, paxos)| {
        let ss = paxos.on_definition(|x| x.paxos.is_reconfigured());
        if let Some(stop_sign) = ss {
            assert_eq!(stop_sign.nodes, new_config);
            x.push(pid);
        }
        x
    });
    let quorum_size = cfg.num_nodes as usize / 2 + 1;
    assert!(decided_nodes.len() >= quorum_size);

    let pid = *decided_nodes.last().unwrap();
    let node = sys.nodes.get(pid).unwrap();
    node.on_definition(|x| {
        x.paxos
            .append(Value(0))
            .expect_err("Should not be able to propose after decided StopSign!")
    });

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}
