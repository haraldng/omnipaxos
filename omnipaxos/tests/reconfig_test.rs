pub mod utils;

use kompact::prelude::{promise, Ask};
use omnipaxos::ClusterConfig;
use serial_test::serial;
use std::time::Duration;
use utils::{TestConfig, TestSystem, Value};

const SS_METADATA: u8 = 255;

/// Verifies that the decided StopSign is correct and error is returned when trying to append after decided StopSign.
#[test]
#[serial]
fn reconfig_test() {
    let cfg = TestConfig::load("consensus_test").expect("Test config loaded");
    let mut sys = TestSystem::with(cfg);

    let first_node = sys.nodes.get(&1).unwrap();
    let mut vec_proposals = vec![];
    let mut futures = vec![];
    for v in utils::create_proposals(cfg.num_proposals) {
        let (kprom, kfuture) = promise::<Value>();
        let value = v;
        vec_proposals.push(value.clone());
        first_node.on_definition(|x| {
            x.paxos.append(value).expect("Failed to append");
            x.decided_futures.push(Ask::new(kprom, ()))
        });
        futures.push(kfuture);
    }

    sys.start_all_nodes();

    let vec_proposals = (1..=cfg.num_proposals).map(Value::with_id).collect();
    sys.make_proposals(1, vec_proposals, Duration::from_millis(cfg.wait_timeout_ms));

    let new_config_id = 2;
    let new_nodes: Vec<u64> = (cfg.num_nodes as u64..(cfg.num_nodes as u64 + 3)).collect();
    let new_config = ClusterConfig {
        configuration_id: new_config_id,
        nodes: new_nodes,
        flexible_quorum: None,
    };
    let metadata = Some(vec![SS_METADATA]);

    let first_node = sys.nodes.get(&1).unwrap();
    let reconfig_f = first_node.on_definition(|x| {
        let (kprom, kfuture) = promise::<Value>();
        x.paxos
            .reconfigure(new_config.clone(), metadata.clone())
            .expect("Failed to reconfigure");
        x.decided_futures.push(Ask::new(kprom, ()));
        kfuture
    });

    let decided_ss_config = reconfig_f
        .wait_timeout(Duration::from_millis(cfg.wait_timeout_ms))
        .expect("Failed to collect reconfiguration future");
    assert_eq!(decided_ss_config, Value::with_id(new_config_id as u64));

    let decided_nodes = sys.nodes.iter().fold(vec![], |mut x, (pid, paxos)| {
        let ss = paxos.on_definition(|x| x.paxos.is_reconfigured());
        if let Some(stop_sign) = ss {
            assert_eq!(stop_sign.next_config, new_config);
            assert_eq!(stop_sign.metadata, metadata);
            x.push(pid);
        }
        x
    });
    let quorum_size = cfg.num_nodes / 2 + 1;
    assert!(decided_nodes.len() >= quorum_size);

    let pid = *decided_nodes.last().unwrap();
    let node = sys.nodes.get(pid).unwrap();
    node.on_definition(|x| {
        x.paxos
            .append(Value::with_id(0))
            .expect_err("Should not be able to propose after decided StopSign!")
    });

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}
