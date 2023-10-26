pub mod utils;

use crate::utils::STOPSIGN_ID;
use kompact::prelude::{promise, Ask};
use omnipaxos::{util::{LogEntry, NodeId}, ClusterConfig};
use serial_test::serial;
use utils::{TestConfig, TestSystem, Value};

const SS_METADATA: u8 = 255;

/// Verifies that the decided StopSign is correct and error is returned when trying to append after decided StopSign.
#[test]
#[serial]
fn reconfig_test() {
    let cfg = TestConfig::load("consensus_test").expect("Test config loaded");
    let mut sys = TestSystem::with(cfg);
    sys.start_all_nodes();
    sys.make_proposals(
        1,
        utils::create_proposals(1, cfg.num_proposals),
        cfg.wait_timeout,
    );

    let new_config_id = 2;
    let new_nodes: Vec<NodeId> = (cfg.num_nodes as NodeId..(cfg.num_nodes as NodeId + 3)).collect();
    let new_config = ClusterConfig {
        configuration_id: new_config_id,
        nodes: new_nodes,
        flexible_quorum: None,
    };
    let metadata = Some(vec![SS_METADATA]);

    let first_node = sys.nodes.get(&1).unwrap();
    let reconfig_f = first_node.on_definition(|x| {
        let (kprom, kfuture) = promise::<()>();
        x.paxos
            .reconfigure(new_config.clone(), metadata.clone())
            .expect("Failed to reconfigure");
        let stopsign_value = Value::with_id(STOPSIGN_ID);
        x.insert_decided_future(Ask::new(kprom, stopsign_value));
        kfuture
    });

    reconfig_f
        .wait_timeout(cfg.wait_timeout)
        .expect("Failed to collect reconfiguration future");

    first_node.on_definition(|x| {
        let decided = x
            .paxos
            .read_decided_suffix(0)
            .expect("Failed to read decided suffix");
        match decided.last().expect("Failed to read last decided entry") {
            LogEntry::StopSign(ss, decided) => {
                assert_eq!(ss.next_config, new_config);
                assert_eq!(ss.metadata, metadata);
                assert!(decided, "StopSign should be decided")
            }
            e => panic!("Last decided entry is not a StopSign: {:?}", e),
        }
    });

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
