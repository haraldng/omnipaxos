pub mod test_config;
pub mod util;

#[cfg(feature = "hocon_config")]
use crate::util::{Value, SS_METADATA};
#[cfg(feature = "hocon_config")]
use kompact::prelude::{promise, Ask, FutureCollection};
#[cfg(feature = "hocon_config")]
use omnipaxos_core::sequence_paxos::ReconfigurationRequest;
#[cfg(feature = "hocon_config")]
use serial_test::serial;
#[cfg(feature = "hocon_config")]
use test_config::TestConfig;
#[cfg(feature = "hocon_config")]
use util::TestSystem;

use rocksdb::{DB, Options};

#[cfg(feature = "hocon_config")]
/// Verifies that the decided StopSign is correct and error is returned when trying to append after decided StopSign.
#[test]
#[serial]
fn reconfig_test() {
    let cfg = TestConfig::load("consensus_test").expect("Test config loaded");

    let sys = TestSystem::with(cfg.num_nodes, cfg.ble_hb_delay, cfg.num_threads);

    let (_, px) = sys.ble_paxos_nodes().get(&1).unwrap();

    let mut vec_proposals = vec![];
    let mut futures = vec![];
    for i in 1..=cfg.num_proposals {
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

    let new_config: Vec<u64> = (cfg.num_nodes as u64..(cfg.num_nodes as u64 + 3)).collect();
    let rc = ReconfigurationRequest::with(new_config.clone(), Some(vec![SS_METADATA]));

    let reconfig_f = px.on_definition(|x| {
        let (kprom, kfuture) = promise::<Value>();
        x.paxos.reconfigure(rc).expect("Failed to reconfigure");
        x.add_ask(Ask::new(kprom, ()));
        kfuture
    });

    let decided_ss_metadata = reconfig_f
        .wait_timeout(cfg.wait_timeout)
        .expect("Failed to collect reconfiguration future");
    assert_eq!(decided_ss_metadata, Value(SS_METADATA as u64));

    let decided_nodes = sys
        .ble_paxos_nodes()
        .iter()
        .fold(vec![], |mut x, (pid, comps)| {
            let paxos = &comps.1;
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
    let (_ble, paxos) = sys.ble_paxos_nodes().get(pid).unwrap();
    paxos.on_definition(|x| {
        x.paxos
            .append(Value(0))
            .expect_err("Should not be able to propose after decided StopSign!")
    });

    match sys.kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };

    let _ = DB::destroy(&Options::default(), "rocksDB");
}
