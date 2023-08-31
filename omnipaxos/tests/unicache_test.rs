pub mod utils;

// #[cfg(feature = "unicache")]
mod unicache_test {
    use super::utils::{
        create_temp_dir, verification::*, LatestValue, StorageType, TestConfig, TestSystem, Value,
    };
    use kompact::prelude::{promise, Ask, FutureCollection};
    use serial_test::serial;
    use std::time::Duration;

    #[test]
    #[serial]
    fn encoding_test() {
        let cfg = TestConfig::load("unicache_test").expect("Test config loaded");
        let mut sys = TestSystem::with(cfg);

        let first_node = sys.nodes.get(&1).unwrap();

        assert!(
            cfg.num_iterations > 1,
            "Number of iterations must be > 1 to test UniCache hits"
        );
        let cache_size = cfg.num_proposals / cfg.num_iterations;
        assert!(cache_size > 0 && cfg.num_proposals > cache_size, "UniCache size must be greater than zero but less than number of proposals to test hits");

        let mut vec_proposals = vec![];
        let mut futures = vec![];
        for _ in 0..cfg.num_iterations {
            for i in 1..=cache_size {
                let (kprom, kfuture) = promise::<Value>();
                vec_proposals.push(Value::with_id(i));
                first_node.on_definition(|x| {
                    x.paxos.append(Value::with_id(i)).expect("Failed to append");
                    x.decided_futures.push(Ask::new(kprom, ()))
                });
                futures.push(kfuture);
            }
        }
        sys.start_all_nodes();

        match FutureCollection::collect_with_timeout::<Vec<_>>(
            futures,
            Duration::from_millis(cfg.wait_timeout_ms),
        ) {
            Ok(_) => {}
            Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
        }

        let mut log = vec![];
        for (pid, node) in sys.nodes {
            log.push(node.on_definition(|comp| {
                let log = comp.get_trimmed_suffix();
                (pid, log.to_vec())
            }));
        }

        let quorum_size = cfg.num_nodes as usize / 2 + 1;
        check_quorum(log.clone(), quorum_size, vec_proposals.clone());
        check_validity(log.clone(), vec_proposals);
        check_uniform_agreement(log);

        let kompact_system =
            std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
        match kompact_system.shutdown() {
            Ok(_) => {}
            Err(e) => panic!("Error on kompact shutdown: {}", e),
        };
    }
}
