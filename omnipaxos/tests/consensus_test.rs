pub mod utils;

use kompact::prelude::{promise, Ask, FutureCollection};
use omnipaxos::{
    storage::{Snapshot, StopSign, Storage},
    ClusterConfig, OmniPaxosConfig,
};
use serial_test::serial;
use utils::{
    create_temp_dir, verification::*, StorageType, TestConfig, TestSystem, Value, ValueSnapshot,
};

/// Verifies the 3 properties that the Paxos algorithm offers
/// Quorum, Validity, Uniform Agreement
#[test]
#[serial]
fn consensus_test() {
    let cfg = TestConfig::load("consensus_test").expect("Test config loaded");
    let mut sys = TestSystem::with(cfg);

    let first_node = sys.nodes.get(&1).unwrap();
    let mut futures = vec![];
    let vec_proposals = utils::create_proposals(1, cfg.num_proposals);
    for v in &vec_proposals {
        let (kprom, kfuture) = promise::<()>();
        first_node.on_definition(|x| {
            x.insert_decided_future(Ask::new(kprom, v.clone()));
            x.paxos.append(v.clone()).expect("Failed to append");
        });
        futures.push(kfuture);
    }

    sys.start_all_nodes();

    /*
    match FutureCollection::collect_with_timeout::<Vec<_>>(futures, cfg.wait_timeout) {
        Ok(_) => {}
        Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
    }
    */
    std::thread::sleep(cfg.wait_timeout);

    let mut log = vec![];
    for (pid, node) in sys.nodes {
        log.push(node.on_definition(|x| {
            let log = x.read_decided_log();
            (pid, log)
        }));
    }

    let quorum_size = cfg.num_nodes / 2 + 1;
    check_quorum(&log, quorum_size, &vec_proposals);
    check_validity(&log, &vec_proposals);
    check_metronome_log_consistency(&log, &vec_proposals, quorum_size);

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}

#[test]
#[serial]
fn read_test() {
    let cfg = TestConfig::load("consensus_test").expect("Test config loaded");

    let log: Vec<Value> = [1, 3, 2, 7, 5, 10, 29, 100, 8, 12]
        .iter()
        .map(|v| Value::with_id(*v as u64))
        .collect();
    let decided_idx = 6;
    let snapshotted_idx = 4;
    let (snapshotted, _suffix) = log.split_at(snapshotted_idx);

    let exp_snapshot = ValueSnapshot::create(snapshotted);

    let temp_dir = create_temp_dir();
    let mut storage = StorageType::<Value>::with(cfg.storage_type, &temp_dir);
    storage
        .append_entries(log.clone())
        .expect("Failed to append entries");
    storage
        .set_decided_idx(decided_idx)
        .expect("Failed to set decided index");

    let mut op_config = OmniPaxosConfig::default();
    op_config.server_config.pid = 1;
    op_config.cluster_config.nodes = vec![1, 2, 3];
    op_config.cluster_config.configuration_id = 1;

    let mut omni_paxos = op_config.clone().build(storage).unwrap();

    // read decided entries
    let entries = omni_paxos
        .read_decided_suffix(0)
        .expect("No decided entries");
    let expected_entries = log.get(0..decided_idx).unwrap();
    verify_entries(entries.as_slice(), expected_entries, 0, decided_idx);

    // create snapshot
    omni_paxos
        .snapshot(Some(snapshotted_idx), true)
        .expect("Failed to snapshot");

    // read entry
    let idx = snapshotted_idx;
    let entry = omni_paxos.read(idx).expect("No entry");
    let expected_entries = log.get(idx..=idx).unwrap();
    verify_entries(&[entry], expected_entries, snapshotted_idx, decided_idx);

    // read snapshot
    let snapshot = omni_paxos.read(0).expect("No snapshot");
    verify_snapshot(&[snapshot], snapshotted_idx, &exp_snapshot);

    // read none
    let idx = log.len();
    let entry = omni_paxos.read(idx);
    assert!(entry.is_none(), "Expected None, got: {:?}", entry);

    // create stopped storage and SequencePaxos to test reading StopSign.
    let ss_temp_dir = create_temp_dir();
    let mut stopped_storage = StorageType::<Value>::with(cfg.storage_type, &ss_temp_dir);
    let ss = StopSign::with(
        ClusterConfig {
            configuration_id: 2,
            ..Default::default()
        },
        None,
    );
    let log_len = log.len();
    stopped_storage
        .append_entries(log.clone())
        .expect("Failed to append entries");
    stopped_storage
        .set_stopsign(Some(ss.clone()))
        .expect("Failed to set StopSign");
    stopped_storage
        .set_decided_idx(log_len + 1)
        .expect("Failed to set decided index");

    let mut stopped_op = op_config.build(stopped_storage).unwrap();
    stopped_op
        .snapshot(Some(snapshotted_idx), true)
        .expect("Failed to snapshot");

    // read stopsign
    let idx = log_len;
    let stopsign = stopped_op.read(idx).expect("No StopSign");
    verify_stopsign(&[stopsign], &ss);
}

#[test]
#[serial]
fn read_entries_test() {
    let cfg = TestConfig::load("consensus_test").expect("Test config loaded");

    let log: Vec<Value> = [1, 3, 2, 7, 5, 10, 29, 100, 8, 12]
        .iter()
        .map(|v| Value::with_id(*v as u64))
        .collect();
    let decided_idx = 6;
    let snapshotted_idx = 4;
    let (snapshotted, _suffix) = log.split_at(snapshotted_idx);
    let exp_snapshot = ValueSnapshot::create(snapshotted);

    let temp_dir = create_temp_dir();
    let mut storage = StorageType::<Value>::with(cfg.storage_type, &temp_dir);
    storage
        .append_entries(log.clone())
        .expect("Failed to append entries");
    storage
        .set_decided_idx(decided_idx)
        .expect("Failed to set decided index");
    let mut op_config = OmniPaxosConfig::default();
    op_config.server_config.pid = 1;
    op_config.cluster_config.nodes = vec![1, 2, 3];
    op_config.cluster_config.configuration_id = 1;

    let mut omni_paxos = op_config.clone().build(storage).unwrap();
    omni_paxos
        .snapshot(Some(snapshotted_idx), true)
        .expect("Failed to snapshot");

    // read entries only
    let from_idx = snapshotted_idx + 1;
    let entries = omni_paxos
        .read_entries(from_idx..=decided_idx)
        .expect("No entries");
    let expected_entries = log.get(from_idx..=decided_idx).unwrap();
    verify_entries(entries.as_slice(), expected_entries, from_idx, decided_idx);
    // read snapshot only
    let entries = omni_paxos
        .read_entries(0..snapshotted_idx)
        .expect("No snapshot");
    verify_snapshot(entries.as_slice(), snapshotted_idx, &exp_snapshot);

    // read snapshot + entries
    let from_idx = 3;
    let to_idx = decided_idx;
    let entries = omni_paxos
        .read_entries(from_idx..to_idx)
        .expect("No snapshot and entries");
    let (snapshot, suffix) = entries.split_at(1);
    let expected_entries = log.get(snapshotted_idx..to_idx).unwrap();
    verify_snapshot(snapshot, snapshotted_idx, &exp_snapshot);
    verify_entries(suffix, expected_entries, snapshotted_idx, decided_idx);

    // read none
    let from_idx = 0;
    let to_idx = log.len();
    let entries = omni_paxos.read_entries(from_idx..=to_idx);
    assert!(entries.is_none(), "Expected None, got: {:?}", entries);

    // create stopped storage and SequencePaxos to test reading StopSign.
    let ss_temp_dir = create_temp_dir();
    let mut stopped_storage = StorageType::<Value>::with(cfg.storage_type, &ss_temp_dir);

    let ss = StopSign::with(
        ClusterConfig {
            configuration_id: 2,
            ..Default::default()
        },
        None,
    );
    let log_len = log.len();
    stopped_storage
        .append_entries(log.clone())
        .expect("Failed to append entries");
    stopped_storage.set_stopsign(Some(ss.clone())).unwrap();
    stopped_storage.set_decided_idx(log_len + 1).unwrap();

    let mut stopped_op = op_config.build(stopped_storage).unwrap();
    stopped_op
        .snapshot(Some(snapshotted_idx), true)
        .expect("Failed to snapshot");

    // read stopsign only
    let idx = log_len;
    let entries = stopped_op.read_entries(idx..=idx).expect("No StopSign");
    verify_stopsign(entries.as_slice(), &ss);

    // read entries + stopsign
    let from_idx = snapshotted_idx + 2;
    let entries = stopped_op
        .read_entries(from_idx..)
        .expect("No StopSign and Entries");
    let (prefix, stopsign) = entries.split_at(entries.len() - 1);
    verify_entries(prefix, log.get(from_idx..).unwrap(), from_idx, log_len);
    verify_stopsign(stopsign, &ss);

    // read snapshot + entries + stopsign
    let from_idx = 0;
    let entries = stopped_op
        .read_entries(from_idx..)
        .expect("No Snapshot, Entries and StopSign");
    let (prefix, stopsign) = entries.split_at(entries.len() - 1);
    let (snapshot, ents) = prefix.split_at(1);
    verify_snapshot(snapshot, snapshotted_idx, &exp_snapshot);
    verify_entries(
        ents,
        log.get(snapshotted_idx..).unwrap(),
        snapshotted_idx,
        log_len,
    );
    verify_stopsign(stopsign, &ss);

    // read snapshot + stopsign
    // snapshot entire log
    stopped_op
        .snapshot(Some(log_len), true)
        .expect("Failed to snapshot");
    let snapshotted_idx = log_len;
    let from_idx = 0;
    let entries = stopped_op
        .read_entries(from_idx..)
        .expect("No StopSign and Entries");
    let (snapshot, stopsign) = entries.split_at(entries.len() - 1);
    verify_snapshot(snapshot, snapshotted_idx, &ValueSnapshot::create(&log));
    verify_stopsign(stopsign, &ss);
}
