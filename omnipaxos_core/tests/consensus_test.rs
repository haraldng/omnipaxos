pub mod utils;

use kompact::prelude::{promise, Ask, FutureCollection};
use omnipaxos_core::{
    omni_paxos::OmniPaxosConfig,
    storage::{Snapshot, StopSign, StopSignEntry, Storage},
    util::LogEntry,
};
use serial_test::serial;
use utils::{create_temp_dir, LatestValue, StorageType, TestConfig, TestSystem, Value};

/// Verifies the 3 properties that the Paxos algorithm offers
/// Quorum, Validity, Uniform Agreement
#[test]
#[serial]
fn consensus_test() {
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

#[test]
#[serial]
fn read_test() {
    let cfg = TestConfig::load("consensus_test").expect("Test config loaded");

    let log: Vec<Value> = vec![1, 3, 2, 7, 5, 10, 29, 100, 8, 12]
        .iter()
        .map(|v| Value(*v as u64))
        .collect();
    let decided_idx = 6;
    let snapshotted_idx: u64 = 4;
    let (snapshotted, _suffix) = log.split_at(snapshotted_idx as usize);

    let exp_snapshot = LatestValue::create(snapshotted);

    let temp_dir = create_temp_dir();
    let mut storage = StorageType::<Value, LatestValue>::with(cfg.storage_type, &temp_dir);
    storage.append_entries(log.clone());
    storage.set_decided_idx(decided_idx);

    let mut op_config = OmniPaxosConfig::default();
    op_config.pid = 1;
    op_config.peers = vec![2, 3];
    op_config.configuration_id = 1;
    let mut omni_paxos = op_config.clone().build(storage);

    // read decided entries
    let entries = omni_paxos
        .read_decided_suffix(0)
        .expect("No decided entries");
    let expected_entries = log.get(0..decided_idx as usize).unwrap();
    verify_entries(entries.as_slice(), expected_entries, 0, decided_idx);

    // create snapshot
    omni_paxos
        .snapshot(Some(snapshotted_idx), true)
        .expect("Failed to snapshot");

    // read entry
    let idx = snapshotted_idx;
    let entry = omni_paxos.read(idx).expect("No entry");
    let expected_entries = log.get(idx as usize..=idx as usize).unwrap();
    verify_entries(&[entry], expected_entries, snapshotted_idx, decided_idx);

    // read snapshot
    let snapshot = omni_paxos.read(0).expect("No snapshot");
    verify_snapshot(&[snapshot], snapshotted_idx, &exp_snapshot);

    // read none
    let idx = log.len() as u64;
    let entry = omni_paxos.read(idx);
    assert!(entry.is_none(), "Expected None, got: {:?}", entry);

    // create stopped storage and SequencePaxos to test reading StopSign.
    let ss_temp_dir = create_temp_dir();
    let mut stopped_storage =
        StorageType::<Value, LatestValue>::with(cfg.storage_type, &ss_temp_dir);
    let ss = StopSign::with(2, vec![], None);
    let log_len = log.len() as u64;
    stopped_storage.append_entries(log.clone());
    stopped_storage.set_stopsign(StopSignEntry::with(ss.clone(), true));
    stopped_storage.set_decided_idx(log_len);

    let mut stopped_op = op_config.build(stopped_storage);
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

    let log: Vec<Value> = vec![1, 3, 2, 7, 5, 10, 29, 100, 8, 12]
        .iter()
        .map(|v| Value(*v as u64))
        .collect();
    let decided_idx = 6;
    let snapshotted_idx: u64 = 4;
    let (snapshotted, _suffix) = log.split_at(snapshotted_idx as usize);

    let exp_snapshot = LatestValue::create(snapshotted);

    let temp_dir = create_temp_dir();
    let mut storage = StorageType::<Value, LatestValue>::with(cfg.storage_type, &temp_dir);
    storage.append_entries(log.clone());
    storage.set_decided_idx(decided_idx);

    let mut op_config = OmniPaxosConfig::default();
    op_config.pid = 1;
    op_config.peers = vec![2, 3];
    op_config.configuration_id = 1;
    let mut omni_paxos = op_config.clone().build(storage);
    omni_paxos
        .snapshot(Some(snapshotted_idx), true)
        .expect("Failed to snapshot");

    // read entries only
    let from_idx = snapshotted_idx + 1;
    let entries = omni_paxos
        .read_entries(from_idx..=decided_idx)
        .expect("No entries");
    let expected_entries = log.get(from_idx as usize..=decided_idx as usize).unwrap();
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
    let expected_entries = log.get(snapshotted_idx as usize..to_idx as usize).unwrap();
    verify_snapshot(snapshot, snapshotted_idx, &exp_snapshot);
    verify_entries(suffix, expected_entries, snapshotted_idx, decided_idx);

    // read none
    let from_idx = 0;
    let to_idx = log.len() as u64;
    let entries = omni_paxos.read_entries(from_idx..=to_idx);
    assert!(entries.is_none(), "Expected None, got: {:?}", entries);

    // create stopped storage and SequencePaxos to test reading StopSign.
    let ss_temp_dir = create_temp_dir();
    let mut stopped_storage =
        StorageType::<Value, LatestValue>::with(cfg.storage_type, &ss_temp_dir);
    let ss = StopSign::with(2, vec![], None);
    let log_len = log.len() as u64;
    stopped_storage.append_entries(log.clone());
    stopped_storage.set_stopsign(StopSignEntry::with(ss.clone(), true));
    stopped_storage.set_decided_idx(log_len);

    let mut stopped_op = op_config.build(stopped_storage);
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
    verify_entries(
        prefix,
        log.get(from_idx as usize..).unwrap(),
        from_idx,
        log_len,
    );
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
        log.get(snapshotted_idx as usize..).unwrap(),
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
    verify_snapshot(snapshot, snapshotted_idx, &LatestValue::create(&log));
    verify_stopsign(stopsign, &ss);
}

fn verify_snapshot(
    read_entries: &[LogEntry<Value, LatestValue>],
    exp_compacted_idx: u64,
    exp_snapshot: &LatestValue,
) {
    assert_eq!(
        read_entries.len(),
        1,
        "Expected only snapshot, got: {:?}",
        read_entries
    );
    match read_entries.first().unwrap() {
        LogEntry::Snapshotted(s) => {
            assert_eq!(s.trimmed_idx, exp_compacted_idx);
            assert_eq!(&s.snapshot, exp_snapshot);
        }
        e => {
            panic!("{}", format!("Not a snapshot: {:?}", e))
        }
    }
}

fn verify_stopsign(read_entries: &[LogEntry<Value, LatestValue>], exp_stopsign: &StopSign) {
    assert_eq!(
        read_entries.len(),
        1,
        "Expected StopSign, read: {:?}",
        read_entries
    );
    match read_entries.first().unwrap() {
        LogEntry::StopSign(ss) => {
            assert_eq!(ss, exp_stopsign);
        }
        e => {
            panic!("{}", format!("Not a StopSign: {:?}", e))
        }
    }
}

fn verify_entries(
    read_entries: &[LogEntry<Value, LatestValue>],
    exp_entries: &[Value],
    offset: u64,
    decided_idx: u64,
) {
    assert_eq!(
        read_entries.len(),
        exp_entries.len(),
        "read: {:?}, expected: {:?}",
        read_entries,
        exp_entries
    );
    for (idx, entry) in read_entries.iter().enumerate() {
        let log_idx = idx as u64 + offset;
        match entry {
            LogEntry::Decided(i) if log_idx <= decided_idx => assert_eq!(*i, exp_entries[idx]),
            LogEntry::Undecided(i) if log_idx > decided_idx => assert_eq!(*i, exp_entries[idx]),
            e => panic!(
                "{}",
                format!(
                    "Unexpected entry at idx {}: {:?}, decided_idx: {}",
                    idx, e, decided_idx
                )
            ),
        }
    }
}
/// Verifies that there is a majority when an entry is proposed.
fn check_quorum(
    log_responses: Vec<(u64, Vec<Value>)>,
    quorum_size: usize,
    num_proposals: Vec<Value>,
) {
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
fn check_validity(log_responses: Vec<(u64, Vec<Value>)>, num_proposals: Vec<Value>) {
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
fn check_uniform_agreement(log_responses: Vec<(u64, Vec<Value>)>) {
    let (_, longest_log) = log_responses
        .iter()
        .max_by(|(_, sr), (_, other_sr)| sr.len().cmp(&other_sr.len()))
        .expect("Empty SequenceResp from nodes!");
    for (_, sr) in &log_responses {
        assert!(longest_log.starts_with(sr.as_slice()));
    }

    println!("Pass check_uniform_agreement");
}
