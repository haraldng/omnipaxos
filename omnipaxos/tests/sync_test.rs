pub mod utils;

use omnipaxos::{
    storage::{Snapshot, StopSign, Storage},
    ClusterConfig,
};
use omnipaxos_storage::memory_storage::MemoryStorage;
use serial_test::serial;
use std::{thread, time::Duration};
use utils::{
    verification::{verify_log, verify_stopsign},
    LatestValue, StorageType, TestConfig, TestSystem, Value,
};

#[derive(Default)]
struct SyncTest {
    leaders_snapshot: Option<LatestValue>,
    leaders_snapshotted_entries: Option<Vec<Value>>,
    leaders_log: Vec<Value>,
    leaders_ss: Option<StopSign>,
    leaders_dec_idx: u64,
    followers_snapshot: Option<LatestValue>,
    followers_snapshotted_entries: Option<Vec<Value>>,
    followers_log: Vec<Value>,
    followers_ss: Option<StopSign>,
    followers_dec_idx: u64,
    followers_ballot_is_outdated: bool,
}

fn sync_test(sync_test: SyncTest) {
    // Set up leader's memory
    let mut leaders_memory = MemoryStorage::default();
    if let Some(entries) = &sync_test.leaders_snapshotted_entries {
        leaders_memory
            .set_snapshot(sync_test.leaders_snapshot)
            .unwrap();
        leaders_memory
            .set_compacted_idx(entries.len() as u64)
            .unwrap();
    }
    leaders_memory
        .append_entries(sync_test.leaders_log.clone())
        .unwrap();
    leaders_memory
        .set_stopsign(sync_test.leaders_ss.clone())
        .unwrap();
    leaders_memory
        .set_decided_idx(sync_test.leaders_dec_idx)
        .unwrap();

    // Set up follower's memory
    let mut followers_memory = MemoryStorage::default();
    if let Some(entries) = &sync_test.followers_snapshotted_entries {
        followers_memory
            .set_snapshot(sync_test.followers_snapshot)
            .unwrap();
        followers_memory
            .set_compacted_idx(entries.len() as u64)
            .unwrap();
    }
    followers_memory
        .append_entries(sync_test.followers_log.clone())
        .unwrap();
    followers_memory
        .set_stopsign(sync_test.followers_ss.clone())
        .unwrap();
    followers_memory
        .set_decided_idx(sync_test.followers_dec_idx)
        .unwrap();

    // Start a Kompact system with no nodes
    let cfg = TestConfig::load("sync_test").expect("Test config couldn't be loaded");
    let mut sys = TestSystem::with(cfg);
    sys.start_all_nodes();
    for node_id in 1..=cfg.num_nodes as u64 {
        sys.kill_node(node_id);
    }

    // Re-create nodes with initial memory
    sys.create_node(1, &cfg, StorageType::with_memory(followers_memory));
    sys.create_node(2, &cfg, StorageType::with_memory(leaders_memory.clone()));
    sys.create_node(3, &cfg, StorageType::with_memory(leaders_memory));
    sys.start_node(2);
    sys.start_node(3);

    // Force a leader change to increase ballot number and make followers ballot outdated
    if sync_test.followers_ballot_is_outdated {
        let leader_id = sys.get_elected_leader(2, Duration::from_millis(cfg.wait_timeout_ms));
        let leader = sys.nodes.get(&leader_id).unwrap();
        leader.on_definition(|comp| {
            comp.paxos.election_timeout();
            comp.paxos.election_timeout();
        });
        sys.get_next_leader(2, Duration::from_millis(cfg.wait_timeout_ms));
    }

    // Wait for follower to finish syncing
    sys.start_node(1);
    thread::sleep(Duration::from_millis(100));

    // Verify log
    let follower = sys.nodes.get(&1).unwrap();
    let mut followers_log = follower.on_definition(|comp| {
        comp.paxos
            .read_decided_suffix(0)
            .expect("Cannot read decided log entry")
    });
    if let Some(ss) = &sync_test.leaders_ss {
        let followers_ss = followers_log.pop().expect("Follower had no entries");
        verify_stopsign(&[followers_ss], ss);
    }

    let leaders_log = match sync_test.leaders_snapshotted_entries {
        Some(entries) => [entries, sync_test.leaders_log].concat(),
        None => sync_test.leaders_log,
    };
    verify_log(followers_log, leaders_log);
}

#[test]
#[serial]
fn sync_basic_test() {
    // Define leader's log
    let snapshotted_log: Vec<Value> = [1, 2].into_iter().map(|x| Value(x)).collect();
    let leaders_snapshot = LatestValue::create(&snapshotted_log);
    let leaders_log = [3, 4, 5, 10, 11, 12]
        .into_iter()
        .map(|x| Value(x))
        .collect();
    let leaders_dec_idx = 5;
    let mut leaders_ss = StopSign::with(ClusterConfig::default(), None);
    leaders_ss.next_config.configuration_id = 2;
    leaders_ss.next_config.nodes = vec![1, 2, 3];

    // Define follower's log
    let followers_log = [1, 2, 3, 6, 7].into_iter().map(|x| Value(x)).collect();
    let followers_dec_idx = 3;

    let test = SyncTest {
        leaders_snapshot: Some(leaders_snapshot),
        leaders_snapshotted_entries: Some(snapshotted_log),
        leaders_log,
        leaders_ss: Some(leaders_ss),
        leaders_dec_idx,
        followers_log,
        followers_dec_idx,
        followers_ballot_is_outdated: true,
        ..Default::default()
    };
    sync_test(test);
}

#[test]
#[serial]
fn sync_decided_stopsign_test() {
    // Define leader's log
    let leaders_log = [1, 2, 3, 4, 5].into_iter().map(|x| Value(x)).collect();
    let leaders_dec_idx = 6;
    let mut leaders_ss = StopSign::with(ClusterConfig::default(), None);
    leaders_ss.next_config.configuration_id = 2;
    leaders_ss.next_config.nodes = vec![1, 2, 3];

    // Define follower's log
    let followers_log = [1, 2, 3, 6, 7].into_iter().map(|x| Value(x)).collect();
    let followers_dec_idx = 3;

    let test = SyncTest {
        leaders_log,
        leaders_ss: Some(leaders_ss),
        leaders_dec_idx,
        followers_log,
        followers_dec_idx,
        followers_ballot_is_outdated: true,
        ..Default::default()
    };
    sync_test(test);
}

#[test]
#[serial]
fn sync_only_stopsign_test() {
    // Define leader's log
    let leaders_dec_idx = 1;
    let mut leaders_ss = StopSign::with(ClusterConfig::default(), None);
    leaders_ss.next_config.configuration_id = 2;
    leaders_ss.next_config.nodes = vec![1, 2, 3];

    // Define follower's log
    let followers_dec_idx = 0;

    let test = SyncTest {
        leaders_ss: Some(leaders_ss),
        leaders_dec_idx,
        followers_dec_idx,
        ..Default::default()
    };
    sync_test(test);
}

#[test]
#[serial]
fn sync_only_snapshot_test() {
    // Define leader's log
    let snapshotted_log: Vec<Value> = [1, 2, 3].into_iter().map(|x| Value(x)).collect();
    let leaders_snapshot = LatestValue::create(&snapshotted_log);
    let leaders_dec_idx = 3;

    // Define follower's log
    let followers_dec_idx = 0;

    let test = SyncTest {
        leaders_snapshot: Some(leaders_snapshot),
        leaders_snapshotted_entries: Some(snapshotted_log),
        leaders_dec_idx,
        followers_dec_idx,
        ..Default::default()
    };
    sync_test(test);
}

#[test]
#[serial]
fn sync_follower_snapshot_test() {
    // Define leader's log
    let leaders_log = [1, 2, 3, 4, 5].into_iter().map(|x| Value(x)).collect();
    let leaders_dec_idx = 5;

    // Define follower's log
    let snapshotted_log: Vec<Value> = [1, 2, 3].into_iter().map(|x| Value(x)).collect();
    let followers_snapshot = LatestValue::create(&snapshotted_log);
    let followers_log = [4].into_iter().map(|x| Value(x)).collect();
    let followers_dec_idx = 4;

    let test = SyncTest {
        leaders_log,
        leaders_dec_idx,
        followers_snapshot: Some(followers_snapshot),
        followers_snapshotted_entries: Some(snapshotted_log),
        followers_log,
        followers_dec_idx,
        ..Default::default()
    };
    sync_test(test);
}
