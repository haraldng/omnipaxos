pub mod utils;

use omnipaxos::{storage::StopSign, ClusterConfig};
use serial_test::serial;
use std::time::Duration;
use utils::{
    verification::{verify_log, verify_stopsign},
    TestConfig, TestSystem, Value,
};

/// The state of the leader's and follower's log at the time of a sync
#[derive(Default)]
struct SyncTest {
    leaders_log: Vec<Value>,
    leaders_dec_idx: usize,
    leaders_compacted_idx: Option<u64>,
    leaders_ss: Option<StopSign>,
    followers_log: Vec<Value>,
    followers_dec_idx: usize,
    followers_compacted_idx: Option<u64>,
}

/// Tests that a leader whose log consists of everything a log can be made up of (snapshot, decided entries,
/// undecided entries, stopsign), correctly syncs a follower who is missing decided entries and
/// has invalid undecided entries.
#[test]
#[serial]
fn sync_full_test() {
    // Define leader's log
    let leaders_log = [1, 2, 3, 4, 5, 10, 11, 12]
        .into_iter()
        .map(|x| Value(x))
        .collect();
    let leaders_dec_idx = 5;
    let leaders_compacted_idx = 2;
    let mut leaders_ss = StopSign::with(ClusterConfig::default(), None);
    leaders_ss.next_config.configuration_id = 2;
    leaders_ss.next_config.nodes = vec![1, 2, 3];

    // Define follower's log
    let followers_log = [1, 2, 3, 6, 7, 8, 9]
        .into_iter()
        .map(|x| Value(x))
        .collect();
    let followers_dec_idx = 3;

    let test = SyncTest {
        leaders_log,
        leaders_dec_idx,
        leaders_compacted_idx: Some(leaders_compacted_idx),
        leaders_ss: Some(leaders_ss),
        followers_log,
        followers_dec_idx,
        ..Default::default()
    };
    sync_test(test);
}

/// Tests that a leader, who has a decided stopsign, correctly syncs a follower who is missing
/// decided entries and has invalid undecided entries.
#[test]
#[serial]
fn sync_decided_ss_test() {
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
        ..Default::default()
    };
    sync_test(test);
}

/// Tests that a leader whose log consists of only a stopsign correctly syncs the follower.
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

/// Tests that the leader syncs the follower using a decided snapshot which the follower correctly
/// merges onto their empty log.
#[test]
#[serial]
fn sync_only_snapshot_test() {
    // Define leader's log
    let leaders_log: Vec<Value> = [1, 2, 3].into_iter().map(|x| Value(x)).collect();
    let leaders_dec_idx = 3;
    let leaders_compacted_idx = 3;

    // Define follower's log
    let followers_dec_idx = 0;

    let test = SyncTest {
        leaders_log,
        leaders_dec_idx,
        leaders_compacted_idx: Some(leaders_compacted_idx),
        followers_dec_idx,
        ..Default::default()
    };
    sync_test(test);
}

/// Tests that the leader syncs the follower using a decided snapshot which correctly merges onto
/// the partly-snapshotted decided entries of the follower.
#[test]
#[serial]
fn sync_follower_snapshot_test() {
    // Define leader's log
    let leaders_log = [1, 2, 3, 4, 5].into_iter().map(|x| Value(x)).collect();
    let leaders_dec_idx = 5;

    // Define follower's log
    let followers_log = [1, 2, 3, 4].into_iter().map(|x| Value(x)).collect();
    let followers_dec_idx = 4;
    let followers_compacted_idx = 3;

    let test = SyncTest {
        leaders_log,
        leaders_dec_idx,
        followers_log,
        followers_dec_idx,
        followers_compacted_idx: Some(followers_compacted_idx),
        ..Default::default()
    };
    sync_test(test);
}

/// Creates a TestSystem cluster which sets up a scenario such that a follower is
/// disconnected from the cluster, is reconnected, and is synced by the leader. The state of the
/// leader's and follower's log at the time of the sync is given by the SyncTest argument.
fn sync_test(test: SyncTest) {
    // Start a Kompact system
    let cfg = TestConfig::load("sync_test").expect("Test config couldn't be loaded");
    let wait_timeout = Duration::from_millis(cfg.wait_timeout_ms);
    let sys = TestSystem::with(cfg);
    sys.start_all_nodes();

    let (followers_decided, followers_accepted) =
        test.followers_log.split_at(test.followers_dec_idx);
    let leaders_ss_is_decided =
        test.leaders_ss.is_some() && test.leaders_dec_idx > test.leaders_log.len();
    let leaders_log_dec_idx = if leaders_ss_is_decided {
        test.leaders_dec_idx - 1
    } else {
        test.leaders_dec_idx
    };
    let leaders_new_decided =
        test.leaders_log[test.followers_dec_idx..leaders_log_dec_idx].to_vec();
    let leaders_accepted = test.leaders_log[leaders_log_dec_idx..].to_vec();

    // Set up followers log
    let follower_id = sys.get_elected_leader(1, wait_timeout);
    let follower = sys.nodes.get(&follower_id).unwrap();
    sys.make_proposals(follower_id, followers_decided.to_vec(), wait_timeout);
    sys.set_node_connections(follower_id, false);
    follower.on_definition(|comp| {
        if let Some(compact_idx) = test.followers_compacted_idx {
            comp.paxos
                .snapshot(Some(compact_idx), true)
                .expect("Couldn't snapshot");
        }
        for entry in followers_accepted.to_vec() {
            comp.paxos.append(entry).expect("Couldn't append");
        }
    });

    // Set up leaders log
    // Wait a bit so next leader is stabilized (otherwise we can lose proposals)
    std::thread::sleep(Duration::from_millis(4 * cfg.election_timeout_ms));
    let leader_id = sys.get_elected_leader(1, wait_timeout);
    assert_ne!(follower_id, leader_id, "New leader must be chosen!");
    let leader = sys.nodes.get(&leader_id).unwrap();
    // Propose leader's decided entries
    sys.make_proposals(leader_id, leaders_new_decided, wait_timeout);
    match test.leaders_ss.clone() {
        Some(ss) if leaders_ss_is_decided => {
            sys.reconfigure(leader_id, ss.next_config, ss.metadata, wait_timeout)
        }
        _ => (),
    }
    // Propose leader's accepted entries and also snapshot
    let write_quorum_size = match cfg.flexible_quorum {
        Some((_, write_quorum)) => write_quorum,
        None => cfg.num_nodes / 2 + 1,
    };
    let num_nodes_to_stop = cfg.num_nodes - write_quorum_size;
    let nodes_to_stop = (1..cfg.num_nodes as u64)
        .filter(|&n| n != follower_id && n != leader_id)
        .take(num_nodes_to_stop);
    for pid in nodes_to_stop {
        sys.stop_node(pid);
    }
    leader.on_definition(|comp| {
        if let Some(compact_idx) = test.leaders_compacted_idx {
            comp.paxos
                .snapshot(Some(compact_idx), true)
                .expect("Couldn't snapshot");
        }
        for entry in leaders_accepted {
            comp.paxos.append(entry).expect("Couldn't append");
        }
        match test.leaders_ss.clone() {
            Some(ss) if !leaders_ss_is_decided => {
                comp.paxos
                    .reconfigure(ss.next_config, ss.metadata)
                    .expect("Couldn't reconfigure");
            }
            _ => (),
        }
    });

    // Reconnect follower and wait for sync
    sys.set_node_connections(follower_id, true);
    let wait_timeout = Duration::from_millis(500);
    std::thread::sleep(wait_timeout);

    // Verify log
    let mut followers_entries = follower.on_definition(|comp| {
        comp.paxos
            .read_decided_suffix(0)
            .expect("Cannot read log entry")
    });
    if let Some(ss) = &test.leaders_ss {
        let followers_ss = followers_entries.pop().expect("Follower had no entries");
        verify_stopsign(&[followers_ss], ss);
    }
    verify_log(followers_entries, test.leaders_log);
}
