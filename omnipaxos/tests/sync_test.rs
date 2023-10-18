pub mod utils;

use crate::utils::STOPSIGN_ID;
use kompact::prelude::{promise, Ask, FutureCollection};
use omnipaxos::{storage::StopSign, ClusterConfig};
use serial_test::serial;
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
        .map(Value::with_id)
        .collect();
    let leaders_dec_idx = 5;
    let leaders_compacted_idx = 2;
    let cluster_config = ClusterConfig::default();
    let mut leaders_ss = StopSign::with(cluster_config, None);
    leaders_ss.next_config.configuration_id = 2;
    leaders_ss.next_config.nodes = vec![1, 2, 3];

    // Define follower's log
    let followers_log = [1, 2, 3, 6, 7, 8, 9]
        .into_iter()
        .map(Value::with_id)
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
    let leaders_log = [1, 2, 3, 4, 5].into_iter().map(Value::with_id).collect();
    let leaders_dec_idx = 6;
    let cluster_config = ClusterConfig::default();
    let mut leaders_ss = StopSign::with(cluster_config, None);
    leaders_ss.next_config.configuration_id = 2;
    leaders_ss.next_config.nodes = vec![1, 2, 3];

    // Define follower's log
    let followers_log = [1, 2, 3, 6, 7].into_iter().map(Value::with_id).collect();
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
    let cluster_config = ClusterConfig::default();
    let mut leaders_ss = StopSign::with(cluster_config, None);
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
    let leaders_log: Vec<Value> = [1, 2, 3].into_iter().map(Value::with_id).collect();
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
    let leaders_log = [1, 2, 3, 4, 5].into_iter().map(Value::with_id).collect();
    let leaders_dec_idx = 5;

    // Define follower's log
    let followers_log = [1, 2, 3, 4].into_iter().map(Value::with_id).collect();
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
    let leaders_new_decided = &test.leaders_log[test.followers_dec_idx..leaders_log_dec_idx];
    let leaders_accepted = &test.leaders_log[leaders_log_dec_idx..];
    let followers_missing_entries = &test.leaders_log[test.followers_dec_idx..];

    // Set up followers log
    let follower_id = sys.get_elected_leader(1, cfg.wait_timeout);
    let follower = sys.nodes.get(&follower_id).unwrap();
    sys.make_proposals(follower_id, followers_decided.to_vec(), cfg.wait_timeout);
    sys.set_node_connections(follower_id, false);
    follower.on_definition(|x| {
        if let Some(compact_idx) = test.followers_compacted_idx {
            x.paxos
                .snapshot(Some(compact_idx), true)
                .expect("Couldn't snapshot");
        }
        for entry in followers_accepted {
            x.paxos.append(entry.clone()).expect("Couldn't append");
        }
    });

    // Wait a bit so next leader is stabilized (otherwise we can lose proposals)
    std::thread::sleep(20 * cfg.election_timeout);
    let node = sys.nodes.keys().find(|x| **x != follower_id).unwrap(); // a node that can actually see the current leader.
    let leader_id = sys.get_elected_leader(*node, cfg.wait_timeout);
    assert_ne!(follower_id, leader_id, "New leader must be chosen!");

    // Set up leaders log
    let leader = sys.nodes.get(&leader_id).unwrap();
    // Propose leader's decided entries
    sys.make_proposals(leader_id, leaders_new_decided.into(), cfg.wait_timeout);
    match test.leaders_ss.clone() {
        Some(ss) if leaders_ss_is_decided => {
            sys.reconfigure(leader_id, ss.next_config, ss.metadata, cfg.wait_timeout)
        }
        _ => (),
    }

    // Propose leader's accepted entries and also snapshot. To ensure they are only accepted, stop a write quorum of nodes.
    let write_quorum_size = match cfg.flexible_quorum {
        Some((_, write_quorum)) => write_quorum,
        None => cfg.num_nodes / 2 + 1,
    };
    let num_nodes_to_stop = cfg.num_nodes - write_quorum_size;
    let nodes_to_stop = (1..cfg.num_nodes as u64)
        .filter(|&n| n != follower_id && n != leader_id)
        .take(num_nodes_to_stop);
    nodes_to_stop.for_each(|pid| sys.stop_node(pid));
    leader.on_definition(|x| {
        if let Some(compact_idx) = test.leaders_compacted_idx {
            x.paxos
                .snapshot(Some(compact_idx), true)
                .expect("Couldn't snapshot");
        }
        for entry in leaders_accepted {
            x.paxos.append(entry.clone()).expect("Couldn't append");
        }
        match &test.leaders_ss {
            Some(ss) if !leaders_ss_is_decided => {
                x.paxos
                    .reconfigure(ss.next_config.clone(), ss.metadata.clone())
                    .expect("Couldn't reconfigure");
            }
            _ => (),
        }
    });

    // Reconnect follower and wait for new entries from AccSync to be decided so we can verify log
    let mut proposal_futures = vec![];
    follower.on_definition(|x| {
        for v in followers_missing_entries {
            let (kprom, kfuture) = promise::<()>();
            x.insert_decided_future(Ask::new(kprom, v.clone()));
            proposal_futures.push(kfuture);
        }
    });
    if test.leaders_ss.is_some() {
        let (kprom, kfuture) = promise::<()>();
        follower.on_definition(|x| {
            x.insert_decided_future(Ask::new(kprom, Value::with_id(STOPSIGN_ID)));
        });
        proposal_futures.push(kfuture);
    }
    sys.set_node_connections(follower_id, true);
    match FutureCollection::collect_with_timeout::<Vec<_>>(proposal_futures, cfg.wait_timeout) {
        Ok(_) => {}
        Err(e) => {
            let follower_entries = follower.on_definition(|x| x.read_decided_log());
            let leader_entries = leader.on_definition(|x| x.read_decided_log());
            panic!("Error on collecting futures of decided proposals: {}. Follower log: {:?}, Leader log: {:?}", e, follower_entries, leader_entries);
        }
    }

    // Verify log
    let mut followers_entries = follower.on_definition(|x| x.read_decided_log());
    if let Some(ss) = &test.leaders_ss {
        let followers_ss = followers_entries.pop().expect("Follower had no entries");
        verify_stopsign(&[followers_ss], ss);
    }
    verify_log(followers_entries, test.leaders_log);
}
