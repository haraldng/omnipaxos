/// This file contains unit-style tests that check the atomicity of storage operations during
/// handling of different messages, while injecting storage errors.
/// We verify this way, that OmniPaxos successfully rolls back changes to a consistent state,
/// when interrupted by a single storage error, before panicking.
///
/// Each test here follows the structure:
///     1. Set up a new OmniPaxos instance.
///     2. Give it any prerequisite messages that are needed to create the state required for the
///        test.
///     3. Schedule a failure in the mock-broken storage.
///     4. Give it the test message and catch the ensuing panic.
///     5. Check if the storage is in a consistent state.
pub mod utils;

use crate::utils::StorageType;
#[cfg(not(feature = "unicache"))]
use omnipaxos::messages::sequence_paxos::{AcceptDecide, Compaction};
#[cfg(feature = "unicache")]
use omnipaxos::storage::Entry;
#[cfg(feature = "unicache")]
use omnipaxos::unicache::UniCache;
use omnipaxos::{
    messages::{
        ballot_leader_election::{BLEMessage, HeartbeatMsg, HeartbeatReply},
        sequence_paxos::{AcceptSync, PaxosMessage, PaxosMsg, Prepare, Promise},
        Message,
    },
    storage::{Snapshot, SnapshotType, Storage},
    util::{LogSync, NodeId, SequenceNumber},
    OmniPaxos, OmniPaxosConfig,
};
use omnipaxos_storage::memory_storage::MemoryStorage;
use serial_test::serial;
use std::{
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{Arc, Mutex},
};
use utils::{BrokenStorageConfig, TestConfig, Value, ValueSnapshot};

type MemoryStore = Arc<Mutex<MemoryStorage<Value>>>;
type BrokenStore = Arc<Mutex<BrokenStorageConfig>>;

/// Creates a new OmniPaxos instance with `BrokenStorage` in its initial state.
/// Also returns an `Arc<Mutex<_>>` pointer to the underlying `MemoryStorage` and
/// `BrokenStorageConfig` to enable injecting storage errors.
fn basic_setup() -> (
    MemoryStore,
    BrokenStore,
    OmniPaxos<Value, StorageType<Value>>,
) {
    let cfg = TestConfig::load("atomic_storage_test").expect("Test config loaded");
    let storage = StorageType::with(cfg.storage_type, "");
    let (mem_storage, storage_conf) = if let StorageType::Broken(ref s, ref c) = storage {
        (s.clone(), c.clone())
    } else {
        panic!("using wrong storage for atomic_storage_test")
    };
    let mut op_config = OmniPaxosConfig::default();
    op_config.server_config.pid = 1;
    op_config.cluster_config.nodes = (1..=cfg.num_nodes as NodeId).collect();
    op_config.cluster_config.configuration_id = 1;
    op_config.server_config.election_tick_timeout = 1; // set tick timeout to 1 as we need to trigger leader change when we call tick() in the tests.
    let op = op_config.build(storage).unwrap();
    (mem_storage, storage_conf, op)
}

/// Creates a new OmniPaxos instance with `BrokenStorage` in a `LEADER ACCEPT` state.
/// Also returns an `Arc<Mutex<_>>` pointer to the underlying `MemoryStorage` and
/// `BrokenStorageConfig` to enable injecting storage errors.
fn _setup_leader() -> (
    MemoryStore,
    BrokenStore,
    OmniPaxos<Value, StorageType<Value>>,
) {
    let (mem_storage, storage_conf, mut op) = setup_follower();
    let mut n = mem_storage.lock().unwrap().get_promise().unwrap().unwrap();
    let n_old = n;
    let setup_msg = Message::<Value>::BLE(BLEMessage {
        from: 2,
        to: 1,
        msg: HeartbeatMsg::Reply(HeartbeatReply {
            round: 1,
            ballot: n_old,
            leader: n_old,
            happy: true,
        }),
    });
    op.handle_incoming(setup_msg);
    op.tick(); // trigger leader change
    let setup_msg = Message::<Value>::BLE(BLEMessage {
        from: 2,
        to: 1,
        msg: HeartbeatMsg::Reply(HeartbeatReply {
            round: 2,
            ballot: n_old,
            leader: n_old,
            happy: false,
        }),
    });
    op.handle_incoming(setup_msg);
    op.tick(); // trigger leader change
    let setup_msg = Message::<Value>::BLE(BLEMessage {
        from: 2,
        to: 1,
        msg: HeartbeatMsg::Reply(HeartbeatReply {
            round: 3,
            ballot: n_old,
            leader: n_old,
            happy: false,
        }),
    });
    op.handle_incoming(setup_msg);
    op.tick(); // trigger leader change
    let msgs = op.outgoing_messages();
    for msg in msgs {
        if let Message::SequencePaxos(ref px_msg) = msg {
            if let PaxosMsg::Prepare(prep) = px_msg.msg {
                n = prep.n;
            }
        }
    }
    let setup_msg = Message::<Value>::SequencePaxos(PaxosMessage {
        from: 2,
        to: 1,
        msg: PaxosMsg::Promise(Promise {
            n,
            decided_idx: 0,
            accepted_idx: 0,
            n_accepted: n_old,
            log_sync: None,
            slots: vec![],
            from: 2,
        }),
    });
    op.handle_incoming(setup_msg);
    assert!(
        op.get_current_leader().expect("should have leader") == 1,
        "should be leader"
    );
    (mem_storage, storage_conf, op)
}

/// Creates a new OmniPaxos instance with `BrokenStorage` in a `FOLLOWER ACCEPT` state.
/// Also returns an `Arc<Mutex<_>>` pointer to the underlying `MemoryStorage` and
/// `BrokenStorageConfig` to enable injecting storage errors.
/// The next expected sequence number is `SequenceNumber{session: 1, counter: 2}`.
fn setup_follower() -> (
    MemoryStore,
    BrokenStore,
    OmniPaxos<Value, StorageType<Value>>,
) {
    let (mem_storage, storage_conf, mut op) = basic_setup();
    let mut n = mem_storage.lock().unwrap().get_promise().unwrap().unwrap();
    n.config_id = 1;
    n.n += 1;
    n.pid = 2;
    let setup_msg = Message::<Value>::SequencePaxos(PaxosMessage {
        from: 2,
        to: 1,
        msg: PaxosMsg::Prepare(Prepare {
            decided_idx: 0,
            accepted_idx: 0,
            n_accepted: mem_storage
                .lock()
                .unwrap()
                .get_accepted_round()
                .unwrap()
                .unwrap_or_default(),
            n,
            forward: false,
        }),
    });
    op.handle_incoming(setup_msg);

    let seq = SequenceNumber {
        session: 1,
        counter: 1,
    };
    let setup_msg = Message::<Value>::SequencePaxos(PaxosMessage {
        from: 2,
        to: 1,
        msg: PaxosMsg::AcceptSync(AcceptSync {
            n,
            seq_num: seq,
            decided_idx: 0,
            log_sync: LogSync {
                decided_snapshot: None,
                suffix: vec![],
                sync_idx: 0,
                stopsign: None,
            },
            #[cfg(feature = "unicache")]
            unicache: <Value as Entry>::UniCache::new(),
        }),
    });
    op.handle_incoming(setup_msg);
    op.outgoing_messages();
    assert!(
        op.get_current_leader().expect("should have leader") == 2,
        "node 2 should be leader"
    );
    (mem_storage, storage_conf, op)
}

#[test]
#[serial]
fn atomic_storage_acceptsync_test() {
    fn run_single_test(fail_after_n_ops: usize) {
        let (mem_storage, storage_conf, mut op) = basic_setup();
        let mut n = mem_storage.lock().unwrap().get_promise().unwrap().unwrap();
        n.n += 1;
        n.pid = 2;
        let setup_msg = Message::<Value>::SequencePaxos(PaxosMessage {
            from: 2,
            to: 1,
            msg: PaxosMsg::Prepare(Prepare {
                decided_idx: 0,
                accepted_idx: 0,
                n_accepted: mem_storage.lock().unwrap().get_promise().unwrap().unwrap(),
                n,
                forward: false,
            }),
        });
        op.handle_incoming(setup_msg);

        let seq = SequenceNumber {
            session: 1,
            counter: 1,
        };
        let old_decided_idx = mem_storage.lock().unwrap().get_decided_idx().unwrap();
        let old_log_len = mem_storage.lock().unwrap().get_log_len().unwrap();
        storage_conf
            .lock()
            .unwrap()
            .schedule_failure_in(fail_after_n_ops);

        let msg = Message::<Value>::SequencePaxos(PaxosMessage {
            from: 2,
            to: 1,
            msg: PaxosMsg::AcceptSync(AcceptSync {
                n,
                seq_num: seq,
                decided_idx: 1,
                log_sync: LogSync {
                    decided_snapshot: None,
                    suffix: vec![Value::with_id(1), Value::with_id(2), Value::with_id(3)],
                    sync_idx: 0,
                    stopsign: None,
                },
                #[cfg(feature = "unicache")]
                unicache: <Value as Entry>::UniCache::new(),
            }),
        });
        let _res = catch_unwind(AssertUnwindSafe(|| op.handle_incoming(msg.clone())));

        // check consistency
        let s = mem_storage.lock().unwrap();
        let new_decided_idx = s.get_decided_idx().unwrap();
        let new_log_len = s.get_log_len().unwrap();
        assert!(
            (old_decided_idx == new_decided_idx && old_log_len == new_log_len)
                || (old_decided_idx != new_decided_idx && old_log_len != new_log_len),
            "decided_idx and log should be updated atomically"
        );
    }
    // run the test with injected failures at different points in time
    for i in 1..10 {
        run_single_test(i);
    }
}

#[cfg(not(feature = "unicache"))]
#[test]
#[serial]
fn atomic_storage_trim_test() {
    fn run_single_test(fail_after_n_ops: usize) {
        let (mem_storage, storage_conf, mut op) = setup_follower();

        let setup_msg = Message::<Value>::SequencePaxos(PaxosMessage {
            from: 2,
            to: 1,
            msg: PaxosMsg::AcceptDecide(AcceptDecide {
                n: mem_storage.lock().unwrap().get_promise().unwrap().unwrap(),
                seq_num: SequenceNumber {
                    session: 1,
                    counter: 2,
                },
                decided_idx: 5,
                entries: vec![
                    Value::with_id(1),
                    Value::with_id(2),
                    Value::with_id(3),
                    Value::with_id(4),
                    Value::with_id(5),
                    Value::with_id(6),
                ],
            }),
        });
        op.handle_incoming(setup_msg);

        let old_compacted_idx = mem_storage.lock().unwrap().get_compacted_idx().unwrap();
        let old_log_len = mem_storage.lock().unwrap().get_log_len().unwrap();
        storage_conf
            .lock()
            .unwrap()
            .schedule_failure_in(fail_after_n_ops);

        // Test handle Trim
        let msg = Message::<Value>::SequencePaxos(PaxosMessage {
            from: 2,
            to: 1,
            msg: PaxosMsg::Compaction(Compaction::Trim(4)),
        });
        let _res = catch_unwind(AssertUnwindSafe(|| op.handle_incoming(msg.clone())));

        // check consistency
        let s = mem_storage.lock().unwrap();
        let new_compacted_idx = s.get_compacted_idx().unwrap();
        let new_log_len = s.get_log_len().unwrap();
        assert!(
            (new_log_len == old_log_len && new_compacted_idx == old_compacted_idx)
                || (new_log_len < old_log_len && new_compacted_idx > old_compacted_idx),
            "compacted_idx and log_len only change together"
        );
        assert!(
            new_log_len + new_compacted_idx == old_log_len + old_compacted_idx,
            "real log len should not change"
        );
    }
    // run the test with injected failures at different points in time
    for i in 1..10 {
        run_single_test(i);
    }
}

#[cfg(not(feature = "unicache"))]
#[test]
#[serial]
fn atomic_storage_snapshot_test() {
    fn run_single_test(fail_after_n_ops: usize) {
        let (mem_storage, storage_conf, mut op) = setup_follower();

        let setup_msg = Message::<Value>::SequencePaxos(PaxosMessage {
            from: 2,
            to: 1,
            msg: PaxosMsg::AcceptDecide(AcceptDecide {
                n: mem_storage.lock().unwrap().get_promise().unwrap().unwrap(),
                seq_num: SequenceNumber {
                    session: 1,
                    counter: 2,
                },
                decided_idx: 5,
                entries: vec![
                    Value::with_id(1),
                    Value::with_id(2),
                    Value::with_id(3),
                    Value::with_id(4),
                    Value::with_id(5),
                    Value::with_id(6),
                ],
            }),
        });
        op.handle_incoming(setup_msg);

        let old_compacted_idx = mem_storage.lock().unwrap().get_compacted_idx().unwrap();
        let old_log_len = mem_storage.lock().unwrap().get_log_len().unwrap();
        storage_conf
            .lock()
            .unwrap()
            .schedule_failure_in(fail_after_n_ops);

        // Test handle Snapshot
        let msg = Message::<Value>::SequencePaxos(PaxosMessage {
            from: 2,
            to: 1,
            msg: PaxosMsg::Compaction(Compaction::Snapshot(Some(4))),
        });
        let _res = catch_unwind(AssertUnwindSafe(|| op.handle_incoming(msg.clone())));

        // check consistency
        let s = mem_storage.lock().unwrap();
        let new_compacted_idx = s.get_compacted_idx().unwrap();
        let new_log_len = s.get_log_len().unwrap();
        let new_snapshot = s.get_snapshot().unwrap();
        assert!(
            (new_log_len == old_log_len && new_compacted_idx == old_compacted_idx)
                || (new_log_len < old_log_len && new_compacted_idx > old_compacted_idx),
            "compacted_idx and log_len only change together"
        );
        assert!(
            new_log_len == old_log_len
                || (new_snapshot.is_some() && new_compacted_idx > old_compacted_idx),
            "trim should only happen if snapshot and compacted_idx are updated successfully"
        );
        assert!(
            new_log_len + new_compacted_idx == old_log_len + old_compacted_idx,
            "real log len should not change"
        );
    }
    // run the test with injected failures at different points in time
    for i in 1..10 {
        run_single_test(i);
    }
}

#[cfg(not(feature = "unicache"))]
#[test]
#[serial]
fn atomic_storage_accept_decide_test() {
    fn run_single_test(fail_after_n_ops: usize) {
        let (mem_storage, storage_conf, mut op) = setup_follower();

        let old_log_len = mem_storage.lock().unwrap().get_log_len().unwrap();
        let old_decided_idx = mem_storage.lock().unwrap().get_decided_idx().unwrap();
        storage_conf
            .lock()
            .unwrap()
            .schedule_failure_in(fail_after_n_ops);

        // Test handle AcceptDecide
        let msg = Message::<Value>::SequencePaxos(PaxosMessage {
            from: 2,
            to: 1,
            msg: PaxosMsg::AcceptDecide(AcceptDecide {
                n: mem_storage.lock().unwrap().get_promise().unwrap().unwrap(),
                seq_num: SequenceNumber {
                    session: 1,
                    counter: 2,
                },
                decided_idx: 5,
                entries: vec![
                    Value::with_id(1),
                    Value::with_id(2),
                    Value::with_id(3),
                    Value::with_id(4),
                    Value::with_id(5),
                    Value::with_id(6),
                ],
            }),
        });
        let _res = catch_unwind(AssertUnwindSafe(|| op.handle_incoming(msg.clone())));

        // check consistency
        let s = mem_storage.lock().unwrap();
        let new_log_len = s.get_log_len().unwrap();
        let new_decided_idx = s.get_decided_idx().unwrap();
        if new_decided_idx > old_decided_idx {
            assert!(
                new_log_len > old_log_len,
                "AcceptDecide operation order didn't ensure safety."
            );
        }
    }
    // run the test with injected failures at different points in time
    for i in 1..10 {
        run_single_test(i);
    }
}

#[test]
#[serial]
fn atomic_storage_majority_promises_test() {
    fn run_single_test(fail_after_n_ops: usize) {
        let (mem_storage, storage_conf, mut op) = setup_follower();
        let mut n = mem_storage.lock().unwrap().get_promise().unwrap().unwrap();
        // Send messages to 1 such that it tries to take over leadership
        let n_old = n;
        let setup_msg = Message::<Value>::BLE(BLEMessage {
            from: 2,
            to: 1,
            msg: HeartbeatMsg::Reply(HeartbeatReply {
                round: 1,
                ballot: n_old,
                leader: n_old,
                happy: true,
            }),
        });
        op.handle_incoming(setup_msg);
        op.tick();
        let setup_msg = Message::<Value>::BLE(BLEMessage {
            from: 2,
            to: 1,
            msg: HeartbeatMsg::Reply(HeartbeatReply {
                round: 2,
                ballot: n_old,
                leader: n_old,
                happy: false,
            }),
        });
        op.handle_incoming(setup_msg);
        let setup_msg = Message::<Value>::BLE(BLEMessage {
            from: 3,
            to: 1,
            msg: HeartbeatMsg::Reply(HeartbeatReply {
                round: 2,
                ballot: n_old,
                leader: n_old,
                happy: false,
            }),
        });
        op.handle_incoming(setup_msg);
        op.tick();
        // Send messages to 1 so it sees it has gained leadership and notifies paxos
        let mut n_new = n_old;
        n_new.n += 1;
        n_new.pid = 1;
        let setup_msg = Message::<Value>::BLE(BLEMessage {
            from: 2,
            to: 1,
            msg: HeartbeatMsg::Reply(HeartbeatReply {
                round: 3,
                ballot: n_new,
                leader: n_new,
                happy: true,
            }),
        });
        op.handle_incoming(setup_msg);
        let setup_msg = Message::<Value>::BLE(BLEMessage {
            from: 3,
            to: 1,
            msg: HeartbeatMsg::Reply(HeartbeatReply {
                round: 3,
                ballot: n_new,
                leader: n_new,
                happy: true,
            }),
        });
        op.handle_incoming(setup_msg);
        op.tick(); // 1 gains leadership here
        let msgs = op.outgoing_messages();
        for msg in msgs {
            if let Message::SequencePaxos(px_msg) = msg {
                if let PaxosMsg::Prepare(prep) = px_msg.msg {
                    n = prep.n_accepted;
                }
            }
        }
        let old_decided_idx = mem_storage.lock().unwrap().get_decided_idx().unwrap();
        let old_compacted_idx = mem_storage.lock().unwrap().get_compacted_idx().unwrap();
        let old_accepted_idx =
            mem_storage.lock().unwrap().get_log_len().unwrap() + old_compacted_idx;
        let old_snapshot = mem_storage.lock().unwrap().get_snapshot().unwrap();
        storage_conf
            .lock()
            .unwrap()
            .schedule_failure_in(fail_after_n_ops);

        let msg = Message::<Value>::SequencePaxos(PaxosMessage {
            from: 2,
            to: 1,
            msg: PaxosMsg::Promise(Promise {
                n,
                decided_idx: 2,
                accepted_idx: 3,
                n_accepted: n_old,
                log_sync: Some(LogSync {
                    decided_snapshot: Some(SnapshotType::Complete(ValueSnapshot::create(&[
                        Value::with_id(1),
                        Value::with_id(2),
                    ]))),
                    suffix: vec![Value::with_id(3)],
                    sync_idx: 2,
                    stopsign: None,
                }),
                slots: vec![],
                from: 2,
            }),
        });
        let _res = catch_unwind(AssertUnwindSafe(|| op.handle_incoming(msg.clone())));

        // check consistency
        let s = mem_storage.lock().unwrap();
        let new_decided_idx = s.get_decided_idx().unwrap();
        let new_accepted_idx = s.get_log_len().unwrap() + s.get_compacted_idx().unwrap();
        let new_snapshot = s.get_snapshot().unwrap();
        let new_accepted_round = s.get_accepted_round().unwrap();
        assert!(
            op.get_current_leader().expect("should have leader") == 1,
            "should be leader"
        );
        assert!(
            old_snapshot.is_none(),
            "sanity check failed: new OP instance has a snapshot set"
        );
        assert!(
            (new_decided_idx == old_decided_idx && new_snapshot.is_none())
                || (new_decided_idx > old_decided_idx && new_snapshot.is_some()),
            "decided_idx and decided_snapshot should be updated atomically"
        );
        assert!(
            (new_accepted_idx == old_accepted_idx && new_accepted_round == Some(n_old))
                || (new_accepted_idx > old_accepted_idx && new_accepted_round == Some(n)),
            "accepted round and the log should be updated atomically"
        );
    }
    // run the test with injected failures at different points in time
    for i in 1..10 {
        run_single_test(i);
    }
}
