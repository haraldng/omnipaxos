pub mod utils;

use crate::utils::StorageType;
use omnipaxos_core::{
    messages::{
        sequence_paxos::{
            AcceptDecide, AcceptStopSign, AcceptSync, Decide, DecideStopSign, PaxosMessage,
            PaxosMsg, Prepare,
        },
        Message,
    },
    omni_paxos::{OmniPaxosConfig, OmniPaxos},
    storage::{Storage, StopSign},
    util::{NodeId, SequenceNumber},
};
use omnipaxos_storage::memory_storage::MemoryStorage;
use serial_test::serial;
use std::{panic::{catch_unwind, AssertUnwindSafe}, sync::{Mutex, Arc}};
use utils::{TestConfig, Value, BrokenStorageConfig};

fn basic_setup() ->
(
    Arc<Mutex<MemoryStorage<Value>>>,
    Arc<Mutex<BrokenStorageConfig>>,
    OmniPaxos<Value, StorageType<Value>>
) {
    let cfg = TestConfig::load("atomic_storage_test").expect("Test config loaded");
    let storage = StorageType::with(cfg.storage_type, "");
    let (mem_storage, storage_conf) = if let StorageType::Broken(ref s, ref c) = storage {
        (s.clone(), c.clone())
    } else {
        panic!("using wrong storage for atomic_storage_test")
    };
    let mut op_config = OmniPaxosConfig::default();
    op_config.pid = 1;
    op_config.peers = (2..=cfg.num_nodes).map(|x| x as NodeId).collect();
    op_config.configuration_id = 1;
    let mut op = op_config.build(storage);
    (mem_storage, storage_conf, op)
}

fn setup_leader() ->
(
    Arc<Mutex<MemoryStorage<Value>>>,
    Arc<Mutex<BrokenStorageConfig>>,
    OmniPaxos<Value, StorageType<Value>>
) {
    let (mem_storage, storage_conf, mut op) = basic_setup();
    (mem_storage, storage_conf, op);
    todo!()
}

fn setup_follower() ->
(
    Arc<Mutex<MemoryStorage<Value>>>,
    Arc<Mutex<BrokenStorageConfig>>,
    OmniPaxos<Value, StorageType<Value>>
) {
    let (mem_storage, storage_conf, mut op) = basic_setup();
    let mut n = mem_storage.lock().unwrap().get_promise().unwrap();
    n.n += 1;
    n.pid = 2;
    let setup_msg = Message::<Value>::SequencePaxos(PaxosMessage {
        from: 2,
        to: 1,
        msg: PaxosMsg::Prepare(Prepare {
            decided_idx: 0,
            accepted_idx: 0,
            n_accepted: mem_storage.lock().unwrap().get_promise().unwrap(),
            n,
        }),
    });
    op.handle_incoming(setup_msg);

    let seq = SequenceNumber{session: 1, counter: 1};

    let setup_msg = Message::<Value>::SequencePaxos(PaxosMessage {
        from: 2,
        to: 1,
        msg: PaxosMsg::AcceptSync(AcceptSync {
            n,
            seq_num: seq,
            decided_snapshot: None,
            suffix: vec![],
            sync_idx: 0,
            decided_idx: 0,
            stopsign: None, 
        }),
    });
    op.handle_incoming(setup_msg);

    (mem_storage, storage_conf, op)
}

#[test]
#[serial]
fn atomic_storage_decide_stopsign_test() {
    let (mem_storage, storage_conf, mut op) = setup_follower();

    let setup_msg = Message::<Value>::SequencePaxos(PaxosMessage {
        from: 2,
        to: 1,
        msg: PaxosMsg::AcceptStopSign(AcceptStopSign {
            n: mem_storage.lock().unwrap().get_promise().unwrap(),
            ss: StopSign { config_id: 2, nodes: vec![1, 2, 3], metadata: None },
        }),
    });
    op.handle_incoming(setup_msg);

    let old_decided_idx = mem_storage.lock().unwrap().get_decided_idx().unwrap();
    storage_conf.lock().unwrap().schedule_failure_in(2);

    // Test handle DecideStopsign rollback
    let msg = Message::<Value>::SequencePaxos(PaxosMessage {
        from: 2,
        to: 1,
        msg: PaxosMsg::DecideStopSign(DecideStopSign {
            n: mem_storage.lock().unwrap().get_promise().unwrap(),
        }),
    });
    let _res = catch_unwind(AssertUnwindSafe(|| op.handle_incoming(msg.clone())));

    // check consistency
    let s = mem_storage.lock().unwrap();
    let new_decided_idx = s.get_decided_idx().unwrap();
    let ss_decided = s.get_stopsign().unwrap().unwrap().decided;
    assert!(
        !ss_decided && new_decided_idx == old_decided_idx
        || ss_decided && new_decided_idx + 1 == old_decided_idx
    );
}

#[test]
#[serial]
fn atomic_storage_acceptsync_test() {
    let (mem_storage, storage_conf, mut op) = basic_setup();
    let mut n = mem_storage.lock().unwrap().get_promise().unwrap();
    n.n += 1;
    n.pid = 2;
    let setup_msg = Message::<Value>::SequencePaxos(PaxosMessage {
        from: 2,
        to: 1,
        msg: PaxosMsg::Prepare(Prepare {
            decided_idx: 0,
            accepted_idx: 0,
            n_accepted: mem_storage.lock().unwrap().get_promise().unwrap(),
            n,
        }),
    });
    op.handle_incoming(setup_msg);

    let seq = SequenceNumber{session: 1, counter: 1};
    let old_decided_idx = mem_storage.lock().unwrap().get_decided_idx().unwrap();
    let old_log_len = mem_storage.lock().unwrap().get_log_len().unwrap();
    // storage_conf.lock().unwrap().schedule_failure_in(1);

    let msg = Message::<Value>::SequencePaxos(PaxosMessage {
        from: 2,
        to: 1,
        msg: PaxosMsg::AcceptSync(AcceptSync {
            n,
            seq_num: seq,
            decided_snapshot: None,
            suffix: vec![Value(1), Value(2), Value(3)],
            sync_idx: 0,
            decided_idx: 1,
            stopsign: None, 
        }),
    });
    let _res = catch_unwind(AssertUnwindSafe(|| op.handle_incoming(msg.clone())));

    // check consistency
    let s = mem_storage.lock().unwrap();
    let new_decided_idx = s.get_decided_idx().unwrap();
    let new_log_len = mem_storage.lock().unwrap().get_log_len().unwrap();
    assert!(
        old_decided_idx == new_decided_idx && old_log_len == new_log_len
        || old_decided_idx != new_decided_idx && old_log_len != new_log_len
    );
}

// TODO:
// Test handle Promise majority rollback
// Test handle AcceptDecide rollback
// Test handle AcceptedStopsign majority rollback
// Test snapshot rollback
// Test trim rollback
