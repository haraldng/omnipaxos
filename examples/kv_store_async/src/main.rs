//! KV-store example built on the async runtime layer.
//!
//! Contrast with `examples/kv_store/`: no `Arc<Mutex<>>`, no `tokio::select!` loop,
//! no polling of `read_decided_suffix` after append, no polling for leader election.
//! The actor owns all of that. The application just:
//!
//! 1. Spawns each node with `spawn_actor::<_, _, TokioRuntime>`.
//! 2. Wires in-process message transport (one task per node).
//! 3. Reads events off `subscribe_events()` and applies the decided log via
//!    `subscribe_decided()` — no per-entry confirmation needed for ordinary writes.
//! 4. Uses `append_notify(...)` only for the one entry where the caller specifically
//!    needs to know its durable decided index before proceeding.

use std::{collections::HashMap, sync::Arc};

use omnipaxos::{
    util::{LogEntry, NodeId},
    ClusterConfig, OmniPaxosConfig, ServerConfig,
};
use omnipaxos_runtime::{
    spawn_actor, OmniPaxosEvent, OmniPaxosHandle, RuntimeConfig, TokioRuntime,
};
use omnipaxos_storage::memory_storage::MemoryStorage;

mod kv;
use kv::KeyValue;

const SERVERS: [NodeId; 3] = [1, 2, 3];

fn build_config(pid: NodeId) -> OmniPaxosConfig {
    OmniPaxosConfig {
        cluster_config: ClusterConfig {
            configuration_id: 1,
            nodes: SERVERS.into(),
            ..Default::default()
        },
        server_config: ServerConfig {
            pid,
            election_tick_timeout: 5,
            ..Default::default()
        },
    }
}

/// Wire each node's `outgoing_messages()` stream into every peer's `handle_incoming`.
/// One task per node; each task forwards messages until its outgoing receiver closes.
fn wire_transport(handles: &HashMap<NodeId, OmniPaxosHandle<KeyValue>>) {
    let handles = Arc::new(handles.clone());
    for &pid in SERVERS.iter() {
        let out = handles[&pid].outgoing_messages();
        let peers = handles.clone();
        tokio::spawn(async move {
            while let Ok(msg) = out.recv().await {
                let dst = msg.get_receiver();
                if let Some(peer) = peers.get(&dst) {
                    peer.handle_incoming(msg).await;
                }
            }
        });
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    // 1. Spawn each node's actor. `spawn_actor` returns a cloneable handle.
    let mut handles: HashMap<NodeId, OmniPaxosHandle<KeyValue>> = HashMap::new();
    for pid in SERVERS {
        let op = build_config(pid)
            .build::<KeyValue, MemoryStorage<KeyValue>>(MemoryStorage::default())
            .unwrap();
        let h = spawn_actor::<_, _, TokioRuntime>(op, RuntimeConfig::default());
        handles.insert(pid, h);
    }
    wire_transport(&handles);

    // 2. Subscribe to events on one node. Any handle would do — events reflect that node's
    //    view. We spawn a task that prints leadership changes as they happen.
    let events = handles[&1].subscribe_events();
    tokio::spawn(async move {
        while let Ok(event) = events.recv().await {
            match event {
                OmniPaxosEvent::LeaderElected { pid, accepted } => {
                    println!("[node 1] LeaderElected: pid={pid}, accepted={accepted}");
                }
                OmniPaxosEvent::Decided { new_decided_idx } => {
                    println!("[node 1] Decided: new_decided_idx={new_decided_idx}");
                }
                OmniPaxosEvent::Reconfigured { stop_sign_idx } => {
                    println!("[node 1] Reconfigured at idx={stop_sign_idx}");
                }
            }
        }
    });

    // 3. Subscribe to the decided log stream on node 1 and run a state machine that
    //    materializes the KV store as entries arrive. No polling, no cursor
    //    bookkeeping — the stream pushes each decided entry in order.
    let decided = handles[&1].subscribe_decided(0).await;
    let store_task = tokio::spawn(async move {
        let mut store: HashMap<String, u64> = HashMap::new();
        while let Ok(entry) = decided.recv().await {
            if let LogEntry::Decided(kv) = entry {
                store.insert(kv.key.clone(), kv.value);
                println!("[state machine] applied {kv:?} — store now {store:?}");
                if store.len() == 3 {
                    // We know we're finishing after three writes — return the final state.
                    break;
                }
            }
        }
        store
    });

    // 4. Append ordinary writes via node 1 (which may be leader or follower — the actor
    //    routes tagged proposals to the current leader). We don't need to know exactly
    //    when each one lands; the state machine above picks them up off the decided
    //    stream as they arrive, so plain fire-and-forget `append` is all we need here.
    for kv in [
        KeyValue {
            key: "a".into(),
            value: 1,
        },
        KeyValue {
            key: "b".into(),
            value: 2,
        },
    ] {
        println!("Adding value {:?} via node 1 (fire-and-forget)", kv);
        handles[&1].append(kv).await.expect("append failed");
    }

    // 4b. "quota" is different: it's a gating value an operator needs to announce as
    //    live, so before doing that we need to know its *exact* decided index rather
    //    than just observing it eventually via the stream. This is what `append_notify`
    //    is for — a specific entry whose durability the caller must confirm
    //    synchronously. It waits until a leader is known and the entry is decided at
    //    its assigned index. No sleep, no polling, no lock.
    let quota = KeyValue {
        key: "quota".into(),
        value: 100,
    };
    println!(
        "Adding value {:?} via node 1 (must confirm durability)",
        quota
    );
    let idx = handles[&1]
        .append_notify(quota)
        .await
        .expect("append_notify failed");
    println!("  -> quota entry durably decided at log idx {idx}, safe to announce");

    // 5. Wait for the state machine to finish applying and print the final store.
    let store = store_task.await.expect("state machine task panicked");
    println!("Final KV store: {store:?}");
}
