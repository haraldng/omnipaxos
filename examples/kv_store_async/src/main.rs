//! KV-store example built on the async runtime layer.
//!
//! Contrast with `examples/kv_store/`: no `Arc<Mutex<>>`, no `tokio::select!` loop,
//! no polling of `read_decided_suffix` after append, no polling for leader election.
//! The actor owns all of that. The application just:
//!
//! 1. Spawns each node with `spawn_actor::<_, _, TokioRuntime>`.
//! 2. Wires in-process message transport (one task per node).
//! 3. Awaits `append_notify(...)` and reads events off `subscribe_events()`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use omnipaxos::messages::Message;
use omnipaxos::runtime::{
    spawn_actor, OmniPaxosEvent, OmniPaxosHandle, RuntimeConfig, TokioRuntime,
};
use omnipaxos::util::{LogEntry, NodeId};
use omnipaxos::{ClusterConfig, OmniPaxosConfig, ServerConfig};
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
                let dst = match &msg {
                    Message::SequencePaxos(p) => p.to,
                    Message::BLE(b) => b.to,
                };
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

    // 3. Wait for a leader by asking any node (fast poll instead of sleep — but this is
    //    optional: `append_notify` on a follower will also work, it just takes longer).
    let leader = loop {
        if let Some((pid, true)) = handles[&1].current_leader().await {
            break pid;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    println!("Elected leader: {leader}");

    // 4. Append entries via the leader and await decision. No sleep, no polling, no lock —
    //    the Future resolves exactly when the entry is decided. `append_notify` requires
    //    the entry to land locally, so it must be called on the leader; a follower call
    //    returns `AppendError::NotLeader` (fire-and-forget `append` on a follower forwards).
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
        println!("Adding value {:?} via leader {leader}", kv);
        let idx = handles[&leader]
            .append_notify(kv)
            .await
            .expect("append_notify failed");
        println!("  -> decided at log idx {idx}");
    }

    // 5. Materialize the KV store from the decided log.
    let mut store = HashMap::new();
    let ents = handles[&leader].read_decided_suffix(0).await.expect("read");
    for e in ents {
        if let LogEntry::Decided(kv) = e {
            store.insert(kv.key, kv.value);
        }
    }
    println!("KV store: {store:?}");
}
