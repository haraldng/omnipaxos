use crate::{kv::KeyValue, server::OmniPaxosServer, util::*};
use omnipaxos::{
    messages::Message,
    util::{LogEntry, NodeId},
    *,
};
use omnipaxos_storage::memory_storage::MemoryStorage;
use tokio::{runtime::Builder, sync::mpsc};

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

mod kv;
mod server;
mod util;

type OmniPaxosKV = OmniPaxos<KeyValue, MemoryStorage<KeyValue>>;

const SERVERS: [u64; 3] = [1, 2, 3];

#[allow(clippy::type_complexity)]
fn initialise_channels() -> (
    HashMap<NodeId, mpsc::Sender<Message<KeyValue>>>,
    HashMap<NodeId, mpsc::Receiver<Message<KeyValue>>>,
) {
    let mut sender_channels = HashMap::new();
    let mut receiver_channels = HashMap::new();

    for pid in SERVERS {
        let (sender, receiver) = mpsc::channel(BUFFER_SIZE);
        sender_channels.insert(pid, sender);
        receiver_channels.insert(pid, receiver);
    }
    (sender_channels, receiver_channels)
}

fn main() {
    let runtime = Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let configuration_id = 1;
    let mut op_server_handles = HashMap::new();
    let (sender_channels, mut receiver_channels) = initialise_channels();

    for pid in SERVERS {
        let peers = SERVERS.iter().filter(|&&p| p != pid).copied().collect();
        let op_config = if pid == 1 {
            // use a toml file to configure the first node
            let mut op_config_temp = OmniPaxosConfig::with_toml(&*(BASE_PATH.to_owned() + "src/configs/node1.toml")).unwrap();
            op_config_temp.peers = peers;
            op_config_temp
        } else {
            OmniPaxosConfig {
                pid,
                configuration_id,
                peers,
                ..Default::default()
            }
        };
        let omni_paxos: Arc<Mutex<OmniPaxosKV>> =
            Arc::new(Mutex::new(op_config.build(MemoryStorage::default())));
        let mut op_server = OmniPaxosServer {
            omni_paxos: Arc::clone(&omni_paxos),
            incoming: receiver_channels.remove(&pid).unwrap(),
            outgoing: sender_channels.clone(),
        };
        let join_handle = runtime.spawn({
            async move {
                op_server.run().await;
            }
        });
        op_server_handles.insert(pid, (omni_paxos, join_handle));
    }

    // wait for leader to be elected...
    std::thread::sleep(WAIT_LEADER_TIMEOUT);
    let (first_server, _) = op_server_handles.get(&1).unwrap();
    // check which server is the current leader
    let leader = first_server
        .lock()
        .unwrap()
        .get_current_leader()
        .expect("Failed to get leader");
    println!("Elected leader: {}", leader);

    let follower = SERVERS.iter().find(|&&p| p != leader).unwrap();
    let (follower_server, _) = op_server_handles.get(follower).unwrap();
    // append kv1 to the replicated log via follower
    let kv1 = KeyValue {
        key: "a".to_string(),
        value: 1,
    };
    println!("Adding value: {:?} via server {}", kv1, follower);
    follower_server
        .lock()
        .unwrap()
        .append(kv1)
        .expect("append failed");
    // append kv2 to the replicated log via the leader
    let kv2 = KeyValue {
        key: "b".to_string(),
        value: 2,
    };
    println!("Adding value: {:?} via server {}", kv2, leader);
    let (leader_server, leader_join_handle) = op_server_handles.get(&leader).unwrap();
    leader_server
        .lock()
        .unwrap()
        .append(kv2)
        .expect("append failed");
    // wait for the entries to be decided...
    std::thread::sleep(WAIT_DECIDED_TIMEOUT);
    let committed_ents = leader_server
        .lock()
        .unwrap()
        .read_decided_suffix(0)
        .expect("Failed to read expected entries");

    let mut simple_kv_store = HashMap::new();
    for ent in committed_ents {
        if let LogEntry::Decided(kv) = ent {
            simple_kv_store.insert(kv.key, kv.value);
        }
        // ignore uncommitted entries
    }
    println!("KV store: {:?}", simple_kv_store);
    println!("Killing leader: {}...", leader);
    leader_join_handle.abort();
    // wait for new leader to be elected...
    std::thread::sleep(WAIT_LEADER_TIMEOUT);
    let leader = follower_server
        .lock()
        .unwrap()
        .get_current_leader()
        .expect("Failed to get leader");
    println!("Elected new leader: {}", leader);
    let kv3 = KeyValue {
        key: "b".to_string(),
        value: 3,
    };
    println!("Adding value: {:?} via server {}", kv3, leader);
    let (leader_server, _) = op_server_handles.get(&leader).unwrap();
    leader_server
        .lock()
        .unwrap()
        .append(kv3)
        .expect("append failed");
    // wait for the entries to be decided...
    std::thread::sleep(WAIT_DECIDED_TIMEOUT);
    let committed_ents = follower_server
        .lock()
        .unwrap()
        .read_decided_suffix(2)
        .expect("Failed to read expected entries");
    for ent in committed_ents {
        if let LogEntry::Decided(kv) = ent {
            simple_kv_store.insert(kv.key, kv.value);
        }
        // ignore uncommitted entries
    }
    println!("KV store: {:?}", simple_kv_store);
}
