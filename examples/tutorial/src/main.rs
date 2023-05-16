use crate::{kv::KeyValue, server::OmniPaxosServer, util::*};
use omnipaxos::{
    messages::Message as OmniPaxosMsg,
    util::{LogEntry, NodeId},
    *,
};
use omnipaxos_storage::persistent_storage::{PersistentStorage, PersistentStorageConfig};
use tokio::{sync::{mpsc, Mutex}, task};
use commitlog::LogOptions;
use sled::Config;

use std::{
    collections::HashMap,
    sync::{Arc},
};

mod kv;
mod server;
mod util;

type OmniPaxosKV = OmniPaxos<KeyValue, PersistentStorage<KeyValue>>;
type Message = OmniPaxosMsg<KeyValue>;
// use the serde feature to make messages of OmniPaxos instances serializable
// More information about this feature can be found in the [documentation](https://omnipaxos.com/docs/omnipaxos/features/).
type SerializedMessage = String;

const COMMITLOG: &str = "/commitlog/";

const SERVERS: [u64; 3] = [1, 2, 3];

#[allow(clippy::type_complexity)]
fn initialise_channels() -> (
    Arc<Mutex<HashMap<NodeId, mpsc::Sender<SerializedMessage>>>>,
    HashMap<NodeId, mpsc::Receiver<SerializedMessage>>,
) {
    let mut sender_channels = HashMap::new();
    let mut receiver_channels = HashMap::new();

    for pid in SERVERS {
        let (sender, receiver) = mpsc::channel(BUFFER_SIZE);
        sender_channels.insert(pid, sender);
        receiver_channels.insert(pid, receiver);
    }
    (Arc::new(Mutex::new(sender_channels)), receiver_channels)
}

fn build_omnipaxos(op_config: OmniPaxosConfig) -> Arc<Mutex<OmniPaxosKV>> {
    // setup OmniPaxos instance using persistent storage
    // More information about the Storage can be found in the [documentation](https://omnipaxos.com/docs/omnipaxos/storage/).
    let store_base_path = format!("{STORAGE_BASE_PATH}node{0}", op_config.pid);
    let my_logopts = LogOptions::new(format!("{store_base_path}{COMMITLOG}"));
    let my_sledopts = Config::new();
    let store_config = PersistentStorageConfig::with(store_base_path, my_logopts, my_sledopts);
    Arc::new(Mutex::new(op_config.build(PersistentStorage::open(store_config))))
}

#[tokio::main]
async fn main() {
    let configuration_id = 1;
    let mut op_server_handles = HashMap::new();
    let (sender_channels, mut receiver_channels) = initialise_channels();

    // initialise the OmniPaxos servers
    for pid in SERVERS {
        let peers = SERVERS.iter().filter(|&&p| p != pid).copied().collect();
        let op_config = if pid == 1 {
            // use a toml file to configure the first node
            // More information about this feature can be found in the [documentation](https://omnipaxos.com/docs/omnipaxos/features/).
            let mut op_config_node1 = OmniPaxosConfig::with_toml(NODE1_CONFIG_PATH).unwrap();
            op_config_node1.peers = peers;
            op_config_node1
        } else {
            OmniPaxosConfig {
                pid,
                configuration_id,
                peers,
                ..Default::default()
            }
        };
        let omni_paxos = build_omnipaxos(op_config);
        let mut op_server = OmniPaxosServer {
            omni_paxos: Arc::clone(&omni_paxos),
            incoming: receiver_channels.remove(&pid).unwrap(),
            outgoing: Arc::clone(&sender_channels),
        };
        let join_handle = task::spawn({
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
        .await
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
        .await
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
        .await
        .append(kv2)
        .expect("append failed");
    // wait for the entries to be decided...
    std::thread::sleep(WAIT_DECIDED_TIMEOUT);
    let committed_ents = leader_server
        .lock()
        .await
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

    let killed_node: NodeId = leader;
    // wait for new leader to be elected...
    std::thread::sleep(WAIT_LEADER_TIMEOUT);
    let leader = follower_server
        .lock()
        .await
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
        .await
        .append(kv3)
        .expect("append failed");
    // wait for the entries to be decided...
    std::thread::sleep(WAIT_DECIDED_TIMEOUT);
    let committed_ents = follower_server
        .lock()
        .await
        .read_decided_suffix(2)
        .expect("Failed to read expected entries");
    for ent in committed_ents {
        if let LogEntry::Decided(kv) = ent {
            simple_kv_store.insert(kv.key, kv.value);
        }
        // ignore uncommitted entries
    }
    println!("KV store: {:?}", simple_kv_store);
    // recover killed node
    let peers = SERVERS.iter().filter(|&&p| p != killed_node).copied().collect();
    let op_config = OmniPaxosConfig {
        pid: killed_node,
        configuration_id: 1,
        peers,
        ..Default::default()
    };
    let (sender, receiver) = mpsc::channel(BUFFER_SIZE);
    sender_channels.lock().await.insert(killed_node, sender);
    let omni_paxos = build_omnipaxos(op_config);
    omni_paxos
        .lock()
        .await
        .fail_recovery();
    let mut op_server = OmniPaxosServer {
        omni_paxos: Arc::clone(&omni_paxos),
        incoming: receiver,
        outgoing: Arc::clone(&sender_channels),
    };
    let join_handle = task::spawn({
        async move {
            op_server.run().await;
        }
    });
    op_server_handles.insert(killed_node, (omni_paxos, join_handle));
    std::thread::sleep(WAIT_LEADER_TIMEOUT);

}
