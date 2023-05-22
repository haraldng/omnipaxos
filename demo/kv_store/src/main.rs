use crate::{
    kv::{KVSnapshot, KeyValue},
    server::OmniPaxosServer,
};
use omnipaxos_core::{
    omni_paxos::*,
    util::NodeId,
};
use omnipaxos_storage::memory_storage::MemoryStorage;
use tokio;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    env,
};

#[macro_use]
extern crate lazy_static;

mod kv;
mod server;
mod util;

lazy_static!{
    static ref PEERS: Vec<NodeId> = if let Ok(var) = env::var("PEERS") {
        serde_json::from_str::<Vec<u64>>(&var).expect("wrong config format")
    } else {
        vec![]
    };
    static ref PEER_ADDRS: Vec<String> = if let Ok(var) = env::var("PEER_ADDRS") {
        serde_json::from_str::<Vec<String>>(&var).expect("wrong config format")
    } else {
        vec![]
    }; 
    static ref API_ADDR: String = if let Ok(var) = env::var("API_ADDR") {
        var
    } else {
        panic!("missing API address")
    }; 
    static ref PID: NodeId = if let Ok(var) = env::var("PID") {
        let x = var.parse().expect("PIDs must be u64");
        if x == 0 { panic!("PIDs cannot be 0") } else { x }
    } else {
        panic!("missing PID")
    };

}

type OmniPaxosKV = OmniPaxos<KeyValue, KVSnapshot, MemoryStorage<KeyValue, KVSnapshot>>;

#[tokio::main]
async fn main() {
    let op_config = OmniPaxosConfig {
        pid: *PID,
        configuration_id: 1,
        peers: PEERS.clone(),
        ..Default::default()
    };
    let omni_paxos: Arc<Mutex<OmniPaxosKV>> =
        Arc::new(Mutex::new(op_config.build(MemoryStorage::default())));
    let mut peer_addrs = HashMap::new();
    for i in 0..PEERS.len() {
        peer_addrs.insert(PEERS[i], PEER_ADDRS[i].clone());
    }
    let mut op_server = OmniPaxosServer {
        omni_paxos: Arc::clone(&omni_paxos),
        sockets: HashMap::new(),
        peer_addrs,
        peers: PEERS.clone(),
        pid: *PID,
        api_addr: *API_ADDR,
        api_socket: None,
    };
    op_server.run().await;
}
