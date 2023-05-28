use crate::{
    server::OmniPaxosServer,
};
use omnipaxos::{*, util::NodeId};
use omnipaxos_storage::memory_storage::MemoryStorage;
use tokio;
use std::{
    sync::{Arc, Mutex},
    env,
};
use crate::kv::KVCommand;

#[macro_use]
extern crate lazy_static;

mod kv;
mod server;
mod util;
mod network;
mod database;

lazy_static!{
    pub static ref PEERS: Vec<NodeId> = if let Ok(var) = env::var("PEERS") {
        serde_json::from_str::<Vec<u64>>(&var).expect("wrong config format")
    } else {
        vec![]
    };
    pub static ref PEER_ADDRS: Vec<String> = if let Ok(var) = env::var("PEER_ADDRS") {
        serde_json::from_str::<Vec<String>>(&var).expect("wrong config format")
    } else {
        vec![]
    }; 
    pub static ref API_ADDR: String = if let Ok(var) = env::var("API_ADDR") {
        var
    } else {
        panic!("missing API address")
    }; 
    pub static ref PID: NodeId = if let Ok(var) = env::var("PID") {
        let x = var.parse().expect("PIDs must be u64");
        if x == 0 { panic!("PIDs cannot be 0") } else { x }
    } else {
        panic!("missing PID")
    };
}

type OmniPaxosKV = OmniPaxos<KVCommand, MemoryStorage<KVCommand>>;

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
    let mut op_server = OmniPaxosServer {
        network: network::Network::new().await,
        omni_paxos: Arc::clone(&omni_paxos),
        peers: PEERS.clone(),
        pid: *PID,
        last_sent_decided_idx: 0,
        database: database::Database::new(format!("db_{}", *PID).as_str()),
    };
    op_server.run().await;
}
