use std::collections::HashMap;
use tokio::io::{AsyncWriteExt, AsyncBufReadExt};
use tokio::io::{BufReader};
use tokio::net::{TcpStream, TcpListener};
use std::net::SocketAddr;
use tokio::sync::mpsc;

use omnipaxos_core::ballot_leader_election::messages::{BLEMessage};

// use std::sync::mpsc::Sender;
use omnipaxos_core::{
    messages::Message,
    storage::{memory_storage::MemoryStorage, Snapshot},
};
use omnipaxos_runtime::omnipaxos::{NodeConfig, OmniPaxosHandle, OmniPaxosNode, ReadEntry};
////
use serde::{Deserialize, Serialize};
use structopt::StructOpt;


/// An wrap structure for network message between different nodes
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Package {
    pub types: Types,
    pub msg: Msg,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Types {
    BLE,
    SP,
    CMD,
}

/// An enum for all the different Network message types.
#[allow(missing_docs)]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Msg {

    BLE(BLEMessage),
    SP(Message<KeyValue, KVSnapshot>),
    CMD(CMDMessage),
}

#[allow(missing_docs)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CMDMessage {
    pub operation: Operaiton,
    pub msg: KeyValue,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Operaiton {

    Read(String),
    Write(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KeyValue {
    pub key: String,
    pub value: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KVSnapshot {
    snapshotted: HashMap<String, u64>,
}

impl Snapshot<KeyValue> for KVSnapshot {
    fn create(entries: &[KeyValue]) -> Self {
        let mut snapshotted = HashMap::new();
        for e in entries {
            let KeyValue { key, value } = e;
            snapshotted.insert(key.clone(), *value);
        }
        Self { snapshotted }
    }

    fn merge(&mut self, delta: Self) {
        for (k, v) in delta.snapshotted {
            self.snapshotted.insert(k, v);
        }
    }

    fn use_snapshots() -> bool {
        true
    }
}

#[derive(Debug, StructOpt, Serialize, Deserialize)]
struct Node {

    #[structopt(long)]
    id: u64,
    
    #[structopt(long)]
    peers: Vec<u64>,

}

const CLIENT: &str = "127.0.0.1:8000";

#[allow(unused_variables, unused_mut)]
#[tokio::main]
async fn main() {
    //configuration
    
    //parse parameters form cmd
    let node = Node::from_args();

    let mut node_conf = NodeConfig::default();
    node_conf.set_pid(node.id);
    node_conf.set_peers(node.peers);
    //println!("node_conf : {:?}", node_conf);

    let storage = MemoryStorage::<KeyValue, KVSnapshot>::default();
    let mut op: OmniPaxosHandle<KeyValue, KVSnapshot> = OmniPaxosNode::new(node_conf, storage);

    let OmniPaxosHandle {
        omni_paxos,
        seq_paxos_handle,
        ble_handle,
    } = op;

    //These channels are extracted for interacting with sequence-paxos conponent and BLE conponent
    let mut sp_in: mpsc::Sender<Message<KeyValue, KVSnapshot>> = seq_paxos_handle.incoming;
    let mut sp_out: mpsc::Receiver<Message<KeyValue, KVSnapshot>> = seq_paxos_handle.outgoing;
    let mut ble_in: mpsc::Sender<BLEMessage> = ble_handle.incoming;
    let mut ble_out: mpsc::Receiver<BLEMessage> = ble_handle.outgoing;

    // Bind own port for listening heartbeat
    let mut addr = "127.0.0.1:808".to_string();
    addr += &node.id.to_string();
    let addr: SocketAddr = addr.parse().unwrap();
    //println!("my ip and port: {:?}", addr);
    let listener = TcpListener::bind(addr).await.unwrap();

    //send different Package received from network to different handle thread
    let (mut sp_sender, mut sp_rec) = mpsc::channel::<String>(24);
    let (mut ble_sender, mut ble_rec) = mpsc::channel::<String>(24);
    let (mut cmd_sender, mut cmd_rec) = mpsc::channel::<String>(24);

    //boot threads
    
    // spawn thread to wait for any outgoing messages produced by SequencePaxos
    tokio::spawn(async move {
        while let Some(message) = sp_out.recv().await {
            //sprintln!("SP message: {:?} is received from SequencePaxos", message);
            //get destination
            let mut addr = "127.0.0.1:".to_string();
            let port = 8080 + message.to;
            addr += &port.to_string();
            let addr: SocketAddr = addr.parse().unwrap();

            //wrap messages
            let wrapped_msg = Package{
                types: Types::SP,
                msg: Msg::SP(message),
            };

            //serialization
            let serialized = serde_json::to_string(&wrapped_msg).unwrap();

            // send Sequence Paxos message over network to the receiver
            if let Ok(mut tcp_stream) = TcpStream::connect(addr).await{
                let (_, mut w) = tcp_stream.split();
                w.write_all(serialized.as_bytes()).await.unwrap();
            }
            
        }
    });

    // spawn thread to wait for any outgoing messages produced by BallotLeaderElection
    tokio::spawn(async move {
        while let Some(message) = ble_out.recv().await {
            //println!("BLE message: {:?} is received from BallotLeaderElection", message);
            //get destination
            let mut addr = "127.0.0.1:".to_string();
            let port = 8080 + message.to;
            addr += &port.to_string();
            let addr: SocketAddr = addr.parse().unwrap();

            //wrap messages
            let wrapped_msg = Package{
                types: Types::BLE,
                msg: Msg::BLE(message),
            };
            //serialization
            let serialized = serde_json::to_string(&wrapped_msg).unwrap();

            // send BLE message over network to the receiver
            if let Ok(mut tcp_stream) = TcpStream::connect(addr).await{
                let (_, mut w) = tcp_stream.split();
                w.write_all(serialized.as_bytes()).await.unwrap();
            }
        }
    });

    // spawn thread to handle incoming Sequence Paxos messages from the network layer
    tokio::spawn(async move {
        loop {
            //pass message to SequencePaxos
            match sp_rec.recv().await {
                Some(msg) => {
                    //println!("SP message: {} is received from network layer", msg);
                    let sp_msg: Message<KeyValue, KVSnapshot> = serde_json::from_str(&msg).unwrap();
                    sp_in
                        .send(sp_msg)
                        .await
                        .expect("Failed to pass message to SequencePaxos");
                }
                None => {} 
            }
        }
    });

    // spawn thread to handle incoming BLE messages from the network layer
    tokio::spawn(async move {
        loop {
            // pass message to BLE
            match ble_rec.recv().await {
                Some(msg) => {
                    //println!("BLE message: {} is received from network layer", msg);
                    let deserialized: BLEMessage = serde_json::from_str(&msg).unwrap();
                    ble_in
                        .send(deserialized)
                        .await
                        .expect("Failed to pass message to BallotLeaderElection");
                }
                None => {} 
            }
        }
    });

    // spawn thread to handle incoming CMD messages from the network layer
    tokio::spawn(async move {
        loop {
            // pass message to CMD
            match cmd_rec.recv().await {
                Some(msg) => {
                    println!("CMD message: {} is received from network layer", msg);
                    //let deserialized: CMDMessage = serde_json::from_str(&msg).unwrap();
                    
                }
                None => {} 
            }
        }
    });
    
    // listen to the incoming network message and distribute them into different handle threads
    loop {
        let (mut socket, addr) = listener.accept().await.unwrap();
        //println!("{} connected", &addr);
        let sp_sender = sp_sender.clone();
        let ble_sender = ble_sender.clone();
        let cmd_sender = cmd_sender.clone();

        tokio::spawn(async move {
            let (r, _) = socket.split();
            let mut reader = BufReader::new(r);
            let mut buf = String::new();
            loop {
                let bytes_read = reader.read_line(&mut buf).await.unwrap();
                //EOF
                if bytes_read == 0{
                    break;
                }
                //println!("receive string: {}", buf);
                let deserialized: Package = serde_json::from_str(&buf).unwrap();
                //println!("deserialized: {:?}", deserialized);
                //send to corresponding thread
                match deserialized.types{
                    Types::SP => {
                        //serialization
                        let serialization = serde_json::to_string(&deserialized.msg).unwrap();
                        sp_sender.send(serialization).await.expect("Failed to pass message to SP thread");
                    }
                    Types::BLE => {
                        //serialization
                        let serialization = serde_json::to_string(&deserialized.msg).unwrap();
                        ble_sender.send(serialization).await.expect("Failed to pass message to BLE thread");
                    }
                    Types::CMD =>{
                        //serialization
                        let serialization = serde_json::to_string(&deserialized.msg).unwrap();
                        cmd_sender.send(serialization).await.expect("Failed to pass message to CMD thread");
                    }
                }
                buf.clear();
            }
        });
    }
}
