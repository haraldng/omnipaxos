use std::fmt::Write;
use std::net::SocketAddr;

use omnipaxos_core::storage;
use tokio::io::{AsyncWriteExt, AsyncBufReadExt, BufReader};
use tokio::net::{TcpStream, TcpListener};
use tokio::sync::mpsc;

use omnipaxos_core::{
    ballot_leader_election::messages::{BLEMessage},
    messages::Message,
    storage::{memory_storage::MemoryStorage, Snapshot},};

use omnipaxos_runtime::omnipaxos::{NodeConfig, OmniPaxosHandle, OmniPaxosNode, ReadEntry,ReadEntry::Decided, ReadEntry::Snapshotted};

mod messages;
use crate::messages::*;
use structopt::StructOpt;

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
    let mut addr = "127.0.0.1:".to_string();
    let port = 8080 + node.id;
    addr += &port.to_string();
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
            println!(" ");
            println!("SP message: {:?} is received from SequencePaxos", message);
            println!(" ");
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

                    println!(" ");
                    println!("SP message: {:?} is received from Network", sp_msg);
                    println!(" ");

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
                    //sprintln!("CMD message: {} is received from network layer", msg);
                    let deserialized: CMDMessage = serde_json::from_str(&msg).unwrap();
                    match deserialized.operation{
                        Operaiton::Read =>{
                            //get the key
                            let key = deserialized.kv.key;
                            if let Some(entries) = omni_paxos.read_entries(0..).await{
                                if let Some(v) = fetch_value(&key, entries.to_vec()).await{
                                    let mut prefix = "This value is :".to_string();
                                    let mut value = v.to_string();
                                    prefix += &value;
                                    feedback_to_client(&prefix).await;
                                }else {
                                    feedback_to_client("The value is none").await;
                                }
                            }else {
                                //println!("This key does not exist");
                                feedback_to_client("The value is none").await;
                            }             
                        }

                        Operaiton::Write =>{
                            //get the key value
                            let write_entry = deserialized.kv;
                            //append
                            if let Ok(_)= omni_paxos
                                .append(write_entry)
                                .await{
                                    feedback_to_client("Successfully to append log").await;
                                }else{
                                    feedback_to_client("Failed to append log").await;
                                }
                        }
                        Operaiton::Snap =>{
                            //something will cause omni paxos wrong
                            if omni_paxos.get_compacted_idx().await == omni_paxos.get_decided_idx().await{
                                feedback_to_client("Successfully to snapshot").await;
                                break;
                            }
                            if let Ok(_) = omni_paxos
                                .snapshot(None, false)
                                .await{
                                    feedback_to_client("Successfully to snapshot").await;
                                }else{
                                    feedback_to_client("Failed to snapshot").await;
                                }
                        }
                    }
                    if let Ok(mut tcp_stream) = TcpStream::connect(CLIENT).await{
                        let (_, mut w) = tcp_stream.split();
                        //w.write_all(serialized.as_bytes()).await.unwrap();
                    }
                    
                }
                None => {} 
            }
        }
    });
    
    // listen to the incoming network message and distribute them into different handle threads
    loop {
        let (mut socket, addr) = listener.accept().await.unwrap();
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

//feedback to the client
async fn feedback_to_client(str: &str){
    //println!("res: {}", str);
    if let Ok(mut tcp_stream) = TcpStream::connect(CLIENT).await{
        let (_, mut w) = tcp_stream.split();
        w.write_all(str.as_bytes()).await.unwrap();
    }else {
        println!("Network failure");
    }
}

//fetch value by key
async fn fetch_value(key: &str, vec: Vec<ReadEntry<KeyValue, KVSnapshot>>) -> Option<u64>{
    print!("vec {:?}", vec);
    let mut index = vec.len()-1;
    let mut value = None;
    let vec = vec.clone();
    loop {
        match vec.get(index).unwrap(){
            Decided(kv)=> {
                println!("Now kv {:?}", kv);
                if kv.key == key {
                    value = Some(kv.value);
                    break;
                }
            }
            Snapshotted(snapshotted_entry)=>{
                let hashmap = snapshotted_entry.snapshot.snapshotted.clone();
                println!("Now snapshot {:?}", hashmap);
                if let Some(v) = hashmap.get(key){
                    value = Some(*v);
                }
            }
            _=>{
                
            }
        }
        if index == 0 || value!=None{
            break;
        }
        index-=1;
    }
    value
}