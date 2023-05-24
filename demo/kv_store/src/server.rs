use crate::{
    util::{ELECTION_TIMEOUT, OUTGOING_MESSAGE_PERIOD},
    OmniPaxosKV,
    kv::KeyValue,
};
use omnipaxos::{messages::Message, util::NodeId};
use serde_json;
use serde::{Serialize, Deserialize};
use tokio::{
    time,
    net::{TcpStream, tcp},
    io::{BufReader, AsyncWriteExt, AsyncBufReadExt},
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

/// Same as in network actor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum APICommand {
    Append(KeyValue),
}

/// Same as in network actor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum APIResponse {
    Decided(u64),
}

pub struct OmniPaxosServer {
    pub omni_paxos: Arc<Mutex<OmniPaxosKV>>,
    pub sockets: HashMap<NodeId, tcp::OwnedWriteHalf>,
    pub peer_addrs: HashMap<NodeId, String>,
    pub peers: Vec<NodeId>,
    pub pid: NodeId,
    pub api_socket: Option<tcp::OwnedWriteHalf>,
    pub api_addr: String,
    pub last_sent_decided_idx: u64,
}

impl OmniPaxosServer {
    async fn send_outgoing_msgs(&mut self) {
        let messages = self.omni_paxos.lock().unwrap().outgoing_messages();
        for msg in messages {
            let receiver = msg.get_receiver();
            let writer = self.sockets.get_mut(&receiver).unwrap();
            let mut data = serde_json::to_vec(&msg).expect("could not serialize msg");
            data.push(b'\n');
            writer.write_all(&data).await.unwrap();
        }
    }

    async fn connect_sockets(&mut self) {
        let api_stream = TcpStream::connect(self.api_addr.clone()).await.unwrap();
        let omni_paxos = self.omni_paxos.clone();
        let (api_reader, api_writer) = api_stream.into_split();
        self.api_socket = Some(api_writer);
        tokio::spawn(async move {
            let mut reader = BufReader::new(api_reader);
            let mut data = Vec::new();
            loop {
                data.clear();
                let bytes_read = reader.read_until(b'\n', &mut data).await;
                if bytes_read.is_err() {
                    // stream ended?
                    panic!("stream ended?")
                }
                let cmd: APICommand = serde_json::from_slice(&data).expect("could not deserialize command");
                println!("CMD: {:?}", cmd);
                match cmd {
                    APICommand::Append(kv) => {
                        omni_paxos.lock().unwrap().append(kv).unwrap();
                    }
                }
            }
        });

        for i in 0..self.peers.len() {
            let peer = self.peers[i];
            let addr = self.peer_addrs.get(&peer).unwrap().clone();
            let stream = TcpStream::connect(addr).await.unwrap();
            let (reader, writer) = stream.into_split();
            self.sockets.insert(peer, writer);
            let omni_paxos = self.omni_paxos.clone();
            tokio::spawn(async move {
                let mut reader = BufReader::new(reader);
                let mut data = Vec::new();
                loop {
                    data.clear();
                    let bytes_read = reader.read_until(b'\n', &mut data).await;
                    if bytes_read.is_err() {
                        // stream ended?
                        panic!("stream ended?")
                    }
                    let msg: Message<KeyValue> = serde_json::from_slice(&data).expect("could not deserialize msg");
                    println!("{:?}", msg);
                    omni_paxos.lock().unwrap().handle_incoming(msg);
                }
            });
        }
    }

    pub(crate) async fn run(&mut self) {
        self.connect_sockets().await;
        let mut outgoing_interval = time::interval(OUTGOING_MESSAGE_PERIOD);
        let mut election_interval = time::interval(ELECTION_TIMEOUT);
        loop {
            tokio::select! {
                biased;
                _ = election_interval.tick() => { self.omni_paxos.lock().unwrap().election_timeout(); },
                _ = outgoing_interval.tick() => {
                    self.send_outgoing_msgs().await;
                    // update the network_actor of latest decided idx
                    let op = self.omni_paxos.lock().unwrap();
                    let new_decided_idx = op.get_decided_idx();
                    if self.last_sent_decided_idx < new_decided_idx {
                        self.last_sent_decided_idx = new_decided_idx;
                        let resp = APIResponse::Decided(new_decided_idx);
                        let mut data = serde_json::to_vec(&resp).expect("could not serialize response");
                        data.push(b'\n');
                        self.api_socket.as_mut().unwrap().write_all(&data).await.unwrap();
                    }
                },
                else => (),
            }
        }
    }
}
