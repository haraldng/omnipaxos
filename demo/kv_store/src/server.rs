use crate::{
    util::{ELECTION_TIMEOUT, OUTGOING_MESSAGE_PERIOD},
    OmniPaxosKV,
    kv::{KVSnapshot, KeyValue},
};
use omnipaxos_core::{messages::Message, util::NodeId};
use serde_json;
use tokio::{
    time,
    net::{TcpStream, tcp},
    io::{BufReader, AsyncWriteExt, AsyncBufReadExt},
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

pub struct OmniPaxosServer {
    pub omni_paxos: Arc<Mutex<OmniPaxosKV>>,
    pub sockets: HashMap<NodeId, tcp::OwnedWriteHalf>,
    pub peer_addrs: HashMap<NodeId, String>,
    pub peers: Vec<NodeId>,
    pub pid: NodeId,
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
                    let msg: Message<KeyValue, KVSnapshot> = serde_json::from_slice(&data).expect("could not deserialize msg");
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
                _ = outgoing_interval.tick() => { self.send_outgoing_msgs().await; },
                else => (),
            }
        }
    }
}
