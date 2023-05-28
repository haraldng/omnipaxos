use omnipaxos::{messages::Message as OPMessage, util::NodeId};
use tokio::{
    net::{TcpStream, tcp},
    io::{BufReader, AsyncWriteExt, AsyncBufReadExt},
    sync::Mutex,
};
use serde::{Serialize, Deserialize};
use serde_json;
use std::collections::HashMap;
use std::sync::Arc;

use crate::{kv::KVCommand, server::APIResponse, API_ADDR, PEERS, PEER_ADDRS};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum Message {
    OmniPaxosMsg(OPMessage<KVCommand>),
    APICommand(KVCommand),
    APIResponse(APIResponse),
}

pub struct Network {
    sockets: HashMap<NodeId, tcp::OwnedWriteHalf>,
    api_socket: Option<tcp::OwnedWriteHalf>,
    incoming_msg_buf: Arc<Mutex<Vec<Message>>>,
}

impl Network {
    /// Sends the message to the receiver.
    /// NodeId 0 is the Client.
    pub(crate) async fn send(&mut self, receiver: NodeId, msg: Message) {
        let writer = if receiver == 0 {
            self.api_socket.as_mut()
        } else {
            self.sockets.get_mut(&receiver)
        };
        if let Some(writer) = writer {
            let mut data = serde_json::to_vec(&msg).expect("could not serialize msg");
            data.push(b'\n');
            writer.write_all(&data).await.unwrap();
        }
    }

    /// Returns all messages received since last called.
    pub(crate) async fn get_received(&mut self) -> Vec<Message> {
        let mut buf = self.incoming_msg_buf.lock().await;
        let ret = buf.to_vec();
        buf.clear();
        ret
    }

    /// Constructs a new Network instance and connects the Sockets.
    pub(crate) async fn new() -> Self {
        let peers = PEERS.clone();
        let mut peer_addrs = HashMap::new();
        for i in 0..PEERS.len() {
            peer_addrs.insert(PEERS[i], PEER_ADDRS[i].clone());
        }
        let api_stream = TcpStream::connect(API_ADDR.clone()).await.unwrap();
        let (api_reader, api_writer) = api_stream.into_split();
        let api_socket = Some(api_writer);
        let incoming_msg_buf = Arc::new(Mutex::new(vec![]));
        let msg_buf = incoming_msg_buf.clone();
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
                let msg: Message = serde_json::from_slice(&data).expect("could not deserialize msg");
                msg_buf.lock().await.push(msg);
            }
        });

        let mut sockets = HashMap::new();
        for i in 0..peers.len() {
            let peer = peers[i];
            let addr = peer_addrs.get(&peer).unwrap().clone();
            let stream = TcpStream::connect(addr).await.unwrap();
            let (reader, writer) = stream.into_split();
            sockets.insert(peer, writer);
            let msg_buf = incoming_msg_buf.clone();
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
                    let msg: Message = serde_json::from_slice(&data).expect("could not deserialize msg");
                    msg_buf.lock().await.push(msg);
                }
            });
        }
        Self {
            sockets,
            api_socket,
            incoming_msg_buf,
        }
    }
}
