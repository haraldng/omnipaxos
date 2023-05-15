use omnipaxos::{util::NodeId};
use tokio::{sync::mpsc, time};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};


use crate::{util::{ELECTION_TIMEOUT, OUTGOING_MESSAGE_PERIOD}, OmniPaxosKV, Message, SerializedMessage};

pub struct OmniPaxosServer {
    pub omni_paxos: Arc<Mutex<OmniPaxosKV>>,
    pub incoming: mpsc::Receiver<SerializedMessage>,
    pub outgoing: HashMap<NodeId, mpsc::Sender<SerializedMessage>>,
}

impl OmniPaxosServer {
    async fn send_outgoing_msgs(&mut self) {
        let messages = self.omni_paxos.lock().unwrap().outgoing_messages();
        for msg in messages {
            let serialized_msg:SerializedMessage = serde_json::to_string(&msg).unwrap();
            let receiver = msg.get_receiver();
            let channel = self
                .outgoing
                .get_mut(&receiver)
                .expect("No channel for receiver");
            let _ = channel.send(serialized_msg).await;
        }
    }

    pub(crate) async fn run(&mut self) {
        let mut outgoing_interval = time::interval(OUTGOING_MESSAGE_PERIOD);
        let mut election_interval = time::interval(ELECTION_TIMEOUT);
        loop {
            tokio::select! {
                biased;

                _ = election_interval.tick() => { self.omni_paxos.lock().unwrap().election_timeout(); },
                _ = outgoing_interval.tick() => { self.send_outgoing_msgs().await; },
                Some(in_msg) = self.incoming.recv() => {
                    let deserialized_msg:Message = serde_json::from_str(&in_msg).unwrap();
                    self.omni_paxos.lock().unwrap().handle_incoming(deserialized_msg);
                },
                else => { }
            }
        }
    }
}