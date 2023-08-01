use crate::kv::KeyValue;
use omnipaxos::{messages::Message, util::NodeId};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    util::{OUTGOING_MESSAGE_PERIOD, TICK_PERIOD},
    OmniPaxosKV,
};
use tokio::{sync::mpsc, time};

pub struct OmniPaxosServer {
    pub omni_paxos: Arc<Mutex<OmniPaxosKV>>,
    pub incoming: mpsc::Receiver<Message<KeyValue>>,
    pub outgoing: HashMap<NodeId, mpsc::Sender<Message<KeyValue>>>,
}

impl OmniPaxosServer {
    async fn send_outgoing_msgs(&mut self) {
        let messages = self.omni_paxos.lock().unwrap().outgoing_messages();
        for msg in messages {
            let receiver = msg.get_receiver();
            let channel = self
                .outgoing
                .get_mut(&receiver)
                .expect("No channel for receiver");
            let _ = channel.send(msg).await;
        }
    }

    pub(crate) async fn run(&mut self) {
        let mut outgoing_interval = time::interval(OUTGOING_MESSAGE_PERIOD);
        let mut tick_interval = time::interval(TICK_PERIOD);
        loop {
            tokio::select! {
                biased;

                _ = tick_interval.tick() => { self.omni_paxos.lock().unwrap().tick(); },
                _ = outgoing_interval.tick() => { self.send_outgoing_msgs().await; },
                Some(in_msg) = self.incoming.recv() => { self.omni_paxos.lock().unwrap().handle_incoming(in_msg); },
                else => { }
            }
        }
    }
}
