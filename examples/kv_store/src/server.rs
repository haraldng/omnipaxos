use tokio::{sync::mpsc, time};
use omnipaxos::{messages::Message, util::NodeId};
#[cfg(feature = "with_omnipaxos_ui")]
use omnipaxos_ui::OmniPaxosUI;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use crate::{
    util::{OUTGOING_MESSAGE_PERIOD, OMNI_PAXOS_TICK_PERIOD, OMNI_PAXOS_UI_TICK_PERIOD},
    OmniPaxosKV,
};
use crate::kv::KeyValue;

pub struct OmniPaxosServer {
    #[cfg(feature = "with_omnipaxos_ui")]
    pub omni_paxos_ui: OmniPaxosUI,
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
        let mut op_tick_interval = time::interval(OMNI_PAXOS_TICK_PERIOD);
        let mut op_ui_tick_interval = time::interval(OMNI_PAXOS_UI_TICK_PERIOD);
        loop {
            tokio::select! {
                biased;

                _ = op_tick_interval.tick() => { self.omni_paxos.lock().unwrap().tick(); },
                _ = outgoing_interval.tick() => { self.send_outgoing_msgs().await; },
                _ = op_ui_tick_interval.tick() => {
                    #[cfg(feature = "with_omnipaxos_ui")]
                    self.omni_paxos_ui.tick(self.omni_paxos.lock().unwrap().get_states());
                },
                Some(in_msg) = self.incoming.recv() => { self.omni_paxos.lock().unwrap().handle_incoming(in_msg); },
                else => { }
            }
        }
    }
}
