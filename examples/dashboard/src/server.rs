use crate::{
    entry::LogEntry,
    util::{OUTGOING_MESSAGE_PERIOD, TICK_PERIOD, UI_TICK_PERIOD},
    OmniPaxosLog,
};
use omnipaxos::{messages::Message, util::NodeId};
use omnipaxos_ui::OmniPaxosUI;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::{sync::mpsc, time};

pub struct OmniPaxosServer {
    pub omni_paxos_ui: OmniPaxosUI,
    pub omni_paxos: Arc<Mutex<OmniPaxosLog>>,
    pub incoming: mpsc::Receiver<Message<LogEntry>>,
    pub outgoing: HashMap<NodeId, mpsc::Sender<Message<LogEntry>>>,
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
        let mut op_tick_interval = time::interval(TICK_PERIOD);
        let mut op_ui_tick_interval = time::interval(UI_TICK_PERIOD);
        loop {
            tokio::select! {
                biased;

                _ = op_tick_interval.tick() => { self.omni_paxos.lock().unwrap().tick(); },
                _ = outgoing_interval.tick() => { self.send_outgoing_msgs().await; },
                _ = op_ui_tick_interval.tick() => {
                    self.omni_paxos_ui.tick(self.omni_paxos.lock().unwrap().get_ui_states());
                },
                Some(in_msg) = self.incoming.recv() => { self.omni_paxos.lock().unwrap().handle_incoming(in_msg); },
                else => { }
            }
        }
    }
}
