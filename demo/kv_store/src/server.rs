use crate::{
    util::{ELECTION_TIMEOUT, OUTGOING_MESSAGE_PERIOD},
    OmniPaxosKV,
    network::{Network, Message},
};
use omnipaxos::util::{LogEntry, NodeId};
use serde::{Serialize, Deserialize};
use tokio::time;
use std::sync::{Arc, Mutex};
use crate::database::Database;
use crate::kv::KVCommand;


/// Same as in network actor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum APIResponse {
    Decided(u64),
}

pub struct OmniPaxosServer {
    pub omni_paxos: Arc<Mutex<OmniPaxosKV>>,
    pub pid: NodeId,
    pub peers: Vec<NodeId>,
    pub last_sent_decided_idx: u64,
    pub network: Network,
    pub database: Database,
}

impl OmniPaxosServer {
    fn handle_decided(&self, decided_entries: Option<Vec<LogEntry<KVCommand>>>) {
        if let Some(decided_entries) = decided_entries {
            for entry in decided_entries {
                match entry {
                    LogEntry::Decided(cmd) => {
                        self.database.handle_command(cmd);
                    },
                    LogEntry::Snapshotted(_s) => unimplemented!(),
                    _ => {}
                }
            }
        }
    }

    async fn send_outgoing_msgs(&mut self) {
        let messages = self.omni_paxos.lock().unwrap().outgoing_messages();
        for msg in messages {
            let receiver = msg.get_receiver();
            self.network.send(receiver, Message::OmniPaxosMsg(msg)).await;
        }
    }

    async fn process_incoming_msgs(&mut self) {
        let messages = self.network.get_received().await;
        let mut op = self.omni_paxos.lock().unwrap();
        for msg in messages {
            match msg {
                Message::OmniPaxosMsg(m) => op.handle_incoming(m),
                Message::APICommand(cmd) => op.append(cmd).unwrap(),
                Message::APIResponse(_) => panic!("received API response"),
            }
        }
    }

    pub(crate) async fn run(&mut self) {
        let mut msg_interval = time::interval(OUTGOING_MESSAGE_PERIOD);
        let mut election_interval = time::interval(ELECTION_TIMEOUT);
        loop {
            tokio::select! {
                biased;
                _ = election_interval.tick() => { self.omni_paxos.lock().unwrap().election_timeout(); },
                _ = msg_interval.tick() => {
                    self.process_incoming_msgs().await;
                    self.send_outgoing_msgs().await;
                    // update the network_actor of latest decided idx
                    let op = self.omni_paxos.lock().unwrap();
                    let new_decided_idx = op.get_decided_idx();
                    if self.last_sent_decided_idx < new_decided_idx {
                        let decided_entries = op.read_decided_suffix(self.last_sent_decided_idx);
                        self.handle_decided(decided_entries);
                        self.last_sent_decided_idx = new_decided_idx;
                        let msg = Message::APIResponse(APIResponse::Decided(new_decided_idx));
                        self.network.send(0, msg).await;
                    }
                },
                else => (),
            }
        }
    }
}
