use std::sync::Arc;
use std::time::Duration;
use kompact::KompactLogger;
use kompact::prelude::*;
use crate::core::leader_election::ballot_leader_election::{Ballot, BallotLeaderElection, BLEConfig};
use crate::core::sequence_paxos::{SequencePaxosConfig, SequencePaxos};
use crate::core::storage::{Entry, Snapshot, Storage};
use crate::core::util::defaults::*;
use crate::runtime::{BLEComp, SequencePaxosComp};

#[derive(Clone, Debug)]
pub struct NodeConfig {
    pub leader_timeout: Duration,
    pub buffer_size: usize,
    pub initial_leader: Option<Ballot>,
    pub initial_leader_timeout: Option<Duration>,
    pub priority: Option<u64>,
    pub logger_path: Option<String>
}

impl NodeConfig {
    pub fn set_leader_timeout(&mut self, timeout: Duration) { self.leader_timeout = timeout; }

    pub fn set_buffer_size(&mut self, size: usize) {
        self.buffer_size = size;
    }

    pub fn set_initial_leader(&mut self, b: Ballot) {
        self.initial_leader = Some(b);
    }

    pub fn set_initial_leader_timeout(&mut self, timeout: Duration) {
        self.initial_leader_timeout = Some(timeout);
    }

    pub fn set_priority(&mut self, priority: u64) { self.priority = Some(priority); }

    pub fn set_logger_path(&mut self, s: String) {
        self.logger_path = Some(s);
    }
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self { buffer_size: BUFFER_SIZE, initial_leader: None, leader_timeout: Duration::from_millis(HB_DELAY), initial_leader_timeout: None, priority: None, logger_path: None }
    }
}

pub struct OmniPaxosNode<T: Entry, S: Snapshot<T>, B: Storage<T, S>>
{
    system: KompactSystem,
    pid: u64,
    sp_comp: Arc<Component<SequencePaxosComp<T, S, B>>>,
    ble_comp: Arc<BLEComp>,
}

impl<T, S, B> OmniPaxosNode<T, S, B>
where
    T: Entry, S: Snapshot<T>, B: Storage<T, S>
{
    pub fn new(
        pid: u64,
        peers: Vec<u64>,
        conf: NodeConfig,
    ) -> Self {
        // TODO user provides config?
        let sp_conf = SequencePaxosConfig::from_node_conf(&conf);
        let ble_conf = BLEConfig::from_node_conf(&conf);

        let system = KompactConfig::default().build().expect("Failed to build KompactSystem!");

        let sp = SequencePaxos::with(1, pid, peers.clone(), storage, sp_conf);
        let ble = BallotLeaderElection::with(pid, peers, ble_conf);
    }

    pub async fn trim() {
        todo!()
    }

    pub fn snapshot() {
        todo!()
    }

    pub fn get_decided_idx() {
        todo!()
    }

    pub fn get_compacted_idx() {
        todo!()
    }

    pub fn fail_recovery() {
        todo!()
    }

    pub fn get_current_leader() {
        todo!()
    }

    pub fn read() {
        todo!()
    }

    pub fn read_entries() {
        todo!()}

    pub fn read_decided_suffix() {
        todo!()}

    pub fn append() {
        todo!()
    }

    pub fn reconfigure() {
        todo!()
    }
}

/*
#[derive(ComponentDefinition)]
struct OmniPaxosComp<T: Entry, S: Snapshot<T>, B: Storage<T, S>> {
    ctx: ComponentContext<Self>,
    sp_comp: Arc<Component<SequencePaxosComp<T, S, B>>>,
    ble_comp: Arc<BLEComp>,
}

impl<T, S, B> OmniPaxosComp<T, S, B>
where
    T: Entry, S: Snapshot<T>, B: Storage<T, S>
{

}

impl<T, S, B> Actor for OmniPaxosComp<T, S, B>
where
    T: Entry, S: Snapshot<T>, B: Storage<T, S>
{
    type Message = ();

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        todo!()
    }

    fn receive_network(&mut self, msg: NetMessage) -> Handled {
        todo!()
    }
}
*/

/*
impl<T, S, B> ComponentLifecycle for OmniPaxosComp<T, S, B>
where
    T: Entry, S: Snapshot<T>, B: Storage<T, S>
{

}
*/