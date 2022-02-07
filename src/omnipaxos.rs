use crate::{
    core::{
        leader_election::ballot_leader_election::{BLEConfig, Ballot, BallotLeaderElection},
        sequence_paxos::{SequencePaxos, SequencePaxosConfig},
        storage::{Entry, Snapshot, Storage},
        util::defaults::*,
    },
    runtime::{BLEComp, SequencePaxosComp},
};
use std::{time::Duration};
use std::marker::PhantomData;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::{mpsc, oneshot, watch};
use crate::core::sequence_paxos::ProposeErr;
use crate::runtime::{BLEHandle, InternalBLEHandle, InternalSPHandle, SequencePaxosHandle, SPRequest};

#[derive(Clone, Debug)]
pub struct NodeConfig {
    pub leader_timeout: Duration,
    pub buffer_size: usize,
    pub initial_leader: Option<Ballot>,
    pub initial_leader_timeout: Option<Duration>,
    pub priority: Option<u64>,
    pub logger_path: Option<String>,
}

impl NodeConfig {
    pub fn set_leader_timeout(&mut self, timeout: Duration) {
        self.leader_timeout = timeout;
    }

    pub fn set_buffer_size(&mut self, size: usize) {
        self.buffer_size = size;
    }

    pub fn set_initial_leader(&mut self, b: Ballot) {
        self.initial_leader = Some(b);
    }

    pub fn set_initial_leader_timeout(&mut self, timeout: Duration) {
        self.initial_leader_timeout = Some(timeout);
    }

    pub fn set_priority(&mut self, priority: u64) {
        self.priority = Some(priority);
    }

    pub fn set_logger_path(&mut self, s: String) {
        self.logger_path = Some(s);
    }
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            buffer_size: BUFFER_SIZE,
            initial_leader: None,
            leader_timeout: Duration::from_millis(HB_DELAY),
            initial_leader_timeout: None,
            priority: None,
            logger_path: None,
        }
    }
}

pub struct OmniPaxosHandle<T: Entry, S: Snapshot<T>, B: Storage<T, S>> {
    pub omni_paxos: OmniPaxosNode<T, S, B>,
    pub seq_paxos_handle: SequencePaxosHandle<T, S>,
    pub ble_handle: BLEHandle,
}

pub struct OmniPaxosNode<T: Entry, S: Snapshot<T>, B: Storage<T, S>> {
    pid: u64,
    sp_comp: InternalSPHandle<T, S>,
    ble_comp: InternalBLEHandle,
    runtime: Runtime,
    _p: PhantomData<B>  // TODO remove?
}

impl<T, S, B> OmniPaxosNode<T, S, B>
where
    T: Entry + Send + 'static,
    S: Snapshot<T> + Send + 'static,
    B: Storage<T, S> + Send + 'static,
{

    pub fn new(pid: u64, peers: Vec<u64>, conf: NodeConfig, storage: B) -> OmniPaxosHandle<T, S, B> {
        // TODO user provides config?
        let sp_conf = SequencePaxosConfig::from_node_conf(&conf);
        let ble_conf = BLEConfig::from_node_conf(&conf);

        let sp = SequencePaxos::with(1, pid, peers.clone(), storage, sp_conf);
        let ble = BallotLeaderElection::with(pid, peers, ble_conf);

        let (leader_send, leader_receive) = watch::channel(Ballot::default());

        let (in_sender, in_receiver) = mpsc::channel(100);  // TODO buffer size
        let (out_sender, out_receiver) = mpsc::channel(100);  // TODO buffer size
        let (local_sender, local_receiver) = mpsc::channel(100);  // TODO buffer size
        let (stop_sender, stop_receiver) = oneshot::channel();
        let mut sp_comp = SequencePaxosComp::new(sp, local_receiver, in_receiver, out_sender, leader_receive, stop_receiver);

        let sp_user_handle = SequencePaxosHandle::with(in_sender, out_receiver);
        let sp_handle = InternalSPHandle::with(stop_sender, local_sender);

        let (ble_in_sender, ble_in_receiver) = mpsc::channel(100);  // TODO buffer size
        let (ble_out_sender, ble_out_receiver) = mpsc::channel(100);  // TODO buffer size
        let (ble_stop_sender, ble_stop_receiver) = oneshot::channel();
        let mut ble_comp = BLEComp::new(ble, leader_send, ble_in_receiver, ble_out_sender, ble_stop_receiver);

        let ble_user_handle = BLEHandle::with(ble_in_sender, ble_out_receiver);
        let ble_handle = InternalBLEHandle::with(ble_stop_sender);

        let runtime = Builder::new_multi_thread()
            .worker_threads(4)
            .enable_time()
            .build()
            .expect("Failed to build runtime");

        runtime.spawn(async move { sp_comp.run().await });
        runtime.spawn(async move { ble_comp.run().await });

        let op = Self { pid, sp_comp: sp_handle, ble_comp: ble_handle, _p: Default::default(), runtime };
        OmniPaxosHandle { omni_paxos: op, seq_paxos_handle: sp_user_handle, ble_handle: ble_user_handle }
    }

    pub async fn append(&self, entry: T) -> Result<(), ProposeErr<T>> {
        let (send_resp, recv_resp) = oneshot::channel();
        let req = SPRequest::Append((entry, send_resp));
        if let Err(_) = self.sp_comp.local_requests.send(req).await {
            todo!()
        }
        recv_resp.await.expect("Sequence Paxos dropped response channel")
    }

    pub fn reconfigure() {
        todo!()
    }
}