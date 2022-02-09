use crate::util::*;
use core::{
    ballot_leader_election::{Ballot, BLEConfig},
    sequence_paxos::{CompactionErr, ProposeErr, ReconfigurationRequest, SequencePaxosConfig},
    storage::{Entry, Snapshot, Storage},
};

use crate::{ballot_leader_election::*, sequence_paxos::*};
use std::{
    ops::{Bound, RangeBounds},
    time::Duration,
};
use tokio::{
    runtime::{Builder, Runtime},
    sync::{mpsc, oneshot, watch},
};
use crate::util::defaults::*;

pub struct OmniPaxosHandle<T: Entry, S: Snapshot<T>> {
    pub omni_paxos: OmniPaxosNode<T, S>,
    pub seq_paxos_handle: SequencePaxosHandle<T, S>,
    pub ble_handle: BLEHandle,
}

pub struct OmniPaxosNode<T: Entry, S: Snapshot<T>> {
    pid: u64,
    sp_comp: InternalSPHandle<T, S>,
    ble_comp: InternalBLEHandle,
    runtime: Option<Runtime>, // wrapped in Option to be able to move when shutting down.
}

impl<T, S> OmniPaxosNode<T, S>
where
    T: Entry + Send + 'static,
    S: Snapshot<T> + Send + 'static,
{
    pub fn new<B: Storage<T, S> + Send + 'static>(
        conf: NodeConfig,
        storage: B,
    ) -> OmniPaxosHandle<T, S> {
        conf.validate()
            .unwrap_or_else(|e| panic!("Configuration error: {:?}", e));
        let sp_conf = conf.create_sequence_paxos_config();
        let ble_conf = conf.create_ble_config();
        let (leader_send, leader_receive) = watch::channel(Ballot::default()); // create leader election watch channel
        let (mut sp_comp, internal_sp_handle, sp_user_handle) =
            Self::create_sequence_paxos(leader_receive, sp_conf, storage);
        let (mut ble_comp, internal_ble_handle, ble_user_handle) =
            Self::create_ble(leader_send, ble_conf);

        // TODO runtime config
        let runtime = Builder::new_multi_thread()
            .worker_threads(4)
            .enable_time()
            .build()
            .expect("Failed to build runtime");

        runtime.spawn(async move { sp_comp.run().await });
        runtime.spawn(async move { ble_comp.run().await });

        let op = Self {
            pid: conf.pid,
            sp_comp: internal_sp_handle,
            ble_comp: internal_ble_handle,
            runtime: Some(runtime),
        };
        OmniPaxosHandle {
            omni_paxos: op,
            seq_paxos_handle: sp_user_handle,
            ble_handle: ble_user_handle,
        }
    }

    fn create_sequence_paxos<B: Storage<T, S> + Send + 'static>(
        ble_recv: watch::Receiver<Ballot>,
        sp_conf: SequencePaxosConfig,
        storage: B,
    ) -> (
        SequencePaxosComp<T, S, B>,
        InternalSPHandle<T, S>,
        SequencePaxosHandle<T, S>,
    ) {
        /* create channels */
        let buffer_size = sp_conf.get_buffer_size();
        let (in_sender, in_receiver) = mpsc::channel(buffer_size);
        let (out_sender, out_receiver) = mpsc::channel(buffer_size);
        let (local_sender, local_receiver) = mpsc::channel(buffer_size);
        let (stop_sender, stop_receiver) = oneshot::channel();

        let sp_comp = SequencePaxosComp::new(
            sp_conf,
            storage,
            local_receiver,
            in_receiver,
            out_sender,
            ble_recv,
            stop_receiver,
        );
        let sp_user_handle = SequencePaxosHandle::with(in_sender, out_receiver);
        let internal_sp_handle = InternalSPHandle::with(stop_sender, local_sender);
        (sp_comp, internal_sp_handle, sp_user_handle)
    }

    fn create_ble(
        ble_send: watch::Sender<Ballot>,
        ble_conf: BLEConfig,
    ) -> (BLEComp, InternalBLEHandle, BLEHandle) {
        /* create channels */
        let buffer_size = ble_conf.get_buffer_size();
        let (ble_in_sender, ble_in_receiver) = mpsc::channel(buffer_size);
        let (ble_out_sender, ble_out_receiver) = mpsc::channel(buffer_size);
        let (ble_stop_sender, ble_stop_receiver) = oneshot::channel();

        let ble_comp = BLEComp::new(
            ble_conf,
            ble_send,
            ble_in_receiver,
            ble_out_sender,
            ble_stop_receiver,
        );
        let ble_user_handle = BLEHandle::with(ble_in_sender, ble_out_receiver);
        let internal_ble_handle = InternalBLEHandle::with(ble_stop_sender);
        (ble_comp, internal_ble_handle, ble_user_handle)
    }

    pub async fn append(&self, entry: T) -> Result<(), ProposeErr<T>> {
        let (send_resp, recv_resp) = oneshot::channel();
        let req = Request::Append(entry, send_resp);
        self.sp_comp
            .local_requests
            .send(req)
            .await
            .unwrap_or_else(|_| panic!("Failed to send local request"));
        recv_resp
            .await
            .expect("Sequence Paxos dropped response channel")
    }

    pub async fn get_decided_idx(&self) -> u64 {
        let (send_resp, recv_resp) = oneshot::channel();
        let req = Request::GetDecidedIdx(send_resp);
        self.sp_comp
            .local_requests
            .send(req)
            .await
            .unwrap_or_else(|_| panic!("Failed to send local request"));
        recv_resp
            .await
            .expect("Sequence Paxos dropped response channel")
    }

    pub async fn get_current_leader(&self) -> u64 {
        let (send_resp, recv_resp) = oneshot::channel();
        let req = Request::GetLeader(send_resp);
        self.sp_comp
            .local_requests
            .send(req)
            .await
            .unwrap_or_else(|_| panic!("Failed to send local request"));
        recv_resp
            .await
            .expect("Sequence Paxos dropped response channel")
    }

    pub async fn read_entries<R: RangeBounds<u64>>(&self, r: R) -> Option<Vec<ReadEntry<T, S>>> {
        let (send_resp, recv_resp) = oneshot::channel();
        let from_idx = match r.start_bound() {
            Bound::Included(i) => *i,
            Bound::Excluded(e) => *e + 1,
            Bound::Unbounded => 0,
        };
        let to_idx = match r.end_bound() {
            Bound::Included(i) => Some(*i + 1),
            Bound::Excluded(e) => Some(*e),
            Bound::Unbounded => None,
        };
        let read = ReadRequest::with(from_idx, to_idx, send_resp);
        let req = Request::Read(read);
        self.sp_comp
            .local_requests
            .send(req)
            .await
            .unwrap_or_else(|_| panic!("Failed to send local request"));
        recv_resp
            .await
            .expect("Sequence Paxos dropped response channel")
    }

    pub async fn trim(&self, trim_idx: Option<u64>) -> Result<(), CompactionErr> {
        let (send_resp, recv_resp) = oneshot::channel();
        self.sp_comp
            .local_requests
            .send(Request::Trim(trim_idx, send_resp))
            .await
            .unwrap_or_else(|_| panic!("Failed to send local request"));
        recv_resp
            .await
            .expect("Sequence Paxos dropped response channel")
    }

    pub async fn snapshot(
        &self,
        snapshot_idx: Option<u64>,
        local_only: bool,
    ) -> Result<(), CompactionErr> {
        let (send_resp, recv_resp) = oneshot::channel();
        self.sp_comp
            .local_requests
            .send(Request::Snapshot(snapshot_idx, local_only, send_resp))
            .await
            .unwrap_or_else(|_| panic!("Failed to send local request"));
        recv_resp
            .await
            .expect("Sequence Paxos dropped response channel")
    }

    pub async fn get_compacted_idx(&self) -> u64 {
        let (send_resp, recv_resp) = oneshot::channel();
        let req = Request::GetCompactedIdx(send_resp);
        self.sp_comp
            .local_requests
            .send(req)
            .await
            .unwrap_or_else(|_| panic!("Failed to send local request"));
        recv_resp
            .await
            .expect("Sequence Paxos dropped response channel")
    }

    pub async fn read_decided_suffix(&self, from_idx: u64) -> Option<Vec<ReadEntry<T, S>>> {
        let (send_resp, recv_resp) = oneshot::channel();
        let read = ReadRequest::with(from_idx, None, send_resp);
        let req = Request::ReadDecidedSuffix(read);
        if let Err(_) = self.sp_comp.local_requests.send(req).await {
            todo!()
        }
        recv_resp
            .await
            .expect("Sequence Paxos dropped response channel")
    }

    pub async fn reconfigure(&self, rc: ReconfigurationRequest) -> Result<(), ProposeErr<T>> {
        let (send_resp, recv_resp) = oneshot::channel();
        let req = Request::Reconfigure(rc, send_resp);
        if let Err(_) = self.sp_comp.local_requests.send(req).await {
            todo!()
        }
        recv_resp
            .await
            .expect("Sequence Paxos dropped response channel")
    }

    pub async fn reconnected(&self, pid: u64) {
        let req = Request::Reconnected(pid);
        if let Err(_) = self.sp_comp.local_requests.send(req).await {
            todo!()
        }
    }

    pub fn stop(&mut self, timeout: Duration) {
        let _ = self
            .sp_comp
            .stop
            .take()
            .expect("No stop channel found for SequencePaxos")
            .send(Stop);
        let _ = self
            .ble_comp
            .stop
            .take()
            .expect("No stop channel found for BLE")
            .send(Stop);
        self.runtime
            .take()
            .expect("No runtime to stop")
            .shutdown_timeout(timeout);
    }
}

#[derive(Clone, Debug)]
pub struct NodeConfig {
    pub pid: u64,
    pub peers: Vec<u64>,
    pub leader_timeout: Duration,
    pub buffer_size: usize,
    pub initial_leader: Option<Ballot>,
    pub initial_leader_timeout: Option<Duration>,
    pub priority: Option<u64>,
    pub logger_path: Option<String>,
}

impl NodeConfig {
    pub fn set_pid(&mut self, pid: u64) {
        self.pid = pid;
    }

    pub fn set_peers(&mut self, peers: Vec<u64>) {
        self.peers = peers;
    }

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

    fn create_sequence_paxos_config(&self) -> SequencePaxosConfig {
        let mut conf = SequencePaxosConfig::default();
        conf.set_pid(self.pid);
        conf.set_peers(self.peers.clone());
        conf.set_buffer_size(self.buffer_size);
        if let Some(l) = self.initial_leader {
            conf.set_skip_prepare_use_leader(l);
        }
        if let Some(p) = &self.logger_path {
            conf.set_logger_file_path(format!("{}/paxos.log", p))
        }
        conf
    }

    fn create_ble_config(&self) -> BLEConfig {
        let mut conf = BLEConfig::default();
        conf.set_pid(self.pid);
        conf.set_peers(self.peers.clone());
        conf.set_hb_delay(duration_to_num_ticks(self.leader_timeout));
        conf.set_buffer_size(self.buffer_size);
        if let Some(l) = self.initial_leader {
            conf.set_initial_leader(l);
        }
        if let Some(d) = self.initial_leader_timeout {
            conf.set_initial_delay(duration_to_num_ticks(d));
        }
        if let Some(prio) = self.priority {
            conf.set_priority(prio);
        }
        if let Some(p) = &self.logger_path {
            conf.set_logger_file_path(format!("{}/ble.log", p))
        }
        conf
    }
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            pid: 0,
            peers: vec![],
            buffer_size: BUFFER_SIZE,
            initial_leader: None,
            leader_timeout: Duration::from_millis(HB_TIMEOUT),
            initial_leader_timeout: None,
            priority: None,
            logger_path: None,
        }
    }
}

impl NodeConfig {
    fn validate(&self) -> Result<(), NodeConfigErr> {
        if self.pid == 0 {
            return Err(NodeConfigErr::InvalidPid(self.pid));
        }
        if self.peers.is_empty() || self.peers.contains(&self.pid) {
            return Err(NodeConfigErr::InvalidPeers(self.pid, self.peers.clone()));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum NodeConfigErr {
    InvalidPid(u64),
    InvalidPeers(u64, Vec<u64>),
}