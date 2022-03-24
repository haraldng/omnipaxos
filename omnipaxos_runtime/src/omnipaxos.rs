use crate::util::*;
use omnipaxos_core::{
    ballot_leader_election::{BLEConfig, Ballot},
    sequence_paxos::{CompactionErr, ProposeErr, ReconfigurationRequest, SequencePaxosConfig},
    storage::{Entry, Snapshot, Storage},
};

use crate::{ballot_leader_election::*, sequence_paxos::*, util::defaults::*};
use omnipaxos_core::{
    storage::StopSign,
    util::{LogEntry, SnapshottedEntry, TrimmedIndex},
};
use std::{
    ops::{Bound, RangeBounds},
    time::Duration,
};
use tokio::{
    runtime::{Builder, Runtime},
    sync::{mpsc, oneshot, watch},
};

/// Handle for interacting with the OmniPaxos omnipaxos_runtime.
pub struct OmniPaxosHandle<T: Entry, S: Snapshot<T>> {
    /// The local instance of this OmniPaxos node. Used to append and read entries to/from the replicated log.
    pub omni_paxos: OmniPaxosNode<T, S>,
    /// The Sequence Paxos handle of this OmniPaxos node.
    /// Consist of incoming and outgoing channels for Sequence Paxos.
    /// The user should send the messages in the outgoing channels on the network layer.
    /// When the user receives any incoming `Message` from the network layer, it should be passed into the incoming channel.
    /// See `SequencePaxosHandle` for more info.
    pub seq_paxos_handle: SequencePaxosHandle<T, S>,
    /// The Ballot Leader Election handle of this OmniPaxos node.
    /// Consist of incoming and outgoing channels for the Ballot Leader Election.
    /// The user should send the messages in the outgoing channels on the network layer.
    /// When the user receives any incoming `BLEMessage` from the network layer, it should be passed into the incoming channel.
    /// See `BLEHandle` for more info.
    pub ble_handle: BLEHandle,
}

/// The local instance of this OmniPaxos node. Used to append and read entries to/from the replicated log.
pub struct OmniPaxosNode<T: Entry, S: Snapshot<T>> {
    sp_comp: InternalSPHandle<T, S>,
    ble_comp: InternalBLEHandle,
    runtime: Option<Runtime>, // wrapped in Option to be able to move when shutting down.
}

impl<T, S> OmniPaxosNode<T, S>
where
    T: Entry + Send + 'static,
    S: Snapshot<T> + Send + 'static,
{
    /// Create a new OmniPaxos node. This starts the local omnipaxos_runtime.
    /// # Arguments
    /// * conf: The configuration for this node.
    /// * storage: User-provided storage backend used to store the log and variables of OmniPaxos.
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

        // TODO omnipaxos_runtime config
        let runtime = Builder::new_multi_thread()
            .worker_threads(4)
            .enable_time()
            .build()
            .expect("Failed to build omnipaxos_runtime");

        runtime.spawn(async move { sp_comp.run().await });
        runtime.spawn(async move { ble_comp.run().await });

        let op = Self {
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

        /* create BLEComp */
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

    /// Append an entry to the replicated log. The call returns when the entry is handled by the local Sequence Paxos component.
    /// ** Note the entry is NOT decided yet when the call returns. **
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

    /// Returns the decided index of the replicated log.
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

    /// Returns the pid of the current leader.
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

    /// Reads the given range in the replicated log. If the range is out of bounds, `None` is returned.
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

    /// Initiates the cluster to trim the replicated log.
    /// # Arguments
    /// * `trim_index` - Deletes all entries up to [`trim_index`], if the [`trim_index`] is `None` then the minimum index accepted by **ALL** servers will be used as the [`trim_index`].
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

    /// Snapshot the replicated log. Note: the log can at most be snapshotted to the decided index.
    /// # Arguments
    /// * `snapshot_idx` - Snapshot up to [`snapshot_idx`], if the [`snapshot_idx`] is `None` then the decided index will be used as the [`snapshot_idx`].
    /// * `local_only` - If `true`, only this node will snapshot. Else, all other nodes in the cluster will create the snapshot.
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

    /// Get the compacted index. This index could be trimmed or snapshotted.
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

    /// Read the decided suffic in the log from `from_idx`.
    pub async fn read_decided_suffix(&self, from_idx: u64) -> Option<Vec<ReadEntry<T, S>>> {
        let (send_resp, recv_resp) = oneshot::channel();
        let read = ReadRequest::with(from_idx, None, send_resp);
        let req = Request::ReadDecidedSuffix(read);
        self.sp_comp
            .local_requests
            .send(req)
            .await
            .unwrap_or_else(|_| panic!("Failed to send local request"));
        recv_resp
            .await
            .expect("Sequence Paxos dropped response channel")
    }

    /// Initiate a reconfiguration. This function returns when the local Sequence Paxos component has handled the request.
    /// ** Note the reconfiguration is NOT completed yet when the call returns. **
    pub async fn reconfigure(&self, rc: ReconfigurationRequest) -> Result<(), ProposeErr<T>> {
        let (send_resp, recv_resp) = oneshot::channel();
        let req = Request::Reconfigure(rc, send_resp);
        self.sp_comp
            .local_requests
            .send(req)
            .await
            .unwrap_or_else(|_| panic!("Failed to send local request"));
        recv_resp
            .await
            .expect("Sequence Paxos dropped response channel")
    }

    /// This should be called when the network layer indicates that the node has been reconnected to another node `pid`
    pub async fn reconnected(&self, pid: u64) {
        let req = Request::Reconnected(pid);
        self.sp_comp
            .local_requests
            .send(req)
            .await
            .unwrap_or_else(|_| panic!("Failed to send local request"));
    }

    /// Shut down the omnipaxos_runtime.
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
            .expect("No omnipaxos_runtime to stop")
            .shutdown_timeout(timeout);
    }
}

/// Configuration for an OmniPaxos node.
/// # Fields
/// * `pid`: The unique identifier of this node. Must be greater than 0.
/// * `peers`: The `pid`s of the other replicas in the configuration.
/// * `leader_timeout`: Timeout before initiating a leader change.
/// * `buffer_size`: The buffer size for channels.
/// * `initial_leader`: Option to use an initial leader.
/// * `initial_leader_timeout`: Initial timeout used for leader election. Can be used to elect a leader quicker initially.
/// * `priority`: Priority of this node to become the leader.
/// * `logger_path`: Path for the logger.
#[derive(Clone, Debug)]
pub struct NodeConfig {
    pid: u64,
    peers: Vec<u64>,
    leader_timeout: Duration,
    buffer_size: usize,
    initial_leader: Option<Ballot>,
    initial_leader_timeout: Option<Duration>,
    priority: Option<u64>,
    logger_path: Option<String>,
}

#[allow(missing_docs)]
impl NodeConfig {
    pub fn set_pid(&mut self, pid: u64) {
        self.pid = pid;
    }

    pub fn get_pid(&self) -> u64 {
        self.pid
    }

    pub fn set_peers(&mut self, peers: Vec<u64>) {
        self.peers = peers;
    }

    pub fn get_peers(&self) -> &[u64] {
        self.peers.as_slice()
    }

    pub fn set_leader_timeout(&mut self, timeout: Duration) {
        self.leader_timeout = timeout;
    }

    pub fn get_leader_timeout(&self) -> Duration {
        self.leader_timeout
    }

    pub fn set_buffer_size(&mut self, size: usize) {
        self.buffer_size = size;
    }

    pub fn get_buffer_size(&self) -> usize {
        self.buffer_size
    }

    pub fn set_initial_leader(&mut self, b: Ballot) {
        self.initial_leader = Some(b);
    }

    pub fn get_initial_leader(&self) -> Option<Ballot> {
        self.initial_leader
    }

    pub fn set_initial_leader_timeout(&mut self, timeout: Duration) {
        self.initial_leader_timeout = Some(timeout);
    }

    pub fn get_initial_leader_timeout(&self) -> Option<Duration> {
        self.initial_leader_timeout
    }

    pub fn set_priority(&mut self, priority: u64) {
        self.priority = Some(priority);
    }

    pub fn get_priority(&self) -> Option<u64> {
        self.priority
    }

    pub fn set_logger_path(&mut self, s: String) {
        self.logger_path = Some(s);
    }

    pub fn get_logger_path(&self) -> Option<&String> {
        self.logger_path.as_ref()
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

/// Errors in `NodeConfig`
#[derive(Debug)]
pub enum NodeConfigErr {
    /// `pid` must not be 0.
    InvalidPid(u64),
    /// `peers` should not include own `pid`.
    InvalidPeers(u64, Vec<u64>),
}

/// Used for reading from the async omnipaxos runtime.
#[derive(Debug, Clone)]
pub enum ReadEntry<T, S>
where
    T: Entry,
    S: Snapshot<T>,
{
    /// The entry is decided.
    Decided(T),
    /// The entry is NOT decided. Might be removed from the log at a later time.
    Undecided(T),
    /// The entry has been trimmed.
    Trimmed(TrimmedIndex),
    /// The entry has been snapshotted.
    Snapshotted(SnapshottedEntry<T, S>),
    /// This Sequence Paxos instance has been stopped for reconfiguration.
    StopSign(StopSign),
}

impl<'a, T, S> From<LogEntry<'a, T, S>> for ReadEntry<T, S>
where
    T: Entry,
    S: Snapshot<T>,
{
    fn from(e: LogEntry<'a, T, S>) -> Self {
        match e {
            LogEntry::Decided(t) => Self::Decided(t.clone()),
            LogEntry::Undecided(t) => Self::Undecided(t.clone()),
            LogEntry::Trimmed(t) => Self::Trimmed(t),
            LogEntry::Snapshotted(s) => Self::Snapshotted(s),
            LogEntry::StopSign(ss) => Self::StopSign(ss),
        }
    }
}
