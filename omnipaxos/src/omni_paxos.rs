#[cfg(feature = "toml_config")]
use crate::errors::ConfigError;
use crate::{
    ballot_leader_election::{Ballot, BallotLeaderElection},
    messages::Message,
    sequence_paxos::SequencePaxos,
    storage::{Entry, StopSign, Storage},
    util::{
        defaults::{BUFFER_SIZE, ELECTION_TIMEOUT, RESEND_MESSAGE_TIMEOUT},
        LogEntry, LogicalClock, NodeId,
    },
};
#[cfg(feature = "toml_config")]
use serde::Deserialize;
#[cfg(feature = "toml_config")]
use std::fs;
use std::{
    error::Error,
    fmt::{Debug, Display},
    ops::RangeBounds,
};
#[cfg(feature = "toml_config")]
use toml;

/// Configuration for `OmniPaxos`.
/// # Fields
/// * `configuration_id`: The identifier for the configuration that this Sequence Paxos replica is part of.
/// * `pid`: The unique identifier of this node. Must not be 0.
/// * `peers`: The peers of this node i.e. the `pid`s of the other replicas in the configuration.
/// * `batch_size`: The size of the buffer for log batching. 1 means no batching.
/// * `buffer_size`: The buffer size for outgoing messages.
/// * `election_tick_timeout`: The number of calls to `tick()` before leader election is updated
/// * `resend_message_tick_timeout`: The number of calls to `tick()` before an omnipaxos message is considered
/// dropped and thus resent.
/// * `skip_prepare_use_leader`: The initial leader of the cluster. Could be used in combination with reconfiguration to skip the prepare phase in the new configuration.
/// * `logger`: Custom logger for logging events of Sequence Paxos.
/// * `logger_file_path`: The path where the default logger logs events.
#[allow(missing_docs)]
#[derive(Clone, Debug)]
#[cfg_attr(feature = "toml_config", derive(Deserialize), serde(default))]
pub struct OmniPaxosConfig {
    pub configuration_id: u32,
    pub pid: NodeId,
    pub peers: Vec<u64>,
    pub batch_size: usize,
    pub buffer_size: usize,
    pub election_tick_timeout: u64,
    pub resend_message_tick_timeout: u64,
    pub skip_prepare_use_leader: Option<Ballot>,
    #[cfg(feature = "logging")]
    pub logger_file_path: Option<String>,
    /*** BLE config fields ***/
    pub leader_priority: u64,
    pub initial_leader: Option<Ballot>,
}

impl OmniPaxosConfig {
    /// Creates a new `OmniPaxosConfig` from a `toml` file.
    #[cfg(feature = "toml_config")]
    pub fn with_toml(file_path: &str) -> Result<Self, ConfigError> {
        let config_file = fs::read_to_string(file_path)?;
        let config: OmniPaxosConfig = toml::from_str(&config_file)?;
        Ok(config)
    }

    /// Checks all configurations and returns the local OmniPaxos node if successful.
    pub fn build<T, B>(self, storage: B) -> OmniPaxos<T, B>
    where
        T: Entry,
        B: Storage<T>,
    {
        assert_ne!(self.pid, 0, "Pid cannot be 0");
        assert_ne!(self.configuration_id, 0, "Configuration id cannot be 0");
        assert!(!self.peers.is_empty(), "Peers cannot be empty");
        assert!(
            !self.peers.contains(&self.pid),
            "Peers should not include self pid"
        );
        assert!(self.batch_size >= 1, "Batch size must be greater than or equal to 1");
        assert!(self.buffer_size > 0, "Buffer size must be greater than 0");
        if let Some(x) = self.skip_prepare_use_leader {
            assert_ne!(x.pid, 0, "Initial leader cannot be 0")
        };
        OmniPaxos {
            seq_paxos: SequencePaxos::with(self.clone().into(), storage),
            ble: BallotLeaderElection::with(self.clone().into()),
            election_clock: LogicalClock::with(self.election_tick_timeout),
            resend_message_clock: LogicalClock::with(self.resend_message_tick_timeout),
        }
    }
}

impl Default for OmniPaxosConfig {
    fn default() -> Self {
        Self {
            configuration_id: 0,
            pid: 0,
            peers: Vec::new(),
            buffer_size: BUFFER_SIZE,
            election_tick_timeout: ELECTION_TIMEOUT,
            resend_message_tick_timeout: RESEND_MESSAGE_TIMEOUT,
            skip_prepare_use_leader: None,
            #[cfg(feature = "logging")]
            logger_file_path: None,
            batch_size: 1,
            leader_priority: 0,
            initial_leader: None,
        }
    }
}

/// The `OmniPaxos` struct represents an OmniPaxos server. Maintains the replicated log that can be read from and appended to.
/// It also handles incoming messages and produces outgoing messages that you need to fetch and send periodically using your own network implementation.
pub struct OmniPaxos<T, B>
where
    T: Entry,
    B: Storage<T>,
{
    seq_paxos: SequencePaxos<T, B>,
    ble: BallotLeaderElection,
    election_clock: LogicalClock,
    resend_message_clock: LogicalClock,
}

impl<T, B> OmniPaxos<T, B>
where
    T: Entry,
    B: Storage<T>,
{
    /// Initiates the trim process.
    /// # Arguments
    /// * `trim_index` - Deletes all entries up to [`trim_index`], if the [`trim_index`] is `None` then the minimum index accepted by **ALL** servers will be used as the [`trim_index`].
    pub fn trim(&mut self, trim_index: Option<u64>) -> Result<(), CompactionErr> {
        self.seq_paxos.trim(trim_index)
    }

    /// Trim the log and create a snapshot. ** Note: only up to the `decided_idx` can be snapshotted **
    /// # Arguments
    /// `compact_idx` - Snapshots all entries < [`compact_idx`], if the [`compact_idx`] is None then the decided index will be used.
    /// `local_only` - If `true`, only this server snapshots the log. If `false` all servers performs the snapshot.
    pub fn snapshot(
        &mut self,
        compact_idx: Option<u64>,
        local_only: bool,
    ) -> Result<(), CompactionErr> {
        self.seq_paxos.snapshot(compact_idx, local_only)
    }

    /// Return the decided index.
    pub fn get_decided_idx(&self) -> u64 {
        self.seq_paxos.get_decided_idx()
    }

    /// Return trim index from storage.
    pub fn get_compacted_idx(&self) -> u64 {
        self.seq_paxos.get_compacted_idx()
    }

    /// Recover from failure. Goes into recover state and sends `PrepareReq` to all peers.
    pub fn fail_recovery(&mut self) {
        self.seq_paxos.fail_recovery()
    }

    /// Returns the id of the current leader.
    pub fn get_current_leader(&self) -> Option<NodeId> {
        self.get_current_leader_ballot().map(|ballot| ballot.pid)
    }

    /// Returns the ballot of the current leader.
    pub fn get_current_leader_ballot(&self) -> Option<Ballot> {
        let ballot = self.seq_paxos.get_current_leader();
        if ballot == Ballot::default() {
            None
        } else {
            Some(ballot)
        }
    }

    /// Returns the outgoing messages from this replica. The messages should then be sent via the network implementation.
    pub fn outgoing_messages(&mut self) -> Vec<Message<T>> {
        let paxos_msgs = self
            .seq_paxos
            .get_outgoing_msgs()
            .into_iter()
            .map(|p| Message::SequencePaxos(p));
        let ble_msgs = self
            .ble
            .get_outgoing_msgs()
            .into_iter()
            .map(|b| Message::BLE(b));
        ble_msgs.chain(paxos_msgs).collect()
    }

    /// Read entry at index `idx` in the log. Returns `None` if `idx` is out of bounds.
    pub fn read(&self, idx: u64) -> Option<LogEntry<T>> {
        match self
            .seq_paxos
            .internal_storage
            .read(idx..idx + 1)
            .expect("storage error while trying to read log entries")
        {
            Some(mut v) => v.pop(),
            None => None,
        }
    }

    /// Read entries in the range `r` in the log. Returns `None` if `r` is out of bounds.
    pub fn read_entries<R>(&self, r: R) -> Option<Vec<LogEntry<T>>>
    where
        R: RangeBounds<u64>,
    {
        self.seq_paxos
            .internal_storage
            .read(r)
            .expect("storage error while trying to read log entries")
    }

    /// Read all decided entries from `from_idx` in the log. Returns `None` if `from_idx` is out of bounds.
    pub fn read_decided_suffix(&self, from_idx: u64) -> Option<Vec<LogEntry<T>>> {
        self.seq_paxos
            .internal_storage
            .read_decided_suffix(from_idx)
            .expect("storage error while trying to read decided log suffix")
    }

    /// Handle an incoming message.
    pub fn handle_incoming(&mut self, m: Message<T>) {
        match m {
            Message::SequencePaxos(p) => self.seq_paxos.handle(p),
            Message::BLE(b) => self.ble.handle(b),
        }
    }

    /// Returns whether this Sequence Paxos has been reconfigured
    pub fn is_reconfigured(&self) -> Option<StopSign> {
        self.seq_paxos.is_reconfigured()
    }

    /// Append an entry to the replicated log.
    pub fn append(&mut self, entry: T) -> Result<(), ProposeErr<T>> {
        self.seq_paxos.append(entry)
    }

    /// Propose a reconfiguration. Returns error if already stopped or new configuration is empty.
    pub fn reconfigure(&mut self, rc: ReconfigurationRequest) -> Result<(), ProposeErr<T>> {
        self.seq_paxos.reconfigure(rc)
    }

    /// Handles re-establishing a connection to a previously disconnected peer.
    /// This should only be called if the underlying network implementation indicates that a connection has been re-established.
    pub fn reconnected(&mut self, pid: NodeId) {
        self.seq_paxos.reconnected(pid)
    }

    /// Drives the election process (see `election_timeout()`) every `election_tick_timeout`
    /// ticks. Also drives the detection and re-sending of dropped OmniPaxos messages every `resend_message_tick_timeout` ticks.
    pub fn tick(&mut self) {
        if self.election_clock.tick_and_check_timeout() {
            self.election_timeout();
        }
        if self.resend_message_clock.tick_and_check_timeout() {
            self.seq_paxos.resend_message_timeout();
        }
    }

    /*** BLE calls ***/
    /// Update the custom priority used in the Ballot for this server.
    pub fn set_priority(&mut self, p: u64) {
        self.ble.set_priority(p)
    }

    /// If the heartbeat of a leader is not received when election_timeout() is called, the server might attempt to become the leader.
    /// It is also used for the election process, where the server checks if it can become the leader.
    /// This function should be called periodically to detect leader failure and drive the election process.
    /// For instance if `election_timeout()` is called every 100ms, then if the leader fails, the servers will detect it after 100ms and elect a new server after another 100ms if possible.
    pub fn election_timeout(&mut self) {
        if let Some(b) = self.ble.hb_timeout() {
            self.seq_paxos.handle_leader(b);
        }
    }
}

/// Used for proposing reconfiguration of the cluster.
#[derive(Debug, Clone)]
pub struct ReconfigurationRequest {
    /// The id of the servers in the new configuration.
    pub(crate) new_configuration: Vec<NodeId>,
    /// Optional metadata to be decided with the reconfiguration.
    pub(crate) metadata: Option<Vec<u8>>,
}

impl ReconfigurationRequest {
    /// create a `ReconfigurationRequest`.
    /// # Arguments
    /// * `new_configuration`: The pids of the nodes in the new configuration.
    /// * `metadata`: Some optional metadata in raw bytes. This could include some auxiliary data for the new configuration to start with.
    pub fn with(new_configuration: Vec<NodeId>, metadata: Option<Vec<u8>>) -> Self {
        Self {
            new_configuration,
            metadata,
        }
    }
}

/// An error returning the proposal that was failed due to that the current configuration is stopped.
#[allow(missing_docs)]
#[derive(Debug)]
pub enum ProposeErr<T>
where
    T: Entry,
{
    Normal(T),
    Reconfiguration(Vec<NodeId>),
}

/// An error returning the proposal that was failed due to that the current configuration is stopped.
#[derive(Copy, Clone, Debug)]
pub enum CompactionErr {
    /// Snapshot was called with an index that is not decided yet. Returns the currently decided index.
    UndecidedIndex(u64),
    /// Trim was called with an index that is not decided by all servers yet. Returns the index decided by ALL servers currently.
    NotAllDecided(u64),
    /// Trim was called at a follower node. Trim must be called by the leader, which is the returned NodeId.
    NotCurrentLeader(NodeId),
}

impl Error for CompactionErr {}
impl Display for CompactionErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}
