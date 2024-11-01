use crate::{
    ballot_leader_election::{Ballot, BallotLeaderElection},
    errors::{valid_config, ConfigError},
    messages::Message,
    sequence_paxos::{Phase, SequencePaxos},
    storage::{Entry, StopSign, Storage},
    util::{
        defaults::{BUFFER_SIZE, ELECTION_TIMEOUT, FLUSH_BATCH_TIMEOUT, RESEND_MESSAGE_TIMEOUT},
        ConfigurationId, FlexibleQuorum, LogEntry, LogicalClock, NodeId,
    },
    utils::{ui, ui::ClusterState},
};
#[cfg(any(feature = "toml_config", feature = "serde"))]
use serde::Deserialize;
#[cfg(feature = "serde")]
use serde::Serialize;
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
/// * `cluster_config`: The configuration settings that are cluster-wide.
/// * `server_config`: The configuration settings that are specific to this OmniPaxos server.
#[allow(missing_docs)]
#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "toml_config", derive(Deserialize), serde(default))]
pub struct OmniPaxosConfig {
    pub cluster_config: ClusterConfig,
    pub server_config: ServerConfig,
}

impl OmniPaxosConfig {
    /// Checks that all the fields of the cluster config are valid.
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.cluster_config.validate()?;
        self.server_config.validate()?;
        valid_config!(
            self.cluster_config.nodes.contains(&self.server_config.pid),
            "Nodes must include own server pid"
        );
        Ok(())
    }

    /// Creates a new `OmniPaxosConfig` from a `toml` file.
    #[cfg(feature = "toml_config")]
    pub fn with_toml(file_path: &str) -> Result<Self, ConfigError> {
        let config_file = fs::read_to_string(file_path)?;
        let config: OmniPaxosConfig = toml::from_str(&config_file)?;
        config.validate()?;
        Ok(config)
    }

    /// Checks all configuration fields and returns the local OmniPaxos node if successful.
    pub fn build<T, B>(self, storage: B) -> Result<OmniPaxos<T, B>, ConfigError>
    where
        T: Entry,
        B: Storage<T>,
    {
        self.validate()?;
        // Use stored ballot as initial BLE leader
        let recovered_leader = storage
            .get_promise()
            .expect("storage error while trying to read promise");
        Ok(OmniPaxos {
            ble: BallotLeaderElection::with(self.clone().into(), recovered_leader),
            election_clock: LogicalClock::with(self.server_config.election_tick_timeout),
            resend_message_clock: LogicalClock::with(
                self.server_config.resend_message_tick_timeout,
            ),
            flush_batch_clock: LogicalClock::with(self.server_config.flush_batch_tick_timeout),
            seq_paxos: SequencePaxos::with(self.into(), storage),
        })
    }
}

/// Configuration for an `OmniPaxos` cluster.
/// # Fields
/// * `configuration_id`: The identifier for the cluster configuration that this OmniPaxos server is part of.
/// * `nodes`: The nodes in the cluster i.e. the `pid`s of the other servers in the configuration.
/// * `flexible_quorum` : Defines read and write quorum sizes. Can be used for different latency vs fault tolerance tradeoffs.
#[derive(Clone, Debug, PartialEq, Default)]
#[cfg_attr(any(feature = "serde", feature = "toml_config"), derive(Deserialize))]
#[cfg_attr(feature = "toml_config", serde(default))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct ClusterConfig {
    /// The identifier for the cluster configuration that this OmniPaxos server is part of. Must
    /// not be 0 and be greater than the previous configuration's id.
    pub configuration_id: ConfigurationId,
    /// The nodes in the cluster i.e. the `pid`s of the servers in the configuration.
    pub nodes: Vec<NodeId>,
    /// Defines read and write quorum sizes. Can be used for different latency vs fault tolerance tradeoffs.
    pub flexible_quorum: Option<FlexibleQuorum>,
    pub use_metronome: usize,
    pub metronome_quorum_size: Option<usize>,
}

impl ClusterConfig {
    /// Checks that all the fields of the cluster config are valid.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let num_nodes = self.nodes.len();
        valid_config!(num_nodes > 1, "Need more than 1 node");
        valid_config!(self.configuration_id != 0, "Configuration ID cannot be 0");
        if let Some(FlexibleQuorum {
            read_quorum_size,
            write_quorum_size,
        }) = self.flexible_quorum
        {
            valid_config!(
                read_quorum_size + write_quorum_size > num_nodes,
                "The quorums must overlap i.e., the sum of their sizes must exceed the # of nodes"
            );
            valid_config!(
                read_quorum_size >= 2 && read_quorum_size <= num_nodes,
                "Read quorum must be in range 2 to # of nodes in the cluster"
            );
            valid_config!(
                write_quorum_size >= 2 && write_quorum_size <= num_nodes,
                "Write quorum must be in range 2 to # of nodes in the cluster"
            );
            valid_config!(
                read_quorum_size >= write_quorum_size,
                "Read quorum size must be >= the write quorum size."
            );
            if let Some(metronome_quorum_size) = self.metronome_quorum_size {
                valid_config!(
                    metronome_quorum_size >= write_quorum_size,
                    "Metronome quorum size cannot be smaller that write quorum size"
                );
            }
        } else {
            if let Some(metronome_quorum_size) = self.metronome_quorum_size {
                valid_config!(
                    metronome_quorum_size >= num_nodes / 2 + 1,
                    "Metronome quorum size cannot be smaller than majority quorum"
                );
            }
        }
        Ok(())
    }

    /// Checks all configuration fields and builds a local OmniPaxos node with settings for this
    /// node defined in `server_config` and using storage `with_storage`.
    pub fn build_for_server<T, B>(
        self,
        server_config: ServerConfig,
        with_storage: B,
    ) -> Result<OmniPaxos<T, B>, ConfigError>
    where
        T: Entry,
        B: Storage<T>,
    {
        let op_config = OmniPaxosConfig {
            cluster_config: self,
            server_config,
        };
        op_config.build(with_storage)
    }
}

/// Configuration for a singular `OmniPaxos` instance in a cluster.
/// # Fields
/// * `pid`: The unique identifier of this node. Must not be 0.
/// * `election_tick_timeout`: The number of calls to `tick()` before leader election is updated.
/// If this is set to 5 and `tick()` is called every 10ms, then the election timeout will be 50ms. Must not be 0.
/// * `resend_message_tick_timeout`: The number of calls to `tick()` before a message is considered dropped and thus resent. Must not be 0.
/// * `buffer_size`: The buffer size for outgoing messages.
/// * `batch_size`: The size of the buffer for log batching. The default is 1, which means no batching.
/// * `logger_file_path`: The path where the default logger logs events.
/// * `leader_priority` : Custom priority for this node to be elected as the leader.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "toml_config", derive(Deserialize), serde(default))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct ServerConfig {
    /// The unique identifier of this node. Must not be 0.
    pub pid: NodeId,
    /// The number of calls to `tick()` before leader election is updated. If this is set to 5 and `tick()` is called every 10ms, then the election timeout will be 50ms.
    pub election_tick_timeout: u64,
    /// The number of calls to `tick()` before a message is considered dropped and thus resent. Must not be 0.
    pub resend_message_tick_timeout: u64,
    /// The buffer size for outgoing messages.
    pub buffer_size: usize,
    /// The size of the buffer for log batching. The default is 1, which means no batching.
    pub batch_size: usize,
    /// The number of calls to `tick()` before the batched log entries are flushed.
    pub flush_batch_tick_timeout: u64,
    /// Custom priority for this node to be elected as the leader.
    pub leader_priority: u32,
    /// The path where the default logger logs events.
    #[cfg(feature = "logging")]
    pub logger_file_path: Option<String>,
    /// Custom logger, if provided, will be used instead of the default logger.
    #[cfg(feature = "logging")]
    #[cfg_attr(feature = "toml_config", serde(skip_deserializing))]
    #[cfg_attr(feature = "serde", serde(skip_serializing))]
    pub custom_logger: Option<slog::Logger>,
}

impl ServerConfig {
    /// Checks that all the fields of the server config are valid.
    pub fn validate(&self) -> Result<(), ConfigError> {
        valid_config!(self.pid != 0, "Server pid cannot be 0");
        valid_config!(self.buffer_size != 0, "Buffer size must be greater than 0");
        // valid_config!(self.batch_size != 0, "Batch size must be greater than 0");
        valid_config!(
            self.election_tick_timeout != 0,
            "Election tick timeout must be greater than 0"
        );
        valid_config!(
            self.resend_message_tick_timeout != 0,
            "Resend message tick timeout must be greater than 0"
        );
        Ok(())
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            pid: 0,
            election_tick_timeout: ELECTION_TIMEOUT,
            resend_message_tick_timeout: RESEND_MESSAGE_TIMEOUT,
            buffer_size: BUFFER_SIZE,
            batch_size: 0,
            flush_batch_tick_timeout: FLUSH_BATCH_TIMEOUT,
            leader_priority: 0,
            #[cfg(feature = "logging")]
            logger_file_path: None,
            #[cfg(feature = "logging")]
            custom_logger: None,
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
    flush_batch_clock: LogicalClock,
}

impl<T, B> OmniPaxos<T, B>
where
    T: Entry,
    B: Storage<T>,
{
    /// Initiates the trim process.
    /// # Arguments
    /// * `trim_index` - Deletes all entries up to [`trim_index`], if the [`trim_index`] is `None` then the minimum index accepted by **ALL** servers will be used as the [`trim_index`].
    pub fn trim(&mut self, trim_index: Option<usize>) -> Result<(), CompactionErr> {
        self.seq_paxos.trim(trim_index)
    }

    /// Trim the log and create a snapshot. ** Note: only up to the `decided_idx` can be snapshotted **
    /// # Arguments
    /// `compact_idx` - Snapshots all entries < [`compact_idx`], if the [`compact_idx`] is None then the decided index will be used.
    /// `local_only` - If `true`, only this server snapshots the log. If `false` all servers performs the snapshot.
    pub fn snapshot(
        &mut self,
        compact_idx: Option<usize>,
        local_only: bool,
    ) -> Result<(), CompactionErr> {
        self.seq_paxos.snapshot(compact_idx, local_only)
    }

    /// Return the decided index. 0 means that no entry has been decided.
    pub fn get_decided_idx(&self) -> usize {
        self.seq_paxos.get_decided_idx()
    }

    /// Return trim index from storage.
    pub fn get_compacted_idx(&self) -> usize {
        self.seq_paxos.get_compacted_idx()
    }

    /// Returns the id of the current leader.
    pub fn get_current_leader(&self) -> Option<NodeId> {
        let promised_pid = self.seq_paxos.get_promise().pid;
        if promised_pid == 0 {
            None
        } else {
            Some(promised_pid)
        }
    }

    /// Get who is the current leader and what phase we're in
    pub fn get_current_leader_state(&self) -> (NodeId, Phase) {
        self.seq_paxos.get_current_leader_state()
    }

    /// Returns the promised ballot of this node.
    pub fn get_promise(&self) -> Ballot {
        self.seq_paxos.get_promise()
    }

    /// Returns the outgoing messages from this server. The messages should then be sent via the network implementation.
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
    pub fn read(&self, idx: usize) -> Option<LogEntry<T>> {
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

    pub fn take_decided_slots_since_last_call(&mut self) -> Vec<usize> {
        self.seq_paxos.take_decided_slots_since_last_call()
    }

    /// Read entries in the range `r` in the log. Returns `None` if `r` is out of bounds.
    pub fn read_entries<R>(&self, r: R) -> Option<Vec<LogEntry<T>>>
    where
        R: RangeBounds<usize>,
    {
        self.seq_paxos
            .internal_storage
            .read(r)
            .expect("storage error while trying to read log entries")
    }

    /// Read all decided entries starting at `from_idx` (inclusive) in the log. Returns `None` if `from_idx` is out of bounds.
    pub fn read_decided_suffix(&self, from_idx: usize) -> Option<Vec<LogEntry<T>>> {
        self.seq_paxos
            .internal_storage
            .read_decided_suffix(from_idx)
            .expect("storage error while trying to read decided log suffix")
    }

    /// Handle an incoming message
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

    /// become leader with the given ballot. Only used for metronome, assumes no leader election.
    pub fn initialize_prepare_phase(&mut self, n: Ballot) {
        self.seq_paxos.handle_leader(n);
    }

    /// Propose a cluster reconfiguration. Returns an error if the current configuration has already been stopped
    /// by a previous reconfiguration request or if the `new_configuration` is invalid.
    /// `new_configuration` defines the cluster-wide configuration settings for the **next** cluster.
    /// `metadata` is optional data to commit alongside the reconfiguration.
    pub fn reconfigure(
        &mut self,
        new_configuration: ClusterConfig,
        metadata: Option<Vec<u8>>,
    ) -> Result<(), ProposeErr<T>> {
        if let Err(config_error) = new_configuration.validate() {
            return Err(ProposeErr::ConfigError(
                config_error,
                new_configuration,
                metadata,
            ));
        }
        self.seq_paxos.reconfigure(new_configuration, metadata)
    }

    /// Handles re-establishing a connection to a previously disconnected peer.
    /// This should only be called if the underlying network implementation indicates that a connection has been re-established.
    pub fn reconnected(&mut self, pid: NodeId) {
        self.seq_paxos.reconnected(pid)
    }

    /// Increments the internal logical clock. This drives the processes for leader changes, resending dropped messages, and flushing batched log entries.
    /// Each of these is triggered every `election_tick_timeout`, `resend_message_tick_timeout`, and `flush_batch_tick_timeout` number of calls to this function
    /// (See how to configure these timeouts in `ServerConfig`).
    pub fn tick(&mut self) {
        if self.election_clock.tick_and_check_timeout() {
            self.election_timeout();
        }
        /* // Don't need this in Metronome
        if self.resend_message_clock.tick_and_check_timeout() {
            self.seq_paxos.resend_message_timeout();
        }
        if self.flush_batch_clock.tick_and_check_timeout() {
            self.seq_paxos.flush_batch_timeout();
        }
         */
    }

    /*** BLE calls ***/
    /// Update the custom priority used in the Ballot for this server. Note that changing the
    /// priority triggers a leader re-election.
    pub fn set_priority(&mut self, p: u32) {
        self.ble.set_priority(p)
    }

    /// If the heartbeat of a leader is not received when election_timeout() is called, the server might attempt to become the leader.
    /// It is also used for the election process, where the server checks if it can become the leader.
    /// For instance if `election_timeout()` is called every 100ms, then if the leader fails, the servers will detect it after 100ms and elect a new server after another 100ms if possible.
    fn election_timeout(&mut self) {
        if let Some(new_leader) = self
            .ble
            .hb_timeout(self.seq_paxos.get_state(), self.seq_paxos.get_promise())
        {
            self.seq_paxos.handle_leader(new_leader);
        }
    }

    /// Returns the current states of the OmniPaxos instance for OmniPaxos UI to display.
    pub fn get_ui_states(&self) -> ui::OmniPaxosStates {
        let mut cluster_state = ClusterState::from(self.seq_paxos.get_leader_state());
        cluster_state.heartbeats = self.ble.get_ballots();

        ui::OmniPaxosStates {
            current_ballot: self.ble.get_current_ballot(),
            current_leader: self.get_current_leader(),
            decided_idx: self.get_decided_idx(),
            heartbeats: self.ble.get_ballots(),
            cluster_state,
        }
    }
}

/// An error indicating a failed proposal due to the current cluster configuration being already stopped
/// or due to an invalid proposed configuration. Returns the failed proposal.
#[derive(Debug)]
pub enum ProposeErr<T>
where
    T: Entry,
{
    /// Couldn't propose entry because a reconfiguration is pending. Returns the failed, proposed entry.
    PendingReconfigEntry(T),
    /// Couldn't propose reconfiguration because a reconfiguration is already pending. Returns the failed, proposed `ClusterConfig` and the metadata.
    /// cluster config and metadata.
    PendingReconfigConfig(ClusterConfig, Option<Vec<u8>>),
    /// Couldn't propose reconfiguration because of an invalid cluster config. Contains the config
    /// error and the failed, proposed cluster config and metadata.
    ConfigError(ConfigError, ClusterConfig, Option<Vec<u8>>),
}

/// An error returning the proposal that was failed due to that the current configuration is stopped.
#[derive(Copy, Clone, Debug)]
pub enum CompactionErr {
    /// Snapshot was called with an index that is not decided yet. Returns the currently decided index.
    UndecidedIndex(usize),
    /// Snapshot was called with an index which is already trimmed. Returns the currently compacted index.
    TrimmedIndex(usize),
    /// Trim was called with an index that is not decided by all servers yet. Returns the index decided by ALL servers currently.
    NotAllDecided(usize),
    /// Trim was called at a follower node. Trim must be called by the leader, which is the returned NodeId.
    NotCurrentLeader(NodeId),
}

impl Error for CompactionErr {}
impl Display for CompactionErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}
