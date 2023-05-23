#[cfg(feature = "toml_config")]
use crate::errors::ConfigError;
use crate::{
    ballot_leader_election::{Ballot, BallotLeaderElection},
    messages::Message,
    sequence_paxos::SequencePaxos,
    storage::{Entry, StopSign, Storage},
    util::{defaults::BUFFER_SIZE, LogEntry, NodeId},
};
#[cfg(any(feature = "toml_config", feature = "serde"))]
use serde::Deserialize;
#[cfg(feature = "serde")]
use serde::Serialize;
#[cfg(feature = "toml_config")]
use std::fs;
use std::ops::RangeBounds;
#[cfg(feature = "toml_config")]
use toml;

/// Configuration for `OmniPaxos`.
/// # Fields
/// * `cluster_config`: The configuration settings that are cluster-wide.
/// * `server_config`: The configuration settings that unique to this OmniPaxos replica.
#[allow(missing_docs)]
#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "toml_config", derive(Deserialize), serde(default))]
pub struct OmniPaxosConfig {
    pub cluster_config: ClusterConfig,
    pub server_config: ServerConfig,
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
        let ClusterConfig {
            configuration_id,
            nodes,
            initial_leader,
        } = &self.cluster_config;
        let ServerConfig {
            pid, buffer_size, ..
        } = &self.server_config;

        // Check that pid, peers, and nodes are consistent
        assert_ne!(*pid, 0, "Pid cannot be 0");
        assert_ne!(*configuration_id, 0, "Configuration id cannot be 0");
        assert!(nodes.contains(pid), "Nodes should include self pid {}", pid);
        assert!(nodes.len() > 1, "Need more than 1 node");
        assert!(*buffer_size > 0, "Buffer size must be greater than 0");
        if let Some(x) = initial_leader {
            assert_ne!(x.pid, 0, "Initial leader cannot be 0")
        };
        OmniPaxos {
            seq_paxos: SequencePaxos::with(self.clone().into(), storage),
            ble: BallotLeaderElection::with(self.into()),
        }
    }
}

/// Configuration for an `OmniPaxos` cluster.
/// # Fields
/// * `configuration_id`: The identifier for the cluster configuration that this Sequence Paxos replica is part of.
/// * `nodes`: The nodes in the cluster i.e. the `pid`s of the other replicas in the configuration.
/// * `initial_leader`: The initial leader of the cluster. Could be used in combination with reconfiguration to skip the prepare phase when switching to a new configuration.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(any(feature = "serde", feature = "toml_config"), derive(Deserialize))]
#[cfg_attr(feature = "toml_config", serde(default))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct ClusterConfig {
    /// The identifier for the cluster configuration that this Sequence Paxos replica is part of.
    pub configuration_id: u32,
    /// The nodes in the cluster i.e. the `pid`s of the other replicas in the configuration.
    pub nodes: Vec<u64>,
    /// The initial leader of the cluster. Could be used in combination with reconfiguration to skip the prepare phase when switching to a new configuration.
    pub initial_leader: Option<Ballot>,
}

impl ClusterConfig {
    /// Checks all configurations and returns the local OmniPaxos node if successful.
    pub fn build_for<T, B>(self, storage: B, server_config: ServerConfig) -> OmniPaxos<T, B>
    where
        T: Entry,
        B: Storage<T>,
    {
        let op_config = OmniPaxosConfig {
            cluster_config: self,
            server_config,
        };
        op_config.build(storage)
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            configuration_id: 1,
            nodes: Vec::new(),
            initial_leader: None,
        }
    }
}

/// Configuration for a singular `OmniPaxos` instance in a cluster.
/// # Fields
/// * `pid`: The unique identifier of this node. Must not be 0.
/// * `buffer_size`: The buffer size for outgoing messages.
/// * `logger_file_path`: The path where the default logger logs events.
/// * `leader_priority` : Custom priority for this node to be elected as the leader.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "toml_config", derive(Deserialize), serde(default))]
pub struct ServerConfig {
    /// The unique identifier of this node. Must not be 0.
    pub pid: NodeId,
    /// The buffer size for outgoing messages.
    pub buffer_size: usize,
    /// The path where the default logger logs events.
    #[cfg(feature = "logging")]
    pub logger_file_path: Option<String>,
    /// Custom priority for this node to be elected as the leader.
    pub leader_priority: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            pid: 0,
            buffer_size: BUFFER_SIZE,
            #[cfg(feature = "logging")]
            logger_file_path: None,
            leader_priority: 0,
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
        match self.seq_paxos.internal_storage.read(idx..idx + 1) {
            Some(mut v) => v.pop(),
            None => None,
        }
    }

    /// Read entries in the range `r` in the log. Returns `None` if `r` is out of bounds.
    pub fn read_entries<R>(&self, r: R) -> Option<Vec<LogEntry<T>>>
    where
        R: RangeBounds<u64>,
    {
        self.seq_paxos.internal_storage.read(r)
    }

    /// Read all decided entries from `from_idx` in the log. Returns `None` if `from_idx` is out of bounds.
    pub fn read_decided_suffix(&self, from_idx: u64) -> Option<Vec<LogEntry<T>>> {
        self.seq_paxos
            .internal_storage
            .read_decided_suffix(from_idx)
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

    /// Propose a reconfiguration. Returns error if already stopped.
    pub fn reconfigure(&mut self, new_configuration: ClusterConfig) -> Result<(), ProposeErr<T>> {
        // TODO: doesn't check to make sure the new_config is valid. A user could propose a flawed
        // ClusterConfig and if it gets decided be stuck with it. Is this acceptable?
        self.seq_paxos.reconfigure(new_configuration)
    }

    /// Handles re-establishing a connection to a previously disconnected peer.
    /// This should only be called if the underlying network implementation indicates that a connection has been re-established.
    pub fn reconnected(&mut self, pid: NodeId) {
        self.seq_paxos.reconnected(pid)
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

/// An error returning the proposal that was failed due to that the current configuration is stopped.
#[allow(missing_docs)]
#[derive(Debug)]
pub enum ProposeErr<T>
where
    T: Entry,
{
    Normal(T),
    Reconfiguration(ClusterConfig),
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
