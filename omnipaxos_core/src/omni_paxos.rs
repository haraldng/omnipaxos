use crate::{
    ballot_leader_election::{BLEConfig, Ballot, BallotLeaderElection},
    messages::Message,
    sequence_paxos::{
        CompactionErr, ProposeErr, ReconfigurationRequest, SequencePaxos, SequencePaxosConfig,
    },
    storage::{Entry, Snapshot, StopSign, Storage},
    util::{LogEntry, NodeId},
};
use std::ops::RangeBounds;

/// The `OmniPaxos` struct represents an OmniPaxos server. Maintains the replicated log that can be read from and appended to.
/// It also handles incoming messages and produces outgoing messages that you need to fetch and send periodically using your own network implementation.
pub struct OmniPaxos<T, S, B>
where
    T: Entry,
    S: Snapshot<T>,
    B: Storage<T, S>,
{
    seq_paxos: SequencePaxos<T, S, B>,
    ble: BallotLeaderElection,
}

impl<T, S, B> OmniPaxos<T, S, B>
where
    T: Entry,
    S: Snapshot<T>,
    B: Storage<T, S>,
{
    /// Creates an `OmniPaxos` server
    pub fn with(sp_config: SequencePaxosConfig, ble_config: BLEConfig, storage: B) -> Self {
        Self {
            seq_paxos: SequencePaxos::with(sp_config, storage),
            ble: BallotLeaderElection::with(ble_config),
        }
    }

    /// Initiates the trim process.
    /// # Arguments
    /// * `trim_index` - Deletes all entries up to [`trim_index`], if the [`trim_index`] is `None` then the minimum index accepted by **ALL** servers will be used as the [`trim_index`].
    pub fn trim(&mut self, trim_index: Option<u64>) -> Result<(), CompactionErr> {
        self.seq_paxos.trim(trim_index)
    }

    /// Trim the log and create a snapshot. ** Note: only up to the `decided_idx` can be snapshotted **
    /// # Arguments
    /// `compact_idx` - Snapshots all entries < [`trim_index`], if the [`trim_index`] is None then the decided index will be used.
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
    pub fn outgoing_messages(&mut self) -> Vec<Message<T, S>> {
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
    pub fn read(&self, idx: u64) -> Option<LogEntry<T, S>> {
        match self.seq_paxos.internal_storage.read(idx..idx + 1) {
            Some(mut v) => v.pop(),
            None => None,
        }
    }

    /// Read entries in the range `r` in the log. Returns `None` if `r` is out of bounds.
    pub fn read_entries<R>(&self, r: R) -> Option<Vec<LogEntry<T, S>>>
    where
        R: RangeBounds<u64>,
    {
        self.seq_paxos.internal_storage.read(r)
    }

    /// Read all decided entries from `from_idx` in the log. Returns `None` if `from_idx` is out of bounds.
    pub fn read_decided_suffix(&self, from_idx: u64) -> Option<Vec<LogEntry<T, S>>> {
        self.seq_paxos
            .internal_storage
            .read_decided_suffix(from_idx)
    }

    /// Handle an incoming message.
    pub fn handle_incoming(&mut self, m: Message<T, S>) {
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

    /*** BLE calls ***/
    /// Update the custom priority used in the Ballot for this server.
    pub fn set_priority(&mut self, p: u64) {
        self.ble.set_priority(p)
    }

    /// Tick is run by all servers to simulate the passage of time
    /// If one wishes to have hb_delay of 500ms, one can set a periodic timer of 100ms to call tick(). After 5 calls to this function, the timeout will occur.
    /// Returns an Option with the elected leader otherwise None
    pub fn tick(&mut self) {
        if let Some(b) = self.ble.tick() {
            self.seq_paxos.handle_leader(b);
        }
    }
}
