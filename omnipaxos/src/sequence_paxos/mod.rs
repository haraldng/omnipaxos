use super::{ballot_leader_election::Ballot, messages::sequence_paxos::*, util::LeaderState};
#[cfg(feature = "logging")]
use crate::utils::logger::create_logger;
use crate::{
    storage::{
        internal_storage::{InternalStorage, InternalStorageConfig},
        metronome::Metronome,
        Entry, Snapshot, StopSign, Storage,
    },
    util::{
        FlexibleQuorum, LogSync, NodeId, Quorum, SequenceNumber, READ_ERROR_MSG, WRITE_ERROR_MSG,
    },
    ClusterConfig, CompactionErr, OmniPaxosConfig, ProposeErr,
};
#[cfg(feature = "logging")]
use slog::{debug, info, trace, warn, Logger};
use std::{fmt::Debug, vec};

const METRONOME_WORKSTEALING: usize = 2;
const METRONOME_FASTEST: usize = 3;

pub mod follower;
pub mod leader;

/// a Sequence Paxos replica. Maintains local state of the replicated log, handles incoming messages and produces outgoing messages that the user has to fetch periodically and send using a network implementation.
/// User also has to periodically fetch the decided entries that are guaranteed to be strongly consistent and linearizable, and therefore also safe to be used in the higher level application.
/// If snapshots are not desired to be used, use `()` for the type parameter `S`.
pub(crate) struct SequencePaxos<T, B>
where
    T: Entry,
    B: Storage<T>,
{
    pub(crate) internal_storage: InternalStorage<B, T>,
    pid: NodeId,
    peers: Vec<NodeId>, // excluding self pid
    state: (Role, Phase),
    buffered_proposals: Vec<T>,
    buffered_stopsign: Option<StopSign>,
    outgoing: Vec<PaxosMessage<T>>,
    leader_state: LeaderState<T>,
    latest_accepted_meta: Option<(Ballot, usize)>,
    // Keeps track of sequence of accepts from leader where AcceptSync = 1
    current_seq_num: SequenceNumber,
    cached_promise_message: Option<Promise<T>>,
    buffer_size: usize,
    metronome: Metronome,
    use_metronome: usize,
    decided_slots_since_last_call: Vec<usize>, // TODO fix this for followers, currently only leader keeps track of this.
    #[cfg(feature = "logging")]
    logger: Logger,
}

impl<T, B> SequencePaxos<T, B>
where
    T: Entry,
    B: Storage<T>,
{
    /*** User functions ***/
    /// Creates a Sequence Paxos replica.
    pub(crate) fn with(config: SequencePaxosConfig, storage: B) -> Self {
        let pid = config.pid;
        let peers = config.peers;
        let num_nodes = &peers.len() + 1;
        let quorum = Quorum::with(config.flexible_quorum, num_nodes);
        let max_peer_pid = peers.iter().max().unwrap();
        let max_pid = *std::cmp::max(max_peer_pid, &pid) as usize;
        let mut outgoing = Vec::with_capacity(config.buffer_size);
        let (state, leader) = match storage
            .get_promise()
            .expect("storage error while trying to read promise")
        {
            // if we recover a promise from storage then we must do failure recovery
            Some(b) => {
                let state = (Role::Follower, Phase::Recover);
                for peer_pid in &peers {
                    let prepreq = PrepareReq { n: b };
                    outgoing.push(PaxosMessage {
                        from: pid,
                        to: *peer_pid,
                        msg: PaxosMsg::PrepareReq(prepreq),
                    });
                }
                (state, b)
            }
            None => ((Role::Follower, Phase::None), Ballot::default()),
        };
        let metronome_quorum_size = match config.metronome_quorum_size {
            Some(size) => size,
            None => quorum.get_write_quorum_size(),
        };
        let metronome = Metronome::with(pid, num_nodes, metronome_quorum_size);
        // batch at least as large as metronome ordering size to prevent out of index for an entry.
        let critical_len = metronome.critical_len;
        let batch_size = if config.batch_size == 0 {
            critical_len
        } else {
            config.batch_size
        };
        let internal_storage_config = InternalStorageConfig { batch_size };
        let mut paxos = SequencePaxos {
            internal_storage: InternalStorage::with(
                storage,
                internal_storage_config,
                #[cfg(feature = "unicache")]
                pid,
            ),
            pid,
            peers,
            state,
            buffered_proposals: vec![],
            buffered_stopsign: None,
            outgoing,
            leader_state: LeaderState::<T>::with(leader, max_pid, quorum),
            latest_accepted_meta: None,
            current_seq_num: SequenceNumber::default(),
            cached_promise_message: None,
            buffer_size: config.buffer_size,
            use_metronome: config.use_metronome,
            metronome,
            decided_slots_since_last_call: Vec::with_capacity(1000),
            #[cfg(feature = "logging")]
            logger: {
                if let Some(logger) = config.custom_logger {
                    logger
                } else {
                    let s = config
                        .logger_file_path
                        .unwrap_or_else(|| format!("logs/paxos_{}.log", pid));
                    create_logger(s.as_str())
                }
            },
        };
        paxos
            .internal_storage
            .set_promise(leader)
            .expect(WRITE_ERROR_MSG);
        #[cfg(feature = "logging")]
        {
            info!(
                paxos.logger,
                "Paxos component pid: {} created! batch size: {}",
                pid,
                batch_size,
            );
            if let Quorum::Flexible(flex_quorum) = quorum {
                if flex_quorum.read_quorum_size > num_nodes - flex_quorum.write_quorum_size + 1 {
                    warn!(
                        paxos.logger,
                        "Unnecessary overlaps in read and write quorums. Read and Write quorums only need to be overlapping by one node i.e., read_quorum_size + write_quorum_size = num_nodes + 1");
                }
            }
        }
        paxos
    }

    pub(crate) fn get_state(&self) -> &(Role, Phase) {
        &self.state
    }

    pub(crate) fn get_promise(&self) -> Ballot {
        self.internal_storage.get_promise()
    }

    pub(crate) fn add_decided_slot(&mut self, slot_idx: usize) {
        self.decided_slots_since_last_call.push(slot_idx);
    }

    pub fn take_decided_slots_since_last_call(&mut self) -> Vec<usize> {
        std::mem::take(&mut self.decided_slots_since_last_call)
    }

    /// Initiates the trim process.
    /// # Arguments
    /// * `trim_idx` - Deletes all entries up to [`trim_idx`], if the [`trim_idx`] is `None` then the minimum index accepted by **ALL** servers will be used as the [`trim_idx`].
    pub(crate) fn trim(&mut self, trim_idx: Option<usize>) -> Result<(), CompactionErr> {
        match self.state {
            (Role::Leader, _) => {
                let min_all_accepted_idx = self.leader_state.get_min_all_accepted_idx();
                let trimmed_idx = match trim_idx {
                    Some(idx) if idx <= *min_all_accepted_idx => idx,
                    None => {
                        #[cfg(feature = "logging")]
                        trace!(
                            self.logger,
                            "No trim index provided, using min_las_idx: {:?}",
                            min_all_accepted_idx
                        );
                        *min_all_accepted_idx
                    }
                    _ => {
                        return Err(CompactionErr::NotAllDecided(*min_all_accepted_idx));
                    }
                };
                let result = self.internal_storage.try_trim(trimmed_idx);
                if result.is_ok() {
                    for pid in &self.peers {
                        let msg = PaxosMsg::Compaction(Compaction::Trim(trimmed_idx));
                        self.outgoing.push(PaxosMessage {
                            from: self.pid,
                            to: *pid,
                            msg,
                        });
                    }
                }
                result.map_err(|e| {
                    *e.downcast()
                        .expect("storage error while trying to trim log")
                })
            }
            _ => Err(CompactionErr::NotCurrentLeader(self.get_current_leader())),
        }
    }

    /// Trim the log and create a snapshot. ** Note: only up to the `decided_idx` can be snapshotted **
    /// # Arguments
    /// `idx` - Snapshots all entries with index < [`idx`], if the [`idx`] is None then the decided index will be used.
    /// `local_only` - If `true`, only this server snapshots the log. If `false` all servers performs the snapshot.
    pub(crate) fn snapshot(
        &mut self,
        idx: Option<usize>,
        local_only: bool,
    ) -> Result<(), CompactionErr> {
        let result = self.internal_storage.try_snapshot(idx);
        if !local_only && result.is_ok() {
            // since it is decided, it is ok even for a follower to send this
            for pid in &self.peers {
                let msg = PaxosMsg::Compaction(Compaction::Snapshot(idx));
                self.outgoing.push(PaxosMessage {
                    from: self.pid,
                    to: *pid,
                    msg,
                });
            }
        }
        result.map_err(|e| {
            *e.downcast()
                .expect("storage error while trying to snapshot log")
        })
    }

    /// Return the decided index.
    pub(crate) fn get_decided_idx(&self) -> usize {
        self.internal_storage.get_decided_idx()
    }

    /// Return trim index from storage.
    pub(crate) fn get_compacted_idx(&self) -> usize {
        self.internal_storage.get_compacted_idx()
    }

    fn handle_compaction(&mut self, c: Compaction) {
        // try trimming and snapshotting forwarded compaction. Errors are ignored as that the data will still be kept.
        match c {
            Compaction::Trim(idx) => {
                let _ = self.internal_storage.try_trim(idx);
            }
            Compaction::Snapshot(idx) => {
                let _ = self.snapshot(idx, true);
            }
        }
    }

    /// Detects if a Prepare, Promise, AcceptStopSign, Decide of a Stopsign, or PrepareReq message
    /// has been sent but not been received. If so resends them. Note: We can't detect if a
    /// StopSign's Decide message has been received so we always resend to be safe.
    pub(crate) fn resend_message_timeout(&mut self) {
        match self.state.0 {
            Role::Leader => self.resend_messages_leader(),
            Role::Follower => self.resend_messages_follower(),
        }
    }

    /// Flushes any batched log entries and sends their corresponding Accept or Accepted messages.
    pub(crate) fn flush_batch_timeout(&mut self) {
        unimplemented!("Not used in Metronome")
        /*
        match self.state {
            (Role::Leader, Phase::Accept) => self.flush_batch_leader(),
            (Role::Follower, Phase::Accept) => self.flush_batch_follower(),
            _ => (),
        }
        */
    }

    /// Returns the outgoing messages from this replica. The messages should then be sent via the network implementation.
    pub(crate) fn get_outgoing_msgs(&mut self) -> Vec<PaxosMessage<T>> {
        let mut outgoing = Vec::with_capacity(self.buffer_size);
        std::mem::swap(&mut self.outgoing, &mut outgoing);
        self.leader_state.reset_batch_accept_meta();
        self.latest_accepted_meta = None;
        outgoing
    }

    /// Handle an incoming message.
    pub(crate) fn handle(&mut self, m: PaxosMessage<T>) {
        match m.msg {
            PaxosMsg::PrepareReq(prepreq) => self.handle_preparereq(prepreq, m.from),
            PaxosMsg::Prepare(prep) => self.handle_prepare(prep, m.from),
            PaxosMsg::Promise(prom) => match &self.state {
                (Role::Leader, Phase::Prepare) => self.handle_promise_prepare(prom, m.from),
                (Role::Leader, Phase::Accept) => self.handle_promise_accept(prom, m.from),
                _ => {}
            },
            PaxosMsg::AcceptSync(acc_sync) => self.handle_acceptsync(acc_sync, m.from),
            PaxosMsg::AcceptDecide(acc) => self.handle_acceptdecide(acc),
            PaxosMsg::NotAccepted(not_acc) => self.handle_notaccepted(not_acc, m.from),
            PaxosMsg::Accepted(accepted) => self.handle_accepted(accepted, m.from),
            PaxosMsg::Decide(d) => self.handle_decide(d),
            PaxosMsg::ProposalForward(proposals) => self.handle_forwarded_proposal(proposals),
            PaxosMsg::Compaction(c) => self.handle_compaction(c),
            PaxosMsg::AcceptStopSign(acc_ss) => self.handle_accept_stopsign(acc_ss),
            PaxosMsg::ForwardStopSign(f_ss) => self.handle_forwarded_stopsign(f_ss),
        }
    }

    pub(crate) fn accept(
        &mut self,
        reply_accepted_with: Option<Ballot>,
        entries: Vec<T>,
        start_idx: usize,
    ) {
        if self.use_metronome > 0 {
            let metronome_fastest_flush = true; // leader always handles it as true
            self.metronome_accept(
                reply_accepted_with,
                entries,
                start_idx,
                metronome_fastest_flush,
            );
        } else {
            let _ = self.normal_accept(reply_accepted_with, entries);
        }
    }

    pub(crate) fn normal_accept(
        &mut self,
        reply_accepted_with: Option<Ballot>,
        entries: Vec<T>,
    ) -> usize {
        for entry in entries {
            let slot_idx = self
                .internal_storage
                .append_entry_no_batching(entry)
                .expect(WRITE_ERROR_MSG)
                - 1;
            if let Some(n) = reply_accepted_with {
                // #[cfg(feature = "logging")]
                // info!(self.logger, "Node: {} replying accepted for slot_idx: {}", self.pid, slot_idx);
                self.reply_accepted(n, slot_idx);
            } else {
                let new_num_accepted = self
                    .leader_state
                    .accepted_per_slot
                    .get(&slot_idx)
                    .copied()
                    .unwrap_or_default()
                    + 1;
                self.leader_state
                    .accepted_per_slot
                    .insert(slot_idx, new_num_accepted);
            }
        }
        self.internal_storage.get_accepted_idx()
    }

    pub(crate) fn metronome_accept(
        &mut self,
        reply_accepted_with: Option<Ballot>,
        entries: Vec<T>,
        start_idx: usize,
        metronome_fastest_flush: bool,
    ) {
        if self.use_metronome == METRONOME_FASTEST {
            if metronome_fastest_flush {
                let num_entries = entries.len();
                let _ = self
                    .internal_storage
                    .append_entries_without_batching(entries)
                    .expect(WRITE_ERROR_MSG);

                match reply_accepted_with {
                    None => {
                        for slot_idx in start_idx..start_idx + num_entries {
                            let new_num_accepted = self
                                .leader_state
                                .accepted_per_slot
                                .get(&slot_idx)
                                .copied()
                                .unwrap_or_default()
                                + 1;
                            self.leader_state
                                .accepted_per_slot
                                .insert(slot_idx, new_num_accepted);
                        }
                    }
                    Some(n) => {
                        for slot_idx in start_idx..start_idx + num_entries {
                            let accepted = Accepted { n, slot_idx, queue_size: 0 }; // send accepted for this slot
                            self.outgoing.push(PaxosMessage {
                                from: self.pid,
                                to: n.pid,
                                msg: PaxosMsg::Accepted(accepted),
                            });
                        }
                    }
                }
            }
        } else {
            for (idx, entry) in entries.into_iter().enumerate() {
                let slot_idx = start_idx + idx;
                let metronome_slot_idx = slot_idx % self.metronome.total_len;
                let in_my_critical_order = self
                    .metronome
                    .my_critical_ordering
                    .contains(&metronome_slot_idx);
                // Leader always needs to save entry so it can reply to client with state.
                if in_my_critical_order || reply_accepted_with.is_none() {
                    let _ = self
                        .internal_storage
                        .append_entry_no_batching(entry)
                        .expect(WRITE_ERROR_MSG);
                }
                if in_my_critical_order {
                    match reply_accepted_with {
                        Some(n) => {
                            let accepted = Accepted { n, slot_idx, queue_size: 0 }; // send accepted for this slot
                            self.outgoing.push(PaxosMessage {
                                from: self.pid,
                                to: n.pid,
                                msg: PaxosMsg::Accepted(accepted),
                            });
                        }
                        None => {
                            let new_num_accepted = self
                                .leader_state
                                .accepted_per_slot
                                .get(&slot_idx)
                                .copied()
                                .unwrap_or_default()
                                + 1;
                            self.leader_state
                                .accepted_per_slot
                                .insert(slot_idx, new_num_accepted);
                        }
                    }
                }
            }
        }
    }

    /// Returns whether this Sequence Paxos has been reconfigured
    pub(crate) fn is_reconfigured(&self) -> Option<StopSign> {
        match self.internal_storage.get_stopsign() {
            Some(ss) if self.internal_storage.stopsign_is_decided() => Some(ss),
            _ => None,
        }
    }

    /// Returns whether this Sequence Paxos instance is stopped, i.e. if it has been reconfigured.
    fn accepted_reconfiguration(&self) -> bool {
        self.internal_storage.get_stopsign().is_some()
    }

    /// Append an entry to the replicated log.
    pub(crate) fn append(&mut self, entry: T) -> Result<(), ProposeErr<T>> {
        if self.accepted_reconfiguration() {
            Err(ProposeErr::PendingReconfigEntry(entry))
        } else {
            self.propose_entry(entry);
            Ok(())
        }
    }

    /// Propose a reconfiguration. Returns an error if already stopped or `new_config` is invalid.
    /// `new_config` defines the cluster-wide configuration settings for the next cluster.
    /// `metadata` is optional data to commit alongside the reconfiguration.
    pub(crate) fn reconfigure(
        &mut self,
        new_config: ClusterConfig,
        metadata: Option<Vec<u8>>,
    ) -> Result<(), ProposeErr<T>> {
        if self.accepted_reconfiguration() {
            return Err(ProposeErr::PendingReconfigConfig(new_config, metadata));
        }
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "Accepting reconfiguration {:?}", new_config.nodes
        );
        let ss = StopSign::with(new_config, metadata);
        match self.state {
            (Role::Leader, Phase::Prepare) => self.buffered_stopsign = Some(ss),
            (Role::Leader, Phase::Accept) => self.accept_stopsign_leader(ss),
            _ => self.forward_stopsign(ss),
        }
        Ok(())
    }

    fn get_current_leader(&self) -> NodeId {
        self.get_promise().pid
    }

    pub(crate) fn get_current_leader_state(&self) -> (NodeId, Phase) {
        (self.get_current_leader(), self.state.1)
    }

    /// Handles re-establishing a connection to a previously disconnected peer.
    /// This should only be called if the underlying network implementation indicates that a connection has been re-established.
    pub(crate) fn reconnected(&mut self, pid: NodeId) {
        if pid == self.pid {
            return;
        } else if pid == self.get_current_leader() {
            self.state = (Role::Follower, Phase::Recover);
        }
        let prepreq = PrepareReq {
            n: self.get_promise(),
        };
        self.outgoing.push(PaxosMessage {
            from: self.pid,
            to: pid,
            msg: PaxosMsg::PrepareReq(prepreq),
        });
    }

    fn propose_entry(&mut self, entry: T) {
        match self.state {
            (Role::Leader, Phase::Prepare) => self.buffered_proposals.push(entry),
            (Role::Leader, Phase::Accept) => self.accept_entry_leader(entry),
            _ => self.forward_proposals(vec![entry]),
        }
    }

    pub(crate) fn get_leader_state(&self) -> &LeaderState<T> {
        &self.leader_state
    }

    pub(crate) fn forward_proposals(&mut self, mut entries: Vec<T>) {
        let leader = self.get_current_leader();
        if leader > 0 && self.pid != leader {
            let pf = PaxosMsg::ProposalForward(entries);
            let msg = PaxosMessage {
                from: self.pid,
                to: leader,
                msg: pf,
            };
            self.outgoing.push(msg);
        } else {
            self.buffered_proposals.append(&mut entries);
        }
    }

    pub(crate) fn forward_stopsign(&mut self, ss: StopSign) {
        let leader = self.get_current_leader();
        if leader > 0 && self.pid != leader {
            #[cfg(feature = "logging")]
            trace!(self.logger, "Forwarding StopSign to Leader {:?}", leader);
            let fs = PaxosMsg::ForwardStopSign(ss);
            let msg = PaxosMessage {
                from: self.pid,
                to: leader,
                msg: fs,
            };
            self.outgoing.push(msg);
        } else if self.buffered_stopsign.as_mut().is_none() {
            self.buffered_stopsign = Some(ss);
        }
    }
    /// Returns `LogSync`, a struct to help other servers synchronize their log to correspond to the
    /// current state of our own log. The `common_prefix_idx` marks where in the log the other server
    /// needs to be sync from.
    fn create_log_sync(
        &self,
        common_prefix_idx: usize,
        other_logs_decided_idx: usize,
    ) -> LogSync<T> {
        let decided_idx = self.internal_storage.get_decided_idx();
        let (decided_snapshot, suffix, sync_idx) =
            if T::Snapshot::use_snapshots() && decided_idx > common_prefix_idx {
                // Note: We snapshot from the other log's decided index and not the common prefix because
                // snapshots currently only work on decided entries.
                let (delta_snapshot, compacted_idx) = self
                    .internal_storage
                    .create_diff_snapshot(other_logs_decided_idx)
                    .expect(READ_ERROR_MSG);
                let suffix = self
                    .internal_storage
                    .get_suffix(decided_idx)
                    .expect(READ_ERROR_MSG);
                (delta_snapshot, suffix, compacted_idx)
            } else {
                let suffix = self
                    .internal_storage
                    .get_suffix(common_prefix_idx)
                    .expect(READ_ERROR_MSG);
                (None, suffix, common_prefix_idx)
            };
        LogSync {
            decided_snapshot,
            suffix,
            sync_idx,
            stopsign: self.internal_storage.get_stopsign(),
        }
    }
}

#[derive(PartialEq, Debug, Copy, Clone)]
pub enum Phase {
    Prepare,
    Accept,
    Recover,
    None,
}

#[derive(PartialEq, Debug)]
pub(crate) enum Role {
    Follower,
    Leader,
}

/// Configuration for `SequencePaxos`.
/// # Fields
/// * `pid`: The unique identifier of this node. Must not be 0.
/// * `peers`: The peers of this node i.e. the `pid`s of the other servers in the configuration.
/// * `flexible_quorum` : Defines read and write quorum sizes. Can be used for different latency vs fault tolerance tradeoffs.
/// * `buffer_size`: The buffer size for outgoing messages.
/// * `batch_size`: The size of the buffer for log batching. The default is 1, which means no batching.
/// * `logger_file_path`: The path where the default logger logs events.
#[derive(Clone, Debug)]
pub(crate) struct SequencePaxosConfig {
    pid: NodeId,
    peers: Vec<NodeId>,
    buffer_size: usize,
    use_metronome: usize,
    pub(crate) batch_size: usize,
    flexible_quorum: Option<FlexibleQuorum>,
    metronome_quorum_size: Option<usize>,
    #[cfg(feature = "logging")]
    logger_file_path: Option<String>,
    #[cfg(feature = "logging")]
    custom_logger: Option<Logger>,
}

impl From<OmniPaxosConfig> for SequencePaxosConfig {
    fn from(config: OmniPaxosConfig) -> Self {
        let pid = config.server_config.pid;
        let peers = config
            .cluster_config
            .nodes
            .into_iter()
            .filter(|x| *x != pid)
            .collect();
        SequencePaxosConfig {
            pid,
            peers,
            flexible_quorum: config.cluster_config.flexible_quorum,
            buffer_size: config.server_config.buffer_size,
            batch_size: config.server_config.batch_size,
            use_metronome: config.cluster_config.use_metronome,
            metronome_quorum_size: config.cluster_config.metronome_quorum_size,
            #[cfg(feature = "logging")]
            logger_file_path: config.server_config.logger_file_path,
            #[cfg(feature = "logging")]
            custom_logger: config.server_config.custom_logger,
        }
    }
}
