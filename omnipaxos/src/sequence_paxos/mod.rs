use super::{ballot_leader_election::Ballot, messages::sequence_paxos::*, util::LeaderState};
#[cfg(feature = "logging")]
use crate::utils::logger::create_logger;
use crate::{
    messages::Message,
    storage::{
        internal_storage::{InternalStorage, InternalStorageConfig},
        Entry, Snapshot, StopSign, Storage,
    },
    util::{
        FlexibleQuorum, LogSync, NodeId, Quorum, SequenceNumber, READ_ERROR_MSG, WRITE_ERROR_MSG,
    },
    ClusterConfig, CompactionErr, OmniPaxosConfig, ProposeErr,
};
#[cfg(feature = "logging")]
use slog::{debug, info, trace, warn, Logger};
use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
    fmt::Debug,
    vec,
};
use crate::clock::Clock;

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
    outgoing: Vec<Message<T>>,
    leader_state: LeaderState<T>,
    latest_accepted_meta: Option<(Ballot, usize)>,
    // Keeps track of sequence of accepts from leader where AcceptSync = 1
    current_seq_num: SequenceNumber,
    cached_promise_message: Option<Promise<T>>,
    // Nezha attributes
    early_buffer: BinaryHeap<Reverse<PrepareWithDeadline<T>>>,
    last_released_deadline: u64, // TODO: use correct type for clock simulator
    late_buffer: HashMap<RequestId, PrepareWithDeadline<T>>,
    reply_set: HashMap<RequestId, (HashMap<NodeId, NezhaReply>, Option<NodeId>)>, // Map<RequestId, (Map<NodeId, NezhaReply>, Optional Leader NodeId that sent FastReply)>
    committed: HashMap<RequestId, bool>,
    #[cfg(feature = "logging")]
    logger: Logger,
    clock: Clock,
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
                    outgoing.push(Message::SequencePaxos(PaxosMessage {
                        from: pid,
                        to: *peer_pid,
                        msg: PaxosMsg::PrepareReq(prepreq),
                    }));
                }
                (state, b)
            }
            None => ((Role::Follower, Phase::None), Ballot::default()),
        };
        let internal_storage_config = InternalStorageConfig {
            batch_size: config.batch_size,
        };
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
            early_buffer: BinaryHeap::new(),
            last_released_deadline: 0,
            late_buffer: HashMap::new(),
            reply_set: HashMap::new(),
            committed: HashMap::new(),
            clock: Clock::new(),
            #[cfg(feature = "logging")]
            logger: {
                if let Some(logger) = config.custom_logger {
                    logger
                } else {
                    let s = config
                        .logger_file_path
                        .unwrap_or_else(|| format!("logs/paxos_{}.log", pid));
                    create_logger(s.as_str(), slog::Level::Trace)
                }
            },
        };
        paxos
            .internal_storage
            .set_promise(leader)
            .expect(WRITE_ERROR_MSG);
        #[cfg(feature = "logging")]
        {
            info!(paxos.logger, "Paxos component pid: {} created!", pid);
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
                        self.outgoing.push(Message::SequencePaxos(PaxosMessage {
                            from: self.pid,
                            to: *pid,
                            msg,
                        }));
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
                self.outgoing.push(Message::SequencePaxos(PaxosMessage {
                    from: self.pid,
                    to: *pid,
                    msg,
                }));
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
        match self.state {
            (Role::Leader, Phase::Accept) => self.flush_batch_leader(),
            (Role::Follower, Phase::Accept) => self.flush_batch_follower(),
            _ => (),
        }
    }

    /// Moves the outgoing messages from this replica into the buffer. The messages should then be sent via the network implementation.
    /// If `buffer` is empty, it gets swapped with the internal message buffer. Otherwise, messages are appended to the buffer. This prevents messages from getting discarded.
    /// the buffer.
    pub(crate) fn take_outgoing_msgs(&mut self, buffer: &mut Vec<Message<T>>) {
        if buffer.is_empty() {
            std::mem::swap(buffer, &mut self.outgoing);
        } else {
            // User has unsent messages in their buffer, must extend their buffer.
            buffer.append(&mut self.outgoing);
        }
        self.leader_state.reset_latest_accept_meta();
        self.latest_accepted_meta = None;
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
            PaxosMsg::PrepareWithDeadline(prep) => self.handle_prepare_with_deadline(prep),
            PaxosMsg::FastReply(freply) => self.handle_fast_reply(freply, m.from),
            PaxosMsg::SlowReply(_sreply) => todo!(),
            PaxosMsg::LogModifications(lm) => self.handle_log_modifications(lm),
            PaxosMsg::LogStatus(ls) => self.handle_log_status(ls, m.from),
            PaxosMsg::CommitStatus(cs) => self.handle_commit_status(cs),
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

    pub(crate) fn handle_prepare_with_deadline(&mut self, prep: PrepareWithDeadline<T>) {
        if prep.entry.get_deadline() > self.last_released_deadline {
            #[cfg(feature = "logging")]
            trace!(self.logger, "PrepareWithDeadline buffered in early_buffer"; "request_id" => ?prep.entry.get_request_id(), "deadline" => prep.entry.get_deadline());
            self.early_buffer.push(Reverse(prep));
        } else {
            #[cfg(feature = "logging")]
            trace!(self.logger, "PrepareWithDeadline buffered in late_buffer"; "request_id" => ?prep.entry.get_request_id(), "deadline" => prep.entry.get_deadline());
            self.late_buffer.insert(prep.entry.get_request_id(), prep);
        }
    }

    pub(crate) fn process_early_buffer(&mut self) {
        // If not in Accept phase, don't process early buffer
        if self.state.1 != Phase::Accept {
            return;
        }
        while let Some(Reverse(prep)) = self.early_buffer.peek().cloned() {
            // TODO: check against clock simulator if deadline has passed
            #[allow(unused_comparisons)]
            if prep.entry.get_deadline() < 1 {
                self.early_buffer.pop();
                self.last_released_deadline = prep.entry.get_deadline();

                // Append entry without incrementing accepted_idx
                let inserted_index = self
                    .internal_storage
                    .append_entries_without_batching(vec![prep.entry.clone()], false)
                    .expect(WRITE_ERROR_MSG);

                let freply = FastReply {
                    request_id: prep.entry.get_request_id(),
                    log_hash: self
                        .internal_storage
                        .get_hash(inserted_index)
                        .expect(READ_ERROR_MSG),
                    n: self.internal_storage.get_promise(),
                    is_leader: self.state.0 == Role::Leader,
                };

                #[cfg(feature = "logging")]
                debug!(self.logger, "Processed entry from early_buffer"; "request_id" => ?prep.entry.get_request_id(), "inserted_index" => inserted_index, "is_leader" => self.state.0 == Role::Leader);

                // If this server was the original receiver of this entry, add its FastReply to reply_set since it will be the one keeping track
                // of replies for this request
                if prep.from == self.pid {
                    self.handle_fast_reply(freply, self.pid);
                } else {
                    // Otherwise, add FastReply to outgoing buffer to be sent to original receiver of this entry
                    self.outgoing.push(Message::SequencePaxos(PaxosMessage {
                        from: self.pid,
                        to: prep.from,
                        msg: PaxosMsg::FastReply(freply),
                    }));
                }
            } else {
                break;
            }
        }
    }

    pub(crate) fn handle_fast_reply(&mut self, freply: FastReply, from: NodeId) {
        // If phase is not Accept, or reply is from a previous ballot, or if we have already received a reply from this node for this request, ignore
        if self.state.1 != Phase::Accept
            || freply.n < self.internal_storage.get_promise()
            || self
                .reply_set
                .get(&freply.request_id)
                .is_some_and(|(replies, _)| replies.contains_key(&from))
        {
            #[cfg(feature = "logging")]
            trace!(self.logger, "Ignoring FastReply"; "from" => from, "request_id" => ?freply.request_id, "ballot" => ?freply.n);
            return;
        }

        let request_id = freply.request_id;
        let entry = self
            .reply_set
            .entry(request_id)
            .or_insert_with(|| (HashMap::new(), None));
        if freply.is_leader {
            entry.1 = Some(from);
        }
        entry.0.insert(from, NezhaReply::Fast(freply));
        #[cfg(feature = "logging")]
        trace!(self.logger, "FastReply recorded"; "from" => from, "request_id" => ?request_id, "is_leader" => entry.1.is_some());

        let is_committed = self.check_committed(request_id);
        if is_committed {
            #[cfg(feature = "logging")]
            debug!(self.logger, "Request committed via fast path"; "request_id" => ?request_id);
            self.committed.insert(request_id, true);
            self.reply_set.remove(&request_id);
        }
    }

    fn check_committed(&self, request_id: RequestId) -> bool {
        // Get replies mapping for this request id
        let (replies, leader_pid_opt) = match self.reply_set.get(&request_id) {
            Some((replies, leader_pid_opt)) => (replies, leader_pid_opt),
            None => return false,
        };

        // Get leader's reply if it exists (it is always a FastReply), otherwise return false as leader's reply is necessary to determine if request is committed
        let leader_pid = match leader_pid_opt {
            Some(pid) => *pid,
            None => return false,
        };
        let leader_reply = match replies.get(&leader_pid) {
            Some(NezhaReply::Fast(f)) => f,
            _ => return false,
        };

        // Count the number of fast and slow replies
        let mut slow_reply_num = 0;
        let mut fast_reply_num = 0;
        for reply in replies.values() {
            match reply {
                NezhaReply::Slow(_) => {
                    // Slow reply counts as a fast reply since follower's log is guaranteed to be up to date with the leader
                    slow_reply_num += 1;
                    fast_reply_num += 1;
                }
                // Fast reply only counts if it has the same log hash as the leader
                NezhaReply::Fast(f) if f.log_hash == leader_reply.log_hash => {
                    fast_reply_num += 1;
                }
                _ => {}
            }
        }

        // Request is committed if it has either a super quorum of fast replies or an accept quorum of slow replies
        let committed = self.leader_state.quorum.is_super_quorum(fast_reply_num)
            || self.leader_state.quorum.is_accept_quorum(slow_reply_num);

        #[cfg(feature = "logging")]
        if committed {
            debug!(
                self.logger,
                "Request {:?} is committed with {} fast replies and {} slow replies",
                request_id,
                fast_reply_num,
                slow_reply_num
            );
        }

        committed
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
        self.outgoing.push(Message::SequencePaxos(PaxosMessage {
            from: self.pid,
            to: pid,
            msg: PaxosMsg::PrepareReq(prepreq),
        }));
    }

    fn propose_entry(&mut self, mut entry: T) {
        // TODO: add deadline using clock simulator
        entry.set_deadline(0);
        entry.set_request_id(RequestId::new_v4());

        match self.state {
            // TODO: replace with commented out paths below once clock simulator is implemented
            (Role::Leader, Phase::Prepare) => self.buffered_proposals.push(entry),
            (Role::Leader, Phase::Accept) => self.accept_entry_leader(entry),
            _ => self.forward_proposals(vec![entry]),
            // While undergoing leader change, fall back to normal OmniPaxos paths
            // (Role::Leader, Phase::Prepare) => self.buffered_proposals.push(entry),
            // (Role::Follower, Phase::Prepare) => self.forward_proposals(vec![entry]),

            // // Otherwise, follow Nezha path- broadcast PrepareWithDeadline to all peers and process it locally
            // _ => {
            //     let prep = PrepareWithDeadline {
            //         from: self.pid,
            //         entry: entry.clone(),
            //         sent: 0, // TODO: add current time from clock simulator
            //     };

            //     for peer_pid in &self.peers {
            //         self.outgoing.push(Message::SequencePaxos(PaxosMessage {
            //             from: self.pid,
            //             to: *peer_pid,
            //             msg: PaxosMsg::PrepareWithDeadline(prep.clone()),
            //         }));
            //     }
            //     self.handle_prepare_with_deadline(prep)
            // }
        }
    }

    pub(crate) fn get_leader_state(&self) -> &LeaderState<T> {
        &self.leader_state
    }

    pub(crate) fn forward_proposals(&mut self, mut entries: Vec<T>) {
        let leader = self.get_current_leader();
        if leader > 0 && self.pid != leader {
            let pf = PaxosMsg::ProposalForward(entries);
            let msg = Message::SequencePaxos(PaxosMessage {
                from: self.pid,
                to: leader,
                msg: pf,
            });
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
            let msg = Message::SequencePaxos(PaxosMessage {
                from: self.pid,
                to: leader,
                msg: fs,
            });
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

#[derive(PartialEq, Debug)]
pub(crate) enum Phase {
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
    pub(crate) batch_size: usize,
    flexible_quorum: Option<FlexibleQuorum>,
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
            #[cfg(feature = "logging")]
            logger_file_path: config.server_config.logger_file_path,
            #[cfg(feature = "logging")]
            custom_logger: config.server_config.custom_logger,
        }
    }
}

#[cfg(all(test, not(feature = "unicache")))]
mod tests {
    use super::*;
    use crate::ballot_leader_election::Ballot;
    use crate::messages::sequence_paxos::{FastReply, NezhaReply, PrepareWithDeadline, SlowReply};
    use crate::messages::RequestId;
    use crate::storage::{Entry, LogHash, Snapshot};
    use crate::test_storage::TestStorage;
    use crate::util::{FlexibleQuorum, WRITE_ERROR_MSG};
    use crate::{ClusterConfig, OmniPaxosConfig, ServerConfig};
    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    // ── Test helpers ──

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct TestEntry {
        value: u64,
        request_id: RequestId,
        deadline: u64,
    }

    #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    struct TestSnapshot;

    impl Snapshot<TestEntry> for TestSnapshot {
        fn create(_: &[TestEntry]) -> Self {
            Self
        }
        fn merge(&mut self, _: Self) {}
        fn use_snapshots() -> bool {
            false
        }
    }

    impl Entry for TestEntry {
        type Snapshot = TestSnapshot;

        fn stable_encode(&self, out: &mut Vec<u8>) {
            out.extend_from_slice(&self.value.to_le_bytes());
            out.extend_from_slice(self.request_id.as_bytes());
            out.extend_from_slice(&self.deadline.to_le_bytes());
        }

        fn get_deadline(&self) -> u64 {
            self.deadline
        }

        fn set_deadline(&mut self, deadline: u64) {
            self.deadline = deadline;
        }

        fn get_request_id(&self) -> RequestId {
            self.request_id
        }

        fn set_request_id(&mut self, request_id: RequestId) {
            self.request_id = request_id;
        }
    }

    impl TestEntry {
        fn new(value: u64, request_id: RequestId, deadline: u64) -> Self {
            Self {
                value,
                request_id,
                deadline,
            }
        }
    }

    /// Create a SequencePaxos instance with the given nodes and this node's pid.
    /// Sets the instance into (role, phase) state and writes a promise for the given ballot.
    fn create_paxos(
        pid: u64,
        nodes: Vec<u64>,
        role: Role,
        phase: Phase,
    ) -> SequencePaxos<TestEntry, TestStorage<TestEntry>> {
        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes,
            flexible_quorum: None,
        };
        let server_config = ServerConfig {
            pid,
            ..ServerConfig::default()
        };
        let omni_config = OmniPaxosConfig {
            cluster_config,
            server_config,
        };
        let storage = TestStorage::default();
        let mut paxos = SequencePaxos::with(omni_config.into(), storage);
        paxos.state = (role, phase);
        paxos
    }

    /// Create a paxos node in Accept phase with a written promise.
    fn create_accept_paxos(
        pid: u64,
        is_leader: bool,
        nodes: Vec<u64>,
    ) -> SequencePaxos<TestEntry, TestStorage<TestEntry>> {
        let role = if is_leader {
            Role::Leader
        } else {
            Role::Follower
        };
        let mut paxos = create_paxos(pid, nodes, role, Phase::Accept);
        let ballot = Ballot::with(1, 1, 1, 5);
        paxos
            .internal_storage
            .set_promise(ballot)
            .expect(WRITE_ERROR_MSG);
        paxos
    }

    #[test]
    fn prepare_with_deadline_goes_to_early_buffer_when_deadline_above_last_released() {
        let mut paxos = create_accept_paxos(1, false, vec![1, 2, 3]);
        let rid = Uuid::new_v4();
        let entry = TestEntry::new(42, rid, 100);
        let prep = PrepareWithDeadline {
            from: 2,
            entry,
            sent: 0,
        };

        paxos.handle_prepare_with_deadline(prep);

        assert_eq!(paxos.early_buffer.len(), 1);
        assert!(paxos.late_buffer.is_empty());
    }

    #[test]
    fn prepare_with_deadline_goes_to_late_buffer_when_deadline_at_or_below_last_released() {
        let mut paxos = create_accept_paxos(1, false, vec![1, 2, 3]);
        paxos.last_released_deadline = 50;

        let rid = Uuid::new_v4();
        let entry = TestEntry::new(42, rid, 30); // deadline < last_released
        let prep = PrepareWithDeadline {
            from: 2,
            entry,
            sent: 0,
        };

        paxos.handle_prepare_with_deadline(prep);

        assert!(paxos.early_buffer.is_empty());
        assert_eq!(paxos.late_buffer.len(), 1);
        assert!(paxos.late_buffer.contains_key(&rid));
    }

    #[test]
    fn prepare_with_deadline_equal_to_last_released_goes_to_late_buffer() {
        let mut paxos = create_accept_paxos(1, false, vec![1, 2, 3]);
        paxos.last_released_deadline = 50;

        let rid = Uuid::new_v4();
        let entry = TestEntry::new(42, rid, 50); // deadline == last_released
        let prep = PrepareWithDeadline {
            from: 2,
            entry,
            sent: 0,
        };

        paxos.handle_prepare_with_deadline(prep);

        assert!(paxos.early_buffer.is_empty());
        assert_eq!(paxos.late_buffer.len(), 1);
    }

    #[test]
    fn early_buffer_orders_by_deadline_ascending() {
        let mut paxos = create_accept_paxos(1, false, vec![1, 2, 3]);

        let rid1 = Uuid::new_v4();
        let rid2 = Uuid::new_v4();
        let rid3 = Uuid::new_v4();
        let prep1 = PrepareWithDeadline {
            from: 2,
            entry: TestEntry::new(1, rid1, 300),
            sent: 0,
        };
        let prep2 = PrepareWithDeadline {
            from: 2,
            entry: TestEntry::new(2, rid2, 100),
            sent: 0,
        };
        let prep3 = PrepareWithDeadline {
            from: 2,
            entry: TestEntry::new(3, rid3, 200),
            sent: 0,
        };

        paxos.handle_prepare_with_deadline(prep1);
        paxos.handle_prepare_with_deadline(prep2);
        paxos.handle_prepare_with_deadline(prep3);

        assert_eq!(paxos.early_buffer.len(), 3);
        // BinaryHeap<Reverse<...>> should give smallest deadline first
        let top = paxos.early_buffer.peek().unwrap().0.entry.get_deadline();
        assert_eq!(top, 100);
    }

    #[test]
    fn process_early_buffer_does_nothing_when_not_in_accept_phase() {
        let mut paxos = create_paxos(1, vec![1, 2, 3], Role::Follower, Phase::Prepare);
        let prep = PrepareWithDeadline {
            from: 2,
            entry: TestEntry::new(1, Uuid::new_v4(), 0),
            sent: 0,
        };
        paxos.early_buffer.push(Reverse(prep));

        paxos.process_early_buffer();

        // Nothing should be consumed
        assert_eq!(paxos.early_buffer.len(), 1);
    }

    #[test]
    fn fast_reply_accumulates_in_reply_set() {
        let mut paxos = create_accept_paxos(1, false, vec![1, 2, 3, 4, 5]);
        let ballot = paxos.internal_storage.get_promise();
        let rid = Uuid::new_v4();
        let log_hash = LogHash::compute::<TestEntry>(&[]);

        let freply = FastReply {
            n: ballot,
            request_id: rid,
            log_hash,
            is_leader: false,
        };

        paxos.handle_fast_reply(freply, 2);

        assert!(paxos.reply_set.contains_key(&rid));
        let (replies, leader_opt) = paxos.reply_set.get(&rid).unwrap();
        assert_eq!(replies.len(), 1);
        assert!(replies.contains_key(&2));
        assert!(leader_opt.is_none()); // is_leader was false, so leader_opt should still be None
    }

    #[test]
    fn fast_reply_tracks_leader_pid() {
        let mut paxos = create_accept_paxos(1, false, vec![1, 2, 3, 4, 5]);
        let ballot = paxos.internal_storage.get_promise();
        let rid = Uuid::new_v4();
        let log_hash = LogHash::compute::<TestEntry>(&[]);

        let freply = FastReply {
            n: ballot,
            request_id: rid,
            log_hash,
            is_leader: true, // leader reply
        };

        paxos.handle_fast_reply(freply, 3);

        let (_, leader_opt) = paxos.reply_set.get(&rid).unwrap();
        assert_eq!(*leader_opt, Some(3));
    }

    #[test]
    fn fast_reply_ignores_stale_ballot() {
        let mut paxos = create_accept_paxos(1, false, vec![1, 2, 3, 4, 5]);
        let rid = Uuid::new_v4();
        let log_hash = LogHash::compute::<TestEntry>(&[]);

        // Use a ballot lower than the current promise (set to 1 in helper function)
        let stale_ballot = Ballot::default();

        let freply = FastReply {
            n: stale_ballot,
            request_id: rid,
            log_hash,
            is_leader: false,
        };

        paxos.handle_fast_reply(freply, 2);

        assert!(!paxos.reply_set.contains_key(&rid));
    }

    #[test]
    fn fast_reply_ignores_duplicate_from_same_node() {
        let mut paxos = create_accept_paxos(1, false, vec![1, 2, 3, 4, 5]);
        let ballot = paxos.internal_storage.get_promise();
        let rid = Uuid::new_v4();
        let log_hash = LogHash::compute::<TestEntry>(&[]);

        let freply1 = FastReply {
            n: ballot,
            request_id: rid,
            log_hash,
            is_leader: false,
        };
        let freply2 = FastReply {
            n: ballot,
            request_id: rid,
            log_hash,
            is_leader: true, // different is_leader to check it's truly ignored
        };

        paxos.handle_fast_reply(freply1, 2);
        paxos.handle_fast_reply(freply2, 2); // duplicate from node 2

        let (replies, leader_opt) = paxos.reply_set.get(&rid).unwrap();
        assert_eq!(replies.len(), 1);
        // leader_opt should still be None since the first reply had is_leader=false
        // and the duplicate was ignored
        assert!(leader_opt.is_none());
    }

    #[test]
    fn fast_reply_ignored_when_not_in_accept_phase() {
        let mut paxos = create_paxos(1, vec![1, 2, 3, 4, 5], Role::Follower, Phase::Prepare);
        let ballot = Ballot::with(1, 1, 1, 5);
        paxos
            .internal_storage
            .set_promise(ballot)
            .expect(WRITE_ERROR_MSG);
        let rid = Uuid::new_v4();
        let log_hash = LogHash::compute::<TestEntry>(&[]);

        let freply = FastReply {
            n: ballot,
            request_id: rid,
            log_hash,
            is_leader: false,
        };

        paxos.handle_fast_reply(freply, 2);

        assert!(!paxos.reply_set.contains_key(&rid));
    }

    #[test]
    fn check_committed_returns_false_without_leader_reply() {
        let mut paxos = create_accept_paxos(1, false, vec![1, 2, 3, 4, 5]);
        let ballot = paxos.internal_storage.get_promise();
        let rid = Uuid::new_v4();
        let log_hash = LogHash::compute::<TestEntry>(&[]);

        // Add replies from followers but none is marked as leader
        for from in [2, 3, 4, 5] {
            let freply = FastReply {
                n: ballot,
                request_id: rid,
                log_hash,
                is_leader: false,
            };
            paxos.handle_fast_reply(freply, from);
        }

        // Should not be committed since no leader reply
        assert!(!paxos.committed.contains_key(&rid));
        assert!(paxos.reply_set.contains_key(&rid));
    }

    #[test]
    fn check_committed_super_quorum_of_matching_fast_replies() {
        // 5 nodes: majority = 3, f = 2, super_quorum = ceil(2/2) + 2 + 1 = 4
        let mut paxos = create_accept_paxos(1, false, vec![1, 2, 3, 4, 5]);
        let ballot = paxos.internal_storage.get_promise();
        let rid = Uuid::new_v4();
        let log_hash = LogHash::compute::<TestEntry>(&[]);

        // Leader reply from node 2
        let leader_reply = FastReply {
            n: ballot,
            request_id: rid,
            log_hash,
            is_leader: true,
        };
        paxos.handle_fast_reply(leader_reply, 2);

        // Follower replies with matching hash from nodes 3, 4
        for from in [3, 4] {
            let freply = FastReply {
                n: ballot,
                request_id: rid,
                log_hash,
                is_leader: false,
            };
            paxos.handle_fast_reply(freply, from);
        }

        // 3 matching fast replies (nodes 2, 3, 4) + need super quorum of 4- shouldn't be committed yet
        assert!(!paxos.committed.contains_key(&rid));

        // One more matching fast reply from node 5- 4 matching replies = super quorum
        let freply = FastReply {
            n: ballot,
            request_id: rid,
            log_hash,
            is_leader: false,
        };
        paxos.handle_fast_reply(freply, 5);

        assert!(paxos.committed.contains_key(&rid));
        assert!(*paxos.committed.get(&rid).unwrap());
        // reply_set should be cleaned up once committed
        assert!(!paxos.reply_set.contains_key(&rid));
    }

    #[test]
    fn check_committed_mismatched_hash_does_not_count_as_fast() {
        // 5 nodes: majority = 3, f = 2, super_quorum = 4
        let mut paxos = create_accept_paxos(1, false, vec![1, 2, 3, 4, 5]);
        let ballot = paxos.internal_storage.get_promise();
        let rid = Uuid::new_v4();
        let leader_hash = LogHash::compute(&[TestEntry::new(1, rid, 0)]);
        let different_hash = LogHash::compute(&[TestEntry::new(99, rid, 0)]);

        // Leader reply
        let leader_reply = FastReply {
            n: ballot,
            request_id: rid,
            log_hash: leader_hash,
            is_leader: true,
        };
        paxos.handle_fast_reply(leader_reply, 2);

        // Followers 3, 4 send matching hash
        for from in [3, 4] {
            let freply = FastReply {
                n: ballot,
                request_id: rid,
                log_hash: leader_hash,
                is_leader: false,
            };
            paxos.handle_fast_reply(freply, from);
        }

        // Follower 5 sends different hash- should not count towards fast quorum
        let freply_mismatch = FastReply {
            n: ballot,
            request_id: rid,
            log_hash: different_hash,
            is_leader: false,
        };
        paxos.handle_fast_reply(freply_mismatch, 5);

        // Only 3 matching fast replies (2, 3, 4) < super quorum of 4- not committed
        assert!(!paxos.committed.contains_key(&rid));
    }

    #[test]
    fn check_committed_returns_false_for_unknown_request_id() {
        let paxos = create_accept_paxos(1, false, vec![1, 2, 3]);
        let unknown_rid = Uuid::new_v4();
        assert!(!paxos.check_committed(unknown_rid));
    }

    #[test]
    fn leader_change_clears_reply_set() {
        let mut paxos = create_accept_paxos(1, true, vec![1, 2, 3, 4, 5]);
        let ballot = paxos.internal_storage.get_promise();
        let rid = Uuid::new_v4();
        let log_hash = LogHash::compute::<TestEntry>(&[]);

        // Accumulate some replies
        let freply = FastReply {
            n: ballot,
            request_id: rid,
            log_hash,
            is_leader: true,
        };
        paxos.handle_fast_reply(freply, 1);

        assert!(!paxos.reply_set.is_empty());

        // Simulate a new leader being elected with a higher ballot
        let new_ballot = Ballot::with(2, 2, 2, 5);
        paxos.handle_leader(new_ballot);

        // reply_set should be cleared after leader change
        assert!(paxos.reply_set.is_empty());
    }

    #[test]
    fn slow_replies_count_towards_accept_quorum() {
        // 5 nodes: majority = 3 (accept quorum). Slow replies count for both slow and fast.
        let mut paxos = create_accept_paxos(1, false, vec![1, 2, 3, 4, 5]);
        let ballot = paxos.internal_storage.get_promise();
        let rid = Uuid::new_v4();
        let log_hash = LogHash::compute::<TestEntry>(&[]);

        // Manually populate reply_set with leader FastReply + slow replies
        let mut replies = HashMap::new();
        replies.insert(
            2u64,
            NezhaReply::Fast(FastReply {
                n: ballot,
                request_id: rid,
                log_hash,
                is_leader: true,
            }),
        );
        replies.insert(
            3u64,
            NezhaReply::Slow(SlowReply {
                n: ballot,
                request_id: rid,
            }),
        );
        replies.insert(
            4u64,
            NezhaReply::Slow(SlowReply {
                n: ballot,
                request_id: rid,
            }),
        );
        replies.insert(
            5u64,
            NezhaReply::Slow(SlowReply {
                n: ballot,
                request_id: rid,
            }),
        );
        paxos.reply_set.insert(rid, (replies, Some(2)));

        // 3 slow replies >= accept quorum of 3 → should be committed
        assert!(paxos.check_committed(rid));
    }
}
