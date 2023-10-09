use super::{
    ballot_leader_election::Ballot,
    messages::sequence_paxos::*,
    storage::{Entry, StopSign, Storage},
    util::LeaderState,
};
#[cfg(feature = "logging")]
use crate::utils::logger::create_logger;
use crate::{
    storage::InternalStorage,
    util::{AcceptedMetaData, FlexibleQuorum, NodeId, Quorum, SequenceNumber},
    ClusterConfig, CompactionErr, OmniPaxosConfig, ProposeErr,
};
#[cfg(feature = "logging")]
use slog::{debug, info, trace, warn, Logger};
use std::{fmt::Debug, vec};

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
    leader: Ballot,
    pending_proposals: Vec<T>,
    pending_stopsign: Option<StopSign>,
    outgoing: Vec<PaxosMessage<T>>,
    leader_state: LeaderState<T>,
    latest_accepted_meta: Option<(Ballot, usize)>,
    // Keeps track of sequence of accepts from leader where AcceptSync = 1
    current_seq_num: SequenceNumber,
    cached_promise: Option<Promise<T>>,
    buffer_size: usize,
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
        let (state, leader, lds) = match storage
            .get_promise()
            .expect("storage error while trying to read promise")
        {
            // if we recover a promise from storage then we must do failure recovery
            Some(b) => {
                let state = (Role::Follower, Phase::Recover);
                for peer_pid in &peers {
                    outgoing.push(PaxosMessage {
                        from: pid,
                        to: *peer_pid,
                        msg: PaxosMsg::PrepareReq,
                    });
                }
                (state, b, None)
            }
            None => ((Role::Follower, Phase::None), Ballot::default(), None),
        };

        let mut paxos = SequencePaxos {
            internal_storage: InternalStorage::with(storage, config.batch_size),
            pid,
            peers,
            state,
            pending_proposals: vec![],
            pending_stopsign: None,
            leader,
            outgoing,
            leader_state: LeaderState::<T>::with(leader, lds, max_pid, quorum),
            latest_accepted_meta: None,
            current_seq_num: SequenceNumber::default(),
            cached_promise: None,
            buffer_size: config.buffer_size,
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
            .expect("storage error while trying to write promise");
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

    /// Initiates the trim process.
    /// # Arguments
    /// * `trim_idx` - Deletes all entries up to [`trim_idx`], if the [`trim_idx`] is `None` then the minimum index accepted by **ALL** servers will be used as the [`trim_idx`].
    pub(crate) fn trim(&mut self, trim_idx: Option<u64>) -> Result<(), CompactionErr> {
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
            _ => Err(CompactionErr::NotCurrentLeader(self.leader.pid)),
        }
    }

    /// Trim the log and create a snapshot. ** Note: only up to the `decided_idx` can be snapshotted **
    /// # Arguments
    /// `idx` - Snapshots all entries with index < [`idx`], if the [`idx`] is None then the decided index will be used.
    /// `local_only` - If `true`, only this server snapshots the log. If `false` all servers performs the snapshot.
    pub(crate) fn snapshot(
        &mut self,
        idx: Option<u64>,
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
    pub(crate) fn get_decided_idx(&self) -> u64 {
        self.internal_storage.get_decided_idx()
    }

    /// Return trim index from storage.
    pub(crate) fn get_compacted_idx(&self) -> u64 {
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
        match &self.state {
            (Role::Leader, Phase::Prepare) => {
                // Resend Prepare
                let unpromised_peers = self.leader_state.get_unpromised_peers();
                for peer in unpromised_peers {
                    self.send_prepare(peer);
                }
            }
            (Role::Leader, Phase::Accept) => {
                // Resend AcceptStopSign or StopSign's decide
                if let Some(ss) = self.internal_storage.get_stopsign() {
                    let decided_idx = self.internal_storage.get_decided_idx();
                    for follower in self.leader_state.get_promised_followers() {
                        if self.internal_storage.stopsign_is_decided() {
                            self.send_decide(follower, decided_idx, true);
                        } else if self.leader_state.get_accepted_idx(follower)
                            != self.internal_storage.get_accepted_idx()
                        {
                            self.send_accept_stopsign(follower, ss.clone(), true);
                        }
                    }
                }
                // Resend Prepare
                let unpromised_peers = self.leader_state.get_unpromised_peers();
                for peer in unpromised_peers {
                    self.send_prepare(peer);
                }
            }
            (Role::Follower, Phase::Prepare) => {
                // Resend Promise
                match &self.cached_promise {
                    Some(promise) => {
                        self.outgoing.push(PaxosMessage {
                            from: self.pid,
                            to: promise.n.pid,
                            msg: PaxosMsg::Promise(promise.clone()),
                        });
                    }
                    None => {
                        // Shouldn't be possible to be in prepare phase without having
                        // cached the promise sent as a response to the prepare
                        #[cfg(feature = "logging")]
                        warn!(self.logger, "In Prepare phase without a cached promise!");
                        self.state = (Role::Follower, Phase::Recover);
                        self.outgoing.push(PaxosMessage {
                            from: self.pid,
                            to: self.leader.pid,
                            msg: PaxosMsg::PrepareReq,
                        });
                    }
                }
            }
            (Role::Follower, Phase::Recover) => {
                // Resend PrepareReq
                self.outgoing.push(PaxosMessage {
                    from: self.pid,
                    to: self.leader.pid,
                    msg: PaxosMsg::PrepareReq,
                });
            }
            _ => (),
        }
    }

    /// Returns the id of the current leader.
    pub(crate) fn get_current_leader(&self) -> Ballot {
        self.leader
    }

    /// Returns the outgoing messages from this replica. The messages should then be sent via the network implementation.
    pub(crate) fn get_outgoing_msgs(&mut self) -> Vec<PaxosMessage<T>> {
        let mut outgoing = Vec::with_capacity(self.buffer_size);
        std::mem::swap(&mut self.outgoing, &mut outgoing);
        #[cfg(feature = "batch_accept")]
        {
            self.leader_state.reset_batch_accept_meta();
        }
        self.latest_accepted_meta = None;
        outgoing
    }

    /// Handle an incoming message.
    pub(crate) fn handle(&mut self, m: PaxosMessage<T>) {
        match m.msg {
            PaxosMsg::PrepareReq => self.handle_preparereq(m.from),
            PaxosMsg::Prepare(prep) => self.handle_prepare(prep, m.from),
            PaxosMsg::Promise(prom) => match &self.state {
                (Role::Leader, Phase::Prepare) => self.handle_promise_prepare(prom, m.from),
                (Role::Leader, Phase::Accept) => self.handle_promise_accept(prom, m.from),
                _ => {}
            },
            PaxosMsg::AcceptSync(acc_sync) => self.handle_acceptsync(acc_sync, m.from),
            PaxosMsg::AcceptDecide(acc) => self.handle_acceptdecide(acc),
            PaxosMsg::Accepted(accepted) => self.handle_accepted(accepted, m.from),
            PaxosMsg::Decide(d) => self.handle_decide(d),
            PaxosMsg::ProposalForward(proposals) => self.handle_forwarded_proposal(proposals),
            PaxosMsg::Compaction(c) => self.handle_compaction(c),
            PaxosMsg::AcceptStopSign(acc_ss) => self.handle_accept_stopsign(acc_ss),
            PaxosMsg::ForwardStopSign(f_ss) => self.handle_forwarded_stopsign(f_ss),
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
    fn pending_reconfiguration(&self) -> bool {
        self.internal_storage.get_stopsign().is_some()
    }

    /// Append an entry to the replicated log.
    pub(crate) fn append(&mut self, entry: T) -> Result<(), ProposeErr<T>> {
        if self.pending_reconfiguration() {
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
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "Propose reconfiguration {:?}", new_config.nodes
        );
        if self.pending_reconfiguration() {
            Err(ProposeErr::PendingReconfigConfig(new_config, metadata))
        } else {
            match self.state {
                (Role::Leader, Phase::Prepare) => {
                    if self.pending_stopsign.is_none() {
                        let ss = StopSign::with(new_config, metadata);
                        self.pending_stopsign = Some(ss);
                    } else {
                        return Err(ProposeErr::PendingReconfigConfig(new_config, metadata));
                    }
                }
                (Role::Leader, Phase::Accept) => {
                    if !self.pending_reconfiguration() {
                        let ss = StopSign::with(new_config, metadata);
                        self.accept_stopsign(ss.clone());
                        for pid in self.leader_state.get_promised_followers() {
                            self.send_accept_stopsign(pid, ss.clone(), false);
                        }
                    } else {
                        return Err(ProposeErr::PendingReconfigConfig(new_config, metadata));
                    }
                }
                _ => {
                    let ss = StopSign::with(new_config, metadata);
                    self.forward_stopsign(ss);
                }
            }
            Ok(())
        }
    }

    fn send_accept_stopsign(&mut self, to: NodeId, ss: StopSign, resend: bool) {
        let seq_num = match resend {
            true => self.leader_state.get_seq_num(to),
            false => self.leader_state.next_seq_num(to),
        };
        let acc_ss = PaxosMsg::AcceptStopSign(AcceptStopSign {
            seq_num,
            n: self.leader_state.n_leader,
            ss,
        });
        self.outgoing.push(PaxosMessage {
            from: self.pid,
            to,
            msg: acc_ss,
        });
    }

    fn accept_stopsign(&mut self, ss: StopSign) {
        self.internal_storage
            .set_stopsign(Some(ss))
            .expect("storage error while trying to write stopsign");
        if self.state.0 == Role::Leader {
            let accepted_idx = self.internal_storage.get_accepted_idx();
            self.leader_state.set_accepted_idx(self.pid, accepted_idx);
        }
    }

    /// Handles re-establishing a connection to a previously disconnected peer.
    /// This should only be called if the underlying network implementation indicates that a connection has been re-established.
    pub(crate) fn reconnected(&mut self, pid: NodeId) {
        if pid == self.pid {
            return;
        } else if pid == self.leader.pid {
            self.state = (Role::Follower, Phase::Recover);
        }
        self.outgoing.push(PaxosMessage {
            from: self.pid,
            to: pid,
            msg: PaxosMsg::PrepareReq,
        });
    }

    fn propose_entry(&mut self, entry: T) {
        match self.state {
            (Role::Leader, Phase::Prepare) => self.pending_proposals.push(entry),
            (Role::Leader, Phase::Accept) => self.accept_entry(entry),
            _ => self.forward_proposals(vec![entry]),
        }
    }

    pub(crate) fn get_leader_state(&self) -> &LeaderState<T> {
        &self.leader_state
    }
}

#[derive(PartialEq, Debug)]
enum Phase {
    Prepare,
    Accept,
    Recover,
    None,
}

#[derive(PartialEq, Debug)]
enum Role {
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
    batch_size: usize,
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
