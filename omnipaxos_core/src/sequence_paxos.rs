use super::{
    ballot_leader_election::Ballot,
    messages::sequence_paxos::*,
    storage::{Entry, Snapshot, SnapshotType, StopSign, StopSignEntry, Storage},
    util::{defaults::BUFFER_SIZE, LeaderState, PromiseMetaData},
};
#[cfg(feature = "hocon_config")]
use crate::utils::hocon_kv::{CONFIG_ID, LOG_FILE_PATH, PEERS, PID, SP_BUFFER_SIZE};
#[cfg(feature = "logging")]
use crate::utils::logger::create_logger;
use crate::{
    storage::InternalStorage,
    util::{ConfigurationId, NodeId},
};
#[cfg(feature = "hocon_config")]
use hocon::Hocon;
#[cfg(feature = "logging")]
use slog::{debug, info, trace, warn, Logger};
use std::{fmt::Debug, marker::PhantomData, vec};

/// a Sequence Paxos replica. Maintains local state of the replicated log, handles incoming messages and produces outgoing messages that the user has to fetch periodically and send using a network implementation.
/// User also has to periodically fetch the decided entries that are guaranteed to be strongly consistent and linearizable, and therefore also safe to be used in the higher level application.
/// If snapshots are not desired to be used, use `()` for the type parameter `S`.
pub struct SequencePaxos<T, S, B>
where
    T: Entry,
    S: Snapshot<T>,
    B: Storage<T, S>,
{
    pub(crate) internal_storage: InternalStorage<B, T, S>,
    config_id: ConfigurationId,
    pid: NodeId,
    peers: Vec<u64>, // excluding self pid
    state: (Role, Phase),
    leader: Ballot,
    pending_proposals: Vec<T>,
    pending_stopsign: Option<StopSign>,
    outgoing: Vec<PaxosMessage<T, S>>,
    leader_state: LeaderState<T, S>,
    latest_accepted_meta: Option<(Ballot, usize)>,
    buffer_size: usize,
    s: PhantomData<S>,
    #[cfg(feature = "logging")]
    logger: Logger,
}

impl<T, S, B> SequencePaxos<T, S, B>
where
    T: Entry,
    S: Snapshot<T>,
    B: Storage<T, S>,
{
    /*** User functions ***/
    /// Creates a Sequence Paxos replica.
    pub(crate) fn with(config: SequencePaxosConfig, storage: B) -> Self {
        let pid = config.pid;
        let peers = config.peers;
        let config_id = config.configuration_id;
        let num_nodes = &peers.len() + 1;
        let majority = num_nodes / 2 + 1;
        let max_peer_pid = peers.iter().max().unwrap();
        let max_pid = *std::cmp::max(max_peer_pid, &pid) as usize;
        let (state, leader, lds) = match &config.skip_prepare_use_leader {
            Some(l) => {
                let (role, lds) = if l.pid == pid {
                    // we are leader in new config
                    let mut v = vec![None; max_pid];
                    for idx in peers.iter().map(|pid| *pid as usize - 1) {
                        // this works as a promise
                        v[idx] = Some(0);
                    }
                    (Role::Leader, Some(v))
                } else {
                    (Role::Follower, None)
                };
                let state = (role, Phase::FirstAccept);
                (state, *l, lds)
            }
            None => {
                let state = (Role::Follower, Phase::None);
                let lds = None;
                (state, Ballot::default(), lds)
            }
        };

        let mut paxos = SequencePaxos {
            internal_storage: InternalStorage::with(storage),
            config_id,
            pid,
            peers,
            state,
            pending_proposals: vec![],
            pending_stopsign: None,
            leader,
            outgoing: Vec::with_capacity(BUFFER_SIZE),
            leader_state: LeaderState::<T, S>::with(leader, lds, max_pid, majority),
            latest_accepted_meta: None,
            buffer_size: config.buffer_size,
            s: PhantomData,
            #[cfg(feature = "logging")]
            logger: {
                let path = config.logger_file_path;
                config.logger.unwrap_or_else(|| {
                    let s = path.unwrap_or_else(|| format!("logs/paxos_{}.log", pid));
                    create_logger(s.as_str())
                })
            },
        };
        paxos.internal_storage.set_promise(leader);
        #[cfg(feature = "logging")]
        {
            info!(paxos.logger, "Paxos component pid: {} created!", pid);
        }
        paxos
    }

    /// Initiates the trim process.
    /// # Arguments
    /// * `trim_index` - Deletes all entries up to [`trim_index`], if the [`trim_index`] is `None` then the minimum index accepted by **ALL** servers will be used as the [`trim_index`].
    pub(crate) fn trim(&mut self, trim_index: Option<u64>) -> Result<(), CompactionErr> {
        match self.state {
            (Role::Leader, _) => {
                let min_all_accepted_idx = self.leader_state.get_min_all_accepted_idx();
                let trimmed_idx = match trim_index {
                    Some(idx) if idx <= *min_all_accepted_idx => idx,
                    None => {
                        #[cfg(feature = "logging")]
                        trace!(
                            self.logger,
                            "No trim index provided, using min_las_index: {:?}",
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
                result
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
        result
    }

    /// Return the decided index.
    pub(crate) fn get_decided_idx(&self) -> u64 {
        self.internal_storage.get_decided_idx()
    }

    /// Return trim index from storage.
    pub(crate) fn get_compacted_idx(&self) -> u64 {
        self.internal_storage.get_compacted_idx()
    }

    /// Recover from failure. Goes into recover state and sends `PrepareReq` to all peers.
    pub(crate) fn fail_recovery(&mut self) {
        self.state = (Role::Follower, Phase::Recover);
        for pid in &self.peers {
            self.outgoing.push(PaxosMessage {
                from: self.pid,
                to: *pid,
                msg: PaxosMsg::PrepareReq,
            });
        }
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

    /// Returns the id of the current leader.
    pub(crate) fn get_current_leader(&self) -> Ballot {
        self.leader
    }

    /// Returns the outgoing messages from this replica. The messages should then be sent via the network implementation.
    pub(crate) fn get_outgoing_msgs(&mut self) -> Vec<PaxosMessage<T, S>> {
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
    pub(crate) fn handle(&mut self, m: PaxosMessage<T, S>) {
        match m.msg {
            PaxosMsg::PrepareReq => self.handle_preparereq(m.from),
            PaxosMsg::Prepare(prep) => self.handle_prepare(prep, m.from),
            PaxosMsg::Promise(prom) => match &self.state {
                (Role::Leader, Phase::Prepare) => self.handle_promise_prepare(prom, m.from),
                (Role::Leader, Phase::Accept) => self.handle_promise_accept(prom, m.from),
                _ => {}
            },
            PaxosMsg::AcceptSync(acc_sync) => self.handle_acceptsync(acc_sync, m.from),
            PaxosMsg::FirstAccept(f) => self.handle_firstaccept(f),
            PaxosMsg::AcceptDecide(acc) => self.handle_acceptdecide(acc),
            PaxosMsg::Accepted(accepted) => self.handle_accepted(accepted, m.from),
            PaxosMsg::Decide(d) => self.handle_decide(d),
            PaxosMsg::ProposalForward(proposals) => self.handle_forwarded_proposal(proposals),
            PaxosMsg::Compaction(c) => self.handle_compaction(c),
            PaxosMsg::AcceptStopSign(acc_ss) => self.handle_accept_stopsign(acc_ss),
            PaxosMsg::AcceptedStopSign(acc_ss) => self.handle_accepted_stopsign(acc_ss, m.from),
            PaxosMsg::DecideStopSign(d_ss) => self.handle_decide_stopsign(d_ss),
            PaxosMsg::ForwardStopSign(f_ss) => self.handle_forwarded_stopsign(f_ss),
        }
    }

    /// Returns whether this Sequence Paxos has been reconfigured
    pub(crate) fn is_reconfigured(&self) -> Option<StopSign> {
        match self.internal_storage.get_stopsign() {
            Some(ss) if ss.decided => Some(ss.stopsign),
            _ => None,
        }
    }

    /// Returns whether this Sequence Paxos instance is stopped, i.e. if it has been reconfigured.
    fn stopped(&self) -> bool {
        self.get_stopsign().is_some()
    }

    /// Append an entry to the replicated log.
    pub(crate) fn append(&mut self, entry: T) -> Result<(), ProposeErr<T>> {
        if self.stopped() {
            Err(ProposeErr::Normal(entry))
        } else {
            self.propose_entry(entry);
            Ok(())
        }
    }

    /// Propose a reconfiguration. Returns error if already stopped or new configuration is empty.
    pub(crate) fn reconfigure(&mut self, rc: ReconfigurationRequest) -> Result<(), ProposeErr<T>> {
        let ReconfigurationRequest {
            new_configuration,
            metadata,
        } = rc;
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "Propose reconfiguration {:?}", new_configuration
        );
        if self.stopped() {
            Err(ProposeErr::Reconfiguration(new_configuration))
        } else {
            match self.state {
                (Role::Leader, Phase::Prepare) => {
                    if self.pending_stopsign.is_none() {
                        let ss = StopSign::with(self.config_id + 1, new_configuration, metadata);
                        self.pending_stopsign = Some(ss);
                    } else {
                        return Err(ProposeErr::Reconfiguration(new_configuration));
                    }
                }
                (Role::Leader, Phase::Accept) => {
                    if !self.stopped() {
                        let ss = StopSign::with(self.config_id + 1, new_configuration, metadata);
                        self.accept_stopsign(ss.clone());
                        self.send_accept_stopsign(ss);
                    } else {
                        return Err(ProposeErr::Reconfiguration(new_configuration));
                    }
                }
                (Role::Leader, Phase::FirstAccept) => {
                    if !self.stopped() {
                        self.send_first_accept();
                        let ss = StopSign::with(self.config_id + 1, new_configuration, metadata);
                        self.accept_stopsign(ss.clone());
                        self.send_accept_stopsign(ss);
                    } else {
                        return Err(ProposeErr::Reconfiguration(new_configuration));
                    }
                }
                _ => {
                    let ss = StopSign::with(self.config_id + 1, new_configuration, metadata);
                    self.forward_stopsign(ss);
                }
            }
            Ok(())
        }
    }

    fn send_accept_stopsign(&mut self, ss: StopSign) {
        let acc_ss = PaxosMsg::AcceptStopSign(AcceptStopSign {
            n: self.leader_state.n_leader,
            ss,
        });
        for pid in self.leader_state.get_promised_followers() {
            self.outgoing.push(PaxosMessage {
                from: self.pid,
                to: pid,
                msg: acc_ss.clone(),
            });
        }
    }

    fn accept_stopsign(&mut self, ss: StopSign) {
        self.internal_storage
            .set_stopsign(StopSignEntry::with(ss, false));
        if self.state.0 == Role::Leader {
            self.leader_state.set_accepted_stopsign(self.pid);
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
            (Role::Leader, Phase::Accept) => self.send_accept(entry),
            (Role::Leader, Phase::FirstAccept) => {
                self.send_first_accept();
                self.send_accept(entry);
            }
            _ => self.forward_proposals(vec![entry]),
        }
    }

    fn get_stopsign(&self) -> Option<StopSign> {
        self.internal_storage.get_stopsign().map(|x| x.stopsign)
    }

    /// Handle a new leader. Should be called when the leader election has elected a new leader with the ballot `n`
    /*** Leader ***/
    pub(crate) fn handle_leader(&mut self, n: Ballot) {
        #[cfg(feature = "logging")]
        debug!(self.logger, "Newly elected leader: {:?}", n);
        if n <= self.leader_state.n_leader || n <= self.internal_storage.get_promise() {
            return;
        }
        if self.stopped() {
            self.pending_proposals.clear();
        }
        if self.pid == n.pid {
            self.leader_state = LeaderState::with(
                n,
                None,
                self.leader_state.max_pid,
                self.leader_state.majority,
            );
            self.leader = n;
            self.internal_storage.set_promise(n);
            /* insert my promise */
            let na = self.internal_storage.get_accepted_round();
            let ld = self.internal_storage.get_decided_idx();
            let la = self.internal_storage.get_log_len();
            let my_promise = Promise {
                n,
                n_accepted: na,
                decided_snapshot: None,
                ld,
                la,
                suffix: vec![],
                stopsign: self.get_stopsign(),
            };
            self.leader_state.set_promise(my_promise, self.pid, true);
            /* initialise longest chosen sequence and update state */
            self.state = (Role::Leader, Phase::Prepare);
            let prep = Prepare {
                n,
                ld,
                n_accepted: self.internal_storage.get_accepted_round(),
                la,
            };
            /* send prepare */
            for pid in &self.peers {
                self.outgoing.push(PaxosMessage {
                    from: self.pid,
                    to: *pid,
                    msg: PaxosMsg::Prepare(prep),
                });
            }
        } else {
            self.state.0 = Role::Follower;
        }
    }

    fn handle_preparereq(&mut self, from: u64) {
        #[cfg(feature = "logging")]
        debug!(self.logger, "Incoming message PrepareReq from {}", from);
        if self.state.0 == Role::Leader {
            self.leader_state.set_decided_idx(from, None);
            #[cfg(feature = "batch_accept")]
            {
                self.leader_state.set_batch_accept_meta(from, None);
            }
            let ld = self.internal_storage.get_decided_idx();
            let n_accepted = self.internal_storage.get_accepted_round();
            let la = self.internal_storage.get_log_len();
            let prep = Prepare {
                n: self.leader_state.n_leader,
                ld,
                n_accepted,
                la,
            };
            self.outgoing.push(PaxosMessage {
                from: self.pid,
                to: from,
                msg: PaxosMsg::Prepare(prep),
            });
        }
    }

    fn forward_proposals(&mut self, mut entries: Vec<T>) {
        if self.leader.pid > 0 && self.leader.pid != self.pid {
            #[cfg(feature = "logging")]
            trace!(
                self.logger,
                "Forwarding proposal to Leader {:?}",
                self.leader
            );
            let pf = PaxosMsg::ProposalForward(entries);
            let msg = PaxosMessage {
                from: self.pid,
                to: self.leader.pid,
                msg: pf,
            };
            self.outgoing.push(msg);
        } else {
            self.pending_proposals.append(&mut entries);
        }
    }

    fn forward_stopsign(&mut self, ss: StopSign) {
        if self.leader.pid > 0 && self.leader.pid != self.pid {
            #[cfg(feature = "logging")]
            trace!(
                self.logger,
                "Forwarding StopSign to Leader {:?}",
                self.leader
            );
            let fs = PaxosMsg::ForwardStopSign(ss);
            let msg = PaxosMessage {
                from: self.pid,
                to: self.leader.pid,
                msg: fs,
            };
            self.outgoing.push(msg);
        } else {
            if self.pending_stopsign.as_mut().is_none() {
                self.pending_stopsign = Some(ss);
            }
        }
    }

    fn handle_forwarded_proposal(&mut self, mut entries: Vec<T>) {
        if !self.stopped() {
            match self.state {
                (Role::Leader, Phase::Prepare) => self.pending_proposals.append(&mut entries),
                (Role::Leader, Phase::Accept) => self.send_batch_accept(entries),
                (Role::Leader, Phase::FirstAccept) => {
                    self.send_first_accept();
                    self.send_batch_accept(entries);
                }
                _ => self.forward_proposals(entries),
            }
        }
    }

    fn handle_forwarded_stopsign(&mut self, ss: StopSign) {
        if !self.stopped() {
            match self.state {
                (Role::Leader, Phase::Prepare) => {
                    if self.pending_stopsign.as_mut().is_none() {
                        self.pending_stopsign = Some(ss);
                    }
                }
                (Role::Leader, Phase::Accept) => {
                    if self.pending_stopsign.is_none() {
                        self.accept_stopsign(ss.clone());
                        self.send_accept_stopsign(ss);
                    }
                }
                (Role::Leader, Phase::FirstAccept) => {
                    self.send_first_accept();
                    self.accept_stopsign(ss.clone());
                    self.send_accept_stopsign(ss);
                }
                _ => self.forward_stopsign(ss),
            }
        }
    }

    fn send_first_accept(&mut self) {
        let f = FirstAccept {
            n: self.leader_state.n_leader,
        };
        for pid in self.leader_state.get_promised_followers() {
            self.outgoing.push(PaxosMessage {
                from: self.pid,
                to: pid,
                msg: PaxosMsg::FirstAccept(f.clone()),
            });
        }
        self.state.1 = Phase::Accept;
    }

    #[cfg(feature = "batch_accept")]
    fn send_accept_and_cache(&mut self, to: u64, entries: Vec<T>) {
        let acc = AcceptDecide {
            n: self.leader_state.n_leader,
            ld: self.leader_state.get_chosen_idx(),
            entries,
        };
        self.outgoing.push(PaxosMessage {
            from: self.pid,
            to,
            msg: PaxosMsg::AcceptDecide(acc),
        });
        self.leader_state
            .set_batch_accept_meta(to, Some(self.outgoing.len() - 1));
    }

    fn send_accept(&mut self, entry: T) {
        let la = self.internal_storage.append_entry(entry.clone());
        self.leader_state.set_accepted_idx(self.pid, la);
        for pid in self.leader_state.get_promised_followers() {
            if cfg!(feature = "batch_accept") {
                #[cfg(feature = "batch_accept")]
                match self.leader_state.get_batch_accept_meta(pid) {
                    Some((n, outgoing_idx)) if n == self.leader_state.n_leader => {
                        let PaxosMessage { msg, .. } = self.outgoing.get_mut(outgoing_idx).unwrap();
                        match msg {
                            PaxosMsg::AcceptDecide(a) => a.entries.push(entry.clone()),
                            _ => self.send_accept_and_cache(pid, vec![entry.clone()]),
                        }
                    }
                    _ => self.send_accept_and_cache(pid, vec![entry.clone()]),
                }
            } else {
                let acc = AcceptDecide {
                    n: self.leader_state.n_leader,
                    ld: self.leader_state.get_chosen_idx(),
                    entries: vec![entry.clone()],
                };
                self.outgoing.push(PaxosMessage {
                    from: self.pid,
                    to: pid,
                    msg: PaxosMsg::AcceptDecide(acc),
                });
            }
        }
    }

    fn send_batch_accept(&mut self, entries: Vec<T>) {
        let la = self.internal_storage.append_entries(entries.clone());
        self.leader_state.set_accepted_idx(self.pid, la);
        for pid in self.leader_state.get_promised_followers() {
            if cfg!(feature = "batch_accept") {
                #[cfg(feature = "batch_accept")]
                match self.leader_state.get_batch_accept_meta(pid) {
                    Some((n, outgoing_idx)) if n == self.leader_state.n_leader => {
                        let PaxosMessage { msg, .. } = self.outgoing.get_mut(outgoing_idx).unwrap();
                        match msg {
                            PaxosMsg::AcceptDecide(a) => a.entries.append(entries.clone().as_mut()),
                            _ => self.send_accept_and_cache(pid, entries.clone()),
                        }
                    }
                    _ => self.send_accept_and_cache(pid, entries.clone()),
                }
            } else {
                let acc = AcceptDecide {
                    n: self.leader_state.n_leader,
                    ld: self.leader_state.get_chosen_idx(),
                    entries: entries.clone(),
                };
                self.outgoing.push(PaxosMessage {
                    from: self.pid,
                    to: pid,
                    msg: PaxosMsg::AcceptDecide(acc),
                });
            }
        }
    }

    fn send_accsync(&mut self, to: NodeId) {
        let decided_idx = self.get_decided_idx();
        let PromiseMetaData {
            n: max_promise_n,
            la: max_la,
            ..
        } = &self.leader_state.get_max_promise_meta();
        let PromiseMetaData {
            n: promise_n,
            la: promise_la,
            pid,
            ..
        } = self.leader_state.get_promise_meta(to);
        let (delta_snapshot, suffix, sync_idx) = if (promise_n == max_promise_n)
            && (promise_la < max_la)
        {
            let sfx = self.internal_storage.get_suffix(*promise_la);
            (None, sfx, *promise_la)
        } else {
            let ld = self
                .leader_state
                .get_decided_idx(*pid)
                .expect("Received PromiseMetaData but not found in ld");
            if ld < decided_idx && Self::use_snapshots() {
                let diff_entries = self.internal_storage.get_entries(ld, decided_idx);
                let delta_snapshot = Some(SnapshotType::Delta(S::create(diff_entries.as_slice())));
                let suffix = self.internal_storage.get_suffix(decided_idx);
                (delta_snapshot, suffix, ld)
            } else {
                let suffix = self.internal_storage.get_suffix(ld);
                (None, suffix, ld)
            }
        };
        let acc_sync = AcceptSync {
            n: self.leader_state.n_leader,
            decided_snapshot: delta_snapshot,
            suffix,
            sync_idx,
            decided_idx,
            stopsign: self.get_stopsign(),
        };
        let msg = PaxosMessage {
            from: self.pid,
            to: *pid,
            msg: PaxosMsg::AcceptSync(acc_sync),
        };
        self.outgoing.push(msg);
    }

    fn adopt_pending_stopsign(&mut self) {
        if let Some(ss) = self.pending_stopsign.take() {
            self.accept_stopsign(ss);
        }
    }

    fn append_pending_proposals(&mut self) {
        if !self.pending_proposals.is_empty() {
            let new_entries = std::mem::take(&mut self.pending_proposals);
            // append new proposals in my sequence
            let la = self.internal_storage.append_entries(new_entries);
            self.leader_state.set_accepted_idx(self.pid, la);
        }
    }

    fn handle_majority_promises(&mut self) {
        self.state = (Role::Leader, Phase::Accept);
        let max_stopsign = self.leader_state.take_max_promise_stopsign();
        let max_promise = self.leader_state.take_max_promise();
        let max_promise_meta = self.leader_state.get_max_promise_meta();
        let decided_idx = self.leader_state.lds.iter().max().unwrap().unwrap();
        match max_promise {
            Some((decided_snapshot, suffix)) => {
                match decided_snapshot {
                    Some(s) => {
                        let ld = self
                            .leader_state
                            .get_decided_idx(max_promise_meta.pid)
                            .unwrap();
                        match s {
                            SnapshotType::Complete(c) => {
                                self.internal_storage.set_snapshot(ld, c);
                            }
                            SnapshotType::Delta(d) => {
                                self.internal_storage.merge_snapshot(ld, d);
                            }
                            _ => unimplemented!(),
                        }
                        self.internal_storage.append_entries(suffix);
                        if let Some(ss) = max_stopsign {
                            self.accept_stopsign(ss);
                        } else {
                            self.append_pending_proposals();
                            self.adopt_pending_stopsign();
                        }
                    }
                    None => {
                        // no snapshot, only suffix
                        if max_promise_meta.n == self.internal_storage.get_accepted_round() {
                            self.internal_storage.append_entries(suffix);
                        } else {
                            self.internal_storage.append_on_decided_prefix(suffix);
                        }
                        if let Some(ss) = max_stopsign {
                            self.accept_stopsign(ss);
                        } else {
                            self.append_pending_proposals();
                            self.adopt_pending_stopsign();
                        }
                    }
                }
            }
            None => {
                // I am the most updated
                self.append_pending_proposals();
                self.adopt_pending_stopsign();
            }
        }
        self.internal_storage
            .set_accepted_round(self.leader_state.n_leader);
        self.internal_storage.set_decided_idx(decided_idx);
        for pid in self.leader_state.get_promised_followers() {
            self.send_accsync(pid);
        }
    }

    fn handle_promise_prepare(&mut self, prom: Promise<T, S>, from: u64) {
        #[cfg(feature = "logging")]
        debug!(
            self.logger,
            "Handling promise from {} in Prepare phase", from
        );
        if prom.n == self.leader_state.n_leader {
            let received_majority = self.leader_state.set_promise(prom, from, true);
            if received_majority {
                self.handle_majority_promises();
            }
        }
    }

    fn handle_promise_accept(&mut self, prom: Promise<T, S>, from: u64) {
        #[cfg(feature = "logging")]
        {
            let (r, p) = &self.state;
            debug!(
                self.logger,
                "Self role {:?}, phase {:?}. Incoming message Promise Accept from {}", r, p, from
            );
        }
        if prom.n == self.leader_state.n_leader {
            self.leader_state.set_promise(prom, from, false);
            self.send_accsync(from);
        }
    }

    fn handle_accepted(&mut self, accepted: Accepted, from: u64) {
        #[cfg(feature = "logging")]
        trace!(
            self.logger,
            "Got Accepted from {}, idx: {}, chosen_idx: {}",
            from,
            accepted.la,
            self.leader_state.get_chosen_idx()
        );
        if accepted.n == self.leader_state.n_leader && self.state == (Role::Leader, Phase::Accept) {
            self.leader_state.set_accepted_idx(from, accepted.la);
            if accepted.la > self.leader_state.get_chosen_idx()
                && self.leader_state.is_chosen(accepted.la)
            {
                self.leader_state.set_chosen_idx(accepted.la);
                let d = Decide {
                    n: self.leader_state.n_leader,
                    ld: self.leader_state.get_chosen_idx(),
                };
                for pid in self.leader_state.get_promised_followers() {
                    if cfg!(feature = "batch_accept") {
                        #[cfg(feature = "batch_accept")]
                        match self.leader_state.get_batch_accept_meta(pid) {
                            Some((n, outgoing_idx)) if n == self.leader_state.n_leader => {
                                let PaxosMessage { msg, .. } =
                                    self.outgoing.get_mut(outgoing_idx).unwrap();
                                match msg {
                                    PaxosMsg::AcceptDecide(a) => {
                                        a.ld = self.leader_state.get_chosen_idx()
                                    }
                                    _ => {
                                        self.outgoing.push(PaxosMessage {
                                            from: self.pid,
                                            to: pid,
                                            msg: PaxosMsg::Decide(d),
                                        });
                                    }
                                }
                            }
                            _ => {
                                self.outgoing.push(PaxosMessage {
                                    from: self.pid,
                                    to: pid,
                                    msg: PaxosMsg::Decide(d),
                                });
                            }
                        }
                    } else {
                        self.outgoing.push(PaxosMessage {
                            from: self.pid,
                            to: pid,
                            msg: PaxosMsg::Decide(d),
                        });
                    }
                }
                self.handle_decide(d);
            }
        }
    }

    fn handle_accepted_stopsign(&mut self, acc_stopsign: AcceptedStopSign, from: u64) {
        if acc_stopsign.n == self.leader_state.n_leader
            && self.state == (Role::Leader, Phase::Accept)
        {
            self.leader_state.set_accepted_stopsign(from);
            if self.leader_state.is_stopsign_chosen() {
                let d = DecideStopSign {
                    n: self.leader_state.n_leader,
                };
                self.handle_decide_stopsign(d);
                for pid in self.leader_state.get_promised_followers() {
                    self.outgoing.push(PaxosMessage {
                        from: self.pid,
                        to: pid,
                        msg: PaxosMsg::DecideStopSign(d),
                    });
                }
            }
        }
    }

    /*** Follower ***/
    fn handle_prepare(&mut self, prep: Prepare, from: u64) {
        if self.internal_storage.get_promise() <= prep.n {
            self.leader = prep.n;
            self.internal_storage.set_promise(prep.n);
            self.state = (Role::Follower, Phase::Prepare);
            let na = self.internal_storage.get_accepted_round();
            let la = self.internal_storage.get_log_len();
            let decided_idx = self.get_decided_idx();
            let (decided_snapshot, suffix) = if na > prep.n_accepted {
                let ld = prep.ld;
                if ld < decided_idx && Self::use_snapshots() {
                    let delta_snapshot =
                        self.internal_storage.create_diff_snapshot(ld, decided_idx);
                    let suffix = self.internal_storage.get_suffix(decided_idx);
                    (Some(delta_snapshot), suffix)
                } else {
                    let suffix = self.internal_storage.get_suffix(ld);
                    (None, suffix)
                }
            } else if na == prep.n_accepted && la > prep.la {
                let suffix = self.internal_storage.get_suffix(prep.la);
                (None, suffix)
            } else {
                (None, vec![])
            };
            let promise = Promise {
                n: prep.n,
                n_accepted: na,
                decided_snapshot,
                suffix,
                ld: decided_idx,
                la,
                stopsign: self.get_stopsign(),
            };
            self.outgoing.push(PaxosMessage {
                from: self.pid,
                to: from,
                msg: PaxosMsg::Promise(promise),
            });
        }
    }

    fn handle_acceptsync(&mut self, accsync: AcceptSync<T, S>, from: u64) {
        if self.internal_storage.get_promise() == accsync.n
            && self.state == (Role::Follower, Phase::Prepare)
        {
            let accepted = match accsync.decided_snapshot {
                Some(s) => {
                    match s {
                        SnapshotType::Complete(c) => {
                            self.internal_storage.set_snapshot(accsync.decided_idx, c);
                        }
                        SnapshotType::Delta(d) => {
                            self.internal_storage.merge_snapshot(accsync.decided_idx, d);
                        }
                        _ => unimplemented!(),
                    }
                    let la = self.internal_storage.append_entries(accsync.suffix);
                    Accepted { n: accsync.n, la }
                }
                None => {
                    // no snapshot, only suffix
                    let la = self
                        .internal_storage
                        .append_on_prefix(accsync.sync_idx, accsync.suffix);
                    Accepted { n: accsync.n, la }
                }
            };
            self.internal_storage.set_accepted_round(accsync.n);
            self.internal_storage.set_decided_idx(accsync.decided_idx);
            self.state = (Role::Follower, Phase::Accept);
            let cached_idx = self.outgoing.len();
            self.latest_accepted_meta = Some((accsync.n, cached_idx));
            self.outgoing.push(PaxosMessage {
                from: self.pid,
                to: from,
                msg: PaxosMsg::Accepted(accepted),
            });
            match accsync.stopsign {
                Some(ss) => {
                    if let Some(ss_entry) = self.internal_storage.get_stopsign() {
                        let StopSignEntry {
                            decided: has_decided,
                            stopsign: _my_ss,
                        } = ss_entry;
                        if !has_decided {
                            self.accept_stopsign(ss);
                        }
                    } else {
                        self.accept_stopsign(ss);
                    }
                    let a = AcceptedStopSign { n: accsync.n };
                    self.outgoing.push(PaxosMessage {
                        from: self.pid,
                        to: from,
                        msg: PaxosMsg::AcceptedStopSign(a),
                    });
                }
                None => self.forward_pending_proposals(),
            }
            self.internal_storage.set_decided_idx(accsync.decided_idx);
        }
    }

    fn forward_pending_proposals(&mut self) {
        let proposals = std::mem::take(&mut self.pending_proposals);
        if !proposals.is_empty() {
            self.forward_proposals(proposals);
        }
    }

    fn handle_firstaccept(&mut self, f: FirstAccept) {
        #[cfg(feature = "logging")]
        debug!(self.logger, "Incoming message First Accept");
        if self.internal_storage.get_promise() == f.n
            && self.state == (Role::Follower, Phase::FirstAccept)
        {
            self.internal_storage.set_accepted_round(f.n);
            self.state.1 = Phase::Accept;
            self.forward_pending_proposals();
        }
    }

    fn handle_acceptdecide(&mut self, acc: AcceptDecide<T>) {
        if self.internal_storage.get_promise() == acc.n
            && self.state == (Role::Follower, Phase::Accept)
        {
            let entries = acc.entries;
            self.accept_entries(acc.n, entries);
            // handle decide
            if acc.ld > self.internal_storage.get_decided_idx() {
                self.internal_storage.set_decided_idx(acc.ld);
            }
        }
    }

    fn handle_accept_stopsign(&mut self, acc_ss: AcceptStopSign) {
        if self.internal_storage.get_promise() == acc_ss.n
            && self.state == (Role::Follower, Phase::Accept)
        {
            self.accept_stopsign(acc_ss.ss);
            let a = AcceptedStopSign { n: acc_ss.n };
            self.outgoing.push(PaxosMessage {
                from: self.pid,
                to: self.leader.pid,
                msg: PaxosMsg::AcceptedStopSign(a),
            });
        }
    }

    fn handle_decide(&mut self, dec: Decide) {
        if self.internal_storage.get_promise() == dec.n && self.state.1 == Phase::Accept {
            self.internal_storage.set_decided_idx(dec.ld);
        }
    }

    fn handle_decide_stopsign(&mut self, dec: DecideStopSign) {
        if self.internal_storage.get_promise() == dec.n && self.state.1 == Phase::Accept {
            let mut ss = self
                .internal_storage
                .get_stopsign()
                .expect("No stopsign found when deciding!");
            ss.decided = true;
            self.internal_storage.set_stopsign(ss); // need to set it again now with the modified decided flag
            self.internal_storage
                .set_decided_idx(self.internal_storage.get_log_len() + 1);
        }
    }

    fn accept_entries(&mut self, n: Ballot, entries: Vec<T>) {
        let la = self.internal_storage.append_entries(entries);
        match &self.latest_accepted_meta {
            Some((round, outgoing_idx)) if round == &n => {
                let PaxosMessage { msg, .. } = self.outgoing.get_mut(*outgoing_idx).unwrap();
                match msg {
                    PaxosMsg::Accepted(a) => a.la = la,
                    _ => panic!("Cached idx is not an Accepted Message<T>!"),
                }
            }
            _ => {
                let accepted = Accepted { n, la };
                let cached_idx = self.outgoing.len();
                self.latest_accepted_meta = Some((n, cached_idx));
                self.outgoing.push(PaxosMessage {
                    from: self.pid,
                    to: self.leader.pid,
                    msg: PaxosMsg::Accepted(accepted),
                });
            }
        }
    }

    fn use_snapshots() -> bool {
        S::use_snapshots()
    }
}

#[derive(PartialEq, Debug)]
enum Phase {
    Prepare,
    FirstAccept,
    Accept,
    Recover,
    None,
}

#[derive(PartialEq, Debug)]
enum Role {
    Follower,
    Leader,
}

/// An error returning the proposal that was failed due to that the current configuration is stopped.
#[allow(missing_docs)]
#[derive(Debug)]
pub enum ProposeErr<T>
where
    T: Entry,
{
    Normal(T),
    Reconfiguration(Vec<u64>), // TODO use a type for ProcessId
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

/// Configuration for `SequencePaxos`.
/// # Fields
/// * `configuration_id`: The identifier for the configuration that this Sequence Paxos replica is part of.
/// * `pid`: The unique identifier of this node. Must not be 0.
/// * `peers`: The peers of this node i.e. the `pid`s of the other replicas in the configuration.
/// * `buffer_size`: The buffer size for outgoing messages.
/// * `skip_prepare_use_leader`: The initial leader of the cluster. Could be used in combination with reconfiguration to skip the prepare phase in the new configuration.
/// * `logger`: Custom logger for logging events of Sequence Paxos.
/// * `logger_file_path`: The path where the default logger logs events.
#[derive(Clone, Debug)]
pub struct SequencePaxosConfig {
    configuration_id: u32,
    pid: NodeId,
    peers: Vec<u64>,
    buffer_size: usize,
    skip_prepare_use_leader: Option<Ballot>,
    logger_file_path: Option<String>,
    #[cfg(feature = "logging")]
    logger: Option<Logger>,
}

#[allow(missing_docs)]
impl SequencePaxosConfig {
    pub fn get_configuration_id(&self) -> u32 {
        self.configuration_id
    }

    pub fn set_configuration_id(&mut self, configuration_id: u32) {
        self.configuration_id = configuration_id;
    }

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

    pub fn set_buffer_size(&mut self, size: usize) {
        self.buffer_size = size;
    }

    pub fn get_buffer_size(&self) -> usize {
        self.buffer_size
    }

    pub fn set_skip_prepare_use_leader(&mut self, b: Ballot) {
        self.skip_prepare_use_leader = Some(b);
    }

    pub fn get_skip_prepare_use_leader(&self) -> Option<Ballot> {
        self.skip_prepare_use_leader
    }

    #[cfg(feature = "logging")]
    pub fn set_logger(&mut self, l: Logger) {
        self.logger = Some(l);
    }

    #[cfg(feature = "logging")]
    pub fn get_logger(&self) -> Option<&Logger> {
        self.logger.as_ref()
    }

    pub fn set_logger_file_path(&mut self, s: String) {
        self.logger_file_path = Some(s);
    }

    pub fn get_logger_file_path(&self) -> Option<&String> {
        self.logger_file_path.as_ref()
    }

    #[cfg(feature = "hocon_config")]
    pub fn with_hocon(h: &Hocon) -> Self {
        let mut config = Self::default();
        config
            .set_configuration_id(h[CONFIG_ID].as_i64().expect("Failed to load config ID") as u32);
        config.set_pid(h[PID].as_i64().expect("Failed to load PID") as u64);
        match &h[PEERS] {
            Hocon::Array(v) => {
                let peers = v
                    .iter()
                    .map(|x| x.as_i64().expect("Failed to load pid in Hocon array") as u64)
                    .collect();
                config.set_peers(peers);
            }
            _ => {
                unimplemented!("Peers in Hocon should be parsed as array!")
            }
        }
        if let Some(p) = h[LOG_FILE_PATH].as_string() {
            config.set_logger_file_path(p);
        }
        if let Some(b) = h[SP_BUFFER_SIZE].as_i64() {
            config.set_buffer_size(b as usize);
        }
        config
    }

    pub fn build<T, S, B>(self, storage: B) -> SequencePaxos<T, S, B>
    where
        T: Entry,
        S: Snapshot<T>,
        B: Storage<T, S>,
    {
        assert_ne!(self.pid, 0, "Pid cannot be 0");
        assert_ne!(self.configuration_id, 0, "Configuration id cannot be 0");
        assert!(!self.peers.is_empty(), "Peers cannot be empty");
        assert!(
            !self.peers.contains(&self.pid),
            "Peers should not include self pid"
        );
        assert!(self.buffer_size > 0, "Buffer size must be greater than 0");
        if let Some(x) = self.skip_prepare_use_leader {
            assert_ne!(x.pid, 0, "Initial leader cannot be 0")
        };
        SequencePaxos::with(self, storage)
    }
}

impl Default for SequencePaxosConfig {
    fn default() -> Self {
        Self {
            configuration_id: 0,
            pid: 0,
            peers: vec![],
            buffer_size: BUFFER_SIZE,
            skip_prepare_use_leader: None,
            logger_file_path: None,
            #[cfg(feature = "logging")]
            logger: None,
        }
    }
}

/// Used for proposing reconfiguration of the cluster.
#[derive(Debug, Clone)]
pub struct ReconfigurationRequest {
    /// The id of the servers in the new configuration.
    new_configuration: Vec<u64>,
    /// Optional metadata to be decided with the reconfiguration.
    metadata: Option<Vec<u8>>,
}

impl ReconfigurationRequest {
    /// create a `ReconfigurationRequest`.
    /// # Arguments
    /// * new_configuration: The pids of the nodes in the new configuration.
    /// * metadata: Some optional metadata in raw bytes. This could include some auxiliary data for the new configuration to start with.
    pub fn with(new_configuration: Vec<u64>, metadata: Option<Vec<u8>>) -> Self {
        Self {
            new_configuration,
            metadata,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SequencePaxosConfig;
    use hocon::HoconLoader;

    #[test]
    fn hocon_conf_test() {
        let raw_cfg = HoconLoader::new()
            .load_file("tests/config/node2.conf")
            .expect("Failed to load hocon file")
            .hocon()
            .unwrap();

        let _ = SequencePaxosConfig::with_hocon(&raw_cfg);
    }
}
