use crate::{
    leader_election::ballot_leader_election::Ballot,
    messages::*,
    storage::{Snapshot, SnapshotType, StopSign, StopSignEntry, Storage},
    util::{LeaderState, PromiseMetaData, SyncItem},
    utils::{
        hocon_kv::{CONFIG_ID, LOG_FILE_PATH, PID},
        logger::create_logger,
    },
};
use hocon::Hocon;
use slog::{debug, info, trace, warn, Logger};
use std::{fmt::Debug, marker::PhantomData};

const BUFFER_SIZE: usize = 100000;

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
    T: Clone + Debug,
{
    Normal(T),
    Reconfiguration(Vec<u64>), // TODO use a type for ProcessId
}

/// An Omni-Paxos replica. Maintains local state of the replicated log, handles incoming messages and produces outgoing messages that the user has to fetch periodically and send using a network implementation.
/// User also has to periodically fetch the decided entries that are guaranteed to be strongly consistent and linearizable, and therefore also safe to be used in the higher level application.
pub struct OmniPaxos<T, S, B>
where
    T: Clone + Debug,
    S: Snapshot<T>,
    B: Storage<T, S>,
{
    storage: B,
    config_id: u32,
    pid: u64,
    peers: Vec<u64>, // excluding self pid
    state: (Role, Phase),
    leader: u64,
    pending_proposals: Vec<T>,
    pending_stopsign: Option<StopSign>,
    prev_ld: u64,
    outgoing: Vec<Message<T, S>>,
    /// Logger used to output the status of the component.
    logger: Logger,
    cached_trim_index: u64,
    leader_state: LeaderState<T, S>,
    latest_accepted_meta: Option<(Ballot, usize)>,
    s: PhantomData<S>,
}

impl<T, S, B> OmniPaxos<T, S, B>
where
    T: Clone + Debug,
    S: Snapshot<T>,
    B: Storage<T, S>,
{
    /*** User functions ***/
    /// Creates an Omni-Paxos replica.
    /// # Arguments
    /// * `config_id` - The identifier for the configuration that this Omni-Paxos replica is part of.
    /// * `pid` - The identifier of this Omni-Paxos replica.
    /// * `peers` - The `pid`s of the other replicas in the configuration.
    /// * `skip_prepare_use_leader` - Initial leader of the cluster. Could be used in combination with reconfiguration to skip the prepare phase in the new configuration.
    /// * `logger` - Used for logging events of OmniPaxos.
    /// * `log_file_path` - Path where the default logger logs events.
    pub fn with(
        config_id: u32,
        pid: u64,
        peers: Vec<u64>,
        storage: B,
        skip_prepare_use_leader: Option<Ballot>, // skipped prepare phase with the following leader event
        logger: Option<Logger>,
        log_file_path: Option<&str>,
    ) -> OmniPaxos<T, S, B> {
        let num_nodes = &peers.len() + 1;
        let majority = num_nodes / 2 + 1;
        let max_peer_pid = peers.iter().max().unwrap();
        let max_pid = *std::cmp::max(max_peer_pid, &pid) as usize;
        let (state, leader, n_leader, lds) = match skip_prepare_use_leader {
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
                (state, l.pid, l, lds)
            }
            None => {
                let state = (Role::Follower, Phase::None);
                let lds = None;
                (state, 0, Ballot::default(), lds)
            }
        };

        let l = logger.unwrap_or_else(|| {
            if let Some(p) = log_file_path {
                create_logger(p)
            } else {
                let t = format!("logs/paxos_{}.log", pid);
                create_logger(log_file_path.unwrap_or_else(|| t.as_str()))
            }
        });

        info!(l, "Paxos component pid: {} created!", pid);

        let mut paxos = OmniPaxos {
            storage,
            pid,
            config_id,
            peers,
            state,
            pending_proposals: vec![],
            pending_stopsign: None,
            leader,
            prev_ld: 0,
            outgoing: Vec::with_capacity(BUFFER_SIZE),
            logger: l,
            cached_trim_index: 0,
            leader_state: LeaderState::with(n_leader, lds, max_pid, majority),
            latest_accepted_meta: None,
            s: PhantomData,
        };
        paxos.storage.set_promise(n_leader);
        paxos
    }

    /// Creates an Omni-Paxos replica.
    /// # Arguments
    /// * `cfg` - Hocon configuration used for paxos replica.
    /// * `peers` - The `pid`s of the other replicas in the configuration.
    /// * `storage` - Implementation of a storage used to store the messages.
    /// * `skip_prepare_use_leader` - Initial leader of the cluster. Could be used in combination with reconfiguration to skip the prepare phase in the new configuration.
    /// * `logger` - Used for logging events of OmniPaxos.
    pub fn with_hocon(
        self,
        cfg: &Hocon,
        peers: Vec<u64>,
        storage: B,
        skip_prepare_use_leader: Option<Ballot>,
        logger: Option<Logger>,
    ) -> OmniPaxos<T, S, B> {
        OmniPaxos::<T, S, B>::with(
            cfg[CONFIG_ID].as_i64().expect("Failed to load config ID") as u32,
            cfg[PID].as_i64().expect("Failed to load PID") as u64,
            peers,
            storage,
            skip_prepare_use_leader,
            logger,
            Option::from(
                cfg[LOG_FILE_PATH]
                    .as_string()
                    .expect("Failed to load log file path")
                    .as_str(),
            ),
        )
    }

    /// Initiates the trim process.
    /// # Arguments
    /// * `trim_index` - Deletes all entries up to [`trim_index`], if the [`trim_index`] is None then the minimum index accepted by **ALL** servers will be used as the [`trim_index`].
    pub fn trim(&mut self, trim_index: Option<u64>) {
        match self.state {
            (Role::Leader, _) => self.trim_prepare(trim_index),
            _ => self.forward_compaction(Compaction::Trim(trim_index)),
        }
    }

    pub fn snapshot(&mut self, trim_idx: Option<u64>, local_only: bool) {
        let decided_idx = self.storage.get_decided_len();
        let trim_idx = match trim_idx {
            Some(idx) => {
                if idx < decided_idx {
                    idx
                } else {
                    todo!("Return Snapshot Error")
                }
            }
            None => decided_idx,
        };
        let snapshot = self.create_snapshot(trim_idx);
        self.set_snapshot(trim_idx, snapshot);
        if !local_only {
            // since it is decided, it is ok even for a follower to send this
            for pid in &self.peers {
                let msg = PaxosMsg::Compaction(Compaction::Snapshot(trim_idx));
                self.outgoing.push(Message::with(self.pid, *pid, msg));
            }
        }
    }

    /// Return trim index from storage.
    pub fn get_trimmed_idx(&self) -> u64 {
        self.storage.get_trimmed_idx()
    }

    /// Recover from failure. Goes into recover state and sends `PrepareReq` to all peers.
    pub fn fail_recovery(&mut self) {
        self.state = (Role::Follower, Phase::Recover);
        for pid in &self.peers {
            let m = Message::with(self.pid, *pid, PaxosMsg::PrepareReq);
            self.outgoing.push(m);
        }
    }

    fn trim_prepare(&mut self, index: Option<u64>) {
        let min_all_accepted_idx = self.leader_state.get_min_all_accepted_idx();
        let trim_idx = match index {
            Some(idx) => {
                if (min_all_accepted_idx < &idx) || (idx < self.cached_trim_index) {
                    warn!(
                        self.logger,
                        "Invalid trim index: {:?}, cached_index: {}, las: {:?}",
                        index,
                        self.cached_trim_index,
                        self.leader_state.las
                    );
                    return;
                }
                idx
            }
            None => {
                trace!(
                    self.logger,
                    "No trim index provided, using min_las_index: {:?}",
                    min_all_accepted_idx
                );
                *min_all_accepted_idx
            }
        };
        for pid in &self.peers {
            let msg = PaxosMsg::Compaction(Compaction::Trim(Some(trim_idx)));
            self.outgoing.push(Message::with(self.pid, *pid, msg));
        }
        self.handle_compaction(Compaction::Trim(Some(trim_idx)));
    }

    fn handle_compaction(&mut self, c: Compaction) {
        let decided_idx = self.storage.get_decided_len();
        match c {
            Compaction::Trim(Some(trim_idx)) if trim_idx <= decided_idx => {
                trace!(self.logger, "trim index: {:?}", trim_idx);
                self.storage.trim(trim_idx - self.cached_trim_index);
                self.storage.set_trimmed_idx(trim_idx);
                self.cached_trim_index = trim_idx;
            }
            Compaction::Snapshot(trim_idx) if trim_idx <= decided_idx => {
                let s = self.create_snapshot(trim_idx);
                self.set_snapshot(trim_idx, s);
            }
            _ => {
                warn!(
                    self.logger,
                    "Received invalid Compaction: {:?}, decided_idx {}", c, decided_idx
                );
            }
        }
    }

    /// Returns the id of the current leader.
    pub fn get_current_leader(&self) -> u64 {
        self.leader
    }

    /// Returns the outgoing messages from this replica. The messages should then be sent via the network implementation.
    pub fn get_outgoing_msgs(&mut self) -> Vec<Message<T, S>> {
        let mut outgoing = Vec::with_capacity(BUFFER_SIZE);
        std::mem::swap(&mut self.outgoing, &mut outgoing);
        #[cfg(feature = "batch_accept")]
        {
            self.leader_state.reset_batch_accept_meta();
        }
        #[cfg(feature = "latest_decide")]
        {
            self.leader_state.reset_latest_decided_meta();
        }
        #[cfg(feature = "latest_accepted")]
        {
            self.latest_accepted_meta = None;
        }
        outgoing
    }

    /// Returns the decided entries since the last call of this function.
    pub fn get_latest_decided_entries(&mut self) -> &[T] {
        let ld = self.storage.get_decided_len();
        if self.prev_ld < ld {
            let decided = self.storage.get_entries(
                self.prev_ld - self.cached_trim_index,
                ld - self.cached_trim_index,
            );
            self.prev_ld = ld;
            decided
        } else {
            &[]
        }
    }

    /// Returns the entire decided entries of this replica.
    pub fn get_decided_entries(&self) -> &[T] {
        self.storage
            .get_entries(0, self.storage.get_decided_len() - self.cached_trim_index)
    }

    /// Handle an incoming message.
    pub fn handle(&mut self, m: Message<T, S>) {
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
            PaxosMsg::ForwardCompaction(c) => self.handle_forwarded_compaction(c),
            PaxosMsg::AcceptStopSign(acc_ss) => self.handle_accept_stopsign(acc_ss),
            PaxosMsg::AcceptedStopSign(acc_ss) => self.handle_accepted_stopsign(acc_ss, m.from),
            PaxosMsg::DecideStopSign(d_ss) => self.handle_decide_stopsign(d_ss),
        }
    }

    /// Returns whether this Omni-Paxos instance is stopped, i.e. if it has been reconfigured.
    pub fn stopped(&self) -> bool {
        self.get_stopsign().is_some()
    }

    /// Propose a normal entry to be replicated.
    pub fn append(&mut self, entry: T) -> Result<(), ProposeErr<T>> {
        if self.stopped() {
            Err(ProposeErr::Normal(entry))
        } else {
            self.propose_entry(entry);
            Ok(())
        }
    }

    /// Propose a reconfiguration. Returns error if already stopped or new configuration is empty.
    /// # Arguments
    /// * `new_configuration` - A vec with the ids of replicas in the new configuration.
    /// * `prio_start_round` - The initial round to be used by the pre-defined leader in the new configuration (if such exists).
    pub fn propose_reconfiguration(
        &mut self,
        new_configuration: Vec<u64>,
        metadata: Option<Vec<u8>>,
    ) -> Result<(), ProposeErr<T>> {
        info!(
            self.logger,
            "Propose reconfiguration {:?}", new_configuration
        );
        if self.stopped() {
            Err(ProposeErr::Reconfiguration(new_configuration))
        } else {
            let ss = StopSign::with(self.config_id + 1, new_configuration, metadata);
            match self.state {
                (Role::Leader, Phase::Prepare) => {
                    if self.pending_stopsign.is_none() {
                        self.pending_stopsign = Some(ss);
                        Ok(())
                    } else {
                        todo!("Return ProposeErr with pending reconfig");
                    }
                }
                (Role::Leader, Phase::Accept) => {
                    if !self.stopped() {
                        self.storage
                            .set_stopsign(StopSignEntry::with(ss.clone(), false));
                        self.leader_state.set_accepted_stopsign(self.pid);
                        self.send_accept_stopsign(ss);
                        Ok(())
                    } else {
                        todo!("Return ProposeErr with pending reconfig");
                    }
                }
                (Role::Leader, Phase::FirstAccept) => todo!("special first accept stopsign? -.-"),
                _ => todo!("forward stopsign"),
            }
        }
    }

    fn send_accept_stopsign(&mut self, ss: StopSign) {
        let acc_ss =
            PaxosMsg::AcceptStopSign(AcceptStopSign::with(self.leader_state.n_leader, ss.clone()));
        for pid in self.leader_state.get_promised_followers() {
            self.outgoing
                .push(Message::with(self.pid, pid, acc_ss.clone()));
        }
    }

    /// Returns chosen entries between the given indices. If no chosen entries in the given interval, an empty vec is returned.
    pub fn get_chosen_entries(&self, from_idx: u64, to_idx: u64) -> Vec<T> {
        let ld = self.storage.get_decided_len();
        let max_idx = std::cmp::max(ld, self.leader_state.get_chosen_idx());
        if to_idx > max_idx {
            vec![]
        } else {
            self.storage
                .get_entries(
                    from_idx - self.cached_trim_index,
                    to_idx - self.cached_trim_index,
                )
                .to_vec()
        }
    }

    /// Returns the currently promised round.
    pub fn get_promise(&self) -> Ballot {
        self.storage.get_promise()
    }

    /// Stops this Paxos to write any new entries to the log and returns the final log.
    /// This should only be called **after a reconfiguration has been decided.**
    // TODO with new reconfiguration
    // pub fn stop_and_get_log(&mut self) -> Arc<L> {
    //     self.storage.stop_and_get_log()
    // }

    /// Handles re-establishing a connection to a previously disconnected peer.
    /// This should only be called if the underlying network implementation indicates that a connection has been re-established.
    pub fn reconnected(&mut self, pid: u64) {
        if pid == self.pid {
            return;
        } else if pid == self.leader {
            self.state = (Role::Follower, Phase::Recover);
        }
        self.outgoing
            .push(Message::with(self.pid, pid, PaxosMsg::PrepareReq));
    }

    fn propose_entry(&mut self, entry: T) {
        match self.state {
            (Role::Leader, Phase::Prepare) => self.pending_proposals.push(entry),
            (Role::Leader, Phase::Accept) => self.send_accept(entry),
            (Role::Leader, Phase::FirstAccept) => self.send_first_accept(entry),
            _ => self.forward_proposals(vec![entry]),
        }
    }

    fn get_stopsign(&self) -> Option<StopSign> {
        self.storage.get_stopsign().map(|x| x.stopsign)
    }

    /// Handle a new leader. Should be called when the leader election has elected a new leader with the ballot `n`
    /*** Leader ***/
    pub fn handle_leader(&mut self, n: Ballot) {
        debug!(self.logger, "Newly elected leader: {:?}", n);
        let leader_pid = n.pid;
        if n <= self.leader_state.n_leader || n <= self.storage.get_promise() {
            return;
        }
        if self.stopped() {
            self.pending_proposals.clear();
        }
        if self.pid == leader_pid {
            self.leader_state = LeaderState::with(
                n,
                None,
                self.leader_state.max_pid,
                self.leader_state.majority,
            );
            self.leader = leader_pid;
            self.storage.set_promise(n);
            /* insert my promise */
            let na = self.storage.get_accepted_round();
            let ld = self.storage.get_decided_len();
            let la = self.storage.get_log_len();
            let my_promise = Promise::with(n, na, None, ld, la, self.get_stopsign());
            self.leader_state.set_promise(my_promise, self.pid);
            /* initialise longest chosen sequence and update state */
            self.state = (Role::Leader, Phase::Prepare);
            let prep = Prepare::with(n, ld, self.storage.get_accepted_round(), la);
            /* send prepare */
            for pid in &self.peers {
                self.outgoing
                    .push(Message::with(self.pid, *pid, PaxosMsg::Prepare(prep)));
            }
        } else {
            self.state.0 = Role::Follower;
        }
    }

    fn handle_preparereq(&mut self, from: u64) {
        debug!(self.logger, "Incoming message PrepareReq from {}", from);
        if self.state.0 == Role::Leader {
            self.leader_state.set_decided_idx(from, None);
            #[cfg(feature = "batch_accept")]
            {
                self.leader_state.set_batch_accept_meta(from, None);
            }
            #[cfg(feature = "latest_decide")]
            {
                self.leader_state.set_latest_decide_meta(from, None);
            }
            let ld = self.storage.get_decided_len();
            let n_accepted = self.storage.get_accepted_round();
            let la = self.storage.get_log_len();
            let prep = Prepare::with(self.leader_state.n_leader, ld, n_accepted, la);
            self.outgoing
                .push(Message::with(self.pid, from, PaxosMsg::Prepare(prep)));
        }
    }

    fn forward_compaction(&mut self, c: Compaction) {
        if self.leader > 0 && self.leader != self.pid {
            trace!(
                self.logger,
                "Forwarding Compaction request to Leader {}, {:?}",
                self.leader,
                c
            );
            let fc = PaxosMsg::ForwardCompaction(c);
            let msg = Message::with(self.pid, self.leader, fc);
            self.outgoing.push(msg);
        }
    }

    fn forward_proposals(&mut self, mut entries: Vec<T>) {
        if self.leader > 0 && self.leader != self.pid {
            trace!(self.logger, "Forwarding proposal to Leader {}", self.leader);
            let pf = PaxosMsg::ProposalForward(entries);
            let msg = Message::with(self.pid, self.leader, pf);
            self.outgoing.push(msg);
        } else {
            self.pending_proposals.append(&mut entries);
        }
    }

    fn handle_forwarded_compaction(&mut self, c: Compaction) {
        trace!(
            self.logger,
            "Incoming Forwarded Compaction Request: {:?}",
            c
        );
        match self.state {
            (Role::Leader, _) => {
                if let Compaction::Trim(idx) = c {
                    self.trim_prepare(idx);
                } else {
                    warn!(self.logger, "Got unexpected forwarded {:?}", c);
                }
            }
            _ => self.forward_compaction(c),
        }
    }

    fn handle_forwarded_proposal(&mut self, mut entries: Vec<T>) {
        trace!(self.logger, "Incoming Forwarded Proposal");
        if !self.stopped() {
            match self.state {
                (Role::Leader, Phase::Prepare) => self.pending_proposals.append(&mut entries),
                (Role::Leader, Phase::Accept) => self.send_batch_accept(entries),
                (Role::Leader, Phase::FirstAccept) => {
                    let rest = entries.split_off(1);
                    self.send_first_accept(entries.pop().unwrap());
                    self.send_batch_accept(rest);
                }
                _ => self.forward_proposals(entries),
            }
        }
    }

    fn send_first_accept(&mut self, entry: T) {
        let f = FirstAccept::with(self.leader_state.n_leader, vec![entry.clone()]);
        for pid in self.leader_state.get_promised_followers() {
            self.outgoing.push(Message::with(
                self.pid,
                pid,
                PaxosMsg::FirstAccept(f.clone()),
            ));
        }
        let la = self.storage.append_entry(entry);
        self.leader_state.set_accepted_idx(self.pid, la);
        self.state.1 = Phase::Accept;
    }

    fn send_accept(&mut self, entry: T) {
        for pid in self.leader_state.get_promised_followers() {
            if cfg!(feature = "batch_accept") {
                match self.leader_state.get_batch_accept_meta(pid) {
                    Some((n, outgoing_idx)) if n == self.leader_state.n_leader => {
                        let Message { msg, .. } = self.outgoing.get_mut(outgoing_idx).unwrap();
                        match msg {
                            PaxosMsg::AcceptDecide(a) => a.entries.push(entry.clone()),
                            PaxosMsg::FirstAccept(f) => f.entries.push(entry.clone()),
                            _ => panic!("Not Accept or AcceptSync when batching"),
                        }
                    }
                    _ => {
                        let acc = AcceptDecide::with(
                            self.leader_state.n_leader,
                            self.leader_state.get_chosen_idx(),
                            vec![entry.clone()],
                        );
                        let cache_idx = self.outgoing.len();
                        self.outgoing.push(Message::with(
                            self.pid,
                            pid,
                            PaxosMsg::AcceptDecide(acc),
                        ));
                        self.leader_state
                            .set_batch_accept_meta(pid, Some(cache_idx));
                        #[cfg(feature = "latest_decide")]
                        {
                            self.leader_state
                                .set_latest_decide_meta(pid, Some(cache_idx));
                        }
                    }
                }
            } else {
                let acc = AcceptDecide::with(
                    self.leader_state.n_leader,
                    self.leader_state.get_chosen_idx(),
                    vec![entry.clone()],
                );
                self.outgoing
                    .push(Message::with(self.pid, pid, PaxosMsg::AcceptDecide(acc)));
            }
        }
        let la = self.storage.append_entry(entry);
        self.leader_state.set_accepted_idx(self.pid, la);
    }

    fn send_batch_accept(&mut self, entries: Vec<T>) {
        for pid in self.leader_state.get_promised_followers() {
            if cfg!(feature = "batch_accept") {
                match self.leader_state.get_batch_accept_meta(pid) {
                    Some((n, outgoing_idx)) if n == self.leader_state.n_leader => {
                        let Message { msg, .. } = self.outgoing.get_mut(outgoing_idx).unwrap();
                        match msg {
                            PaxosMsg::AcceptDecide(a) => a.entries.append(entries.clone().as_mut()),
                            PaxosMsg::FirstAccept(f) => f.entries.append(entries.clone().as_mut()),
                            _ => panic!("Not Accept or AcceptSync when batching"),
                        }
                    }
                    _ => {
                        let acc = AcceptDecide::with(
                            self.leader_state.n_leader,
                            self.leader_state.get_chosen_idx(),
                            entries.clone(),
                        );
                        let cache_idx = self.outgoing.len();
                        self.outgoing.push(Message::with(
                            self.pid,
                            pid,
                            PaxosMsg::AcceptDecide(acc),
                        ));
                        self.leader_state
                            .set_batch_accept_meta(pid, Some(cache_idx));
                        #[cfg(feature = "latest_decide")]
                        {
                            self.leader_state
                                .set_latest_decide_meta(pid, Some(cache_idx));
                        }
                    }
                }
            } else {
                let acc = AcceptDecide::with(
                    self.leader_state.n_leader,
                    self.leader_state.get_chosen_idx(),
                    entries.clone(),
                );
                self.outgoing
                    .push(Message::with(self.pid, pid, PaxosMsg::AcceptDecide(acc)));
            }
        }
        let la = self.storage.append_entries(entries);
        self.leader_state.set_accepted_idx(self.pid, la);
    }

    fn create_pending_proposals_snapshot(&mut self) -> (u64, S) {
        let pending_proposals = std::mem::take(&mut self.pending_proposals);
        let s = S::create(pending_proposals.as_slice());
        let trimmed_idx = self.storage.get_trimmed_idx() + pending_proposals.len() as u64;
        (trimmed_idx, s)
    }

    fn send_accsync_with_snapshot(&mut self) {
        let current_snapshot = self.storage.get_snapshot();
        let (trim_idx, snapshot) = match current_snapshot {
            Some(s) => (self.storage.get_trimmed_idx(), s),
            None => {
                let trim_idx = self.storage.get_log_len();
                let snapshot = self.create_snapshot(trim_idx);
                self.set_snapshot(trim_idx, snapshot.clone());
                (trim_idx, snapshot)
            }
        };
        let acc_sync = AcceptSync::with(
            self.leader_state.n_leader,
            SyncItem::Snapshot(SnapshotType::Complete(snapshot)),
            trim_idx,
            None,
            self.get_stopsign(),
        );
        for pid in self.leader_state.get_promised_followers() {
            let msg = Message::with(self.pid, pid, PaxosMsg::AcceptSync(acc_sync.clone()));
            self.outgoing.push(msg);
        }
    }

    fn send_accsync_with_entries(&mut self) {
        // create accept_sync with only new proposals for all pids with max_promise
        let PromiseMetaData {
            n: max_promise_n,
            la: max_la,
            ..
        } = &self.leader_state.get_max_promise_meta();
        for pid in self.leader_state.get_promised_followers() {
            let PromiseMetaData {
                n: promise_n,
                la: promise_la,
                pid,
                ..
            } = self.leader_state.get_promise_meta(pid);
            let (sfx, sync_idx) = if (promise_n == max_promise_n) && (promise_la < max_la) {
                let sfx = self
                    .storage
                    .get_suffix(*promise_la - self.cached_trim_index)
                    .to_vec();
                (sfx, *promise_la)
            } else {
                let ld = self
                    .leader_state
                    .get_decided_idx(*pid)
                    .expect("Received PromiseMetaData but not found in ld");
                let sfx = self
                    .storage
                    .get_suffix(ld - self.cached_trim_index)
                    .to_vec();
                (sfx, ld)
            };
            let acc_sync = AcceptSync::with(
                self.leader_state.n_leader,
                SyncItem::Entries(sfx),
                sync_idx,
                None,
                self.get_stopsign(),
            );
            let msg = Message::with(self.pid, *pid, PaxosMsg::AcceptSync(acc_sync));
            self.outgoing.push(msg);
        }
    }

    fn adopt_pending_stopsign(&mut self) {
        if let Some(ss) = self.pending_stopsign.take() {
            self.storage.set_stopsign(StopSignEntry::with(ss, false));
            self.leader_state.set_accepted_stopsign(self.pid);
        }
    }

    fn append_pending_proposals(&mut self) {
        if !self.pending_proposals.is_empty() {
            let new_entries = std::mem::take(&mut self.pending_proposals);
            // append new proposals in my sequence
            let la = self.storage.append_entries(new_entries);
            self.leader_state.set_accepted_idx(self.pid, la);
        }
    }

    fn set_snapshot(&mut self, trimmed_idx: u64, snapshot: S) {
        // TODO use and_then
        self.storage.set_snapshot(snapshot);
        self.storage.set_trimmed_idx(trimmed_idx);
        self.cached_trim_index = trimmed_idx;
    }

    fn merge_snapshot(&mut self, trimmed_idx: u64, delta: S) {
        let mut snapshot = self.storage.get_snapshot().unwrap();
        snapshot.merge(delta);
        self.set_snapshot(trimmed_idx, snapshot);
    }

    fn merge_pending_proposals_with_snapshot(&mut self) {
        if !self.pending_proposals.is_empty() {
            let (trimmed_idx, delta) = self.create_pending_proposals_snapshot();
            self.storage.set_accepted_round(self.leader_state.n_leader);
            self.merge_snapshot(trimmed_idx, delta);
        }
    }

    fn handle_majority_promises(&mut self) {
        self.state = (Role::Leader, Phase::Accept);
        let max_stopsign = self.leader_state.take_max_promise_stopsign();
        let max_promise = self.leader_state.take_max_promise();
        let max_promise_meta = self.leader_state.get_max_promise_meta();
        match max_promise {
            SyncItem::Entries(sfx) => {
                if max_promise_meta.n == self.storage.get_accepted_round() {
                    self.storage.append_entries(sfx);
                } else {
                    // TODO check all decided/trim index
                    let ld = self.storage.get_decided_len();
                    self.storage.append_on_prefix(ld, sfx);
                }
                if let Some(ss) = max_stopsign {
                    self.storage
                        .set_stopsign(StopSignEntry::with(ss.clone(), false));
                    self.leader_state.set_accepted_stopsign(self.pid);
                } else {
                    self.append_pending_proposals();
                    self.adopt_pending_stopsign();
                }
                self.send_accsync_with_entries();
            }
            SyncItem::Snapshot(s) => {
                match s {
                    SnapshotType::Complete(c) => {
                        // TODO chain together these calls using Result and and_then
                        self.storage
                            .set_trimmed_idx(self.leader_state.get_max_promise_meta().la);
                        self.storage.set_snapshot(c);
                    }
                    SnapshotType::Delta(d) => {
                        self.merge_snapshot(self.leader_state.get_max_promise_meta().la, d);
                    }
                    _ => unimplemented!(),
                }
                if let Some(ss) = max_stopsign {
                    self.storage
                        .set_stopsign(StopSignEntry::with(ss.clone(), false));
                    self.leader_state.set_accepted_stopsign(self.pid);
                } else {
                    self.merge_pending_proposals_with_snapshot();
                    self.adopt_pending_stopsign();
                }
                self.send_accsync_with_snapshot();
            }
            SyncItem::None => {
                // I am the most updated
                if S::snapshottable() {
                    self.merge_pending_proposals_with_snapshot();
                    self.adopt_pending_stopsign();
                    self.send_accsync_with_snapshot();
                } else {
                    self.append_pending_proposals();
                    self.adopt_pending_stopsign();
                    self.send_accsync_with_entries();
                }
            }
        }
    }

    fn handle_promise_prepare(&mut self, prom: Promise<T, S>, from: u64) {
        debug!(
            self.logger,
            "Handling promise from {} in Prepare phase", from
        );
        if prom.n == self.leader_state.n_leader {
            let received_majority = self.leader_state.set_promise(prom, from);
            if received_majority {
                self.handle_majority_promises();
            }
        }
    }

    fn handle_promise_accept(&mut self, prom: Promise<T, S>, from: u64) {
        let (r, p) = &self.state;
        debug!(
            self.logger,
            "Self role {:?}, phase {:?}. Incoming message Promise Accept from {}", r, p, from
        );
        if prom.n == self.leader_state.n_leader {
            self.leader_state.set_decided_idx(from, Some(prom.ld));
            let acc_sync = if S::snapshottable() {
                let (trimmed_idx, snapshot) = if prom.n_accepted
                    == self.leader_state.get_max_promise_meta().n
                    && prom.la < self.leader_state.get_max_promise_meta().la
                {
                    let trimmed_idx = self.storage.get_trimmed_idx() + self.storage.get_log_len(); // TODO use a wrapper around storage and implement these functions?
                    let entries = self.storage.get_suffix(prom.la);
                    let snapshot = SnapshotType::Delta(S::create(entries));
                    (trimmed_idx, snapshot)
                } else {
                    let trim_idx = self.storage.get_log_len();
                    let snapshot = self.create_snapshot(trim_idx);
                    (trim_idx, SnapshotType::Complete(snapshot))
                };
                AcceptSync::with(
                    self.leader_state.n_leader,
                    SyncItem::Snapshot(snapshot),
                    trimmed_idx,
                    None,
                    self.get_stopsign(),
                ) // TODO decided_idx with snapshot?
            } else {
                let sync_idx = if prom.n_accepted == self.leader_state.get_max_promise_meta().n
                    && prom.la < self.leader_state.get_max_promise_meta().la
                {
                    prom.la
                } else {
                    prom.ld
                };
                let sfx = self
                    .storage
                    .get_suffix(sync_idx - self.cached_trim_index)
                    .to_vec();
                // inform what got decided already
                let ld = if self.leader_state.get_chosen_idx() > 0 {
                    self.leader_state.get_chosen_idx()
                } else {
                    self.storage.get_decided_len()
                };
                AcceptSync::with(
                    self.leader_state.n_leader,
                    SyncItem::Entries(sfx),
                    sync_idx,
                    Some(ld),
                    self.get_stopsign(),
                )
            };
            let msg = Message::with(self.pid, from, PaxosMsg::AcceptSync(acc_sync));
            self.outgoing.push(msg);
        }
    }

    fn handle_accepted(&mut self, accepted: Accepted, from: u64) {
        trace!(
            self.logger,
            "Got Accepted from {}, idx: {}, chosen_idx: {}",
            from,
            accepted.la,
            self.leader_state.get_chosen_idx()
        );
        if accepted.n == self.leader_state.n_leader && self.state == (Role::Leader, Phase::Accept) {
            self.leader_state.set_accepted_idx(from, accepted.la);
            if accepted.la > self.leader_state.get_chosen_idx() {
                if self.leader_state.is_chosen(accepted.la) {
                    self.leader_state.set_chosen_idx(accepted.la);
                    let d = Decide::with(
                        self.leader_state.n_leader,
                        self.leader_state.get_chosen_idx(),
                    );
                    if cfg!(feature = "latest_decide") {
                        let promised_followers = self.leader_state.get_promised_followers();
                        for pid in promised_followers {
                            match self.leader_state.get_latest_decide_meta(pid) {
                                Some((n, outgoing_dec_idx)) if n == self.leader_state.n_leader => {
                                    let Message { msg, .. } =
                                        self.outgoing.get_mut(outgoing_dec_idx).unwrap();
                                    match msg {
                                        PaxosMsg::AcceptDecide(a) => {
                                            a.ld = self.leader_state.get_chosen_idx()
                                        }
                                        PaxosMsg::Decide(d) => {
                                            d.ld = self.leader_state.get_chosen_idx()
                                        }
                                        _ => {
                                            panic!("Cached Message<T> in outgoing was not Decide")
                                        }
                                    }
                                }
                                _ => {
                                    let cache_dec_idx = self.outgoing.len();
                                    self.leader_state
                                        .set_latest_decide_meta(pid, Some(cache_dec_idx));
                                    self.outgoing.push(Message::with(
                                        self.pid,
                                        pid,
                                        PaxosMsg::Decide(d),
                                    ));
                                }
                            }
                        }
                    } else {
                        for pid in self.leader_state.get_promised_followers() {
                            self.outgoing
                                .push(Message::with(self.pid, pid, PaxosMsg::Decide(d)));
                        }
                    }
                    self.handle_decide(d);
                }
            }
        }
    }

    fn handle_accepted_stopsign(&mut self, acc_stopsign: AcceptedStopSign, from: u64) {
        if acc_stopsign.n == self.leader_state.n_leader
            && self.state == (Role::Leader, Phase::Accept)
        {
            self.leader_state.set_accepted_stopsign(from);
            if self.leader_state.is_stopsign_chosen() {
                self.handle_decide_stopsign(DecideStopSign::with(self.leader_state.n_leader));
                for pid in self.leader_state.get_promised_followers() {
                    let d = DecideStopSign::with(self.leader_state.n_leader);
                    self.outgoing
                        .push(Message::with(self.pid, pid, PaxosMsg::DecideStopSign(d)));
                }
            }
        }
    }

    fn create_snapshot(&mut self, trim_idx: u64) -> S {
        let entries = self.storage.get_entries(0, trim_idx);
        let delta = S::create(entries);
        match self.storage.get_snapshot() {
            Some(mut s) => {
                s.merge(delta);
                s
            }
            None => delta,
        }
    }

    /*** Follower ***/
    fn handle_prepare(&mut self, prep: Prepare, from: u64) {
        if self.storage.get_promise() <= prep.n {
            self.leader = from;
            self.storage.set_promise(prep.n);
            self.state = (Role::Follower, Phase::Prepare);
            let na = self.storage.get_accepted_round();
            let la = self.storage.get_log_len();
            let promise = if S::snapshottable() {
                let (trimmed_idx, sync_item, stopsign) = if na > prep.n_accepted {
                    let trim_idx = self.storage.get_log_len();
                    let snapshot = self.create_snapshot(trim_idx);
                    (
                        trim_idx,
                        Some(SyncItem::Snapshot(SnapshotType::Complete(snapshot))),
                        self.get_stopsign(),
                    )
                } else if na == prep.n_accepted && la > prep.la {
                    let entries = self.storage.get_suffix(prep.la);
                    let snapshot = SnapshotType::Delta(S::create(entries));
                    let trimmed_idx = self.storage.get_trimmed_idx() + la;
                    (
                        trimmed_idx,
                        Some(SyncItem::Snapshot(snapshot)),
                        self.get_stopsign(),
                    )
                } else {
                    (la, None, None)
                };
                Promise::with(
                    prep.n,
                    na,
                    sync_item,
                    self.storage.get_decided_len(),
                    trimmed_idx,
                    stopsign,
                )
            } else {
                let (sync_item, stopsign) = if na > prep.n_accepted {
                    let entries = self
                        .storage
                        .get_suffix(prep.ld - self.cached_trim_index)
                        .to_vec();
                    (Some(SyncItem::Entries(entries)), self.get_stopsign())
                } else if na == prep.n_accepted && la > prep.la {
                    let entries = self
                        .storage
                        .get_suffix(prep.la - self.cached_trim_index)
                        .to_vec();
                    (Some(SyncItem::Entries(entries)), self.get_stopsign())
                } else {
                    (None, None)
                };
                Promise::with(
                    prep.n,
                    na,
                    sync_item,
                    self.storage.get_decided_len(),
                    la,
                    stopsign,
                )
            };
            self.outgoing
                .push(Message::with(self.pid, from, PaxosMsg::Promise(promise)));
        }
    }

    fn handle_acceptsync(&mut self, accsync: AcceptSync<T, S>, from: u64) {
        if self.storage.get_promise() == accsync.n && self.state == (Role::Follower, Phase::Prepare)
        {
            let accepted = match accsync.sync_item {
                SyncItem::Entries(e) => {
                    let la = self.storage.append_on_prefix(accsync.sync_idx, e);
                    Accepted::with(accsync.n, la)
                }
                SyncItem::Snapshot(s) => {
                    match s {
                        SnapshotType::Complete(c) => {
                            // TODO use and_then
                            self.storage.set_snapshot(c);
                            self.storage.set_trimmed_idx(accsync.sync_idx);
                            self.cached_trim_index = accsync.sync_idx;
                        }
                        SnapshotType::Delta(d) => {
                            self.merge_snapshot(accsync.sync_idx, d);
                        }
                        _ => unimplemented!(),
                    };
                    Accepted::with(accsync.n, accsync.sync_idx)
                }
                _ => unimplemented!(),
            };
            self.storage.set_accepted_round(accsync.n);
            self.state = (Role::Follower, Phase::Accept);
            #[cfg(feature = "latest_accepted")]
            {
                let cached_idx = self.outgoing.len();
                self.latest_accepted_meta = Some((accsync.n, cached_idx));
            }
            self.outgoing
                .push(Message::with(self.pid, from, PaxosMsg::Accepted(accepted)));

            if let Some(idx) = accsync.decide_idx {
                self.storage.set_decided_len(idx);
            }
            match accsync.stopsign {
                Some(ss) => {
                    if let Some(ss_entry) = self.storage.get_stopsign() {
                        let StopSignEntry {
                            decided: has_decided,
                            stopsign: _my_ss,
                        } = ss_entry;
                        if !has_decided {
                            self.storage.set_stopsign(StopSignEntry::with(ss, false));
                        } /*else {
                              assert_eq!(ss, my_ss);
                          }*/
                    } else {
                        self.storage.set_stopsign(StopSignEntry::with(ss, false));
                    }
                    let a = AcceptedStopSign::with(accsync.n);
                    self.outgoing.push(Message::with(
                        self.pid,
                        from,
                        PaxosMsg::AcceptedStopSign(a),
                    ));
                }
                None => self.forward_pending_proposals(),
            }
        }
    }

    fn forward_pending_proposals(&mut self) {
        let proposals = std::mem::take(&mut self.pending_proposals);
        if !proposals.is_empty() {
            self.forward_proposals(proposals);
        }
    }

    fn handle_firstaccept(&mut self, f: FirstAccept<T>) {
        debug!(self.logger, "Incoming message First Accept");
        if self.storage.get_promise() == f.n && self.state == (Role::Follower, Phase::FirstAccept) {
            let entries = f.entries;
            self.storage.set_accepted_round(f.n);
            self.accept_entries(f.n, entries);
            self.state.1 = Phase::Accept;
            self.forward_pending_proposals();
        }
    }

    fn handle_acceptdecide(&mut self, acc: AcceptDecide<T>) {
        if self.storage.get_promise() == acc.n && self.state == (Role::Follower, Phase::Accept) {
            let entries = acc.entries;
            self.accept_entries(acc.n, entries);
            // handle decide
            if acc.ld > self.storage.get_decided_len() {
                self.storage.set_decided_len(acc.ld);
            }
        }
    }

    fn handle_accept_stopsign(&mut self, acc_ss: AcceptStopSign) {
        if self.storage.get_promise() == acc_ss.n && self.state == (Role::Follower, Phase::Accept) {
            self.storage
                .set_stopsign(StopSignEntry::with(acc_ss.ss, false));
            let a = AcceptedStopSign::with(acc_ss.n);
            self.outgoing.push(Message::with(
                self.pid,
                self.leader,
                PaxosMsg::AcceptedStopSign(a),
            ));
        }
    }

    fn handle_decide(&mut self, dec: Decide) {
        if self.storage.get_promise() == dec.n && self.state.1 == Phase::Accept {
            self.storage.set_decided_len(dec.ld);
        }
    }

    fn handle_decide_stopsign(&mut self, dec: DecideStopSign) {
        if self.storage.get_promise() == dec.n && self.state.1 == Phase::Accept {
            let mut ss = self
                .storage
                .get_stopsign()
                .expect("No stopsign found when deciding!");
            ss.decided = true;
            self.storage.set_stopsign(ss)
        }
    }

    fn accept_entries(&mut self, n: Ballot, entries: Vec<T>) {
        let la = self.storage.append_entries(entries);
        if cfg!(feature = "latest_accepted") {
            match &self.latest_accepted_meta {
                Some((round, outgoing_idx)) if round == &n => {
                    let Message { msg, .. } = self.outgoing.get_mut(*outgoing_idx).unwrap();
                    match msg {
                        PaxosMsg::Accepted(a) => a.la = la,
                        _ => panic!("Cached idx is not an Accepted Message<T>!"),
                    }
                }
                _ => {
                    let accepted = Accepted::with(n, la);
                    let cached_idx = self.outgoing.len();
                    self.latest_accepted_meta = Some((n, cached_idx));
                    self.outgoing.push(Message::with(
                        self.pid,
                        self.leader,
                        PaxosMsg::Accepted(accepted),
                    ));
                }
            }
        } else {
            let accepted = Accepted::with(n, la);
            self.outgoing.push(Message::with(
                self.pid,
                self.leader,
                PaxosMsg::Accepted(accepted),
            ));
        }
    }
}
