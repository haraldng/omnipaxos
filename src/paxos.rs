use crate::{
    leader_election::ballot_leader_election::Ballot,
    messages::{AcceptSync, *},
    storage::{Entry, Snapshot, SnapshotType, StopSign, Storage},
    util::{PromiseMetaData, PromiseType},
    utils::{
        hocon_kv::{CONFIG_ID, LOG_FILE_PATH, PID},
        logger::create_logger,
    },
};
use hocon::Hocon;
use slog::{crit, debug, info, trace, warn, Logger};
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
    T: Clone,
{
    Normal(T),
    Reconfiguration(Vec<u64>), // TODO use a type for ProcessId
}

/// An Omni-Paxos replica. Maintains local state of the replicated log, handles incoming messages and produces outgoing messages that the user has to fetch periodically and send using a network implementation.
/// User also has to periodically fetch the decided entries that are guaranteed to be strongly consistent and linearizable, and therefore also safe to be used in the higher level application.
pub struct OmniPaxos<T, S, B>
where
    T: Clone,
    S: Snapshot<T>,
    B: Storage<T, S>,
{
    storage: B,
    config_id: u32,
    pid: u64,
    majority: usize,
    peers: Vec<u64>, // excluding self pid
    state: (Role, Phase),
    leader: u64,
    n_leader: Ballot,
    promises_meta: Vec<Option<PromiseMetaData>>,
    las: Vec<u64>,
    lds: Vec<Option<u64>>,
    pending_proposals: Vec<Entry<T>>,
    lc: u64, // length of longest chosen seq
    prev_ld: u64,
    max_promise_meta: PromiseMetaData,
    max_promise: PromiseType<T, S>,
    batch_accept_meta: Vec<Option<(Ballot, usize)>>, //  index in outgoing
    latest_decide_meta: Vec<Option<(Ballot, usize)>>,
    latest_accepted_meta: Option<(Ballot, usize)>,
    outgoing: Vec<Message<T, S>>,
    num_nodes: usize,
    /// Logger used to output the status of the component.
    logger: Logger,
    cached_gc_index: u64,
    s: PhantomData<S>,
}

impl<T, S, B> OmniPaxos<T, S, B>
where
    T: Clone,
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
        let max_pid = std::cmp::max(max_peer_pid, &pid);
        let num_nodes = *max_pid as usize;
        let (state, leader, n_leader, lds) = match skip_prepare_use_leader {
            Some(l) => {
                let (role, lds) = if l.pid == pid {
                    // we are leader in new config
                    let mut v = vec![None; num_nodes];
                    for peer in &peers {
                        // this works as a promise
                        let idx = *peer as usize - 1;
                        v[idx] = Some(0);
                    }
                    (Role::Leader, v)
                } else {
                    (Role::Follower, vec![None; num_nodes])
                };
                let state = (role, Phase::FirstAccept);
                (state, l.pid, l, lds)
            }
            None => {
                let state = (Role::Follower, Phase::None);
                let lds = vec![None; num_nodes];
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
            majority,
            peers,
            state,
            leader,
            n_leader,
            promises_meta: vec![None; num_nodes],
            las: vec![0; num_nodes],
            lds,
            pending_proposals: Vec::with_capacity(BUFFER_SIZE),
            lc: 0,
            prev_ld: 0,
            max_promise_meta: PromiseMetaData::with(Ballot::default(), 0, 0),
            max_promise: PromiseType::None,
            batch_accept_meta: vec![None; num_nodes],
            latest_decide_meta: vec![None; num_nodes],
            latest_accepted_meta: None,
            outgoing: Vec::with_capacity(BUFFER_SIZE),
            num_nodes,
            logger: l,
            cached_gc_index: 0,
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
        &self,
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

    /*
    /// Initiates the garbage collection process.
    /// # Arguments
    /// * `index` - Deletes all entries up to [`index`], if the [`index`] is None then the minimum index accepted by **ALL** servers will be used as the [`index`].
    pub fn garbage_collect(&mut self, index: Option<u64>) {
        match self.state {
            (Role::Leader, _) => self.gc_prepare(index),
            _ => self.forward_gc_request(index),
        }
    }
    */

    /// Return garbage collection index from storage.
    pub fn get_garbage_collected_idx(&self) -> u64 {
        self.storage.get_trim_idx()
    }

    /// Recover from failure. Goes into recover state and sends `PrepareReq` to all peers.
    pub fn fail_recovery(&mut self) {
        self.state = (Role::Follower, Phase::Recover);
        for pid in &self.peers {
            let m = Message::with(self.pid, *pid, PaxosMsg::PrepareReq);
            self.outgoing.push(m);
        }
    }

    fn gc_prepare(&mut self, index: Option<u64>) {
        let min_all_accepted_idx = self.las.iter().min().cloned();
        if min_all_accepted_idx.is_none() {
            return;
        }
        let gc_idx;
        match index {
            Some(idx) => {
                if (min_all_accepted_idx.unwrap() < idx) || (idx < self.cached_gc_index) {
                    warn!(
                        self.logger,
                        "Invalid garbage collector index: {:?}, cached_index: {}, min_las_index: {:?}",
                        index,
                        self.cached_gc_index,
                        min_all_accepted_idx
                    );
                    return;
                }
                gc_idx = idx;
            }
            None => {
                trace!(
                    self.logger,
                    "No garbage collector index provided, using min_las_index: {:?}",
                    min_all_accepted_idx
                );
                gc_idx = min_all_accepted_idx.unwrap();
            }
        }
        for pid in &self.peers {
            self.outgoing.push(Message::with(
                self.pid,
                *pid,
                PaxosMsg::GarbageCollect(gc_idx),
            ));
        }
        self.gc(gc_idx);
    }

    fn gc(&mut self, index: u64) {
        let decided_len = self.storage.get_decided_len();
        if decided_len < index {
            crit!(
                self.logger,
                "Received invalid garbage collection index! index: {:?}, prev_ld {}",
                index,
                decided_len
            );
            return;
        }
        trace!(self.logger, "Garbage Collection index: {:?}", index);
        self.storage.trim(index);
        self.cached_gc_index = index;
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
            self.batch_accept_meta = vec![None; self.num_nodes];
        }
        #[cfg(feature = "latest_decide")]
        {
            self.latest_decide_meta = vec![None; self.num_nodes];
        }
        #[cfg(feature = "latest_accepted")]
        {
            self.latest_accepted_meta = None;
        }
        outgoing
    }

    /// Returns the decided entries since the last call of this function.
    pub fn get_latest_decided_entries(&mut self) -> &[Entry<T>] {
        let ld = self.storage.get_decided_len();
        if self.prev_ld < ld {
            let decided = self.storage.get_entries(
                self.prev_ld - self.cached_gc_index,
                ld - self.cached_gc_index,
            );
            self.prev_ld = ld;
            decided
        } else {
            &[]
        }
    }

    /// Returns the entire decided entries of this replica.
    pub fn get_decided_entries(&self) -> &[Entry<T>] {
        self.storage
            .get_entries(0, self.storage.get_decided_len() - self.cached_gc_index)
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
            PaxosMsg::SnapshotPromise(snap_prom) => match &self.state {
                (Role::Leader, Phase::Prepare) => {
                    self.handle_snapshot_promise_prepare(snap_prom, m.from)
                }
                (Role::Leader, Phase::Accept) => {
                    self.handle_snapshot_promise_accept(snap_prom, m.from)
                }
                _ => {}
            },
            PaxosMsg::AcceptSync(acc_sync) => self.handle_acceptsync(acc_sync, m.from),
            PaxosMsg::SnapshotAcceptSync(snap_acc_sync) => {
                self.handle_snapshot_acceptsync(snap_acc_sync, m.from)
            }
            PaxosMsg::FirstAccept(f) => self.handle_firstaccept(f),
            PaxosMsg::AcceptDecide(acc) => self.handle_acceptdecide(acc),
            PaxosMsg::Accepted(accepted) => self.handle_accepted(accepted, m.from),
            PaxosMsg::Decide(d) => self.handle_decide(d),
            PaxosMsg::ProposalForward(proposals) => self.handle_forwarded_proposal(proposals),
            PaxosMsg::GarbageCollect(index) => self.gc(index),
            PaxosMsg::ForwardGarbageCollect(index) => self.handle_forwarded_gc_request(index),
        }
    }

    /// Returns whether this Omni-Paxos instance is stopped, i.e. if it has been reconfigured.
    pub fn stopped(&self) -> bool {
        self.storage.stopped().is_some()
    }

    /// Propose a normal entry to be replicated.
    pub fn append(&mut self, entry: T) -> Result<(), ProposeErr<T>> {
        if self.stopped() {
            Err(ProposeErr::Normal(entry))
        } else {
            let e = Entry::Normal(entry);
            self.propose_entry(e);
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
            let entry = Entry::StopSign(ss);
            self.propose_entry(entry);
            Ok(())
        }
    }

    /// Returns chosen entries between the given indices. If no chosen entries in the given interval, an empty vec is returned.
    pub fn get_chosen_entries(&self, from_idx: u64, to_idx: u64) -> Vec<Entry<T>> {
        let ld = self.storage.get_decided_len();
        let max_idx = std::cmp::max(ld, self.lc);
        if to_idx > max_idx {
            vec![]
        } else {
            self.storage
                .get_entries(
                    from_idx - self.cached_gc_index,
                    to_idx - self.cached_gc_index,
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

    fn propose_entry(&mut self, entry: Entry<T>) {
        match self.state {
            (Role::Leader, Phase::Prepare) => self.pending_proposals.push(entry),
            (Role::Leader, Phase::Accept) => self.send_accept(entry),
            (Role::Leader, Phase::FirstAccept) => self.send_first_accept(entry),
            _ => self.forward_proposals(vec![entry]),
        }
    }

    fn clear_peers_state(&mut self) {
        self.las = vec![0; self.num_nodes];
        self.promises_meta = vec![None; self.num_nodes];
        self.lds = vec![None; self.num_nodes];
    }

    fn get_idx_from_pid(pid: u64) -> usize {
        pid as usize - 1
    }

    /// Handle a new leader. Should be called when the leader election has elected a new leader with the ballot `n`
    /*** Leader ***/
    pub fn handle_leader(&mut self, n: Ballot) {
        debug!(self.logger, "Newly elected leader: {:?}", n);
        let leader_pid = n.pid;
        if n <= self.n_leader || n <= self.storage.get_promise() {
            return;
        }
        self.clear_peers_state();
        if self.stopped() {
            self.pending_proposals.clear();
        }
        if self.pid == leader_pid {
            self.n_leader = n;
            self.leader = leader_pid;
            self.storage.set_promise(n);
            /* insert my promise */
            let na = self.storage.get_accepted_round();
            let ld = self.storage.get_decided_len();
            let la = self.storage.get_log_len();
            let promise_meta = PromiseMetaData::with(na, la, self.pid);
            self.max_promise_meta = promise_meta;
            self.promises_meta[self.pid as usize - 1] = Some(promise_meta);
            self.max_promise = PromiseType::None;
            /* initialise longest chosen sequence and update state */
            self.lc = 0;
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
            let idx = Self::get_idx_from_pid(from);
            self.lds[idx] = None;
            #[cfg(feature = "batch_accept")]
            {
                self.batch_accept_meta[idx] = None;
            }
            #[cfg(feature = "latest_decide")]
            {
                self.latest_decide_meta[idx] = None;
            }
            let ld = self.storage.get_decided_len();
            let n_accepted = self.storage.get_accepted_round();
            let la = self.storage.get_log_len();
            let prep = Prepare::with(self.n_leader, ld, n_accepted, la);
            self.outgoing
                .push(Message::with(self.pid, from, PaxosMsg::Prepare(prep)));
        }
    }

    fn forward_gc_request(&mut self, index: Option<u64>) {
        if self.leader > 0 && self.leader != self.pid {
            trace!(
                self.logger,
                "Forwarding gc request to Leader {}, index {:?}",
                self.leader,
                index
            );
            let pf = PaxosMsg::ForwardGarbageCollect(index);
            let msg = Message::with(self.pid, self.leader, pf);
            self.outgoing.push(msg);
        }
    }

    fn forward_proposals(&mut self, mut entries: Vec<Entry<T>>) {
        if self.leader > 0 && self.leader != self.pid {
            trace!(self.logger, "Forwarding proposal to Leader {}", self.leader);
            let pf = PaxosMsg::ProposalForward(entries);
            let msg = Message::with(self.pid, self.leader, pf);
            self.outgoing.push(msg);
        } else {
            self.pending_proposals.append(&mut entries);
        }
    }

    fn handle_forwarded_gc_request(&mut self, index: Option<u64>) {
        trace!(
            self.logger,
            "Incoming Forwarded GC Request, index: {:?}",
            index
        );
        match self.state {
            (Role::Leader, _) => self.gc_prepare(index),
            _ => self.forward_gc_request(index),
        }
    }

    fn handle_forwarded_proposal(&mut self, mut entries: Vec<Entry<T>>) {
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

    fn send_first_accept(&mut self, entry: Entry<T>) {
        let promised_pids = self
            .lds
            .iter()
            .enumerate()
            .filter(|(_, x)| x.is_some())
            .map(|(idx, _)| idx as u64 + 1);
        let f = FirstAccept::with(self.n_leader, vec![entry.clone()]);
        for pid in promised_pids {
            self.outgoing.push(Message::with(
                self.pid,
                pid,
                PaxosMsg::FirstAccept(f.clone()),
            ));
        }
        let la = self.storage.append_entry(entry);
        self.las[self.pid as usize - 1] = la;
        self.state.1 = Phase::Accept;
    }

    fn send_accept(&mut self, entry: Entry<T>) {
        let promised_idx = self
            .lds
            .iter()
            .enumerate()
            .filter(|(_, x)| x.is_some())
            .map(|(idx, _)| idx);
        for idx in promised_idx {
            if cfg!(feature = "batch_accept") {
                match self.batch_accept_meta.get_mut(idx).unwrap() {
                    Some((n, outgoing_idx)) if n == &self.n_leader => {
                        let Message { msg, .. } = self.outgoing.get_mut(*outgoing_idx).unwrap();
                        match msg {
                            PaxosMsg::AcceptDecide(a) => a.entries.push(entry.clone()),
                            PaxosMsg::AcceptSync(acc) => acc.entries.push(entry.clone()),
                            PaxosMsg::FirstAccept(f) => f.entries.push(entry.clone()),
                            _ => panic!("Not Accept or AcceptSync when batching"),
                        }
                    }
                    _ => {
                        let acc = AcceptDecide::with(self.n_leader, self.lc, vec![entry.clone()]);
                        let cache_idx = self.outgoing.len();
                        let pid = idx as u64 + 1;
                        self.outgoing.push(Message::with(
                            self.pid,
                            pid,
                            PaxosMsg::AcceptDecide(acc),
                        ));
                        self.batch_accept_meta[idx] = Some((self.n_leader, cache_idx));
                        #[cfg(feature = "latest_decide")]
                        {
                            self.latest_decide_meta[idx] = Some((self.n_leader, cache_idx));
                        }
                    }
                }
            } else {
                let pid = idx as u64 + 1;
                let acc = AcceptDecide::with(self.n_leader, self.lc, vec![entry.clone()]);
                self.outgoing
                    .push(Message::with(self.pid, pid, PaxosMsg::AcceptDecide(acc)));
            }
        }
        let la = self.storage.append_entry(entry);
        self.las[self.pid as usize - 1] = la;
    }

    fn send_batch_accept(&mut self, mut entries: Vec<Entry<T>>) {
        let promised_idx = self
            .lds
            .iter()
            .enumerate()
            .filter(|(_, x)| x.is_some())
            .map(|(idx, _)| idx);
        for idx in promised_idx {
            if cfg!(feature = "batch_accept") {
                match self.batch_accept_meta.get_mut(idx).unwrap() {
                    Some((n, outgoing_idx)) if n == &self.n_leader => {
                        let Message { msg, .. } = self.outgoing.get_mut(*outgoing_idx).unwrap();
                        match msg {
                            PaxosMsg::AcceptDecide(a) => a.entries.append(entries.clone().as_mut()),
                            PaxosMsg::AcceptSync(acc) => {
                                acc.entries.append(entries.clone().as_mut())
                            }
                            PaxosMsg::FirstAccept(f) => f.entries.append(entries.clone().as_mut()),
                            _ => panic!("Not Accept or AcceptSync when batching"),
                        }
                    }
                    _ => {
                        let acc = AcceptDecide::with(self.n_leader, self.lc, entries.clone());
                        let cache_idx = self.outgoing.len();
                        let pid = idx as u64 + 1;
                        self.outgoing.push(Message::with(
                            self.pid,
                            pid,
                            PaxosMsg::AcceptDecide(acc),
                        ));
                        self.batch_accept_meta[idx] = Some((self.n_leader, cache_idx));
                        #[cfg(feature = "latest_decide")]
                        {
                            self.latest_decide_meta[idx] = Some((self.n_leader, cache_idx));
                        }
                    }
                }
            } else {
                let pid = idx as u64 + 1;
                let acc = AcceptDecide::with(self.n_leader, self.lc, entries.clone());
                self.outgoing
                    .push(Message::with(self.pid, pid, PaxosMsg::AcceptDecide(acc)));
            }
        }
        let la = self.storage.append_entries(&mut entries);
        self.las[self.pid as usize - 1] = la;
    }

    fn create_pending_proposals_snapshot(&mut self) -> (u64, S) {
        let pending_proposals = std::mem::take(&mut self.pending_proposals);
        let s = S::create(pending_proposals.as_slice());
        let trimmed_idx = self.storage.get_trim_idx() + pending_proposals.len() as u64;
        (trimmed_idx, s)
    }

    fn send_accsync_with_snapshot(&mut self) {
        let (trim_idx, snapshot) = self.storage.get_snapshot().unwrap();
        let acc_sync =
            SnapshotAcceptSync::with(self.n_leader, SnapshotType::Complete(snapshot), trim_idx);
        let promised_followers = self
            .promises_meta
            .iter()
            .filter_map(|p| p.map(|s| s.pid))
            .filter(|p| p != &self.pid);
        let mut acc_sync_msgs = promised_followers
            .map(|pid| {
                Message::with(
                    self.pid,
                    pid,
                    PaxosMsg::SnapshotAcceptSync(acc_sync.clone()),
                )
            })
            .collect();
        self.outgoing.append(&mut acc_sync_msgs);
    }

    fn send_accsync_with_entries(&mut self) {
        // create accept_sync with only new proposals for all pids with max_promise
        let PromiseMetaData {
            n: max_promise_n,
            la: max_la,
            ..
        } = &self.max_promise_meta;
        let leader_pid = self.pid;
        let promised_followers = self
            .promises_meta
            .iter()
            .filter_map(|p| p.as_ref())
            .filter(|p| p.pid != leader_pid);
        for PromiseMetaData {
            n: promise_n,
            la: promise_la,
            pid,
        } in promised_followers
        {
            let msg = if (promise_n == max_promise_n) && (promise_la < max_la) {
                let sfx = self.storage.get_suffix(*promise_la - self.cached_gc_index);
                let acc_sync = AcceptSync::with(self.n_leader, sfx, *promise_la, None);
                Message::with(self.pid, *pid, PaxosMsg::AcceptSync(acc_sync))
            } else {
                let idx = Self::get_idx_from_pid(*pid);
                let ld = self
                    .lds
                    .get(idx)
                    .expect("Received PromiseMetaData but not found in ld")
                    .unwrap();
                let sfx = self.storage.get_suffix(ld - self.cached_gc_index);
                let acc_sync = AcceptSync::with(self.n_leader, sfx, ld, None);
                Message::with(self.pid, *pid, PaxosMsg::AcceptSync(acc_sync))
            };
            self.outgoing.push(msg);
            #[cfg(feature = "batch_accept")]
            {
                let idx = Self::get_idx_from_pid(*pid);
                self.batch_accept_meta[idx] = Some((self.n_leader, self.outgoing.len() - 1));
            }
        }
    }

    fn append_pending_proposals(&mut self) {
        if !self.pending_proposals.is_empty() {
            let mut new_entries = std::mem::take(&mut self.pending_proposals);
            // append new proposals in my sequence
            let la = self.storage.append_entries(&mut new_entries);
            self.las[Self::get_idx_from_pid(self.pid)] = la;
        }
    }

    fn merge_pending_proposals_with_snapshot(&mut self) {
        if !self.pending_proposals.is_empty() {
            let (trimmed_idx, snapshot) = self.create_pending_proposals_snapshot();
            self.storage.set_accepted_round(self.n_leader);
            self.storage.merge_snapshot(trimmed_idx, snapshot);
        }
    }

    /// updates the received promises and return true if `promise_meta` is the maxiumum received promise.
    fn update_received_promises(&mut self, promise_meta: PromiseMetaData, ld: u64) -> bool {
        let idx = Self::get_idx_from_pid(promise_meta.pid);
        self.promises_meta[idx] = Some(promise_meta);
        self.lds[idx] = Some(ld);
        if promise_meta > self.max_promise_meta {
            self.max_promise_meta = promise_meta;
            true
        } else {
            false
        }
    }

    fn handle_majority_promises(&mut self) {
        self.state = (Role::Leader, Phase::Accept);
        let mut max_promise = PromiseType::None;
        std::mem::swap(&mut self.max_promise, &mut max_promise);
        match max_promise {
            PromiseType::Entries(mut sfx) => {
                if self.max_promise_meta.n == self.storage.get_accepted_round() {
                    self.storage.append_entries(&mut sfx);
                } else {
                    // TODO check all decided/trim index
                    let ld = self.storage.get_decided_len();
                    self.storage.append_on_prefix(ld, &mut sfx);
                }
                Self::drop_after_stopsign(&mut self.pending_proposals); // TODO check for StopSign in log?
                self.append_pending_proposals();
                self.send_accsync_with_entries();
            }
            PromiseType::Snapshot(s) => {
                match s {
                    SnapshotType::Complete(c) => {
                        self.storage.set_snapshot(self.max_promise_meta.la, c).expect("Failed to set snapshot");
                    }
                    SnapshotType::Delta(d) => {
                        self.storage.merge_snapshot(self.max_promise_meta.la, d);
                    }
                    _ => unimplemented!(),
                }
                self.merge_pending_proposals_with_snapshot();
                self.send_accsync_with_snapshot();
             }
            PromiseType::None => {
                // I am the most updated
                Self::drop_after_stopsign(&mut self.pending_proposals); // TODO check for StopSign in log?
                if S::snapshottable() {
                    self.merge_pending_proposals_with_snapshot();
                    self.send_accsync_with_snapshot();
                } else {
                    self.append_pending_proposals();
                    self.send_accsync_with_entries();
                }
            }
        }
    }

    fn handle_snapshot_promise_prepare(&mut self, prom: SnapshotPromise<T, S>, from: u64) {
        if prom.n == self.n_leader {
            let promise_meta = PromiseMetaData::with(prom.n_accepted, prom.trimmed_idx, from);
            let is_max_promise = self.update_received_promises(promise_meta, prom.ld);
            if is_max_promise {
                self.max_promise =
                    PromiseType::Snapshot(prom.snapshot.expect("No Snapshot in Promise"));
            }
            let num_promised = self.promises_meta.iter().filter(|x| x.is_some()).count();
            if num_promised >= self.majority {
                self.handle_majority_promises();
            }
        }
    }

    fn handle_promise_prepare(&mut self, prom: Promise<T>, from: u64) {
        debug!(
            self.logger,
            "Handling promise from {} in Prepare phase", from
        );
        if prom.n == self.n_leader {
            let promise_meta = PromiseMetaData::with(prom.n_accepted, prom.la, from);
            let is_max_promise = self.update_received_promises(promise_meta, prom.ld);
            if is_max_promise {
                self.max_promise = PromiseType::Entries(prom.sfx);
            }
            let num_promised = self.promises_meta.iter().filter(|x| x.is_some()).count();
            if num_promised >= self.majority {
                self.handle_majority_promises();
            }
        }
    }

    fn handle_snapshot_promise_accept(&mut self, prom: SnapshotPromise<T, S>, from: u64) {
        if prom.n == self.n_leader {
            let idx = Self::get_idx_from_pid(from);
            self.lds[idx] = Some(prom.ld);
            let (trimmed_idx, snapshot) = if prom.n_accepted == self.max_promise_meta.n
                && prom.trimmed_idx < self.max_promise_meta.la
            {
                let trimmed_idx = self.storage.get_trim_idx() + self.storage.get_log_len(); // TODO use a wrapper around storage and implement these functions?
                let snapshot = SnapshotType::Delta(self.create_delta_snapshot(prom.trimmed_idx));
                (trimmed_idx, snapshot)
            } else {
                let (trimmed_idx, snapshot) = self.create_complete_snapshot();
                (trimmed_idx, SnapshotType::Complete(snapshot))
            };
            let acc_sync = SnapshotAcceptSync::with(self.n_leader, snapshot, trimmed_idx);
            let msg = Message::with(self.pid, from, PaxosMsg::SnapshotAcceptSync(acc_sync));
            self.outgoing.push(msg);
        }
    }

    fn handle_promise_accept(&mut self, prom: Promise<T>, from: u64) {
        let (r, p) = &self.state;
        debug!(
            self.logger,
            "Self role {:?}, phase {:?}. Incoming message Promise Accept from {}", r, p, from
        );
        if prom.n == self.n_leader {
            let idx = Self::get_idx_from_pid(from);
            self.lds[idx] = Some(prom.ld);
            let acc_sync = {
                let sync_idx = if prom.n_accepted == self.max_promise_meta.n
                    && prom.la < self.max_promise_meta.la
                {
                    prom.la
                } else {
                    prom.ld
                };
                let sfx = self.storage.get_suffix(sync_idx - self.cached_gc_index);
                // inform what got decided already
                let ld = if self.lc > 0 {
                    self.lc
                } else {
                    self.storage.get_decided_len()
                };
                AcceptSync::with(self.n_leader, sfx, sync_idx, Some(ld))
            };
            let msg = Message::with(self.pid, from, PaxosMsg::AcceptSync(acc_sync));
            self.outgoing.push(msg);
        }
    }

    fn handle_accepted(&mut self, accepted: Accepted, from: u64) {
        trace!(self.logger, "Incoming message Accepted {}", from);
        if accepted.n == self.n_leader && self.state == (Role::Leader, Phase::Accept) {
            self.las[from as usize - 1] = accepted.la;
            if accepted.la > self.lc {
                let chosen =
                    self.las.iter().filter(|la| *la >= &accepted.la).count() >= self.majority;
                if chosen {
                    self.lc = accepted.la;
                    let d = Decide::with(self.n_leader, self.lc);
                    if cfg!(feature = "latest_decide") {
                        let promised_idx =
                            self.lds.iter().enumerate().filter(|(_, ld)| ld.is_some());
                        for (idx, _) in promised_idx {
                            match self.latest_decide_meta.get_mut(idx).unwrap() {
                                Some((n, outgoing_dec_idx)) if n == &self.n_leader => {
                                    let Message { msg, .. } =
                                        self.outgoing.get_mut(*outgoing_dec_idx).unwrap();
                                    match msg {
                                        PaxosMsg::AcceptDecide(a) => a.ld = self.lc,
                                        PaxosMsg::Decide(d) => d.ld = self.lc,
                                        _ => {
                                            panic!("Cached Message<T> in outgoing was not Decide")
                                        }
                                    }
                                }
                                _ => {
                                    let cache_dec_idx = self.outgoing.len();
                                    self.latest_decide_meta[idx] =
                                        Some((self.n_leader, cache_dec_idx));
                                    let pid = idx as u64 + 1;
                                    self.outgoing.push(Message::with(
                                        self.pid,
                                        pid,
                                        PaxosMsg::Decide(d),
                                    ));
                                }
                            }
                        }
                    } else {
                        let promised_pids = self
                            .lds
                            .iter()
                            .enumerate()
                            .filter(|(_, ld)| ld.is_some())
                            .map(|(idx, _)| idx as u64 + 1);
                        for pid in promised_pids {
                            self.outgoing
                                .push(Message::with(self.pid, pid, PaxosMsg::Decide(d)));
                        }
                    }
                    self.handle_decide(d);
                }
            }
        }
    }

    fn create_complete_snapshot(&mut self) -> (u64, S) {
        let log_len = self.storage.get_log_len();
        let snapshot = S::create(self.storage.get_entries(0, log_len));
        match self.storage.get_snapshot() {
            Some((trimmed_idx, snapshot)) => {
                let trim_idx = trimmed_idx + log_len;
                self.storage.set_snapshot(trim_idx, snapshot.clone()).expect("Failed to set snapshot");
                (trim_idx, snapshot)
            }
            None => {
                self.storage.set_snapshot(log_len, snapshot.clone()).expect("Failed to set snapshot");
                (log_len, snapshot)
            }
        }
    }

    fn create_delta_snapshot(&self, idx: u64) -> S {
        let changes = self.storage.get_suffix(idx);
        S::create(changes.as_slice())
    }

    /*** Follower ***/
    fn handle_prepare(&mut self, prep: Prepare, from: u64) {
        if self.storage.get_promise() <= prep.n {
            self.leader = from;
            self.storage.set_promise(prep.n);
            self.state = (Role::Follower, Phase::Prepare);
            let na = self.storage.get_accepted_round();
            let la = self.storage.get_log_len();
            let prom_msg = if S::snapshottable() {
                let (trimmed_idx, snapshot, stop_sign) = if na > prep.n_accepted {
                    let stop_sign = self.storage.stopped();
                    let (trimmed_idx, snapshot) = self.create_complete_snapshot();
                    (trimmed_idx, Some(SnapshotType::Complete(snapshot)), stop_sign)
                } else if na == prep.n_accepted && la > prep.la {
                    let stop_sign = self.storage.stopped();
                    let d = self.create_delta_snapshot(prep.la);
                    let trimmed_idx = self.storage.get_trim_idx() + la;
                    (trimmed_idx, Some(SnapshotType::Delta(d)), stop_sign)
                } else {
                    (la, None, None)
                };
                let promise = SnapshotPromise::with(
                    prep.n,
                    na,
                    snapshot,
                    self.storage.get_decided_len(),
                    trimmed_idx,
                    stop_sign,
                );
                PaxosMsg::SnapshotPromise(promise)
            } else {
                let sfx = if na > prep.n_accepted {
                    self.storage.get_suffix(prep.ld - self.cached_gc_index)
                } else if na == prep.n_accepted && la > prep.la {
                    self.storage.get_suffix(prep.la - self.cached_gc_index)
                } else {
                    vec![]
                };
                let promise =
                    Promise::with(prep.n, na, sfx, self.storage.get_decided_len(), la, None); // TODO stopsign
                PaxosMsg::Promise(promise)
            };
            self.outgoing.push(Message::with(self.pid, from, prom_msg));
        }
    }

    fn handle_acceptsync(&mut self, accsync: AcceptSync<T>, from: u64) {
        if self.storage.get_promise() == accsync.n && self.state == (Role::Follower, Phase::Prepare)
        {
            let mut entries = accsync.entries;
            let la = self
                .storage
                .append_on_prefix(accsync.sync_idx, &mut entries);
            let accepted = Accepted::with(accsync.n, la);
            self.outgoing
                .push(Message::with(self.pid, from, PaxosMsg::Accepted(accepted)));
            #[cfg(feature = "latest_accepted")]
            {
                let cached_idx = self.outgoing.len();
                self.latest_accepted_meta = Some((accsync.n, cached_idx));
            }
            self.state = (Role::Follower, Phase::Accept);
            self.storage.set_accepted_round(accsync.n);

            if let Some(idx) = accsync.decide_idx {
                self.storage.set_decided_len(idx);
            }
            self.forward_pending_proposals();
        }
    }

    fn handle_snapshot_acceptsync(&mut self, accsync: SnapshotAcceptSync<T, S>, from: u64) {
        if self.storage.get_promise() == accsync.n && self.state == (Role::Follower, Phase::Prepare)
        {
            self.state = (Role::Follower, Phase::Accept);
            match accsync.snapshot {
                SnapshotType::Complete(c) => {
                    self.storage.set_snapshot(accsync.trimmed_idx, c).expect("Failed to set snapshot");
                }
                SnapshotType::Delta(d) => {
                    self.storage.merge_snapshot(accsync.trimmed_idx, d);
                }
                _ => unimplemented!(),
            };
            self.storage.set_accepted_round(accsync.n);
            let accepted = Accepted::with(accsync.n, accsync.trimmed_idx);
            self.outgoing
                .push(Message::with(self.pid, from, PaxosMsg::Accepted(accepted)));
            self.forward_pending_proposals();
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
            let mut entries = f.entries;
            self.storage.set_accepted_round(f.n);
            self.accept_entries(f.n, &mut entries);
            self.state.1 = Phase::Accept;
            self.forward_pending_proposals();
        }
    }

    fn handle_acceptdecide(&mut self, acc: AcceptDecide<T>) {
        if self.storage.get_promise() == acc.n && self.state == (Role::Follower, Phase::Accept) {
            let mut entries = acc.entries;
            self.accept_entries(acc.n, &mut entries);
            // handle decide
            if acc.ld > self.storage.get_decided_len() {
                self.storage.set_decided_len(acc.ld);
            }
        }
    }

    fn handle_decide(&mut self, dec: Decide) {
        if self.storage.get_promise() == dec.n && self.state.1 == Phase::Accept {
            self.storage.set_decided_len(dec.ld);
        }
    }

    /*** algorithm specific functions ***/
    fn drop_after_stopsign(entries: &mut Vec<Entry<T>>) {
        // drop all entries ordered after stopsign (if any)
        let ss_idx = entries.iter().position(|e| e.is_stopsign());
        if let Some(idx) = ss_idx {
            entries.truncate(idx + 1);
        };
    }

    fn accept_entries(&mut self, n: Ballot, entries: &mut Vec<Entry<T>>) {
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
