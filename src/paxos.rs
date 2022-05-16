use crate::{
    leader_election::*,
    messages::*,
    storage::{Entry, SequenceTraits, StateTraits, StopSign, Storage},
    util::PromiseMetaData,
};
use std::{fmt::Debug, sync::Arc};

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
pub enum ProposeErr {
    Normal(Vec<u8>),
    Reconfiguration(Vec<u64>), // TODO use a type for ProcessId
}

/// An Omni-Paxos replica. Maintains local state of the replicated log, handles incoming messages and produces outgoing messages that the user has to fetch periodically and send using a network implementation.
/// User also has to periodically fetch the decided entries that are guaranteed to be strongly consistent and linearizable, and therefore also safe to be used in the higher level application.
pub struct Paxos<R, S, P>
where
    R: Round,
    S: SequenceTraits<R>,
    P: StateTraits<R>,
{
    storage: Storage<R, S, P>,
    config_id: u32,
    pid: u64,
    majority: usize,
    peers: Vec<u64>, // excluding self pid
    state: (Role, Phase),
    leader: u64,
    n_leader: R,
    promises_meta: Vec<Option<PromiseMetaData<R>>>,
    las: Vec<u64>,
    lds: Vec<Option<u64>>,
    proposals: Vec<Entry<R>>,
    lc: u64, // length of longest chosen seq
    prev_ld: u64,
    max_promise_meta: PromiseMetaData<R>,
    max_promise_sfx: Vec<Entry<R>>,             // TODO put in Storage
    batch_accept_meta: Vec<Option<(R, usize)>>, //  R, index in outgoing
    latest_decide_meta: Vec<Option<(R, usize)>>,
    latest_accepted_meta: Option<(R, usize)>,
    outgoing: Vec<Message<R>>,
    num_nodes: usize,
}

impl<R, S, P> Paxos<R, S, P>
where
    R: Round,
    S: SequenceTraits<R>,
    P: StateTraits<R>,
{
    /*** User functions ***/
    /// Creates an Omni-Paxos replica.
    /// # Arguments
    /// * `config_id` - The identifier for the configuration that this Omni-Paxos replica is part of.
    /// * `pid` - The identifier of this Omni-Paxos replica.
    /// * `peers` - The `pid`s of the other replicas in the configuration.
    /// * `skip_prepare_use_leader` - Initial leader of the cluster. Could be used in combination with reconfiguration to skip the prepare phase in the new configuration.
    pub fn with(
        config_id: u32,
        pid: u64,
        peers: Vec<u64>,
        storage: Storage<R, S, P>,
        skip_prepare_use_leader: Option<Leader<R>>, // skipped prepare phase with the following leader event
    ) -> Paxos<R, S, P> {
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
                (state, l.pid, l.round, lds)
            }
            None => {
                let state = (Role::Follower, Phase::None);
                let lds = vec![None; num_nodes];
                (state, 0, R::default(), lds)
            }
        };
        let mut paxos = Paxos {
            storage,
            pid,
            config_id,
            majority,
            peers,
            state,
            leader,
            n_leader: n_leader.clone(),
            promises_meta: vec![None; num_nodes],
            las: vec![0; num_nodes],
            lds,
            proposals: Vec::with_capacity(BUFFER_SIZE),
            lc: 0,
            prev_ld: 0,
            max_promise_meta: PromiseMetaData::with(R::default(), 0, 0),
            max_promise_sfx: vec![],
            batch_accept_meta: vec![None; num_nodes],
            latest_decide_meta: vec![None; num_nodes],
            latest_accepted_meta: None,
            outgoing: Vec::with_capacity(BUFFER_SIZE),
            num_nodes,
            // log,
        };
        paxos.storage.set_promise(n_leader);
        paxos
    }

    /// Returns the id of the current leader.
    pub fn get_current_leader(&self) -> u64 {
        self.leader
    }

    /// Returns the outgoing messages from this replica. The messages should then be sent via the network implementation.
    pub fn get_outgoing_msgs(&mut self) -> Vec<Message<R>> {
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
    pub fn get_decided_entries(&mut self) -> &[Entry<R>] {
        let ld = self.storage.get_decided_len();
        if self.prev_ld < ld {
            let decided = self.storage.get_entries(self.prev_ld, ld);
            self.prev_ld = ld;
            decided
        } else {
            &[]
        }
    }

    /// Handle an incoming message.
    pub fn handle(&mut self, m: Message<R>) {
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
        }
    }

    /// Returns whether this Omni-Paxos instance is stopped, i.e. if it has been reconfigured.
    pub fn stopped(&self) -> bool {
        self.storage.stopped()
    }

    /// Propose a normal entry to be replicated.
    pub fn propose_normal(&mut self, data: Vec<u8>) -> Result<(), ProposeErr> {
        if self.stopped() {
            Err(ProposeErr::Normal(data))
        } else {
            let entry = Entry::Normal(data);
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
        prio_start_round: Option<R>,
    ) -> Result<(), ProposeErr> {
        if self.stopped() {
            Err(ProposeErr::Reconfiguration(new_configuration))
        } else {
            let continued_nodes: Vec<&u64> = new_configuration
                .iter()
                .filter(|&pid| pid == &self.pid || self.peers.contains(pid))
                .collect();
            let skip_prepare_use_leader = {
                let max_pid = if cfg!(feature = "continued_leader_reconfiguration")
                    && continued_nodes.contains(&&self.pid)
                {
                    // make ourselves the initial leader in the next configuration
                    self.pid
                } else {
                    let my_idx = self.pid as usize - 1;
                    let max_idx = self
                        .las
                        .iter()
                        .enumerate()
                        .filter(|(idx, _)| {
                            idx != &my_idx && continued_nodes.contains(&&(*idx as u64 + 1))
                        })
                        .max_by(|(_, la), (_, other_la)| la.cmp(other_la));
                    match max_idx {
                        Some((other_idx, _)) => other_idx as u64 + 1, // give leadership of new config to most up-to-date follower
                        None => self.pid,
                    }
                };
                Some(Leader::with(max_pid, prio_start_round.unwrap_or_default()))
            };
            let ss = StopSign::with(
                self.config_id + 1,
                new_configuration,
                skip_prepare_use_leader,
            );
            let entry = Entry::StopSign(ss);
            self.propose_entry(entry);
            Ok(())
        }
    }

    /// Returns chosen entries between the given indices. If no chosen entries in the given interval, an empty vec is returned.
    pub fn get_chosen_entries(&self, from_idx: u64, to_idx: u64) -> Vec<Entry<R>> {
        let ld = self.storage.get_decided_len();
        let max_idx = std::cmp::max(ld, self.lc);
        if to_idx > max_idx {
            vec![]
        } else {
            self.storage.get_entries(from_idx, to_idx).to_vec()
        }
    }

    /// Returns the currently promised round.
    pub fn get_promise(&self) -> R {
        self.storage.get_promise()
    }

    /// Stops this Paxos to write any new entries to the log and returns the final log.
    /// This should only be called **after a reconfiguration has been decided.**
    pub fn stop_and_get_sequence(&mut self) -> Arc<S> {
        self.storage.stop_and_get_sequence()
    }

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

    fn propose_entry(&mut self, entry: Entry<R>) {
        match self.state {
            (Role::Leader, Phase::Prepare) => self.proposals.push(entry),
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

    /// Handle becoming the leader. Should be called when the leader election has elected this replica as the leader
    /*** Leader ***/
    pub fn handle_leader(&mut self, l: Leader<R>) {
        let n = l.round;
        let leader_pid = l.pid;
        if n <= self.n_leader || n <= self.storage.get_promise() {
            return;
        }
        self.clear_peers_state();
        if self.stopped() {
            self.proposals.clear();
        }
        if self.pid == leader_pid {
            self.n_leader = n.clone();
            self.leader = leader_pid;
            self.storage.set_promise(n.clone());
            /* insert my promise */
            let na = self.storage.get_accepted_round();
            let ld = self.storage.get_decided_len();
            let sfx = self.storage.get_suffix(ld);
            let la = self.storage.get_sequence_len();
            let promise_meta = PromiseMetaData::with(na, la, self.pid);
            self.max_promise_meta = promise_meta.clone();
            self.promises_meta[self.pid as usize - 1] = Some(promise_meta);
            self.max_promise_sfx = sfx;
            /* initialise longest chosen sequence and update state */
            self.lc = 0;
            self.state = (Role::Leader, Phase::Prepare);
            let prep = Prepare::with(n, ld, self.storage.get_accepted_round(), la);
            /* send prepare */
            for pid in &self.peers {
                self.outgoing.push(Message::with(
                    self.pid,
                    *pid,
                    PaxosMsg::Prepare(prep.clone()),
                ));
            }
        } else {
            self.state.0 = Role::Follower;
        }
    }

    fn handle_preparereq(&mut self, from: u64) {
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
            let la = self.storage.get_sequence_len();
            let prep = Prepare::with(self.n_leader.clone(), ld, n_accepted, la);
            self.outgoing
                .push(Message::with(self.pid, from, PaxosMsg::Prepare(prep)));
        }
    }

    fn forward_proposals(&mut self, mut entries: Vec<Entry<R>>) {
        if self.leader > 0 && self.leader != self.pid {
            let pf = PaxosMsg::ProposalForward(entries);
            let msg = Message::with(self.pid, self.leader, pf);
            self.outgoing.push(msg);
        } else {
            self.proposals.append(&mut entries);
        }
    }

    fn handle_forwarded_proposal(&mut self, mut entries: Vec<Entry<R>>) {
        if !self.stopped() {
            match self.state {
                (Role::Leader, Phase::Prepare) => self.proposals.append(&mut entries),
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

    fn send_first_accept(&mut self, entry: Entry<R>) {
        let promised_pids = self
            .lds
            .iter()
            .enumerate()
            .filter(|(_, x)| x.is_some())
            .map(|(idx, _)| idx as u64 + 1);
        let f = FirstAccept::with(self.n_leader.clone(), vec![entry.clone()]);
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

    fn send_accept(&mut self, entry: Entry<R>) {
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
                        let acc =
                            AcceptDecide::with(self.n_leader.clone(), self.lc, vec![entry.clone()]);
                        let cache_idx = self.outgoing.len();
                        let pid = idx as u64 + 1;
                        self.outgoing.push(Message::with(
                            self.pid,
                            pid,
                            PaxosMsg::AcceptDecide(acc),
                        ));
                        self.batch_accept_meta[idx] = Some((self.n_leader.clone(), cache_idx));
                        #[cfg(feature = "latest_decide")]
                        {
                            self.latest_decide_meta[idx] = Some((self.n_leader.clone(), cache_idx));
                        }
                    }
                }
            } else {
                let pid = idx as u64 + 1;
                let acc = AcceptDecide::with(self.n_leader.clone(), self.lc, vec![entry.clone()]);
                self.outgoing
                    .push(Message::with(self.pid, pid, PaxosMsg::AcceptDecide(acc)));
            }
        }
        let la = self.storage.append_entry(entry);
        self.las[self.pid as usize - 1] = la;
    }

    fn send_batch_accept(&mut self, mut entries: Vec<Entry<R>>) {
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
                        let acc =
                            AcceptDecide::with(self.n_leader.clone(), self.lc, entries.clone());
                        let cache_idx = self.outgoing.len();
                        let pid = idx as u64 + 1;
                        self.outgoing.push(Message::with(
                            self.pid,
                            pid,
                            PaxosMsg::AcceptDecide(acc),
                        ));
                        self.batch_accept_meta[idx] = Some((self.n_leader.clone(), cache_idx));
                        #[cfg(feature = "latest_decide")]
                        {
                            self.latest_decide_meta[idx] = Some((self.n_leader.clone(), cache_idx));
                        }
                    }
                }
            } else {
                let pid = idx as u64 + 1;
                let acc = AcceptDecide::with(self.n_leader.clone(), self.lc, entries.clone());
                self.outgoing
                    .push(Message::with(self.pid, pid, PaxosMsg::AcceptDecide(acc)));
            }
        }
        let la = self.storage.append_sequence(&mut entries);
        self.las[self.pid as usize - 1] = la;
    }

    fn handle_promise_prepare(&mut self, prom: Promise<R>, from: u64) {
        if prom.n == self.n_leader {
            let promise_meta = PromiseMetaData::with(prom.n_accepted, prom.la, from);
            if promise_meta > self.max_promise_meta {
                self.max_promise_meta = promise_meta.clone();
                self.max_promise_sfx = prom.sfx;
            }
            let idx = Self::get_idx_from_pid(from);
            self.promises_meta[idx] = Some(promise_meta);
            self.lds[idx] = Some(prom.ld);
            let num_promised = self.promises_meta.iter().filter(|x| x.is_some()).count();
            if num_promised >= self.majority {
                let PromiseMetaData {
                    n: max_promise_n,
                    la: max_la,
                    pid: max_pid,
                } = &self.max_promise_meta;
                let last_is_stop = match self.max_promise_sfx.last() {
                    Some(e) => e.is_stopsign(),
                    None => false,
                };
                if max_pid != &self.pid {
                    // sync self with max pid's log
                    if max_promise_n == &self.storage.get_accepted_round() {
                        self.storage.append_sequence(&mut self.max_promise_sfx);
                    } else {
                        self.storage
                            .append_on_decided_prefix(std::mem::take(&mut self.max_promise_sfx))
                    }
                }
                if last_is_stop {
                    self.proposals.clear(); // will never be decided
                } else {
                    Self::drop_after_stopsign(&mut self.proposals); // drop after ss, if ss exists
                }
                // create accept_sync with only new proposals for all pids with max_promise
                let mut new_entries = std::mem::take(&mut self.proposals);
                let max_promise_acc_sync =
                    AcceptSync::with(self.n_leader.clone(), new_entries.clone(), *max_la);
                // append new proposals in my sequence
                let la = self.storage.append_sequence(&mut new_entries);
                self.storage.set_accepted_round(self.n_leader.clone());
                self.las[Self::get_idx_from_pid(self.pid)] = la;
                self.state = (Role::Leader, Phase::Accept);
                let leader_pid = self.pid;
                // send accept_sync to followers
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
                    let msg = if cfg!(feature = "max_accsync")
                        && (promise_n, promise_la) == (max_promise_n, max_la)
                    {
                        Message::with(
                            self.pid,
                            *pid,
                            PaxosMsg::AcceptSync(max_promise_acc_sync.clone()),
                        )
                    } else if cfg!(feature = "max_accsync")
                        && promise_n == max_promise_n
                        && promise_la < max_la
                    {
                        let sfx = self.storage.get_suffix(*promise_la);
                        let acc_sync = AcceptSync::with(self.n_leader.clone(), sfx, *promise_la);
                        Message::with(self.pid, *pid, PaxosMsg::AcceptSync(acc_sync))
                    } else {
                        let idx = Self::get_idx_from_pid(*pid);
                        let ld = self
                            .lds
                            .get(idx)
                            .expect("Received PromiseMetaData but not found in ld")
                            .unwrap();
                        let sfx = self.storage.get_suffix(ld);
                        let acc_sync = AcceptSync::with(self.n_leader.clone(), sfx, ld);
                        Message::with(self.pid, *pid, PaxosMsg::AcceptSync(acc_sync))
                    };
                    self.outgoing.push(msg);
                    #[cfg(feature = "batch_accept")]
                    {
                        let idx = Self::get_idx_from_pid(*pid);
                        self.batch_accept_meta[idx] =
                            Some((self.n_leader.clone(), self.outgoing.len() - 1));
                    }
                }
            }
        }
    }

    fn handle_promise_accept(&mut self, prom: Promise<R>, from: u64) {
        if prom.n == self.n_leader {
            let idx = Self::get_idx_from_pid(from);
            self.lds[idx] = Some(prom.ld);
            let PromiseMetaData {
                n: max_round,
                la: max_la,
                ..
            } = &self.max_promise_meta;
            let sync_idx = if cfg!(feature = "max_accsync") {
                if &prom.n_accepted == max_round {
                    if &prom.la > max_la {
                        *max_la
                    } else {
                        prom.la
                    }
                } else {
                    prom.ld
                }
            } else {
                prom.ld
            };
            let sfx = self.storage.get_suffix(sync_idx);
            let acc_sync = AcceptSync::with(self.n_leader.clone(), sfx, sync_idx);
            let msg = Message::with(self.pid, from, PaxosMsg::AcceptSync(acc_sync));
            self.outgoing.push(msg);
            #[cfg(feature = "batch_accept")]
            {
                self.batch_accept_meta[idx] =
                    Some((self.n_leader.clone(), self.outgoing.len() - 1));
            }
            // inform what got decided already
            let ld = if self.lc > 0 {
                self.lc
            } else {
                self.storage.get_decided_len()
            };
            if ld > prom.ld {
                let d = Decide::with(ld, self.n_leader.clone());
                self.outgoing
                    .push(Message::with(self.pid, from, PaxosMsg::Decide(d)));
                #[cfg(feature = "latest_decide")]
                {
                    let cached_idx = self.outgoing.len() - 1;
                    self.latest_decide_meta[idx] = Some((self.n_leader.clone(), cached_idx));
                }
            }
        }
    }

    fn handle_accepted(&mut self, accepted: Accepted<R>, from: u64) {
        if accepted.n == self.n_leader && self.state == (Role::Leader, Phase::Accept) {
            self.las[from as usize - 1] = accepted.la;
            if accepted.la > self.lc {
                let chosen =
                    self.las.iter().filter(|la| *la >= &accepted.la).count() >= self.majority;
                if chosen {
                    self.lc = accepted.la;
                    let d = Decide::with(self.lc, self.n_leader.clone());
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
                                            panic!("Cached Message<R> in outgoing was not Decide")
                                        }
                                    }
                                }
                                _ => {
                                    let cache_dec_idx = self.outgoing.len();
                                    self.latest_decide_meta[idx] =
                                        Some((self.n_leader.clone(), cache_dec_idx));
                                    let pid = idx as u64 + 1;
                                    self.outgoing.push(Message::with(
                                        self.pid,
                                        pid,
                                        PaxosMsg::Decide(d.clone()),
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
                            self.outgoing.push(Message::with(
                                self.pid,
                                pid,
                                PaxosMsg::Decide(d.clone()),
                            ));
                        }
                    }
                    self.handle_decide(d);
                }
            }
        }
    }

    /*** Follower ***/
    fn handle_prepare(&mut self, prep: Prepare<R>, from: u64) {
        if self.storage.get_promise() <= prep.n {
            self.leader = from;
            self.storage.set_promise(prep.n.clone());
            self.state = (Role::Follower, Phase::Prepare);
            let na = self.storage.get_accepted_round();
            let la = self.storage.get_sequence_len();
            let sfx = if na > prep.n_accepted {
                self.storage.get_suffix(prep.ld)
            } else if na == prep.n_accepted && la > prep.la {
                self.storage.get_suffix(prep.la)
            } else {
                vec![]
            };
            let p = Promise::with(prep.n, na, sfx, self.storage.get_decided_len(), la);
            self.outgoing
                .push(Message::with(self.pid, from, PaxosMsg::Promise(p)));
        }
    }

    fn handle_acceptsync(&mut self, accsync: AcceptSync<R>, from: u64) {
        if self.storage.get_promise() == accsync.n && self.state == (Role::Follower, Phase::Prepare)
        {
            self.storage.set_accepted_round(accsync.n.clone());
            let mut entries = accsync.entries;
            let la = self
                .storage
                .append_on_prefix(accsync.sync_idx, &mut entries);
            self.state = (Role::Follower, Phase::Accept);
            #[cfg(feature = "latest_accepted")]
            {
                let cached_idx = self.outgoing.len();
                self.latest_accepted_meta = Some((accsync.n.clone(), cached_idx));
            }
            let accepted = Accepted::with(accsync.n, la);
            self.outgoing
                .push(Message::with(self.pid, from, PaxosMsg::Accepted(accepted)));
            /*** Forward proposals ***/
            let proposals = std::mem::take(&mut self.proposals);
            if !proposals.is_empty() {
                self.forward_proposals(proposals);
            }
        }
    }

    fn handle_firstaccept(&mut self, f: FirstAccept<R>) {
        if self.storage.get_promise() == f.n && self.state == (Role::Follower, Phase::FirstAccept) {
            let mut entries = f.entries;
            self.storage.set_accepted_round(f.n.clone());
            self.accept_entries(f.n, &mut entries);
            self.state.1 = Phase::Accept;
            /*** Forward proposals ***/
            let proposals = std::mem::take(&mut self.proposals);
            if !proposals.is_empty() {
                self.forward_proposals(proposals);
            }
        }
    }

    fn handle_acceptdecide(&mut self, acc: AcceptDecide<R>) {
        if self.storage.get_promise() == acc.n && self.state == (Role::Follower, Phase::Accept) {
            let mut entries = acc.entries;
            self.accept_entries(acc.n, &mut entries);
            // handle decide
            if acc.ld > self.storage.get_decided_len() {
                self.storage.set_decided_len(acc.ld);
            }
        }
    }

    fn handle_decide(&mut self, dec: Decide<R>) {
        if self.storage.get_promise() == dec.n && self.state.1 == Phase::Accept {
            self.storage.set_decided_len(dec.ld);
        }
    }

    /*** algorithm specific functions ***/
    fn drop_after_stopsign(entries: &mut Vec<Entry<R>>) {
        // drop all entries ordered after stopsign (if any)
        let ss_idx = entries.iter().position(|e| e.is_stopsign());
        if let Some(idx) = ss_idx {
            entries.truncate(idx + 1);
        };
    }

    fn accept_entries(&mut self, n: R, entries: &mut Vec<Entry<R>>) {
        let la = self.storage.append_sequence(entries);
        if cfg!(feature = "latest_accepted") {
            match &self.latest_accepted_meta {
                Some((round, outgoing_idx)) if round == &n => {
                    let Message { msg, .. } = self.outgoing.get_mut(*outgoing_idx).unwrap();
                    match msg {
                        PaxosMsg::Accepted(a) => a.la = la,
                        _ => panic!("Cached idx is not an Accepted Message<R>!"),
                    }
                }
                _ => {
                    let accepted = Accepted::with(n.clone(), la);
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
