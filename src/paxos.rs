use crate::{
    leader_election::*,
    messages::*,
    storage::{Entry, SequenceTraits, StateTraits, StopSign, Storage},
};
use std::{fmt::Debug, sync::Arc};

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

#[derive(Debug)]
pub enum ProposeErr {
    Normal(Vec<u8>),
    Reconfiguration(Vec<u64>), // TODO use a type for ProcessId
}

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
    promises_meta: Vec<Option<(R, usize)>>,
    las: Vec<u64>,
    lds: Vec<Option<u64>>,
    proposals: Vec<Entry<R>>,
    lc: u64, // length of longest chosen seq
    prev_ld: u64,
    acc_sync_ld: u64,
    max_promise_meta: (R, usize, u64), // R, sfx len, pid
    max_promise_sfx: Vec<Entry<R>>,
    batch_accept_meta: Vec<Option<(R, usize)>>, //  R, index in outgoing
    latest_decide_meta: Vec<Option<(R, usize)>>,
    latest_accepted_meta: Option<(R, usize)>,
    outgoing: Vec<Message<R>>,
    num_nodes: usize,
    // pub log: KompactLogger, // TODO provide kompact independent log when used as a library
    max_inflight: usize,
}

impl<R, S, P> Paxos<R, S, P>
where
    R: Round,
    S: SequenceTraits<R>,
    P: StateTraits<R>,
{
    /*** User functions ***/
    pub fn with(
        config_id: u32,
        pid: u64,
        peers: Vec<u64>,
        storage: Storage<R, S, P>,
        // log: KompactLogger,  TODO
        skip_prepare_use_leader: Option<Leader<R>>, // skipped prepare phase with the following leader event
        max_inflight: Option<usize>,
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
        // info!(log, "Start raw paxos pid: {}, state: {:?}, n_leader: {:?}", pid, state, n_leader);
        let max_inflight = max_inflight.unwrap_or(100000);
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
            proposals: Vec::with_capacity(max_inflight),
            lc: 0,
            prev_ld: 0,
            acc_sync_ld: 0,
            max_promise_meta: (R::default(), 0, 0),
            max_promise_sfx: vec![],
            batch_accept_meta: vec![None; num_nodes],
            latest_decide_meta: vec![None; num_nodes],
            latest_accepted_meta: None,
            outgoing: Vec::with_capacity(max_inflight),
            num_nodes,
            // log,
            max_inflight,
        };
        paxos.storage.set_promise(n_leader);
        paxos
    }

    pub fn get_current_leader(&self) -> u64 {
        self.leader
    }

    pub fn get_outgoing_msgs(&mut self) -> Vec<Message<R>> {
        let mut outgoing = Vec::with_capacity(self.max_inflight);
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

    pub fn handle(&mut self, m: Message<R>) {
        match m.msg {
            PaxosMsg::PrepareReq => self.handle_preparereq(m.from),
            PaxosMsg::Prepare(prep) => self.handle_prepare(prep, m.from),
            PaxosMsg::Promise(prom) => match &self.state {
                (Role::Leader, Phase::Prepare) => self.handle_promise_prepare(prom, m.from),
                (Role::Leader, Phase::Accept) => self.handle_promise_accept(prom, m.from),
                _ => {}
            },
            PaxosMsg::AcceptSync(acc_sync) => self.handle_accept_sync(acc_sync, m.from),
            PaxosMsg::FirstAccept(f) => self.handle_firstaccept(f),
            PaxosMsg::AcceptDecide(acc) => self.handle_acceptdecide(acc),
            PaxosMsg::Accepted(accepted) => self.handle_accepted(accepted, m.from),
            PaxosMsg::Decide(d) => self.handle_decide(d),
            PaxosMsg::ProposalForward(proposals) => self.handle_forwarded_proposal(proposals),
        }
    }

    pub fn stopped(&self) -> bool {
        self.storage.stopped()
    }

    pub fn propose_normal(&mut self, data: Vec<u8>) -> Result<(), ProposeErr> {
        if self.stopped() {
            Err(ProposeErr::Normal(data))
        } else {
            let entry = Entry::Normal(data);
            match self.state {
                (Role::Leader, Phase::Prepare) => self.proposals.push(entry),
                (Role::Leader, Phase::Accept) => self.send_accept(entry),
                (Role::Leader, Phase::FirstAccept) => self.send_first_accept(entry),
                _ => self.forward_proposals(vec![entry]),
            }
            Ok(())
        }
    }

    pub fn propose_reconfiguration(
        &mut self,
        nodes: Vec<u64>,
        prio_start_round: Option<R>,
    ) -> Result<(), ProposeErr> {
        if self.stopped() {
            Err(ProposeErr::Reconfiguration(nodes))
        } else {
            let continued_nodes: Vec<&u64> = nodes
                .iter()
                .filter(|&pid| pid == &self.pid || self.peers.contains(pid))
                .collect();
            let skip_prepare_use_leader = if !continued_nodes.is_empty() {
                let my_idx = self.pid as usize - 1;
                let max_idx = self
                    .las
                    .iter()
                    .enumerate()
                    .filter(|(idx, _)| {
                        idx != &my_idx && continued_nodes.contains(&&(*idx as u64 + 1))
                    })
                    .max_by(|(_, la), (_, other_la)| la.cmp(other_la));
                let max_pid = match max_idx {
                    Some((other_idx, _)) => other_idx as u64 + 1, // give leadership of new config to most up-to-date peer
                    None => self.pid,
                };
                let l = Leader::with(max_pid, prio_start_round.unwrap_or(R::default()));
                Some(l)
            } else {
                None
            };
            let ss = StopSign::with(self.config_id + 1, nodes, skip_prepare_use_leader);
            let entry = Entry::StopSign(ss);
            match self.state {
                (Role::Leader, Phase::Prepare) => self.proposals.push(entry),
                (Role::Leader, Phase::Accept) => self.send_accept(entry),
                (Role::Leader, Phase::FirstAccept) => self.send_first_accept(entry),
                _ => self.forward_proposals(vec![entry]),
            }
            Ok(())
        }
    }

    pub fn get_chosen_entries(&self, from_idx: u64, to_idx: u64) -> (bool, Vec<Entry<R>>) {
        let ld = self.storage.get_decided_len();
        let max_idx = std::cmp::max(ld, self.lc);
        if to_idx > max_idx {
            (false, vec![])
        } else {
            (true, self.storage.get_entries(from_idx, to_idx).to_vec())
        }
    }

    pub fn stop_and_get_sequence(&mut self) -> Arc<S> {
        self.storage.stop_and_get_sequence()
    }

    #[allow(dead_code)]
    pub fn connection_lost(&mut self, pid: u64) {
        // TODO remove allow dead code when fail-recovery is supported
        if self.state.0 == Role::Follower && self.leader == pid {
            self.state = (Role::Follower, Phase::Recover);
        }
    }

    fn clear_peers_state(&mut self) {
        self.las = vec![0; self.num_nodes];
        self.promises_meta = vec![None; self.num_nodes];
        self.lds = vec![None; self.num_nodes];
    }

    /*** Leader ***/
    pub fn handle_leader(&mut self, l: Leader<R>) {
        let n = l.round;
        if n <= self.n_leader || n <= self.storage.get_promise() {
            return;
        }
        self.clear_peers_state();
        if self.stopped() {
            self.proposals.clear();
        }
        if self.pid == l.pid {
            self.n_leader = n.clone();
            self.leader = l.pid;
            self.storage.set_promise(n.clone());
            /* insert my promise */
            let na = self.storage.get_accepted_round();
            let ld = self.storage.get_decided_len();
            let sfx = self.storage.get_suffix(ld);
            let sfx_len = sfx.len();
            self.max_promise_meta = (na.clone(), sfx_len, self.pid);
            self.promises_meta[self.pid as usize - 1] = Some((na, sfx_len));
            self.max_promise_sfx = sfx;
            /* insert my longest decided sequence */
            self.acc_sync_ld = ld;
            /* initialise longest chosen sequence and update state */
            self.lc = 0;
            self.state = (Role::Leader, Phase::Prepare);
            /* send prepare */
            for pid in &self.peers {
                let prep = Prepare::with(n.clone(), ld, self.storage.get_accepted_round());
                self.outgoing
                    .push(Message::with(self.pid, *pid, PaxosMsg::Prepare(prep)));
            }
        } else {
            if self.state.1 == Phase::Recover {
                self.outgoing
                    .push(Message::with(self.pid, l.pid, PaxosMsg::PrepareReq));
            }
            self.state.0 = Role::Follower;
        }
    }

    fn handle_preparereq(&mut self, from: u64) {
        if self.state.0 == Role::Leader {
            let ld = self.storage.get_decided_len();
            let n_accepted = self.storage.get_accepted_round();
            let prep = Prepare::with(self.n_leader.clone().clone(), ld, n_accepted);
            self.outgoing
                .push(Message::with(self.pid, from, PaxosMsg::Prepare(prep)));
        }
    }

    fn forward_proposals(&mut self, mut entries: Vec<Entry<R>>) {
        if self.leader > 0 && self.leader != self.pid {
            let pf = PaxosMsg::ProposalForward(entries);
            let msg = Message::with(self.pid, self.leader, pf);
            // println!("Forwarding to node {}", self.leader);
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
        // info!(self.log, "Sending first accept");
        let promised_pids = self
            .lds
            .iter()
            .enumerate()
            .filter(|(_, x)| x.is_some())
            .map(|(idx, _)| idx as u64 + 1);
        for pid in promised_pids {
            let f = FirstAccept::with(self.n_leader.clone().clone(), vec![entry.clone()]);
            self.outgoing
                .push(Message::with(self.pid, pid, PaxosMsg::FirstAccept(f)));
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
                        let acc = AcceptDecide::with(
                            self.n_leader.clone().clone(),
                            self.lc,
                            vec![entry.clone()],
                        );
                        let cache_idx = self.outgoing.len();
                        let pid = idx as u64 + 1;
                        self.outgoing.push(Message::with(
                            self.pid,
                            pid,
                            PaxosMsg::AcceptDecide(acc),
                        ));
                        self.batch_accept_meta[idx] =
                            Some((self.n_leader.clone().clone(), cache_idx));
                        #[cfg(feature = "latest_decide")]
                        {
                            self.latest_decide_meta[idx] =
                                Some((self.n_leader.clone().clone(), cache_idx));
                        }
                    }
                }
            } else {
                let pid = idx as u64 + 1;
                let acc =
                    AcceptDecide::with(self.n_leader.clone().clone(), self.lc, vec![entry.clone()]);
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
                        let acc = AcceptDecide::with(
                            self.n_leader.clone().clone(),
                            self.lc,
                            entries.clone(),
                        );
                        let cache_idx = self.outgoing.len();
                        let pid = idx as u64 + 1;
                        self.outgoing.push(Message::with(
                            self.pid,
                            pid,
                            PaxosMsg::AcceptDecide(acc),
                        ));
                        self.batch_accept_meta[idx] =
                            Some((self.n_leader.clone().clone(), cache_idx));
                        #[cfg(feature = "latest_decide")]
                        {
                            self.latest_decide_meta[idx] =
                                Some((self.n_leader.clone().clone(), cache_idx));
                        }
                    }
                }
            } else {
                let pid = idx as u64 + 1;
                let acc =
                    AcceptDecide::with(self.n_leader.clone().clone(), self.lc, entries.clone());
                self.outgoing
                    .push(Message::with(self.pid, pid, PaxosMsg::AcceptDecide(acc)));
            }
        }
        let la = self.storage.append_sequence(&mut entries);
        self.las[self.pid as usize - 1] = la;
    }

    fn handle_promise_prepare(&mut self, prom: Promise<R>, from: u64) {
        if prom.n == self.n_leader {
            let sfx_len = prom.sfx.len();
            let promise_meta = &(prom.n_accepted.clone(), sfx_len, from);
            if promise_meta > &self.max_promise_meta && sfx_len > 0
                || (sfx_len == 0 && prom.ld >= self.acc_sync_ld)
            {
                self.max_promise_meta = promise_meta.clone();
                self.max_promise_sfx = prom.sfx;
            }
            let idx = from as usize - 1;
            self.promises_meta[idx] = Some((prom.n_accepted, sfx_len));
            self.lds[idx] = Some(prom.ld);
            let num_promised = self.promises_meta.iter().filter(|x| x.is_some()).count();
            if num_promised >= self.majority {
                let (max_promise_n, max_sfx_len, max_pid) = &self.max_promise_meta;
                let last_is_stop = match self.max_promise_sfx.last() {
                    Some(e) => e.is_stopsign(),
                    None => false,
                };
                let max_sfx_is_empty = self.max_promise_sfx.is_empty();
                if max_pid != &self.pid {
                    // sync self with max pid's sequence
                    let (leader_n, leader_sfx_len) =
                        &(self.promises_meta[self.pid as usize - 1].as_ref().unwrap());
                    if (leader_n, leader_sfx_len) != (max_promise_n, max_sfx_len) {
                        self.storage
                            .append_on_decided_prefix(std::mem::take(&mut self.max_promise_sfx));
                    }
                }
                if last_is_stop {
                    self.proposals.clear(); // will never be decided
                } else {
                    Self::drop_after_stopsign(&mut self.proposals); // drop after ss, if ss exists
                }
                // create accept_sync with only new proposals for all pids with max_promise
                let mut new_entries = std::mem::take(&mut self.proposals);
                let max_ld = self.lds[*max_pid as usize - 1].unwrap_or(self.acc_sync_ld); // unwrap_or: if we are max_pid then unwrap will be none
                let max_promise_acc_sync =
                    AcceptSync::with(self.n_leader.clone(), new_entries.clone(), max_ld, false);
                // append new proposals in my sequence
                let la = self.storage.append_sequence(&mut new_entries);
                self.las[self.pid as usize - 1] = la;
                self.state = (Role::Leader, Phase::Accept);
                // send accept_sync to followers
                let my_idx = self.pid as usize - 1;
                let promised = self
                    .lds
                    .iter()
                    .enumerate()
                    .filter(|(idx, ld)| idx != &my_idx && ld.is_some());
                for (idx, l) in promised {
                    let pid = idx as u64 + 1;
                    let ld = l.unwrap();
                    let (promise_n, promise_sfx_len) = &self.promises_meta[idx]
                        .as_ref()
                        .unwrap_or_else(|| panic!("No promise from {}. Max pid: {}", pid, max_pid));
                    if cfg!(feature = "max_accsync") {
                        if (promise_n, promise_sfx_len) == (max_promise_n, max_sfx_len) {
                            if !max_sfx_is_empty || ld >= self.acc_sync_ld {
                                let msg = Message::with(
                                    self.pid,
                                    pid,
                                    PaxosMsg::AcceptSync(max_promise_acc_sync.clone()),
                                );
                                self.outgoing.push(msg);
                            }
                        } else {
                            let sfx = self.storage.get_suffix(ld);
                            let acc_sync = AcceptSync::with(self.n_leader.clone(), sfx, ld, true);
                            let msg = Message::with(self.pid, pid, PaxosMsg::AcceptSync(acc_sync));
                            self.outgoing.push(msg);
                        }
                    } else {
                        let sfx = self.storage.get_suffix(ld);
                        let acc_sync = AcceptSync::with(self.n_leader.clone(), sfx, ld, true);
                        let msg = Message::with(self.pid, pid, PaxosMsg::AcceptSync(acc_sync));
                        self.outgoing.push(msg);
                    }
                    #[cfg(feature = "batch_accept")]
                    {
                        self.batch_accept_meta[idx] =
                            Some((self.n_leader.clone(), self.outgoing.len() - 1));
                    }
                }
            }
        }
    }

    fn handle_promise_accept(&mut self, prom: Promise<R>, from: u64) {
        if prom.n == self.n_leader {
            let idx = from as usize - 1;
            self.lds[idx] = Some(prom.ld);
            let sfx_len = prom.sfx.len();
            let (promise_n, promise_sfx_len) = &(prom.n_accepted, sfx_len);
            let (max_round, max_sfx_len, _) = &self.max_promise_meta;
            let (sync, sfx_start) = if (promise_n, promise_sfx_len) == (max_round, max_sfx_len)
                && cfg!(feature = "max_accsync")
            {
                match max_sfx_len == &0 {
                    false => (false, self.acc_sync_ld + sfx_len as u64),
                    true if prom.ld >= self.acc_sync_ld => {
                        (false, self.acc_sync_ld + sfx_len as u64)
                    }
                    _ => (true, prom.ld),
                }
            } else {
                (true, prom.ld)
            };
            let sfx = self.storage.get_suffix(sfx_start);
            // println!("Handle promise from {} in Accept phase: {:?}, sfx len: {}", from, (sync, sfx_start), sfx.len());
            let acc_sync = AcceptSync::with(self.n_leader.clone(), sfx, prom.ld, sync);
            self.outgoing.push(Message::with(
                self.pid,
                from,
                PaxosMsg::AcceptSync(acc_sync),
            ));
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
                    let idx = from as usize - 1;
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
        if self.storage.get_promise() < prep.n {
            self.leader = from;
            self.storage.set_promise(prep.n.clone());
            self.state = (Role::Follower, Phase::Prepare);
            let na = self.storage.get_accepted_round();
            let sfx = if na >= prep.n_accepted {
                self.storage.get_suffix(prep.ld)
            } else {
                vec![]
            };
            let p = Promise::with(prep.n, na, sfx, self.storage.get_decided_len());
            self.outgoing
                .push(Message::with(self.pid, from, PaxosMsg::Promise(p)));
        }
    }

    fn handle_accept_sync(&mut self, acc_sync: AcceptSync<R>, from: u64) {
        if self.state == (Role::Follower, Phase::Prepare)
            && self.storage.get_promise() == acc_sync.n
        {
            self.storage.set_accepted_round(acc_sync.n.clone());
            let mut entries = acc_sync.entries;
            let la = if acc_sync.sync {
                self.storage.append_on_prefix(acc_sync.ld, &mut entries)
            } else {
                self.storage.append_sequence(&mut entries)
            };
            self.state = (Role::Follower, Phase::Accept);
            #[cfg(feature = "latest_accepted")]
            {
                let cached_idx = self.outgoing.len();
                self.latest_accepted_meta = Some((acc_sync.n.clone(), cached_idx));
            }
            let accepted = Accepted::with(acc_sync.n, la);
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
        if self.storage.get_promise() == f.n {
            match self.state {
                (Role::Follower, Phase::FirstAccept) => {
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
                _ => {}
            }
        }
    }

    fn handle_acceptdecide(&mut self, acc: AcceptDecide<R>) {
        if self.storage.get_promise() == acc.n {
            match self.state {
                (Role::Follower, Phase::Accept) => {
                    let mut entries = acc.entries;
                    self.accept_entries(acc.n, &mut entries);
                    // handle decide
                    if acc.ld > self.storage.get_decided_len() {
                        self.storage.set_decided_len(acc.ld);
                    }
                }
                _ => {}
            }
        }
    }

    fn handle_decide(&mut self, dec: Decide<R>) {
        if self.storage.get_promise() == dec.n {
            if self.state.1 != Phase::FirstAccept {
                self.storage.set_decided_len(dec.ld);
            }
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

    pub fn get_sequence(&self) -> Vec<Entry<R>> {
        self.storage.get_sequence()
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
