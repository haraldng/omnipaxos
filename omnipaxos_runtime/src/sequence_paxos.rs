use omnipaxos_core::{
    ballot_leader_election::Ballot,
    messages::Message,
    sequence_paxos::{
        CompactionErr, ProposeErr, ReconfigurationRequest, SequencePaxos, SequencePaxosConfig,
    },
    storage::{Entry, Snapshot, Storage},
};
use std::time::Duration;

use crate::{omnipaxos::ReadEntry, util::Stop};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time,
};

pub(crate) struct SequencePaxosComp<T, S, B>
where
    T: Entry + Send + 'static,
    S: Snapshot<T> + Send + 'static,
    B: Storage<T, S> + Send + 'static,
{
    outgoing: mpsc::Sender<Message<T, S>>,
    incoming: mpsc::Receiver<Message<T, S>>,
    local_requests: mpsc::Receiver<Request<T, S>>,
    ble: watch::Receiver<Ballot>,
    seq_paxos: SequencePaxos<T, S, B>,
    stop: oneshot::Receiver<Stop>,
}

impl<T, S, B> SequencePaxosComp<T, S, B>
where
    T: Entry + Send + 'static,
    S: Snapshot<T> + Send + 'static,
    B: Storage<T, S> + Send + 'static,
{
    pub(crate) fn new(
        sp_config: SequencePaxosConfig,
        storage: B,
        local: mpsc::Receiver<Request<T, S>>,
        incoming: mpsc::Receiver<Message<T, S>>,
        outgoing: mpsc::Sender<Message<T, S>>,
        ble: watch::Receiver<Ballot>,
        stop: oneshot::Receiver<Stop>,
    ) -> Self {
        let sp = SequencePaxos::with(sp_config, storage);
        Self {
            seq_paxos: sp,
            incoming,
            outgoing,
            ble,
            local_requests: local,
            stop,
        }
    }

    fn handle_local(&mut self, r: Request<T, S>) {
        match r {
            Request::Append(entry, sender) => {
                //println!("{:?}", entry);
                sender
                    .send(self.seq_paxos.append(entry))
                    .expect("Failed to reply append request");
            }
            Request::GetDecidedIdx(sender) => {
                sender
                    .send(self.seq_paxos.get_decided_idx())
                    .expect("Failed to reply get_decided_idx request");
            }
            Request::GetLeader(sender) => {
                sender
                    .send(self.seq_paxos.get_current_leader())
                    .expect("Failed to reply current leader request");
            }
            Request::Read(r) => {
                let sender = r.sender;
                let ents = match r.to_idx {
                    Some(idx) => self.seq_paxos.read_entries(r.from_idx..idx),
                    None => self.seq_paxos.read_entries(r.from_idx..),
                };
                let resp =
                    ents.map(|entries| entries.into_iter().map(|l| ReadEntry::from(l)).collect());
                sender
                    .send(resp)
                    .unwrap_or_else(|_| panic!("Failed to reply read"));
            }
            Request::Trim(idx, sender) => {
                sender
                    .send(self.seq_paxos.trim(idx))
                    .expect("Failed to reply current leader request");
            }
            Request::Snapshot(idx, local_only, sender) => {
                sender
                    .send(self.seq_paxos.snapshot(idx, local_only))
                    .expect("Failed to reply current leader request");
            }
            Request::GetCompactedIdx(sender) => {
                sender
                    .send(self.seq_paxos.get_compacted_idx())
                    .expect("Failed to reply get_decided_idx request");
            }
            Request::ReadDecidedSuffix(r) => {
                let sender = r.sender;
                let idx = r.from_idx;
                let ents = self.seq_paxos.read_decided_suffix(idx);
                let resp =
                    ents.map(|entries| entries.into_iter().map(|l| ReadEntry::from(l)).collect());
                sender
                    .send(resp)
                    .unwrap_or_else(|_| panic!("Failed to reply read"));
            }
            Request::Reconfigure(rc, sender) => {
                sender
                    .send(self.seq_paxos.reconfigure(rc))
                    .expect("Failed to reply reconfiguration request");
            }
            Request::Reconnected(pid) => {
                self.seq_paxos.reconnected(pid);
            }
        }
    }

    fn handle_incoming(&mut self, msg: Message<T, S>) {
        self.seq_paxos.handle(msg);
    }

    fn handle_leader_change(&mut self, b: Ballot) {
        self.seq_paxos.handle_leader(b);
    }

    async fn send_outgoing_msgs(&mut self) {
        for msg in self.seq_paxos.get_outgoing_msgs() {
            if let Err(_) = self.outgoing.send(msg).await {
                panic!("Outgoing channel dropped");
            }
        }
    }

    pub(crate) async fn run(&mut self) {
        let mut interval = time::interval(Duration::from_millis(100)); // TODO
        loop {
            tokio::select! {
                //biased; // TODO
                //周期性的发送outgoing massages
                _ = interval.tick() => { self.send_outgoing_msgs().await; },
                //wait for a change notification
                Ok(_) = self.ble.changed() => {
                    //return the most recently sent value
                    let ballot = *self.ble.borrow();
                    self.handle_leader_change(ballot);
                },
                Some(in_msg) = self.incoming.recv() => { self.handle_incoming(in_msg); },
                Some(local) = self.local_requests.recv() => { 
                    self.handle_local(local); 
                },
                _ = &mut self.stop => { break; },
                else => { }
            }
        }
    }
}

pub struct SequencePaxosHandle<T: Entry, S: Snapshot<T>> {
    pub incoming: mpsc::Sender<Message<T, S>>,
    pub outgoing: mpsc::Receiver<Message<T, S>>,
}

impl<T: Entry, S: Snapshot<T>> SequencePaxosHandle<T, S> {
    pub(crate) fn with(
        incoming: mpsc::Sender<Message<T, S>>,
        outgoing: mpsc::Receiver<Message<T, S>>,
    ) -> Self {
        Self { incoming, outgoing }
    }
}

pub(crate) struct InternalSPHandle<T: Entry, S: Snapshot<T>> {
    pub stop: Option<oneshot::Sender<Stop>>, // wrap in option to be able to move it when stopping
    pub local_requests: mpsc::Sender<Request<T, S>>,
}

impl<T, S> InternalSPHandle<T, S>
where
    T: Entry + Send + 'static,
    S: Snapshot<T> + Send + 'static,
{
    pub(crate) fn with(
        stop: oneshot::Sender<Stop>,
        local_requests: mpsc::Sender<Request<T, S>>,
    ) -> Self {
        Self {
            stop: Some(stop),
            local_requests,
        }
    }
}

#[derive(Debug)]
pub(crate) enum Request<T: Entry, S: Snapshot<T>> {
    Append(T, oneshot::Sender<Result<(), ProposeErr<T>>>),
    GetDecidedIdx(oneshot::Sender<u64>),
    GetLeader(oneshot::Sender<u64>),
    Read(ReadRequest<T, S>),
    Trim(Option<u64>, oneshot::Sender<Result<(), CompactionErr>>),
    Snapshot(
        Option<u64>,
        bool,
        oneshot::Sender<Result<(), CompactionErr>>,
    ),
    GetCompactedIdx(oneshot::Sender<u64>),
    ReadDecidedSuffix(ReadRequest<T, S>),
    Reconfigure(
        ReconfigurationRequest,
        oneshot::Sender<Result<(), ProposeErr<T>>>,
    ),
    Reconnected(u64),
}

#[derive(Debug)]
pub(crate) struct ReadRequest<T: Entry, S: Snapshot<T>> {
    from_idx: u64,
    to_idx: Option<u64>,
    sender: oneshot::Sender<Option<Vec<ReadEntry<T, S>>>>,
}

impl<T: Entry, S: Snapshot<T>> ReadRequest<T, S> {
    pub(crate) fn with(
        from_idx: u64,
        to_idx: Option<u64>,
        sender: oneshot::Sender<Option<Vec<ReadEntry<T, S>>>>,
    ) -> Self {
        Self {
            from_idx,
            to_idx,
            sender,
        }
    }
}
