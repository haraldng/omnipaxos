use core::time::Duration;
use std::collections::VecDeque;
use std::marker::PhantomData;

use futures::channel::oneshot;
use futures::{select_biased, FutureExt, StreamExt};

use crate::ballot_leader_election::Ballot;
use crate::messages::Message;
use crate::storage::{Entry, Storage};
use crate::util::NodeId;
use crate::OmniPaxos;

use super::event::{AppendError, Command, OmniPaxosEvent};
use super::traits::{ActorEntry, AsyncRuntime};

/// A pending `append_notify` awaiting decision.
struct Pending<T>
where
    T: Entry,
{
    /// The 1-based accepted index at which we believe the entry landed.
    tentative: usize,
    /// The ballot round under which the entry was accepted. If the current ballot
    /// promotes above this, the entry may have been overwritten and we fail with
    /// [`AppendError::Superseded`].
    promise_n: u32,
    reply: oneshot::Sender<Result<usize, AppendError<T>>>,
}

pub(crate) struct ActorState<T, B>
where
    T: Entry,
    B: Storage<T>,
{
    pub(crate) op: OmniPaxos<T, B>,
    pub(crate) cmd_rx: async_channel::Receiver<Command<T>>,
    pub(crate) incoming_rx: async_channel::Receiver<Message<T>>,
    pub(crate) outgoing_tx: async_channel::Sender<Message<T>>,
    pub(crate) event_tx: async_channel::Sender<OmniPaxosEvent>,
    pub(crate) tick_period: Duration,
    pub(crate) egress_period: Duration,

    outgoing_buf: Vec<Message<T>>,
    pending: VecDeque<Pending<T>>,
    last_leader: Option<(NodeId, bool)>,
    last_decided_idx: usize,
    last_reconfigured: bool,
}

impl<T, B> ActorState<T, B>
where
    T: Entry,
    B: Storage<T>,
{
    pub(crate) fn new(
        op: OmniPaxos<T, B>,
        cmd_rx: async_channel::Receiver<Command<T>>,
        incoming_rx: async_channel::Receiver<Message<T>>,
        outgoing_tx: async_channel::Sender<Message<T>>,
        event_tx: async_channel::Sender<OmniPaxosEvent>,
        tick_period: Duration,
        egress_period: Duration,
    ) -> Self {
        let last_decided_idx = op.get_decided_idx();
        let last_leader = op.get_current_leader();
        Self {
            op,
            cmd_rx,
            incoming_rx,
            outgoing_tx,
            event_tx,
            tick_period,
            egress_period,
            outgoing_buf: Vec::new(),
            pending: VecDeque::new(),
            last_leader,
            last_decided_idx,
            last_reconfigured: false,
        }
    }

    async fn detect_and_emit_events(&mut self) {
        let cur = self.op.get_current_leader();
        if cur != self.last_leader {
            if let Some((pid, accepted)) = cur {
                let _ = self
                    .event_tx
                    .send(OmniPaxosEvent::LeaderElected { pid, accepted })
                    .await;
            }
            self.last_leader = cur;
        }
        let d = self.op.get_decided_idx();
        if d > self.last_decided_idx {
            let _ = self
                .event_tx
                .send(OmniPaxosEvent::Decided { new_decided_idx: d })
                .await;
            self.last_decided_idx = d;
        }
        if !self.last_reconfigured {
            if let Some(_ss) = self.op.is_reconfigured() {
                let _ = self
                    .event_tx
                    .send(OmniPaxosEvent::Reconfigured {
                        stop_sign_idx: self.op.get_decided_idx(),
                    })
                    .await;
                self.last_reconfigured = true;
            }
        }
    }

    fn drain_notifiers(&mut self) {
        let decided = self.op.get_decided_idx();
        let cur_ballot: Ballot = self.op.get_promise();
        while let Some(head) = self.pending.front() {
            if head.tentative <= decided {
                let p = self.pending.pop_front().unwrap();
                let _ = p.reply.send(Ok(p.tentative));
            } else if head.promise_n < cur_ballot.n {
                let p = self.pending.pop_front().unwrap();
                let _ = p.reply.send(Err(AppendError::Superseded));
            } else {
                break;
            }
        }
    }

    async fn flush_outgoing(&mut self) {
        self.op.take_outgoing_messages(&mut self.outgoing_buf);
        for msg in self.outgoing_buf.drain(..) {
            if self.outgoing_tx.send(msg).await.is_err() {
                // Receiver dropped — user is not draining outgoing. Discard.
                break;
            }
        }
    }

    fn handle_command(&mut self, cmd: Command<T>) {
        match cmd {
            Command::Append { entry, reply } => {
                let res = self.op.append(entry);
                let _ = reply.send(res);
            }
            Command::AppendNotify { entry, reply } => {
                let before = self.op.get_accepted_idx();
                match self.op.append(entry) {
                    Err(e) => {
                        let _ = reply.send(Err(AppendError::Propose(e)));
                    }
                    Ok(()) => {
                        let after = self.op.get_accepted_idx();
                        if after > before {
                            // Locally accepted (we are the leader in Accept phase).
                            let promise_n = self.op.get_promise().n;
                            self.pending.push_back(Pending {
                                tentative: after,
                                promise_n,
                                reply,
                            });
                        } else {
                            // Entry was forwarded (or leader is in Prepare phase and buffered
                            // the proposal) — we can't correlate it with a decided index.
                            let current_leader = self.op.get_current_leader().map(|(pid, _)| pid);
                            let _ = reply.send(Err(AppendError::NotLeader { current_leader }));
                        }
                    }
                }
            }
            Command::CurrentLeader { reply } => {
                let _ = reply.send(self.op.get_current_leader());
            }
            Command::DecidedIdx { reply } => {
                let _ = reply.send(self.op.get_decided_idx());
            }
            Command::ReadDecidedSuffix { from, reply } => {
                let _ = reply.send(self.op.read_decided_suffix(from));
            }
            Command::Reconfigure {
                new_configuration,
                metadata,
                reply,
            } => {
                let _ = reply.send(self.op.reconfigure(new_configuration, metadata));
            }
            Command::TryBecomeLeader => {
                self.op.try_become_leader();
            }
            Command::Reconnected { pid } => {
                self.op.reconnected(pid);
            }
        }
    }
}

/// Drives the actor. Runs until all handles are dropped (both `cmd_tx` and
/// `incoming_tx` senders closed).
pub(crate) async fn run<T, B, R>(mut state: ActorState<T, B>, _rt: PhantomData<fn() -> R>)
where
    T: ActorEntry,
    B: Storage<T> + Send + 'static,
    R: AsyncRuntime,
{
    let mut tick_fut = Box::pin(R::sleep(state.tick_period)).fuse();
    let mut egress_fut = Box::pin(R::sleep(state.egress_period)).fuse();

    // async_channel::Receiver is !Unpin; keep it in a Pin<Box<...>> so the Stream
    // impl works under select_biased!.
    let cmd_rx = std::mem::replace(&mut state.cmd_rx, async_channel::bounded(1).1);
    let incoming_rx = std::mem::replace(&mut state.incoming_rx, async_channel::bounded(1).1);
    let mut cmd_rx = Box::pin(cmd_rx);
    let mut incoming_rx = Box::pin(incoming_rx);

    loop {
        select_biased! {
            _ = tick_fut => {
                state.op.tick();
                state.detect_and_emit_events().await;
                state.drain_notifiers();
                tick_fut = Box::pin(R::sleep(state.tick_period)).fuse();
            }
            _ = egress_fut => {
                state.flush_outgoing().await;
                egress_fut = Box::pin(R::sleep(state.egress_period)).fuse();
            }
            in_msg = incoming_rx.next() => {
                match in_msg {
                    Some(m) => state.op.handle_incoming(m),
                    None => break,
                }
            }
            cmd = cmd_rx.next() => {
                match cmd {
                    Some(c) => state.handle_command(c),
                    None => break,
                }
            }
        }
    }
}
