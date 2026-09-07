use core::time::Duration;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::time::Instant;

use futures::channel::oneshot;
use futures::{select_biased, FutureExt, StreamExt};

use omnipaxos::messages::async_runtime::{AsyncRuntimeMessage, AsyncRuntimeMsg, EntryId};
use omnipaxos::messages::Message;
use omnipaxos::storage::{Entry, Storage};
use omnipaxos::util::{LogEntry, NodeId};
use omnipaxos::OmniPaxos;

use super::event::{AppendError, Command, OmniPaxosEvent};
use super::traits::{ActorEntry, AsyncRuntime};

/// A pending `append_notify` awaiting decision. Uniform representation regardless
/// of whether the entry was accepted locally (we are leader) or forwarded (we are
/// follower). The `assigned` field is filled in by either code path; the drain
/// loop treats every pending item the same way.
struct Pending<T>
where
    T: Entry,
{
    id: EntryId,
    /// The entry, retained until we've dispatched it. Cleared once we've either
    /// accepted it locally or sent a `TaggedProposal` to the leader. When `Some`
    /// on each tick, `try_dispatch_undispatched` attempts to route it. This is
    /// what enables `append_notify` to be called before a leader is elected —
    /// the entry simply waits until routing becomes possible.
    undispatched: Option<T>,
    /// `(assigned_idx, ballot_n_at_assignment)`. `None` until either:
    ///   (a) we accepted the entry ourselves as leader, or
    ///   (b) we received `AsyncRuntimeMsg::Assigned` from the leader.
    /// The ballot lets us detect supersession after a leader change.
    assigned: Option<(usize, u32)>,
    deadline: Instant,
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

    own_pid: NodeId,
    append_notify_timeout: Duration,
    max_pending_appends: usize,
    max_decided_subscribers: usize,
    outgoing_buf: Vec<Message<T>>,
    pending: VecDeque<Pending<T>>,
    decided_subscribers: Vec<DecidedSub<T>>,
    last_leader: Option<(NodeId, bool)>,
    last_decided_idx: usize,
    last_reconfigured: bool,
}

/// A single subscriber to the decided-log stream. `next_idx` is the next log
/// index the actor must deliver; incremented on every successful `try_send`.
struct DecidedSub<T>
where
    T: Entry,
{
    next_idx: usize,
    tx: async_channel::Sender<LogEntry<T>>,
}

impl<T, B> ActorState<T, B>
where
    T: Entry,
    B: Storage<T>,
{
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        op: OmniPaxos<T, B>,
        cmd_rx: async_channel::Receiver<Command<T>>,
        incoming_rx: async_channel::Receiver<Message<T>>,
        outgoing_tx: async_channel::Sender<Message<T>>,
        event_tx: async_channel::Sender<OmniPaxosEvent>,
        tick_period: Duration,
        egress_period: Duration,
        append_notify_timeout: Duration,
        max_pending_appends: usize,
        max_decided_subscribers: usize,
    ) -> Self {
        let last_decided_idx = op.get_decided_idx();
        let last_leader = op.get_current_leader();
        let own_pid = op.get_pid();
        Self {
            op,
            cmd_rx,
            incoming_rx,
            outgoing_tx,
            event_tx,
            tick_period,
            egress_period,
            own_pid,
            append_notify_timeout,
            max_pending_appends,
            max_decided_subscribers,
            outgoing_buf: Vec::new(),
            pending: VecDeque::new(),
            decided_subscribers: Vec::new(),
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

    /// One drain path for every pending item. Never branches on "were we leader
    /// when this was created" — that context lives only in the code that writes
    /// `assigned`.
    fn drain_notifiers(&mut self) {
        let decided = self.op.get_decided_idx();
        let now = Instant::now();
        let cur_n = self.op.get_promise().n;
        let old = std::mem::take(&mut self.pending);
        for p in old {
            match p.assigned {
                Some((idx, _)) if idx <= decided => {
                    let _ = p.reply.send(Ok(idx));
                }
                Some((_, ballot_n)) if ballot_n < cur_n => {
                    let _ = p.reply.send(Err(AppendError::Superseded));
                }
                _ if now > p.deadline => {
                    let _ = p.reply.send(Err(AppendError::Timeout));
                }
                _ => self.pending.push_back(p),
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

    /// Push newly-decided entries to a single, freshly-added subscriber. Used
    /// only on the immediate catch-up path from `Command::SubscribeDecided`,
    /// where there is no shared read to amortize against. Returns `false` if
    /// the receiver was already closed.
    fn push_to_sub(op: &mut OmniPaxos<T, B>, sub: &mut DecidedSub<T>) -> bool {
        let decided = op.get_decided_idx();
        if sub.next_idx >= decided {
            return true;
        }
        let Some(entries) = op.read_decided_suffix(sub.next_idx) else {
            return true;
        };
        for entry in entries {
            match sub.tx.try_send(entry) {
                Ok(()) => sub.next_idx += 1,
                Err(async_channel::TrySendError::Full(_)) => return true,
                Err(async_channel::TrySendError::Closed(_)) => return false,
            }
        }
        true
    }

    /// Push newly-decided entries to every subscriber, reading the log suffix
    /// **once** and fanning out. Each sub keeps its own cursor, so slow subs
    /// don't block fast ones; the storage read is amortized across all subs
    /// starting from the trailing sub's position.
    fn push_to_all_subs(&mut self) {
        if self.decided_subscribers.is_empty() {
            return;
        }
        let cur = self.op.get_decided_idx();
        let min_next = self
            .decided_subscribers
            .iter()
            .map(|s| s.next_idx)
            .min()
            .expect("non-empty checked above");
        if min_next >= cur {
            return;
        }
        // Read once, from the trailing sub's position. `entries[i]` is the log
        // entry at position `min_next + i`.
        let Some(entries) = self.op.read_decided_suffix(min_next) else {
            return;
        };
        // Fan out. Preserve the drain-and-refill idiom so closed subs are
        // pruned without borrow-checker gymnastics.
        let subs = std::mem::take(&mut self.decided_subscribers);
        for mut sub in subs {
            // sub.next_idx should always be >= min_next; guard defensively.
            let start = sub.next_idx.saturating_sub(min_next);
            let mut alive = true;
            for entry in entries.iter().skip(start) {
                match sub.tx.try_send(entry.clone()) {
                    Ok(()) => sub.next_idx += 1,
                    Err(async_channel::TrySendError::Full(_)) => break,
                    Err(async_channel::TrySendError::Closed(_)) => {
                        alive = false;
                        break;
                    }
                }
            }
            if alive {
                self.decided_subscribers.push(sub);
            }
        }
    }

    fn record_assignment(&mut self, id: EntryId, idx: usize, ballot_n: u32) {
        for p in self.pending.iter_mut() {
            if p.id == id {
                p.assigned = Some((idx, ballot_n));
                return;
            }
        }
        // No matching pending — late reply after timeout drain, or duplicate. Ignore.
    }

    /// Leader-side accept. Runs when we originate an `append_notify` locally *or*
    /// when we receive a `TaggedProposal` from a follower. Populates the local
    /// pending's `assigned` and returns `(id, assigned_idx, promise_n)` if the
    /// entry was actually accepted, so callers processing a batch can collect
    /// these and send a single `Assigned` reply covering the whole batch.
    fn accept_as_leader(&mut self, id: EntryId, entry: T) -> Option<(EntryId, usize, u32)> {
        let before = self.op.get_accepted_idx();
        if self.op.append(entry).is_ok() {
            let after = self.op.get_accepted_idx();
            if after > before {
                let ballot_n = self.op.get_promise().n;
                self.record_assignment(id, after, ballot_n);
                return Some((id, after, ballot_n));
            }
            // else: leader in Prepare phase, entry buffered. No assignment recorded.
            // Origin (or ourselves) will resolve via Timeout unless we re-route later.
        }
        None
    }

    /// Attempt to dispatch every pending entry that hasn't yet been sent or
    /// accepted locally. Called from `Command::AppendNotify` and on each tick,
    /// which lets a call issued *before* an election eventually succeed once a
    /// leader is known.
    async fn try_dispatch_undispatched(&mut self) {
        let Some((leader, accepted)) = self.op.get_current_leader() else {
            return;
        };
        if leader == self.own_pid && !accepted {
            // We're leader but not yet in Accept phase — leave entries undispatched
            // and wait for the next tick.
            return;
        }
        // Every pending entry takes the same path this tick (leader/accepted are
        // fixed above), so snapshot them all at once. We take() out of pending so
        // the borrow doesn't overlap the following async sends/accepts.
        let entries: Vec<(EntryId, T)> = self
            .pending
            .iter_mut()
            .filter_map(|p| p.undispatched.take().map(|entry| (p.id, entry)))
            .collect();
        if leader == self.own_pid {
            for (id, entry) in entries {
                self.accept_as_leader(id, entry);
            }
        } else if !entries.is_empty() {
            // One wire message for the whole batch, rather than one per entry.
            let msg = Message::AsyncRuntime(AsyncRuntimeMessage {
                from: self.own_pid,
                to: leader,
                msg: AsyncRuntimeMsg::TaggedProposal { entries },
            });
            let _ = self.outgoing_tx.send(msg).await;
        }
    }

    async fn handle_runtime_msg(&mut self, arm: AsyncRuntimeMessage<T>) {
        match arm.msg {
            AsyncRuntimeMsg::TaggedProposal { entries } => {
                let cur = self.op.get_current_leader().map(|(p, _)| p);
                if cur == Some(self.own_pid) {
                    // Collect accepted assignments and reply to the origin with a
                    // single batched `Assigned`, rather than one message per entry.
                    let assigned: Vec<(EntryId, usize, u32)> = entries
                        .into_iter()
                        .filter_map(|(id, entry)| self.accept_as_leader(id, entry))
                        .collect();
                    if !assigned.is_empty() {
                        let reply_msg = Message::AsyncRuntime(AsyncRuntimeMessage {
                            from: self.own_pid,
                            to: arm.from,
                            msg: AsyncRuntimeMsg::Assigned { entries: assigned },
                        });
                        let _ = self.outgoing_tx.send(reply_msg).await;
                    }
                } else if let Some(new_leader) = cur {
                    // Leadership drifted since sender chose us. Hop the whole batch
                    // forward, preserving the original `from` so the leader replies
                    // to the true origin.
                    let fwd = Message::AsyncRuntime(AsyncRuntimeMessage {
                        from: arm.from,
                        to: new_leader,
                        msg: AsyncRuntimeMsg::TaggedProposal { entries },
                    });
                    let _ = self.outgoing_tx.send(fwd).await;
                }
                // Else: no leader; drop. Originators will timeout.
            }
            AsyncRuntimeMsg::Assigned { entries } => {
                for (id, assigned_idx, promise_n) in entries {
                    self.record_assignment(id, assigned_idx, promise_n);
                }
            }
        }
    }

    async fn handle_command(&mut self, cmd: Command<T>) {
        match cmd {
            Command::Append { entry, reply } => {
                let res = self.op.append(entry);
                let _ = reply.send(res);
            }
            Command::AppendNotify { entry, reply } => {
                if self.pending.len() >= self.max_pending_appends {
                    let _ = reply.send(Err(AppendError::TooManyOutstanding));
                    return;
                }
                let id = EntryId(uuid::Uuid::new_v4());
                self.pending.push_back(Pending {
                    id,
                    undispatched: Some(entry),
                    assigned: None,
                    deadline: Instant::now() + self.append_notify_timeout,
                    reply,
                });
                self.try_dispatch_undispatched().await;
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
            Command::SubscribeDecided { from, tx } => {
                if self.decided_subscribers.len() >= self.max_decided_subscribers {
                    // Reject: drop `tx` without storing it, closing the
                    // subscriber's channel immediately with no entries delivered.
                    return;
                }
                let mut sub = DecidedSub { next_idx: from, tx };
                // Immediate catch-up: push whatever is already decided from `from`.
                if Self::push_to_sub(&mut self.op, &mut sub) {
                    self.decided_subscribers.push(sub);
                }
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
                state.try_dispatch_undispatched().await;
                state.drain_notifiers();
                state.push_to_all_subs();
                tick_fut = Box::pin(R::sleep(state.tick_period)).fuse();
            }
            _ = egress_fut => {
                state.flush_outgoing().await;
                egress_fut = Box::pin(R::sleep(state.egress_period)).fuse();
            }
            in_msg = incoming_rx.next() => {
                match in_msg {
                    Some(Message::AsyncRuntime(arm)) => state.handle_runtime_msg(arm).await,
                    Some(m) => state.op.handle_incoming(m),
                    None => break,
                }
            }
            cmd = cmd_rx.next() => {
                match cmd {
                    Some(c) => state.handle_command(c).await,
                    None => break,
                }
            }
        }
    }
}
