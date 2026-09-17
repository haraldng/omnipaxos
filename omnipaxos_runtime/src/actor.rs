use core::time::Duration;
use std::collections::{HashMap, VecDeque};
use std::marker::PhantomData;
use std::time::Instant;

use futures::channel::oneshot;
use futures::{select_biased, FutureExt, StreamExt};

use omnipaxos::ballot_leader_election::Ballot;
use omnipaxos::messages::async_runtime::{AsyncRuntimeMessage, AsyncRuntimeMsg, EntryId};
use omnipaxos::messages::Message;
use omnipaxos::storage::{Entry, Storage};
use omnipaxos::util::{LogEntry, NodeId};
use omnipaxos::{OmniPaxos, ProposeErr};

use super::event::{AppendError, Command, OmniPaxosEvent};
use super::handle::RuntimeConfig;
use super::traits::{ActorEntry, AsyncRuntime};

/// Actor-loop-relevant subset of [`RuntimeConfig`]. Excludes the channel-capacity
/// fields, which are only needed once, to construct the channels in `spawn_actor`
/// before the actor itself is built — mirrors how `SequencePaxosConfig` in the core
/// crate trims down the public `OmniPaxosConfig` to just what that component needs.
pub(crate) struct ActorConfig {
    tick_period: Duration,
    egress_period: Duration,
    append_notify_timeout: Duration,
    max_pending_appends: usize,
    max_decided_subscribers: usize,
    max_outgoing_buffered: usize,
}

impl From<&RuntimeConfig> for ActorConfig {
    fn from(cfg: &RuntimeConfig) -> Self {
        Self {
            tick_period: cfg.tick_period,
            egress_period: cfg.egress_period,
            append_notify_timeout: cfg.append_notify_timeout,
            max_pending_appends: cfg.max_pending_appends,
            max_decided_subscribers: cfg.max_decided_subscribers,
            max_outgoing_buffered: cfg.max_outgoing_buffered,
        }
    }
}

/// A pending `append_notify` awaiting decision. Uniform representation regardless
/// of whether the entry was accepted locally (we are leader) or forwarded (we are
/// follower). The `assigned` field is filled in by either code path; the drain
/// loop treats every pending item the same way.
struct Pending<T>
where
    T: Entry,
{
    id: EntryId,
    /// The entry, retained until dispatched (accepted locally or sent as a
    /// `TaggedProposal`). While `Some`, `try_dispatch_undispatched` retries it
    /// each tick — this is what lets `append_notify` be called before a
    /// leader is known.
    undispatched: Option<T>,
    /// `(assigned_idx, ballot_at_assignment)`. `None` until either:
    ///   (a) we accepted the entry ourselves as leader, or
    ///   (b) we received `AsyncRuntimeMsg::Assigned` from the leader.
    assigned: Option<(usize, Ballot)>,
    deadline: Instant,
    reply: oneshot::Sender<Result<usize, AppendError<T>>>,
}

/// What to do, once its index is known, for a write buffered in the shared
/// batch buffer. Every code path that can add to that buffer pushes one of
/// these (see `track_untracked_batch_growth`), keeping `unconfirmed_local`'s
/// FIFO position in sync with the buffer regardless of source.
enum TrackIntent {
    /// Not tracked: a plain `Command::Append`, or an entry that entered the
    /// buffer via core-internal protocol handling we have no id for (a
    /// forwarded proposal, replicated entries, a reconfiguration stopsign).
    Untracked,
    /// Resolve a pending `append_notify` we own directly, via `self.pending`,
    /// once this index is known.
    Local(EntryId),
    /// Reply with `AsyncRuntimeMsg::Assigned` to `from` once this index is
    /// known -- used for entries that arrived via a remote `TaggedProposal`
    /// and got deferred by batching rather than accepted immediately (the
    /// immediate case is handled inline by `handle_runtime_msg` instead).
    Remote { id: EntryId, from: NodeId },
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
    pub(crate) config: ActorConfig,

    pid: NodeId,
    outgoing_buf: Vec<Message<T>>,
    pending: VecDeque<Pending<T>>,
    decided_subscribers: Vec<DecidedSub<T>>,
    last_leader: Option<(NodeId, bool)>,
    last_decided_idx: usize,
    last_reconfigured: bool,
    /// One entry per write currently sitting in the shared per-node batch
    /// buffer, in the order it was added, awaiting the flush that assigns it
    /// an index. Popping one entry per unit of `get_accepted_idx()` advance
    /// (`reconcile_unconfirmed`) lines up with what actually flushed. Paired
    /// with the ballot in effect when buffered so a leader change before the
    /// flush is detected rather than mis-assigned.
    unconfirmed_local: VecDeque<(TrackIntent, Ballot)>,
    last_seen_accepted_idx: usize,
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
    pub(crate) fn new(
        op: OmniPaxos<T, B>,
        cmd_rx: async_channel::Receiver<Command<T>>,
        incoming_rx: async_channel::Receiver<Message<T>>,
        outgoing_tx: async_channel::Sender<Message<T>>,
        event_tx: async_channel::Sender<OmniPaxosEvent>,
        config: ActorConfig,
    ) -> Self {
        let last_decided_idx = op.get_decided_idx();
        let last_leader = op.get_current_leader();
        let pid = op.get_pid();
        let accepted_idx = op.get_accepted_idx();
        Self {
            op,
            cmd_rx,
            incoming_rx,
            outgoing_tx,
            event_tx,
            config,
            pid,
            outgoing_buf: Vec::new(),
            pending: VecDeque::new(),
            decided_subscribers: Vec::new(),
            last_leader,
            last_decided_idx,
            last_reconfigured: false,
            unconfirmed_local: VecDeque::new(),
            last_seen_accepted_idx: accepted_idx,
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
        let now = Instant::now();
        let cur_ballot = self.op.get_promise();
        let old = std::mem::take(&mut self.pending);
        for p in old {
            match p.assigned {
                Some((idx, _)) if idx <= decided => {
                    let _ = p.reply.send(Ok(idx));
                }
                Some((idx, ballot)) if ballot < cur_ballot => {
                    let _ = p
                        .reply
                        .send(Err(AppendError::Superseded { log_entry_idx: idx }));
                }
                _ if now > p.deadline => {
                    let _ = p.reply.send(Err(AppendError::Timeout));
                }
                _ => self.pending.push_back(p),
            }
        }
    }

    /// Effective cap for `outgoing_buf`: the channel's own capacity when known,
    /// else [`ActorConfig::max_outgoing_buffered`].
    fn outgoing_buf_cap(&self) -> usize {
        self.outgoing_tx
            .capacity()
            .unwrap_or(self.config.max_outgoing_buffered)
    }

    /// Non-blocking drain of `outgoing_buf` onto `outgoing_tx`. Uses `try_send`
    /// so a merely-full channel (slow consumer) doesn't block the actor loop;
    /// unsent messages stay in `outgoing_buf` (in order) for the next attempt.
    /// If the buffer exceeds [`Self::outgoing_buf_cap`], oldest messages are
    /// dropped — see [`RuntimeConfig::max_outgoing_buffered`].
    fn drain_outgoing_buf(&mut self) {
        let cap = self.outgoing_buf_cap();
        trim_oldest(&mut self.outgoing_buf, cap);

        let pending = std::mem::take(&mut self.outgoing_buf);
        let mut iter = pending.into_iter();
        for msg in iter.by_ref() {
            match self.outgoing_tx.try_send(msg) {
                Ok(()) => {}
                Err(async_channel::TrySendError::Full(msg)) => {
                    self.outgoing_buf.push(msg);
                    break;
                }
                Err(async_channel::TrySendError::Closed(_)) => {
                    // Receiver dropped — user is not draining outgoing. Discard
                    // everything remaining; nothing left to retry against.
                    return;
                }
            }
        }
        self.outgoing_buf.extend(iter);
    }

    /// Enqueue one outgoing message without awaiting. Appends behind any
    /// already-buffered messages (preserves order), then tries to drain.
    /// Used for both core protocol traffic (via [`Self::flush_outgoing`]) and
    /// runtime control-plane messages (`TaggedProposal` / `Assigned`).
    fn enqueue_outgoing(&mut self, msg: Message<T>) {
        self.outgoing_buf.push(msg);
        self.drain_outgoing_buf();
    }

    /// Pull newly-produced OmniPaxos/BLE messages into `outgoing_buf` and drain.
    fn flush_outgoing(&mut self) {
        self.op.take_outgoing_messages(&mut self.outgoing_buf);
        self.drain_outgoing_buf();
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

    fn record_assignment(&mut self, id: EntryId, idx: usize, ballot: Ballot) {
        for p in self.pending.iter_mut() {
            if p.id == id {
                p.assigned = Some((idx, ballot));
                return;
            }
        }
        // No matching pending — late reply after timeout drain, or duplicate. Ignore.
    }

    /// Resolves a pending `append_notify` immediately with the propose error that
    /// just failed it, instead of leaving it to silently time out. Only affects
    /// entries this node itself owns in `self.pending` — when `id` came from a
    /// remote `TaggedProposal`, the owning `Pending` lives on the origin node, so
    /// this is a no-op there and that call still resolves via `Timeout`.
    fn resolve_pending_failed(&mut self, id: EntryId, err: ProposeErr<T>) {
        if let Some(pos) = self.pending.iter().position(|p| p.id == id) {
            let p = self.pending.remove(pos).unwrap();
            let _ = p.reply.send(Err(AppendError::Propose(err)));
        }
    }

    /// Appends `entry` to the local log, tracking it in `unconfirmed_local` if
    /// the write is genuinely deferred by batching (`get_batched_len()` grew)
    /// rather than immediately reflected in `get_accepted_idx()`. `intent`
    /// says what to do with the eventual index; plain `Command::Append` passes
    /// `TrackIntent::Untracked`.
    ///
    /// Must check `get_batched_len()` rather than inferring "batched" from
    /// `before == after`: a follower, or a leader still in `Phase::Prepare`,
    /// also has `before == after`, but the entry went to `buffered_proposals`
    /// or out as a `ProposalForward`, not the shared batch buffer — pushing a
    /// placeholder for that case would desync `unconfirmed_local`'s FIFO
    /// count. `accept_as_leader`'s stable-leader gate means only plain
    /// `Command::Append` can hit this case.
    fn append_tracked(
        &mut self,
        intent: TrackIntent,
        entry: T,
    ) -> Result<Option<(usize, Ballot)>, ProposeErr<T>> {
        let accepted_before = self.op.get_accepted_idx();
        let batched_before = self.op.get_batched_len();
        self.op.append(entry)?;
        let accepted_after = self.op.get_accepted_idx();
        let batched_after = self.op.get_batched_len();
        if accepted_after > accepted_before {
            let ballot = self.op.get_promise();
            if let TrackIntent::Local(id) = intent {
                self.record_assignment(id, accepted_after, ballot);
            }
            Ok(Some((accepted_after, ballot)))
        } else if batched_after > batched_before {
            // Genuinely deferred by batching.
            self.unconfirmed_local
                .push_back((intent, self.op.get_promise()));
            Ok(None)
        } else {
            // Went somewhere else entirely (forwarded, or buffered pending a
            // Prepare -> Accept transition) -- nothing entered the shared
            // batch buffer, so there's nothing to track here.
            Ok(None)
        }
    }

    /// Leader-side accept, for an entry we originate locally (`from: None`)
    /// or received via `TaggedProposal` from follower `from`. Populates the
    /// pending's `assigned` (or, if buffered by batching, queues a
    /// `TrackIntent` for `reconcile_unconfirmed` to resolve later) and
    /// returns `(id, assigned_idx)` if accepted immediately, so batch callers
    /// can collect these and send one `Assigned` reply for the whole batch.
    fn accept_as_leader(
        &mut self,
        id: EntryId,
        entry: T,
        from: Option<NodeId>,
    ) -> Option<(EntryId, usize)> {
        // Only accept while leadership is stable (Accept phase).
        let stable_leader =
            matches!(self.op.get_current_leader(), Some((pid, true)) if pid == self.pid);
        if !stable_leader {
            return None;
        }
        let intent = match from {
            None => TrackIntent::Local(id),
            Some(from) => TrackIntent::Remote { id, from },
        };
        match self.append_tracked(intent, entry) {
            Ok(Some((after, _ballot))) => Some((id, after)),
            Ok(None) => None,
            Err(err) => {
                self.resolve_pending_failed(id, err);
                None
            }
        }
    }

    /// Matches everything buffered in `unconfirmed_local` to the index it's
    /// assigned once its batch flushes and `get_accepted_idx()` advances.
    /// FIFO correspondence is exact here regardless of what added to the
    /// shared batch buffer (see `unconfirmed_local`'s doc comment), as long as
    /// the ballot hasn't changed since the entry was buffered; if it has,
    /// leadership moved on and this index may belong to different writes, so
    /// we drop tracking for it and let the caller time out.
    fn reconcile_unconfirmed(&mut self) {
        let cur_ballot = self.op.get_promise();
        let accepted = self.op.get_accepted_idx();
        let mut idx = self.last_seen_accepted_idx;
        // Entries from a single remote TaggedProposal batch can resolve
        // across several different `idx` values here (their own batch may
        // have been interleaved with others' writes); group by origin so
        // each gets one `Assigned` reply instead of one message per entry.
        let mut remote_assigned: HashMap<NodeId, Vec<(EntryId, usize)>> = HashMap::new();
        while idx < accepted {
            idx += 1;
            let Some((intent, ballot)) = self.unconfirmed_local.pop_front() else {
                break;
            };
            if ballot != cur_ballot {
                // Leadership moved on since this was buffered; the index may
                // belong to different writes. Drop tracking -- Local/Remote
                // origins simply time out, matching the ballot-mismatch
                // handling in `drain_notifiers` for already-assigned entries.
                continue;
            }
            match intent {
                TrackIntent::Untracked => {}
                TrackIntent::Local(id) => self.record_assignment(id, idx, ballot),
                TrackIntent::Remote { id, from } => {
                    remote_assigned.entry(from).or_default().push((id, idx));
                }
            }
        }
        self.last_seen_accepted_idx = accepted;

        for (from, entries) in remote_assigned {
            self.enqueue_outgoing(Message::AsyncRuntime(AsyncRuntimeMessage {
                from: self.pid,
                to: from,
                msg: AsyncRuntimeMsg::Assigned {
                    ballot: cur_ballot,
                    entries,
                },
            }));
        }
    }

    /// Attempt to dispatch every pending entry that hasn't yet been sent or
    /// accepted locally. Called from `Command::AppendNotify` and on each tick,
    /// which lets a call issued *before* an election eventually succeed once a
    /// leader is known.
    fn try_dispatch_undispatched(&mut self) {
        let Some((leader, accepted)) = self.op.get_current_leader() else {
            return;
        };
        if leader == self.pid && !accepted {
            // We're leader but not yet in Accept phase — leave entries undispatched
            // and wait for the next tick.
            return;
        }
        let entries: Vec<(EntryId, T)> = self
            .pending
            .iter_mut()
            .filter_map(|p| p.undispatched.take().map(|entry| (p.id, entry)))
            .collect();
        if leader == self.pid {
            for (id, entry) in entries {
                self.accept_as_leader(id, entry, None);
            }
        } else if !entries.is_empty() {
            self.enqueue_outgoing(Message::AsyncRuntime(AsyncRuntimeMessage {
                from: self.pid,
                to: leader,
                msg: AsyncRuntimeMsg::TaggedProposal { entries },
            }));
        }
    }

    fn handle_runtime_msg(&mut self, arm: AsyncRuntimeMessage<T>) {
        match arm.msg {
            AsyncRuntimeMsg::TaggedProposal { entries } => {
                let cur = self.op.get_current_leader().map(|(p, _)| p);
                if cur == Some(self.pid) {
                    // Collect accepted assignments and reply to the origin with a
                    // single batched `Assigned`.
                    let from = arm.from;
                    let assigned: Vec<(EntryId, usize)> = entries
                        .into_iter()
                        .filter_map(|(id, entry)| self.accept_as_leader(id, entry, Some(from)))
                        .collect();
                    if !assigned.is_empty() {
                        let ballot = self.op.get_promise();
                        self.enqueue_outgoing(Message::AsyncRuntime(AsyncRuntimeMessage {
                            from: self.pid,
                            to: arm.from,
                            msg: AsyncRuntimeMsg::Assigned {
                                ballot,
                                entries: assigned,
                            },
                        }));
                    }
                } else if let Some(new_leader) = cur {
                    // Leadership drifted since sender chose us. Hop the whole batch
                    // forward, preserving the original `from` so the leader replies
                    // to the true origin.
                    self.enqueue_outgoing(Message::AsyncRuntime(AsyncRuntimeMessage {
                        from: arm.from,
                        to: new_leader,
                        msg: AsyncRuntimeMsg::TaggedProposal { entries },
                    }));
                }
                // Else: no leader; drop. Originators will timeout.
            }
            AsyncRuntimeMsg::Assigned { ballot, entries } => {
                for (id, assigned_idx) in entries {
                    self.record_assignment(id, assigned_idx, ballot);
                }
            }
        }
    }

    /// Given a `(batched_len, accepted_idx)` snapshot taken before some call
    /// that may have silently added to the shared batch buffer (a forwarded
    /// proposal, an incoming replication batch, a reconfiguration stopsign),
    /// pushes a `TrackIntent::Untracked` placeholder in `unconfirmed_local`
    /// for each newly-added entry, keeping its FIFO count accurate.
    ///
    /// Growth is `(accepted_after - accepted_before) + (batched_after -
    /// batched_before)`, computed with signed arithmetic and clamped at 0:
    /// `accepted_idx` can legitimately *decrease* when a follower's log is
    /// truncated by an incoming `AcceptSync` from a new leader, which isn't a
    /// case with anything new to track.
    fn track_batch_growth(&mut self, batched_before: usize, accepted_before: usize) {
        let batched_after = self.op.get_batched_len();
        let accepted_after = self.op.get_accepted_idx();
        let added = accepted_after as isize - accepted_before as isize + batched_after as isize
            - batched_before as isize;
        let ballot = self.op.get_promise();
        for _ in 0..added.max(0) {
            self.unconfirmed_local
                .push_back((TrackIntent::Untracked, ballot));
        }
    }

    /// Delivers an incoming `SequencePaxos`/BLE message to the core, tracking
    /// any entries it silently adds to the shared batch buffer -- see
    /// `track_batch_growth`.
    fn handle_incoming_tracked(&mut self, m: Message<T>) {
        let batched_before = self.op.get_batched_len();
        let accepted_before = self.op.get_accepted_idx();
        self.op.handle_incoming(m);
        self.track_batch_growth(batched_before, accepted_before);
    }

    async fn handle_command(&mut self, cmd: Command<T>) {
        match cmd {
            Command::Append { entry, reply } => {
                // Via append_tracked (not self.op.append) so a deferred write
                // still gets its unconfirmed_local placeholder, keeping FIFO
                // position exact for any append_notify entries batched alongside it.
                let res = self
                    .append_tracked(TrackIntent::Untracked, entry)
                    .map(|_| ());
                let _ = reply.send(res);
            }
            Command::AppendNotify {
                entry,
                timeout,
                reply,
            } => {
                if self.pending.len() >= self.config.max_pending_appends {
                    let _ = reply.send(Err(AppendError::TooManyOutstanding));
                    return;
                }
                let id = EntryId(uuid::Uuid::new_v4());
                let timeout = timeout.unwrap_or(self.config.append_notify_timeout);
                self.pending.push_back(Pending {
                    id,
                    undispatched: Some(entry),
                    assigned: None,
                    deadline: Instant::now() + timeout,
                    reply,
                });
                self.try_dispatch_undispatched();
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
                if self.decided_subscribers.len() >= self.config.max_decided_subscribers {
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
                // Wrapped like handle_incoming_tracked: a stopsign accepted as
                // leader is itself an untracked addition to the shared batch buffer.
                let batched_before = self.op.get_batched_len();
                let accepted_before = self.op.get_accepted_idx();
                let res = self.op.reconfigure(new_configuration, metadata);
                self.track_batch_growth(batched_before, accepted_before);
                let _ = reply.send(res);
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
    let mut tick_fut = Box::pin(R::sleep(state.config.tick_period)).fuse();
    let mut egress_fut = Box::pin(R::sleep(state.config.egress_period)).fuse();

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
                state.try_dispatch_undispatched();
                state.reconcile_unconfirmed();
                state.drain_notifiers();
                state.push_to_all_subs();
                tick_fut = Box::pin(R::sleep(state.config.tick_period)).fuse();
            }
            _ = egress_fut => {
                state.flush_outgoing();
                egress_fut = Box::pin(R::sleep(state.config.egress_period)).fuse();
            }
            in_msg = incoming_rx.next() => {
                match in_msg {
                    Some(Message::AsyncRuntime(arm)) => state.handle_runtime_msg(arm),
                    Some(m) => state.handle_incoming_tracked(m),
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

/// Drops the oldest elements of `buf` until its length is at most `cap`, preserving
/// the relative order of what remains. A no-op if `buf` is already within `cap`.
fn trim_oldest<M>(buf: &mut Vec<M>, cap: usize) {
    if buf.len() > cap {
        let excess = buf.len() - cap;
        buf.drain(..excess);
    }
}

#[cfg(test)]
mod tests {
    use super::trim_oldest;

    #[test]
    fn trim_oldest_drops_front_keeps_cap_most_recent() {
        let mut buf = vec![1, 2, 3, 4, 5];
        trim_oldest(&mut buf, 3);
        assert_eq!(buf, vec![3, 4, 5]);
    }

    #[test]
    fn trim_oldest_is_noop_when_within_cap() {
        let mut buf = vec![1, 2, 3];
        trim_oldest(&mut buf, 5);
        assert_eq!(buf, vec![1, 2, 3]);
    }

    #[test]
    fn trim_oldest_is_noop_when_exactly_at_cap() {
        let mut buf = vec![1, 2, 3];
        trim_oldest(&mut buf, 3);
        assert_eq!(buf, vec![1, 2, 3]);
    }

    #[test]
    fn trim_oldest_can_drop_everything() {
        let mut buf = vec![1, 2, 3];
        trim_oldest(&mut buf, 0);
        assert!(buf.is_empty());
    }
}
