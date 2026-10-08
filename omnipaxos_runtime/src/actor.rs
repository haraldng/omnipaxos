use core::time::Duration;
use std::{
    collections::{HashMap, VecDeque},
    marker::PhantomData,
    time::Instant,
};

use futures::{select_biased, FutureExt, StreamExt};

use omnipaxos::{
    ballot_leader_election::Ballot,
    messages::{
        async_runtime::{AsyncRuntimeMessage, AsyncRuntimeMsg, EntryId},
        Message,
    },
    storage::{Entry, Storage},
    util::{LogEntry, NodeId},
    OmniPaxos, ProposeErr,
};

use super::{
    event::{AppendError, Command, OmniPaxosEvent},
    handle::RuntimeConfig,
    pending::PendingAppends,
    traits::{ActorEntry, AsyncRuntime},
};

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
    pending: PendingAppends<T>,
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
            pending: PendingAppends::new(),
            decided_subscribers: Vec::new(),
            last_leader,
            last_decided_idx,
            last_reconfigured: false,
            unconfirmed_local: VecDeque::new(),
            last_seen_accepted_idx: accepted_idx,
        }
    }

    fn detect_and_emit_events(&mut self) {
        let cur = self.op.get_current_leader();
        if cur != self.last_leader {
            if let Some((pid, accepted)) = cur {
                self.emit_event(OmniPaxosEvent::LeaderElected { pid, accepted });
            }
            self.last_leader = cur;
        }
        let d = self.op.get_decided_idx();
        if d > self.last_decided_idx {
            self.emit_event(OmniPaxosEvent::Decided { new_decided_idx: d });
            self.last_decided_idx = d;
        }
        if !self.last_reconfigured {
            if let Some(_ss) = self.op.is_reconfigured() {
                self.emit_event(OmniPaxosEvent::Reconfigured {
                    stop_sign_idx: self.op.get_decided_idx(),
                });
                self.last_reconfigured = true;
            }
        }
    }

    /// Never blocks: the handle keeps the event receiver alive even if nobody
    /// reads it, so awaiting room would stall the actor loop forever. When the
    /// channel is full the oldest event is dropped; every event carries absolute
    /// state, so the newest ones are what a lagging consumer needs.
    fn emit_event(&self, event: OmniPaxosEvent) {
        let _ = self.event_tx.force_send(event);
    }

    fn drain_notifiers(&mut self) {
        self.pending.resolve(
            self.op.get_decided_idx(),
            self.op.get_promise(),
            Instant::now(),
        );
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

    /// Push newly-decided entries to every subscriber, reading only what fits in
    /// their channels. Each sub keeps its own cursor, so slow subs don't block
    /// fast ones. Subs whose wanted ranges overlap share one read, so subs near
    /// the head of the log cost a single read between them, and a lagging sub
    /// never causes more than its free channel space to be read. Cheap when
    /// there is nothing to push, so it can run after every handled message.
    fn push_to_all_subs(&mut self) {
        self.decided_subscribers.retain(|s| !s.tx.is_closed());
        let decided = self.op.get_decided_idx();
        let mut wanted: Vec<(usize, usize)> = self
            .decided_subscribers
            .iter()
            .filter_map(|s| {
                let free =
                    s.tx.capacity()
                        .map_or(usize::MAX, |cap| cap.saturating_sub(s.tx.len()));
                let end = decided.min(s.next_idx.saturating_add(free));
                (s.next_idx < end).then_some((s.next_idx, end))
            })
            .collect();
        wanted.sort_unstable();
        let mut reads: Vec<(usize, usize)> = Vec::new();
        for (from, to) in wanted {
            match reads.last_mut() {
                Some((_, end)) if from <= *end => *end = (*end).max(to),
                _ => reads.push((from, to)),
            }
        }
        for (from, to) in reads {
            let Some(entries) = self.op.read_entries(from..to) else {
                continue;
            };
            let ends = entry_ends(from, &entries);
            for sub in self
                .decided_subscribers
                .iter_mut()
                .filter(|s| (from..to).contains(&s.next_idx))
            {
                for (entry, &end) in entries.iter().zip(&ends) {
                    if end <= sub.next_idx {
                        continue;
                    }
                    if sub.tx.try_send(entry.clone()).is_err() {
                        break;
                    }
                    sub.next_idx = end;
                }
            }
        }
    }

    /// Reacts to whatever just changed: dispatches waiting entries, assigns
    /// indices to flushed batches, resolves finished `append_notify` calls,
    /// emits events, pushes decided entries, and forwards outgoing messages.
    /// Runs after every handled burst of messages or commands and after every
    /// tick, so callers see a decision as soon as the message carrying it is
    /// handled rather than on the next tick.
    fn react(&mut self) {
        self.try_dispatch_undispatched();
        self.reconcile_unconfirmed();
        self.drain_notifiers();
        self.detect_and_emit_events();
        self.push_to_all_subs();
        self.flush_outgoing();
    }

    fn record_assignment(&mut self, id: EntryId, idx: usize, ballot: Ballot) {
        self.pending.assign(id, idx, ballot);
    }

    /// Resolves a pending `append_notify` immediately with the propose error that
    /// just failed it, instead of leaving it to silently time out. Only affects
    /// entries this node itself owns in `self.pending` — when `id` came from a
    /// remote `TaggedProposal`, the owning call lives on the origin node, so
    /// this is a no-op there and that call still resolves via `Timeout`.
    fn resolve_pending_failed(&mut self, id: EntryId, err: ProposeErr<T>) {
        self.pending.fail(id, err);
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
        let entries = self.pending.take_undispatched();
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
    fn handle_incoming(&mut self, m: Message<T>) {
        match m {
            Message::AsyncRuntime(arm) => self.handle_runtime_msg(arm),
            m => self.handle_incoming_tracked(m),
        }
    }

    fn handle_incoming_tracked(&mut self, m: Message<T>) {
        let batched_before = self.op.get_batched_len();
        let accepted_before = self.op.get_accepted_idx();
        self.op.handle_incoming(m);
        self.track_batch_growth(batched_before, accepted_before);
    }

    fn handle_command(&mut self, cmd: Command<T>) {
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
                self.pending
                    .insert(id, entry, Instant::now() + timeout, reply);
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
                // The backlog from `from` is pushed by the `react` that follows.
                self.decided_subscribers
                    .push(DecidedSub { next_idx: from, tx });
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

    // After a message or command wakes the actor, everything else already queued
    // is handled before reacting, so a burst gets one round of resolution and one
    // flush (letting the core batch replies) instead of one per message. Capped so
    // a sustained stream can't starve ticks.
    const MAX_BURST: usize = 1024;

    loop {
        select_biased! {
            _ = tick_fut => {
                state.op.tick();
                state.react();
                tick_fut = Box::pin(R::sleep(state.config.tick_period)).fuse();
            }
            _ = egress_fut => {
                state.flush_outgoing();
                egress_fut = Box::pin(R::sleep(state.config.egress_period)).fuse();
            }
            in_msg = incoming_rx.next() => {
                let Some(m) = in_msg else { break };
                state.handle_incoming(m);
                for _ in 1..MAX_BURST {
                    let Ok(m) = incoming_rx.try_recv() else { break };
                    state.handle_incoming(m);
                }
                state.react();
            }
            cmd = cmd_rx.next() => {
                let Some(c) = cmd else { break };
                state.handle_command(c);
                for _ in 1..MAX_BURST {
                    let Ok(c) = cmd_rx.try_recv() else { break };
                    state.handle_command(c);
                }
                state.react();
            }
        }
    }
}

/// For entries returned by a log read starting at `from`, the log index just past
/// each entry. A read starting below the compacted index begins with one trimmed or
/// snapshotted entry standing for every index up to the compacted index; every
/// other entry stands for a single index.
fn entry_ends<T: Entry>(from: usize, entries: &[LogEntry<T>]) -> Vec<usize> {
    let mut end = from;
    entries
        .iter()
        .map(|entry| {
            end = match entry {
                LogEntry::Trimmed(compacted_idx) => *compacted_idx,
                LogEntry::Snapshotted(s) => s.trimmed_idx,
                _ => end + 1,
            };
            end
        })
        .collect()
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
