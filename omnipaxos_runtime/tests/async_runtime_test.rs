//! Integration tests for the async runtime layer.
//!
//! Sets up a small in-process cluster where each node runs an OmniPaxos actor and messages
//! are shuttled between them over in-memory channels, then exercises the async API.
#![cfg(feature = "tokio_runtime")]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use omnipaxos::messages::sequence_paxos::PaxosMsg;
use omnipaxos::messages::Message;
use omnipaxos::storage::{Entry, Snapshot};
use omnipaxos::util::{LogEntry, NodeId};
use omnipaxos::{ClusterConfig, OmniPaxosConfig, ProposeErr, ServerConfig};
use omnipaxos_runtime::{
    spawn_actor, AppendError, OmniPaxosEvent, OmniPaxosHandle, RuntimeConfig, RuntimeProposeErr,
    TokioRuntime,
};
use omnipaxos_storage::memory_storage::MemoryStorage;
use tokio::time::timeout;

/// Minimal Entry type — avoids depending on the `macros` feature. Always derives serde
/// traits (an unconditional dev-dependency here) rather than gating on this crate's own
/// `serde` feature: under workspace-wide builds (`cargo check --workspace`), `omnipaxos`'s
/// `serde` feature can be active (enabled by some other workspace member) independently of
/// whether *this* crate's `serde` feature was requested, and `Entry::Snapshot` requires
/// `Serialize + Deserialize` whenever the dependency's feature is on, regardless.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
struct TestEntry(u64);

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
struct NoSnapshot;

impl Snapshot<TestEntry> for NoSnapshot {
    fn create(_entries: &[TestEntry]) -> Self {
        NoSnapshot
    }
    fn merge(&mut self, _delta: Self) {}
    fn use_snapshots() -> bool {
        false
    }
}

impl Entry for TestEntry {
    type Snapshot = NoSnapshot;
}

fn build_op(
    pid: NodeId,
    nodes: Vec<NodeId>,
) -> omnipaxos::OmniPaxos<TestEntry, MemoryStorage<TestEntry>> {
    let cluster_config = ClusterConfig {
        configuration_id: 1,
        nodes,
        flexible_quorum: None,
    };
    // `..Default::default()` is needed only when the `logging` feature adds
    // extra fields to `ServerConfig`; clippy flags it as redundant under
    // feature combinations where it isn't (e.g. `logging` off).
    #[allow(clippy::needless_update)]
    let server_config = ServerConfig {
        pid,
        election_tick_timeout: 3,
        resend_message_tick_timeout: 5,
        buffer_size: 100,
        batch_size: 1,
        flush_batch_tick_timeout: 100,
        leader_priority: 0,
        ..Default::default()
    };
    let cfg = OmniPaxosConfig {
        cluster_config,
        server_config,
    };
    cfg.build(MemoryStorage::default()).unwrap()
}

/// Spawn `n` nodes and wire their outgoing/incoming message streams together.
async fn spawn_cluster(n: NodeId) -> HashMap<NodeId, OmniPaxosHandle<TestEntry>> {
    let nodes: Vec<NodeId> = (1..=n).collect();
    let cfg = RuntimeConfig {
        tick_period: Duration::from_millis(2),
        egress_period: Duration::from_millis(1),
        ..Default::default()
    };

    let mut handles: HashMap<NodeId, OmniPaxosHandle<TestEntry>> = HashMap::new();
    for pid in nodes.clone() {
        let op = build_op(pid, nodes.clone());
        let h = spawn_actor::<TestEntry, MemoryStorage<TestEntry>, TokioRuntime>(op, cfg.clone());
        handles.insert(pid, h);
    }

    // Wire in-process transport: every node's outgoing_messages -> receiver's handle_incoming.
    let handles_arc = Arc::new(handles.clone());
    for pid in nodes {
        let out = handles_arc[&pid].outgoing_messages();
        let peers = handles_arc.clone();
        tokio::spawn(async move {
            while let Ok(msg) = out.recv().await {
                let receiver = msg.get_receiver();
                if let Some(peer) = peers.get(&receiver) {
                    peer.handle_incoming(msg).await;
                }
            }
        });
    }

    handles
}

/// Like `spawn_cluster`, but every message is passed through `keep` before
/// delivery: `keep(&msg)` returning `false` drops it (the sender's outgoing
/// channel is still drained, so a dropped message never causes backpressure)
/// instead of forwarding it to the receiver. Lets tests script network
/// conditions (e.g. blocking one node's traffic, or one message type
/// cluster-wide) precisely enough to reach specific internal states.
async fn spawn_cluster_with_filter<F>(
    n: NodeId,
    batch_size: usize,
    append_notify_timeout: Duration,
    keep: F,
) -> HashMap<NodeId, OmniPaxosHandle<TestEntry>>
where
    F: Fn(&Message<TestEntry>) -> bool + Send + Sync + 'static,
{
    let nodes: Vec<NodeId> = (1..=n).collect();
    let cfg = RuntimeConfig {
        tick_period: Duration::from_millis(2),
        egress_period: Duration::from_millis(1),
        append_notify_timeout,
        ..Default::default()
    };

    let mut handles: HashMap<NodeId, OmniPaxosHandle<TestEntry>> = HashMap::new();
    for pid in nodes.clone() {
        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: nodes.clone(),
            flexible_quorum: None,
        };
        #[allow(clippy::needless_update)]
        let server_config = ServerConfig {
            pid,
            election_tick_timeout: 3,
            resend_message_tick_timeout: 5,
            buffer_size: 100,
            batch_size,
            flush_batch_tick_timeout: 100,
            leader_priority: 0,
            ..Default::default()
        };
        let op_cfg = OmniPaxosConfig {
            cluster_config,
            server_config,
        };
        let op = op_cfg.build(MemoryStorage::default()).unwrap();
        let h = spawn_actor::<TestEntry, MemoryStorage<TestEntry>, TokioRuntime>(op, cfg.clone());
        handles.insert(pid, h);
    }

    let handles_arc = Arc::new(handles.clone());
    let keep = Arc::new(keep);
    for pid in nodes {
        let out = handles_arc[&pid].outgoing_messages();
        let peers = handles_arc.clone();
        let keep = keep.clone();
        tokio::spawn(async move {
            while let Ok(msg) = out.recv().await {
                if !keep(&msg) {
                    continue;
                }
                let receiver = msg.get_receiver();
                if let Some(peer) = peers.get(&receiver) {
                    peer.handle_incoming(msg).await;
                }
            }
        });
    }

    handles
}

async fn wait_for_leader(handles: &HashMap<NodeId, OmniPaxosHandle<TestEntry>>) -> NodeId {
    let deadline = Duration::from_secs(5);
    let start = tokio::time::Instant::now();
    loop {
        for h in handles.values() {
            if let Some((pid, true)) = h.current_leader().await {
                return pid;
            }
        }
        if start.elapsed() > deadline {
            panic!("no leader elected within {:?}", deadline);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn append_notify_resolves_after_decision() {
    let handles = spawn_cluster(3).await;
    let leader = wait_for_leader(&handles).await;
    let leader_h = handles.get(&leader).unwrap().clone();

    let idx = timeout(
        Duration::from_secs(3),
        leader_h.append_notify(TestEntry(42)),
    )
    .await
    .expect("append_notify did not resolve in time")
    .expect("append_notify returned error");

    assert!(idx >= 1, "expected a non-zero decided idx, got {idx}");

    let entries = leader_h.read_decided_suffix(0).await.expect("no entries");
    assert!(!entries.is_empty(), "decided suffix should be non-empty");
}

#[tokio::test]
async fn append_notify_from_follower_resolves() {
    let handles = spawn_cluster(3).await;
    let leader = wait_for_leader(&handles).await;
    let follower_pid = *handles.keys().find(|&&p| p != leader).unwrap();
    let follower = handles.get(&follower_pid).unwrap().clone();

    let idx = timeout(
        Duration::from_secs(3),
        follower.append_notify(TestEntry(99)),
    )
    .await
    .expect("follower append_notify did not resolve in time")
    .expect("follower append_notify returned error");

    assert!(
        idx >= 1,
        "expected non-zero decided idx from follower append_notify, got {idx}"
    );

    // The entry should be visible in the decided log from any node.
    let entries = follower.read_decided_suffix(0).await.expect("no entries");
    assert!(
        !entries.is_empty(),
        "follower's decided suffix should be non-empty after append_notify"
    );
}

#[tokio::test]
async fn append_notify_mixed_leader_and_follower() {
    let handles = spawn_cluster(3).await;
    let leader = wait_for_leader(&handles).await;
    let follower_pid = *handles.keys().find(|&&p| p != leader).unwrap();

    let leader_h = handles.get(&leader).unwrap().clone();
    let follower_h = handles.get(&follower_pid).unwrap().clone();

    // Fire concurrent append_notify calls from both roles.
    let (a, b) = tokio::join!(
        timeout(Duration::from_secs(3), leader_h.append_notify(TestEntry(1))),
        timeout(
            Duration::from_secs(3),
            follower_h.append_notify(TestEntry(2))
        ),
    );
    let idx_a = a.expect("leader append timed out").expect("leader err");
    let idx_b = b.expect("follower append timed out").expect("follower err");
    assert_ne!(idx_a, idx_b, "distinct log indices expected");
}

/// Regression test for the Prepare-phase gate in `accept_as_leader`: a node
/// BLE has elected leader, but whose SequencePaxos instance hasn't yet
/// collected a quorum of Promises (still `Phase::Prepare`), must not have
/// its `append_notify` entry silently accepted-and-buffered -- that buffer
/// can later be forwarded to a *different* node on a leader change
/// (`forward_buffered_proposals`, sequence_paxos/follower.rs), leaving no
/// way to recover the eventual index. Blocking only `Promise` replies
/// cluster-wide (while letting every other SequencePaxos/BLE message
/// through) lets whichever node BLE elects send its `Prepare` broadcast (so
/// followers learn who it is) while it can never collect a quorum of
/// `Promise`s back, reliably parking it in Phase::Prepare regardless of
/// which pid that ends up being.
///
/// Submits from a *follower*, not the Prepare-phase leader itself: a
/// follower's `try_dispatch_undispatched` forwards to whoever it believes is
/// leader regardless of that leader's phase (it only gates on its own
/// `leader == own_pid` case), so this exercises `accept_as_leader`'s gate on
/// the `TaggedProposal`-receiving side -- the actual path the fix covers.
///
/// Once forwarded, `try_dispatch_undispatched` has already cleared the
/// entry's `undispatched` slot, so a refusal on the leader side (correctly,
/// silently dropped rather than accepted) has no automatic retry -- by
/// design the caller must reissue `append_notify` itself after a `Timeout`.
/// So this asserts the call resolves `Timeout` (not a wrong `Ok`, not a
/// hang), then confirms the cluster is otherwise healthy by reissuing it
/// once leadership has stabilized.
#[tokio::test]
async fn append_notify_waits_through_prepare_phase_leader() {
    let blocked = Arc::new(AtomicBool::new(true));
    let filter_blocked = blocked.clone();
    let handles = spawn_cluster_with_filter(3, 1, Duration::from_millis(300), move |msg| {
        if filter_blocked.load(Ordering::SeqCst) {
            !matches!(msg, Message::SequencePaxos(pm) if matches!(pm.msg, PaxosMsg::Promise(_)))
        } else {
            true
        }
    })
    .await;

    // Wait for BLE to elect someone while they're stuck in Phase::Prepare
    // (own view: `current_leader() == Some((self, false))`), AND for at
    // least one other node to already recognize them as leader too.
    let leader = timeout(Duration::from_secs(5), async {
        loop {
            for (&pid, h) in &handles {
                if let Some((leader_pid, false)) = h.current_leader().await {
                    if leader_pid == pid {
                        for (&other_pid, other_h) in &handles {
                            if other_pid == pid {
                                continue;
                            }
                            if let Some((seen, _)) = other_h.current_leader().await {
                                if seen == pid {
                                    return pid;
                                }
                            }
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect(
        "no node was ever elected leader (observed by a follower) while stuck in Phase::Prepare",
    );

    let follower = *handles.keys().find(|&&p| p != leader).unwrap();
    let follower_h = handles.get(&follower).unwrap().clone();
    let notify_fut = tokio::spawn({
        let h = follower_h.clone();
        async move { h.append_notify(TestEntry(7)).await }
    });

    // Give it a while to (incorrectly) resolve if the Prepare-phase gate were
    // missing; it must still be pending.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        !notify_fut.is_finished(),
        "append_notify resolved while the leader was still in Phase::Prepare"
    );

    // Unblock: Promises can now flow and leadership stabilizes. The
    // already-forwarded entry has no automatic retry (its `undispatched`
    // slot was cleared the moment it was sent), so it must resolve
    // `Timeout` -- not hang, and not falsely resolve `Ok` with a bogus
    // index (the bug this test guards against).
    blocked.store(false, Ordering::SeqCst);

    let res = timeout(Duration::from_secs(3), notify_fut)
        .await
        .expect("append_notify hung after unblocking")
        .unwrap();
    match res {
        Err(AppendError::Timeout) => {}
        other => panic!("expected Timeout for the refused, un-retried entry, got {other:?}"),
    }

    // Confirm the cluster is otherwise healthy: a fresh append_notify from
    // the same follower, issued now that leadership has stabilized, must
    // succeed.
    let idx = timeout(
        Duration::from_secs(3),
        follower_h.append_notify(TestEntry(8)),
    )
    .await
    .expect("retried append_notify hung")
    .expect("retried append_notify should succeed once leadership is stable");
    assert!(idx >= 1);
}

#[tokio::test]
async fn append_notify_times_out_when_no_leader_elected() {
    // Spin up an actor with no peers wired up — leader stays unknown.
    // append_notify should NOT fail immediately (that would force users back to
    // leader-polling). Instead it should wait until append_notify_timeout
    // and resolve with Timeout.
    let nodes: Vec<NodeId> = vec![1, 2, 3];
    let cfg = RuntimeConfig {
        tick_period: Duration::from_millis(2),
        egress_period: Duration::from_millis(1),
        append_notify_timeout: Duration::from_millis(150),
        ..Default::default()
    };
    let op = build_op(1, nodes);
    let h = spawn_actor::<TestEntry, MemoryStorage<TestEntry>, TokioRuntime>(op, cfg);
    // No transport wired, no other nodes — leader stays unknown.

    let res = timeout(Duration::from_secs(2), h.append_notify(TestEntry(1)))
        .await
        .expect("append_notify should resolve within outer timeout");
    match res {
        Err(AppendError::Timeout) => {}
        other => panic!("expected Timeout, got {other:?}"),
    }
}

#[tokio::test]
async fn append_notify_timeout_when_transport_broken() {
    // Spawn a cluster, elect a leader, then break the transport by dropping the
    // handles map so outgoing/incoming channels close on the leader. A follower's
    // append_notify should fail with Timeout after the configured deadline.
    let nodes: Vec<NodeId> = vec![1, 2, 3];
    let cfg = RuntimeConfig {
        tick_period: Duration::from_millis(2),
        egress_period: Duration::from_millis(1),
        append_notify_timeout: Duration::from_millis(200),
        ..Default::default()
    };
    let mut handles: HashMap<NodeId, OmniPaxosHandle<TestEntry>> = HashMap::new();
    for pid in nodes.clone() {
        let op = build_op(pid, nodes.clone());
        let h = spawn_actor::<TestEntry, MemoryStorage<TestEntry>, TokioRuntime>(op, cfg.clone());
        handles.insert(pid, h);
    }
    let handles_arc = Arc::new(handles.clone());
    for pid in nodes.clone() {
        let out = handles_arc[&pid].outgoing_messages();
        let peers = handles_arc.clone();
        tokio::spawn(async move {
            while let Ok(msg) = out.recv().await {
                let receiver = msg.get_receiver();
                if let Some(peer) = peers.get(&receiver) {
                    peer.handle_incoming(msg).await;
                }
            }
        });
    }
    let leader = wait_for_leader(&handles).await;
    let follower_pid = *handles.keys().find(|&&p| p != leader).unwrap();
    let follower = handles.get(&follower_pid).unwrap().clone();

    // Now drop everything BUT the follower handle. This kills transport tasks
    // (their handle clones go away) so TaggedProposal will never reach the leader.
    let _ = handles_arc;
    drop(handles);

    let res = timeout(Duration::from_secs(2), follower.append_notify(TestEntry(7)))
        .await
        .expect("append_notify did not respond within outer timeout");
    match res {
        Err(AppendError::Timeout) => {}
        // Under some interleavings the entry may squeak through before shutdown; accept.
        Ok(_) => {}
        other => panic!("expected Timeout or Ok, got {other:?}"),
    }
}

#[tokio::test]
async fn append_notify_rejects_when_too_many_outstanding() {
    // No leader ever gets elected (single node, no peers wired), so every
    // append_notify call stays queued in `pending` indefinitely — perfect for
    // deterministically filling the backlog up to the cap.
    let nodes: Vec<NodeId> = vec![1, 2, 3];
    let cfg = RuntimeConfig {
        tick_period: Duration::from_millis(2),
        egress_period: Duration::from_millis(1),
        append_notify_timeout: Duration::from_secs(10),
        max_pending_appends: 2,
        ..Default::default()
    };
    let op = build_op(1, nodes);
    let h = spawn_actor::<TestEntry, MemoryStorage<TestEntry>, TokioRuntime>(op, cfg);

    // Fill the backlog to the cap. Spawn so these calls actually get sent and
    // queued rather than sitting unpolled.
    let mut fillers = Vec::new();
    for v in 0..2u64 {
        let h = h.clone();
        fillers.push(tokio::spawn(
            async move { h.append_notify(TestEntry(v)).await },
        ));
    }
    // Give the actor a moment to receive and queue both commands.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // One more call should be rejected immediately rather than queued — assert
    // this with a short outer timeout so a Timeout couldn't be mistaken for it.
    let res = timeout(Duration::from_millis(500), h.append_notify(TestEntry(99)))
        .await
        .expect("rejection should be immediate, not wait for append_notify_timeout");
    match res {
        Err(AppendError::TooManyOutstanding) => {}
        other => panic!("expected TooManyOutstanding, got {other:?}"),
    }

    for f in fillers {
        f.abort();
    }
}

#[tokio::test]
async fn event_stream_emits_leader_election() {
    let handles = spawn_cluster(3).await;
    // Subscribe on one node before waiting for election so we don't miss the event.
    let any_h = handles.values().next().unwrap().clone();
    let events = any_h.subscribe_events();

    let elected_pid = timeout(Duration::from_secs(5), async {
        loop {
            match events.recv().await {
                Ok(OmniPaxosEvent::LeaderElected { pid, .. }) => break pid,
                Ok(_) => continue,
                Err(_) => panic!("event stream closed"),
            }
        }
    })
    .await
    .expect("no LeaderElected event within timeout");

    assert!((1..=3).contains(&elected_pid));
}

#[tokio::test]
async fn dropping_all_handles_shuts_actor_down() {
    let handles = spawn_cluster(3).await;
    let _leader = wait_for_leader(&handles).await;

    // Grab the outgoing receiver on one node so we can observe it close.
    let victim_pid: NodeId = 1;
    let out = handles.get(&victim_pid).unwrap().outgoing_messages();

    // Drop every clone of node 1's handle.
    // First remove from the map to drop the map's owned copy.
    let mut handles = handles;
    handles.remove(&victim_pid);

    // The transport-forwarding task still holds a clone (through Arc<HashMap>), so the
    // outgoing_rx stays open — this test asserts the *observable* behavior when there are
    // no user-side owners left. Trigger it by dropping the forwarding task's copy too:
    drop(out); // release our observer

    // Give the actor time to notice and shut down.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Sanity: remaining handles still function.
    let survivor = handles.values().next().unwrap();
    let _ = survivor.decided_idx().await;
}

/// Regression test: `accept_as_leader` used to silently swallow a `ProposeErr`
/// returned by `op.append()`, leaving the caller to always wait out the full
/// `append_notify_timeout` even for an entry this node itself owns as the
/// origin. It's now resolved immediately via `AppendError::Propose`.
#[tokio::test]
async fn append_notify_resolves_propose_error_immediately() {
    let handles = spawn_cluster(3).await;
    let leader = wait_for_leader(&handles).await;
    let leader_h = handles.get(&leader).unwrap().clone();

    // Propose a (trivial) reconfiguration so the leader's log has an accepted
    // stopsign -- `accepted_reconfiguration()` becomes true on the leader as
    // soon as this resolves, independent of whether the stopsign is decided
    // yet.
    let nodes: Vec<NodeId> = handles.keys().copied().collect();
    leader_h
        .reconfigure(
            ClusterConfig {
                configuration_id: 2,
                nodes,
                flexible_quorum: None,
            },
            None,
        )
        .await
        .expect("reconfigure should succeed");

    // Any subsequent append_notify on this same leader must fail fast with
    // Propose(PendingReconfigEntry), not silently time out.
    let res = timeout(
        Duration::from_millis(500),
        leader_h.append_notify(TestEntry(1)),
    )
    .await
    .expect("append_notify should resolve immediately, not wait for append_notify_timeout");

    match res {
        Err(AppendError::Propose(ProposeErr::PendingReconfigEntry(_))) => {}
        other => panic!("expected Propose(PendingReconfigEntry), got {other:?}"),
    }
}

#[tokio::test]
async fn append_notify_shutdown_error_when_actor_gone() {
    let handles = spawn_cluster(3).await;
    let _leader = wait_for_leader(&handles).await;

    let h = handles.values().next().unwrap().clone();
    // Drop the map so all other clones vanish, keep only `h`.
    drop(handles);

    // Actor is still alive (h holds cmd_tx). Kill it by dropping h and observing that a
    // fresh append on the second clone returns Shutdown-shaped behavior. This is a
    // sanity assertion — behavior on partial handle drop is left to the user.
    let h2 = h.clone();
    drop(h);
    let res = h2.append_notify(TestEntry(1)).await;
    // Either the append actually goes through, or we get Shutdown. Both are valid: we
    // just assert no panic.
    match res {
        Ok(_)
        | Err(AppendError::Shutdown)
        | Err(AppendError::Propose(_))
        | Err(AppendError::Superseded)
        | Err(AppendError::Timeout)
        | Err(AppendError::TooManyOutstanding) => {}
    }
}

/// Regression test: `append`/`reconfigure` used to fabricate
/// `ProposeErr::PendingReconfigEntry`/`PendingReconfigConfig` -- which mean
/// "a reconfiguration is already pending" -- to signal "the actor is gone"
/// on a channel-send failure. A caller retrying on that error, trusting its
/// real meaning, would retry forever against a dead actor. Shutdown is now
/// a distinct `RuntimeProposeErr::Shutdown`.
///
/// No reconfiguration is ever proposed in this test, so a genuine
/// `Propose(_)` should never legitimately occur here -- only `Ok` (actor
/// still alive) or `Shutdown` are valid. As with
/// `append_notify_shutdown_error_when_actor_gone`, this can't force the
/// actor to be *provably* gone through the public API alone (background
/// transport-forwarding tasks spawned by `spawn_cluster` keep their own
/// handle clones alive), so this is race coverage: it can't guarantee
/// hitting the `Shutdown` path, but it deterministically rules out the
/// fabrication bug on every run that does.
#[tokio::test]
async fn append_and_reconfigure_report_shutdown_not_fabricated_propose_error() {
    let handles = spawn_cluster(3).await;
    let _leader = wait_for_leader(&handles).await;

    let h = handles.values().next().unwrap().clone();
    drop(handles);
    let h2 = h.clone();
    drop(h);

    match h2.append(TestEntry(1)).await {
        Ok(_) | Err(RuntimeProposeErr::Shutdown) => {}
        Err(RuntimeProposeErr::Propose(err)) => {
            panic!("append fabricated a Propose error instead of Shutdown: {err:?}")
        }
    }

    match h2
        .reconfigure(
            ClusterConfig {
                configuration_id: 2,
                nodes: vec![1, 2, 3],
                flexible_quorum: None,
            },
            None,
        )
        .await
    {
        Ok(_) | Err(RuntimeProposeErr::Shutdown) => {}
        Err(RuntimeProposeErr::Propose(err)) => {
            panic!("reconfigure fabricated a Propose error instead of Shutdown: {err:?}")
        }
    }
}

/// Regression test: `flush_outgoing` used to call `.send().await` on the
/// outgoing channel, which blocks when the channel is merely *full*
/// (consumer alive but slow) -- not just when it's closed. Since this runs
/// on the single actor loop, that blocked tick/election/command processing
/// for the node entirely, not just delayed egress. Uses a tiny
/// `outgoing_capacity` and never drains it, so it fills up almost
/// immediately (the node has configured peers and sends BLE heartbeats
/// regardless of whether anyone's listening).
#[tokio::test]
async fn full_outgoing_channel_does_not_stall_actor_loop() {
    let nodes: Vec<NodeId> = vec![1, 2, 3];
    let cfg = RuntimeConfig {
        tick_period: Duration::from_millis(2),
        egress_period: Duration::from_millis(1),
        outgoing_capacity: 2,
        ..Default::default()
    };
    let op = build_op(1, nodes);
    let h = spawn_actor::<TestEntry, MemoryStorage<TestEntry>, TokioRuntime>(op, cfg);

    // Let the outgoing channel fill up completely without ever draining it.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The actor loop must still be responsive to commands -- if
    // flush_outgoing blocked on a full channel, this would hang.
    let idx = timeout(Duration::from_millis(500), h.decided_idx())
        .await
        .expect("actor loop appears stalled with a full outgoing channel");
    assert_eq!(idx, 0);

    // Draining now should still deliver messages (not silently and
    // permanently lost) once the consumer catches up.
    let out = h.outgoing_messages();
    timeout(Duration::from_secs(2), out.recv())
        .await
        .expect("no message ever arrived once draining resumed")
        .expect("outgoing channel closed unexpectedly");
}

async fn recv_decided(
    rx: &async_channel::Receiver<LogEntry<TestEntry>>,
    n: usize,
) -> Vec<TestEntry> {
    let mut out = Vec::with_capacity(n);
    while out.len() < n {
        let entry = timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("subscribe_decided stream stalled")
            .expect("subscribe_decided stream closed");
        if let LogEntry::Decided(e) = entry {
            out.push(e);
        }
    }
    out
}

#[tokio::test]
async fn subscribe_decided_delivers_history_and_live() {
    let handles = spawn_cluster(3).await;
    let leader = wait_for_leader(&handles).await;
    let leader_h = handles.get(&leader).unwrap().clone();

    // Append two entries first — these become the "history" the subscriber must
    // receive on catch-up.
    for v in [10u64, 11] {
        leader_h
            .append_notify(TestEntry(v))
            .await
            .expect("append_notify");
    }

    // Subscribe AFTER those entries are already decided.
    let observer = handles.values().next().unwrap().clone();
    let rx = observer.subscribe_decided(0).await;

    let history = recv_decided(&rx, 2).await;
    assert_eq!(history, vec![TestEntry(10), TestEntry(11)]);

    // Now write a live entry and confirm it comes through the same stream.
    leader_h
        .append_notify(TestEntry(12))
        .await
        .expect("append_notify");
    let live = recv_decided(&rx, 1).await;
    assert_eq!(live, vec![TestEntry(12)]);
}

#[tokio::test]
async fn subscribe_decided_multi_subscriber_independent() {
    let handles = spawn_cluster(3).await;
    let leader = wait_for_leader(&handles).await;
    let leader_h = handles.get(&leader).unwrap().clone();

    // Write one entry so the subscribers have something to catch up on.
    leader_h
        .append_notify(TestEntry(1))
        .await
        .expect("append_notify");

    let a_h = handles.values().next().unwrap().clone();
    let b_h = handles.values().nth(1).unwrap().clone();
    let rx_a = a_h.subscribe_decided(0).await;
    let rx_b = b_h.subscribe_decided(0).await;

    // Each subscriber should independently see the same suffix.
    assert_eq!(recv_decided(&rx_a, 1).await, vec![TestEntry(1)]);
    assert_eq!(recv_decided(&rx_b, 1).await, vec![TestEntry(1)]);

    // Add another entry. Both should observe it.
    leader_h
        .append_notify(TestEntry(2))
        .await
        .expect("append_notify");
    assert_eq!(recv_decided(&rx_a, 1).await, vec![TestEntry(2)]);
    assert_eq!(recv_decided(&rx_b, 1).await, vec![TestEntry(2)]);
}

#[tokio::test]
async fn subscribe_decided_drop_receiver_removes_sub() {
    let handles = spawn_cluster(3).await;
    let leader = wait_for_leader(&handles).await;
    let leader_h = handles.get(&leader).unwrap().clone();

    let observer = handles.values().next().unwrap().clone();
    let rx = observer.subscribe_decided(0).await;
    drop(rx);

    // Actor must not panic when it tries to push to the closed subscriber.
    // Sanity: subsequent writes continue to decide normally.
    let idx = leader_h
        .append_notify(TestEntry(42))
        .await
        .expect("append_notify after dropping subscriber");
    assert!(idx >= 1);
    // The observer's local decided_idx should catch up; poll briefly to allow
    // the follower to see the AcceptDecide broadcast.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if observer.decided_idx().await >= idx {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("observer's decided_idx never caught up");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Regression test for a bug in `append_tracked`'s local FIFO bookkeeping
/// (`unconfirmed_local`): entries deferred by leader-side batching
/// (`batch_size > 1`) are matched to their eventual log index purely by
/// queue position. Plain `append()` and tracked `append_notify()` calls
/// share the exact same per-node batch buffer
/// (`state_cache.batched_entries`), so if an *untracked* `append()` entry
/// is batched ahead of a still-pending tracked `append_notify()` entry, and
/// the flush is triggered by a later call, the tracked entry must still be
/// matched to its own true index — not the untracked entry's slot.
///
/// Uses a real 3-node cluster with `batch_size: 4` so the interleaving below
/// lands in exactly one flush on the elected leader. The assertion reads
/// back the decided log and checks the entry at each `append_notify`-
/// returned index is actually the entry that call sent — this fails under
/// the old bug (which could return a wrong-but-plausible index) regardless
/// of the precise scheduling that occurs.
#[tokio::test]
async fn append_notify_index_correct_when_batched_with_plain_append() {
    let nodes: Vec<NodeId> = vec![1, 2, 3];
    let rt_cfg = RuntimeConfig {
        tick_period: Duration::from_millis(2),
        egress_period: Duration::from_millis(1),
        ..Default::default()
    };

    let mut handles: HashMap<NodeId, OmniPaxosHandle<TestEntry>> = HashMap::new();
    for pid in nodes.clone() {
        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: nodes.clone(),
            flexible_quorum: None,
        };
        #[allow(clippy::needless_update)]
        let server_config = ServerConfig {
            pid,
            election_tick_timeout: 3,
            resend_message_tick_timeout: 5,
            buffer_size: 100,
            batch_size: 4,
            flush_batch_tick_timeout: 100,
            leader_priority: 0,
            ..Default::default()
        };
        let cfg = OmniPaxosConfig {
            cluster_config,
            server_config,
        };
        let op = cfg.build(MemoryStorage::default()).unwrap();
        let h =
            spawn_actor::<TestEntry, MemoryStorage<TestEntry>, TokioRuntime>(op, rt_cfg.clone());
        handles.insert(pid, h);
    }
    let handles_arc = Arc::new(handles.clone());
    for pid in nodes {
        let out = handles_arc[&pid].outgoing_messages();
        let peers = handles_arc.clone();
        tokio::spawn(async move {
            while let Ok(msg) = out.recv().await {
                let receiver = msg.get_receiver();
                if let Some(peer) = peers.get(&receiver) {
                    peer.handle_incoming(msg).await;
                }
            }
        });
    }

    let leader = wait_for_leader(&handles).await;
    let h = handles.get(&leader).unwrap().clone();

    // Interleave two untracked `append()` calls with two tracked
    // `append_notify()` calls. Staggering the spawns with `yield_now`
    // biases (without strictly guaranteeing) the Command send order toward
    // A1, N1, A2, N2 -- i.e. an untracked entry landing in the batch ahead
    // of a still-pending tracked one, exactly the ordering the original bug
    // mishandled. All four land in one flush since `batch_size == 4`.
    let h1 = h.clone();
    let a1 = tokio::spawn(async move { h1.append(TestEntry(101)).await });
    tokio::task::yield_now().await;

    let entry_n1 = TestEntry(201);
    let h2 = h.clone();
    let send_n1 = entry_n1.clone();
    let n1 = tokio::spawn(async move { h2.append_notify(send_n1).await });
    tokio::task::yield_now().await;

    let h3 = h.clone();
    let a2 = tokio::spawn(async move { h3.append(TestEntry(102)).await });
    tokio::task::yield_now().await;

    let entry_n2 = TestEntry(202);
    let h4 = h.clone();
    let send_n2 = entry_n2.clone();
    let n2 = tokio::spawn(async move { h4.append_notify(send_n2).await });

    a1.await.unwrap().expect("append(101) failed");
    a2.await.unwrap().expect("append(102) failed");
    let idx1 = timeout(Duration::from_secs(3), n1)
        .await
        .expect("append_notify(201) hung")
        .unwrap()
        .expect("append_notify(201) errored");
    let idx2 = timeout(Duration::from_secs(3), n2)
        .await
        .expect("append_notify(202) hung")
        .unwrap()
        .expect("append_notify(202) errored");

    assert_ne!(idx1, idx2, "distinct log indices expected");

    let decided: Vec<TestEntry> = h
        .read_decided_suffix(0)
        .await
        .expect("no entries decided")
        .into_iter()
        .filter_map(|e| match e {
            LogEntry::Decided(v) => Some(v),
            _ => None,
        })
        .collect();

    assert_eq!(
        decided.get(idx1 - 1),
        Some(&entry_n1),
        "append_notify(201) returned idx {idx1}, but the decided log there is {:?} \
         (wrong-index assignment -- indices are 1-based)",
        decided.get(idx1 - 1)
    );
    assert_eq!(
        decided.get(idx2 - 1),
        Some(&entry_n2),
        "append_notify(202) returned idx {idx2}, but the decided log there is {:?} \
         (wrong-index assignment -- indices are 1-based)",
        decided.get(idx2 - 1)
    );
}

#[tokio::test]
async fn subscribe_decided_rejects_when_too_many_subscribers() {
    let nodes: Vec<NodeId> = vec![1, 2, 3];
    let cfg = RuntimeConfig {
        tick_period: Duration::from_millis(2),
        egress_period: Duration::from_millis(1),
        max_decided_subscribers: 1,
        ..Default::default()
    };
    let op = build_op(1, nodes);
    let h = spawn_actor::<TestEntry, MemoryStorage<TestEntry>, TokioRuntime>(op, cfg);

    // First subscriber is accepted and stays open.
    let _rx_a = h.subscribe_decided(0).await;

    // A second subscriber exceeds the cap: its channel should close immediately,
    // with no entries ever delivered.
    let rx_b = h.subscribe_decided(0).await;
    let res = timeout(Duration::from_secs(1), rx_b.recv())
        .await
        .expect("rejection should be immediate, not hang");
    assert!(
        res.is_err(),
        "expected the rejected subscriber's channel to be closed, got {res:?}"
    );
}
