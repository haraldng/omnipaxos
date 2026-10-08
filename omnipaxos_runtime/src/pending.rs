use std::{
    collections::{BTreeSet, HashMap},
    time::Instant,
};

use futures::channel::oneshot;

use omnipaxos::{
    ballot_leader_election::Ballot, messages::async_runtime::EntryId, storage::Entry, ProposeErr,
};

use super::event::AppendError;

/// A pending `append_notify` awaiting decision. Uniform representation regardless
/// of whether the entry was accepted locally (we are leader) or forwarded (we are
/// follower).
struct Pending<T>
where
    T: Entry,
{
    id: EntryId,
    /// The entry, retained until dispatched (accepted locally or sent as a
    /// `TaggedProposal`). While `Some`, the actor retries dispatching it each
    /// tick -- this is what lets `append_notify` be called before a leader is
    /// known.
    entry: Option<T>,
    /// `(assigned_idx, ballot_at_assignment)`. `None` until either:
    ///   (a) we accepted the entry ourselves as leader, or
    ///   (b) we received `AsyncRuntimeMsg::Assigned` from the leader.
    assigned: Option<(usize, Ballot)>,
    deadline: Instant,
    reply: oneshot::Sender<Result<usize, AppendError<T>>>,
}

/// The outstanding `append_notify` calls of one actor, indexed so that no
/// operation scans all of them: inserting, dispatching, assigning, and failing
/// cost O(log n) per affected call, and [`Self::resolve`] costs O(log n) per call
/// it resolves. The one exception is a leader change, which scans the assigned
/// calls once to find the superseded ones.
///
/// Each call gets an internal key from a counter, so ordering by key is
/// insertion order. A call is removed from every index as soon as it resolves.
pub(crate) struct PendingAppends<T>
where
    T: Entry,
{
    next_key: u64,
    entries: HashMap<u64, Pending<T>>,
    keys: HashMap<EntryId, u64>,
    undispatched: BTreeSet<u64>,
    assigned: BTreeSet<(usize, u64)>,
    deadlines: BTreeSet<(Instant, u64)>,
    /// The ballot as of the last `resolve`. An assigned call can only become
    /// superseded when the ballot changes or when it is assigned under an older
    /// ballot, so the supersede check runs only then.
    last_ballot: Ballot,
    supersede_check_needed: bool,
}

impl<T> PendingAppends<T>
where
    T: Entry,
{
    pub(crate) fn new() -> Self {
        Self {
            next_key: 0,
            entries: HashMap::new(),
            keys: HashMap::new(),
            undispatched: BTreeSet::new(),
            assigned: BTreeSet::new(),
            deadlines: BTreeSet::new(),
            last_ballot: Ballot::default(),
            supersede_check_needed: false,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn insert(
        &mut self,
        id: EntryId,
        entry: T,
        deadline: Instant,
        reply: oneshot::Sender<Result<usize, AppendError<T>>>,
    ) {
        let key = self.next_key;
        self.next_key += 1;
        self.entries.insert(
            key,
            Pending {
                id,
                entry: Some(entry),
                assigned: None,
                deadline,
                reply,
            },
        );
        self.keys.insert(id, key);
        self.undispatched.insert(key);
        self.deadlines.insert((deadline, key));
    }

    /// Takes every not-yet-dispatched entry, in insertion order.
    pub(crate) fn take_undispatched(&mut self) -> Vec<(EntryId, T)> {
        std::mem::take(&mut self.undispatched)
            .into_iter()
            .filter_map(|key| {
                let p = self.entries.get_mut(&key)?;
                p.entry.take().map(|entry| (p.id, entry))
            })
            .collect()
    }

    /// Records that the call `id` was assigned log index `idx` under `ballot`.
    /// Ignored if `id` is unknown, e.g. a late reply for a call that already
    /// timed out.
    pub(crate) fn assign(&mut self, id: EntryId, idx: usize, ballot: Ballot) {
        let Some(&key) = self.keys.get(&id) else {
            return;
        };
        let p = self
            .entries
            .get_mut(&key)
            .expect("every key in `keys` has an entry");
        if let Some((old_idx, _)) = p.assigned.replace((idx, ballot)) {
            self.assigned.remove(&(old_idx, key));
        }
        self.assigned.insert((idx, key));
        if ballot < self.last_ballot {
            self.supersede_check_needed = true;
        }
    }

    /// Resolves the call `id` immediately with the propose error that failed it.
    pub(crate) fn fail(&mut self, id: EntryId, err: ProposeErr<T>) {
        if let Some(p) = self.keys.get(&id).copied().and_then(|key| self.remove(key)) {
            let _ = p.reply.send(Err(AppendError::Propose(err)));
        }
    }

    /// Resolves every call that is finished, in priority order:
    /// 1. assigned an index `<= decided`: `Ok(idx)`;
    /// 2. assigned under a ballot older than `cur_ballot`: `Superseded`;
    /// 3. past its deadline: `Timeout`.
    pub(crate) fn resolve(&mut self, decided: usize, cur_ballot: Ballot, now: Instant) {
        while let Some(&(idx, key)) = self.assigned.first() {
            if idx > decided {
                break;
            }
            let p = self.remove(key).expect("every assigned key has an entry");
            let _ = p.reply.send(Ok(idx));
        }

        if cur_ballot != self.last_ballot {
            self.last_ballot = cur_ballot;
            self.supersede_check_needed = true;
        }
        if std::mem::take(&mut self.supersede_check_needed) {
            let superseded: Vec<u64> = self
                .assigned
                .iter()
                .filter(|(_, key)| {
                    matches!(self.entries[key].assigned, Some((_, ballot)) if ballot < cur_ballot)
                })
                .map(|(_, key)| *key)
                .collect();
            for key in superseded {
                let p = self.remove(key).expect("every assigned key has an entry");
                let (log_entry_idx, _) = p.assigned.expect("filtered on assigned");
                let _ = p.reply.send(Err(AppendError::Superseded { log_entry_idx }));
            }
        }

        while let Some(&(deadline, key)) = self.deadlines.first() {
            if deadline >= now {
                break;
            }
            let p = self.remove(key).expect("every deadline key has an entry");
            let _ = p.reply.send(Err(AppendError::Timeout));
        }
    }

    fn remove(&mut self, key: u64) -> Option<Pending<T>> {
        let p = self.entries.remove(&key)?;
        self.keys.remove(&p.id);
        self.undispatched.remove(&key);
        self.deadlines.remove(&(p.deadline, key));
        if let Some((idx, _)) = p.assigned {
            self.assigned.remove(&(idx, key));
        }
        Some(p)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::time::Duration;
    #[cfg(feature = "unicache")]
    use omnipaxos::unicache::UniCache;
    use omnipaxos::{storage::NoSnapshot, ClusterConfig};

    /// Implemented by hand, like the integration tests' entry type, since the `Entry`
    /// derive macro does not support the `unicache` feature.
    #[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct E(u64);

    #[cfg(feature = "unicache")]
    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    struct NoUniCache;

    #[cfg(feature = "unicache")]
    impl UniCache for NoUniCache {
        type T = E;

        fn new() -> Self {
            NoUniCache
        }

        fn try_encode(&mut self, entry: &E) -> E {
            entry.clone()
        }

        fn decode(&mut self, processed: E) -> E {
            processed
        }
    }

    impl Entry for E {
        type Snapshot = NoSnapshot;
        #[cfg(feature = "unicache")]
        type Encoded = ();
        #[cfg(feature = "unicache")]
        type Encodable = E;
        #[cfg(feature = "unicache")]
        type NotEncodable = ();
        #[cfg(feature = "unicache")]
        type EncodeResult = E;
        #[cfg(feature = "unicache")]
        type UniCache = NoUniCache;
    }

    type Rx = oneshot::Receiver<Result<usize, AppendError<E>>>;

    fn id(n: u128) -> EntryId {
        EntryId(uuid::Uuid::from_u128(n))
    }

    fn ballot(n: u32) -> Ballot {
        Ballot::with(1, n, 0, 1)
    }

    /// Inserts call `n` with entry `E(n)`, expiring `timeout` after `now`.
    fn insert(p: &mut PendingAppends<E>, n: u128, now: Instant, timeout: Duration) -> Rx {
        let (tx, rx) = oneshot::channel();
        p.insert(id(n), E(n as u64), now + timeout, tx);
        rx
    }

    fn result(rx: &mut Rx) -> Option<Result<usize, AppendError<E>>> {
        rx.try_recv().expect("sender dropped without a reply")
    }

    const LONG: Duration = Duration::from_secs(60);

    #[test]
    fn decided_calls_resolve_ok_up_to_decided_idx() {
        let now = Instant::now();
        let mut p = PendingAppends::new();
        let mut rx: Vec<Rx> = (0..3).map(|n| insert(&mut p, n, now, LONG)).collect();
        p.assign(id(0), 5, ballot(1));
        p.assign(id(1), 6, ballot(1));
        p.assign(id(2), 7, ballot(1));

        p.resolve(6, ballot(1), now);
        assert!(matches!(result(&mut rx[0]), Some(Ok(5))));
        assert!(matches!(result(&mut rx[1]), Some(Ok(6))));
        assert!(result(&mut rx[2]).is_none());
        assert_eq!(p.len(), 1);
    }

    #[test]
    fn decided_takes_priority_over_superseded() {
        let now = Instant::now();
        let mut p = PendingAppends::new();
        let mut decided = insert(&mut p, 0, now, LONG);
        let mut undecided = insert(&mut p, 1, now, LONG);
        p.assign(id(0), 5, ballot(1));
        p.assign(id(1), 6, ballot(1));

        p.resolve(5, ballot(2), now);
        assert!(matches!(result(&mut decided), Some(Ok(5))));
        assert!(matches!(
            result(&mut undecided),
            Some(Err(AppendError::Superseded { log_entry_idx: 6 }))
        ));
        assert_eq!(p.len(), 0);
    }

    #[test]
    fn assignment_under_older_ballot_is_superseded_after_ballot_already_changed() {
        let now = Instant::now();
        let mut p = PendingAppends::new();
        let mut rx = insert(&mut p, 0, now, LONG);
        p.resolve(0, ballot(1), now);
        p.resolve(0, ballot(2), now);

        // A late `Assigned` from the old leader arrives after the change was observed.
        p.assign(id(0), 5, ballot(1));
        p.resolve(0, ballot(2), now);
        assert!(matches!(
            result(&mut rx),
            Some(Err(AppendError::Superseded { log_entry_idx: 5 }))
        ));
    }

    #[test]
    fn assignment_under_current_ballot_is_not_superseded() {
        let now = Instant::now();
        let mut p = PendingAppends::new();
        let mut rx = insert(&mut p, 0, now, LONG);
        p.resolve(0, ballot(2), now);
        p.assign(id(0), 5, ballot(2));
        p.resolve(0, ballot(2), now);
        assert!(result(&mut rx).is_none());
    }

    #[test]
    fn calls_time_out_strictly_after_deadline() {
        let now = Instant::now();
        let mut p = PendingAppends::new();
        let mut unassigned = insert(&mut p, 0, now, Duration::from_secs(1));
        let mut assigned = insert(&mut p, 1, now, Duration::from_secs(2));
        p.assign(id(1), 5, ballot(1));

        p.resolve(0, ballot(1), now + Duration::from_secs(1));
        assert!(result(&mut unassigned).is_none());

        p.resolve(0, ballot(1), now + Duration::from_millis(1001));
        assert!(matches!(
            result(&mut unassigned),
            Some(Err(AppendError::Timeout))
        ));
        assert!(result(&mut assigned).is_none());

        p.resolve(0, ballot(1), now + Duration::from_millis(2001));
        assert!(matches!(
            result(&mut assigned),
            Some(Err(AppendError::Timeout))
        ));
        assert_eq!(p.len(), 0);
    }

    #[test]
    fn reassignment_replaces_previous_index() {
        let now = Instant::now();
        let mut p = PendingAppends::new();
        let mut rx = insert(&mut p, 0, now, LONG);
        p.assign(id(0), 5, ballot(1));
        p.assign(id(0), 9, ballot(1));

        p.resolve(5, ballot(1), now);
        assert!(result(&mut rx).is_none());
        p.resolve(9, ballot(1), now);
        assert!(matches!(result(&mut rx), Some(Ok(9))));
    }

    #[test]
    fn undispatched_taken_once_in_insertion_order_skipping_resolved() {
        let now = Instant::now();
        let mut p = PendingAppends::new();
        let _rx0 = insert(&mut p, 10, now, LONG);
        let _rx1 = insert(&mut p, 11, now, Duration::from_millis(1));
        let _rx2 = insert(&mut p, 12, now, LONG);
        p.resolve(0, ballot(1), now + Duration::from_millis(2));

        let taken = p.take_undispatched();
        assert_eq!(taken, vec![(id(10), E(10)), (id(12), E(12))]);
        assert!(p.take_undispatched().is_empty());

        let _rx3 = insert(&mut p, 13, now, LONG);
        assert_eq!(p.take_undispatched(), vec![(id(13), E(13))]);
    }

    #[test]
    fn fail_resolves_with_propose_error_and_ignores_unknown_ids() {
        let now = Instant::now();
        let mut p = PendingAppends::new();
        let mut rx = insert(&mut p, 0, now, LONG);
        let err = || ProposeErr::PendingReconfigConfig(ClusterConfig::default(), None);

        p.fail(id(99), err());
        assert_eq!(p.len(), 1);

        p.fail(id(0), err());
        assert!(matches!(
            result(&mut rx),
            Some(Err(AppendError::Propose(_)))
        ));
        assert_eq!(p.len(), 0);
        assert!(p.take_undispatched().is_empty());

        // A late assignment for the failed call is ignored.
        p.assign(id(0), 5, ballot(1));
        p.resolve(5, ballot(1), now);
        assert_eq!(p.len(), 0);
    }
}
