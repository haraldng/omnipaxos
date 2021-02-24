use crate::leader_election::{Leader, Round};
use std::{fmt::Debug, marker::PhantomData, sync::Arc};

pub trait SequenceTraits<R>: Sequence<R> + Debug + Send + Sync + 'static
where
    R: Round,
{
}
pub trait StateTraits<R>: PaxosState<R> + Send + 'static
where
    R: Round,
{
}

#[derive(Clone, Debug, PartialEq)]
pub enum Entry<R>
where
    R: Round,
{
    Normal(Vec<u8>),
    StopSign(StopSign<R>),
}

impl<R> Entry<R>
where
    R: Round,
{
    pub fn is_stopsign(&self) -> bool {
        matches!(self, Entry::StopSign(_))
    }
}

#[derive(Clone, Debug)]
pub struct StopSign<R>
where
    R: Round,
{
    pub config_id: u32,
    pub nodes: Vec<u64>,
    pub skip_prepare_use_leader: Option<Leader<R>>, // skip prepare phase in new config with the given leader
}

impl<R> StopSign<R>
where
    R: Round,
{
    pub fn with(
        config_id: u32,
        nodes: Vec<u64>,
        skip_prepare_use_leader: Option<Leader<R>>,
    ) -> Self {
        StopSign {
            config_id,
            nodes,
            skip_prepare_use_leader,
        }
    }
}

impl<R> PartialEq for StopSign<R>
where
    R: Round,
{
    fn eq(&self, other: &Self) -> bool {
        self.config_id == other.config_id && self.nodes == other.nodes
    }
}

pub trait Sequence<R>
where
    R: Round,
{
    fn new() -> Self;

    fn new_with_sequence(seq: Vec<Entry<R>>) -> Self;

    fn append_entry(&mut self, entry: Entry<R>);

    fn append_sequence(&mut self, seq: &mut Vec<Entry<R>>);

    fn append_on_prefix(&mut self, from_idx: u64, seq: &mut Vec<Entry<R>>);

    fn get_entries(&self, from: u64, to: u64) -> &[Entry<R>];

    fn get_ser_entries(&self, from: u64, to: u64) -> Option<Vec<u8>>;

    fn get_suffix(&self, from: u64) -> Vec<Entry<R>>;

    fn get_ser_suffix(&self, from: u64) -> Option<Vec<u8>>;

    fn get_sequence(&self) -> Vec<Entry<R>>;

    fn get_sequence_len(&self) -> u64;

    fn stopped(&self) -> bool;
}

pub trait PaxosState<R>
where
    R: Round,
{
    fn new() -> Self;

    fn set_promise(&mut self, nprom: R);

    fn set_decided_len(&mut self, ld: u64);

    fn set_accepted_round(&mut self, na: R);

    fn get_accepted_round(&self) -> R;

    fn get_decided_len(&self) -> u64;

    fn get_promise(&self) -> R;
}

enum PaxosSequence<R, S>
where
    R: Round,
    S: Sequence<R>,
{
    Active(S),
    Stopped(Arc<S>),
    None,
    _Never(PhantomData<R>), // make cargo happy for unused type R
}

pub struct Storage<R, S, P>
where
    R: Round,
    S: Sequence<R>,
    P: PaxosState<R>,
{
    sequence: PaxosSequence<R, S>,
    paxos_state: P,
    _round_type: PhantomData<R>, // make cargo happy for unused type R
}

impl<R, S, P> Storage<R, S, P>
where
    R: Round,
    S: Sequence<R>,
    P: PaxosState<R>,
{
    pub fn with(seq: S, paxos_state: P) -> Storage<R, S, P> {
        let sequence = PaxosSequence::Active(seq);
        Storage {
            sequence,
            paxos_state,
            _round_type: PhantomData,
        }
    }

    pub fn append_entry(&mut self, entry: Entry<R>) -> u64 {
        match &mut self.sequence {
            PaxosSequence::Active(s) => {
                s.append_entry(entry);
                s.get_sequence_len()
            }
            PaxosSequence::Stopped(_) => {
                panic!("Sequence should not be modified after reconfiguration");
            }
            _ => panic!("Got unexpected intermediate PaxosSequence::None"),
        }
    }

    pub fn append_sequence(&mut self, sequence: &mut Vec<Entry<R>>) -> u64 {
        match &mut self.sequence {
            PaxosSequence::Active(s) => {
                s.append_sequence(sequence);
                s.get_sequence_len()
            }
            PaxosSequence::Stopped(_) => {
                panic!("Sequence should not be modified after reconfiguration");
            }
            _ => panic!("Got unexpected intermediate PaxosSequence::None"),
        }
    }

    pub fn append_on_prefix(&mut self, from_idx: u64, seq: &mut Vec<Entry<R>>) -> u64 {
        match &mut self.sequence {
            PaxosSequence::Active(s) => {
                s.append_on_prefix(from_idx, seq);
                s.get_sequence_len()
            }
            PaxosSequence::Stopped(s) => {
                if &s.get_suffix(from_idx) != seq {
                    panic!("Sequence should not be modified after reconfiguration");
                } else {
                    s.get_sequence_len()
                }
            }
            _ => panic!("Got unexpected intermediate PaxosSequence::None"),
        }
    }

    pub fn append_on_decided_prefix(&mut self, seq: Vec<Entry<R>>) {
        let from_idx = self.get_decided_len();
        match &mut self.sequence {
            PaxosSequence::Active(s) => {
                let mut sequence = seq;
                s.append_on_prefix(from_idx, &mut sequence);
            }
            PaxosSequence::Stopped(_) => {
                if !seq.is_empty() {
                    panic!("Sequence should not be modified after reconfiguration");
                }
            }
            _ => panic!("Got unexpected intermediate PaxosSequence::None"),
        }
    }

    pub fn set_promise(&mut self, nprom: R) {
        self.paxos_state.set_promise(nprom);
    }

    pub fn set_decided_len(&mut self, ld: u64) {
        self.paxos_state.set_decided_len(ld);
    }

    pub fn set_accepted_round(&mut self, na: R) {
        self.paxos_state.set_accepted_round(na);
    }

    pub fn get_accepted_round(&self) -> R {
        self.paxos_state.get_accepted_round()
    }

    pub fn get_entries(&self, from: u64, to: u64) -> &[Entry<R>] {
        match &self.sequence {
            PaxosSequence::Active(s) => s.get_entries(from, to),
            PaxosSequence::Stopped(s) => s.get_entries(from, to),
            _ => panic!("Got unexpected intermediate PaxosSequence::None in get_entries"),
        }
    }

    pub fn get_sequence_len(&self) -> u64 {
        match self.sequence {
            PaxosSequence::Active(ref s) => s.get_sequence_len(),
            PaxosSequence::Stopped(ref arc_s) => arc_s.get_sequence_len(),
            _ => panic!("Got unexpected intermediate PaxosSequence::None in get_sequence_len"),
        }
    }

    pub fn get_decided_len(&self) -> u64 {
        self.paxos_state.get_decided_len()
    }

    pub fn get_suffix(&self, from: u64) -> Vec<Entry<R>> {
        match self.sequence {
            PaxosSequence::Active(ref s) => s.get_suffix(from),
            PaxosSequence::Stopped(ref arc_s) => arc_s.get_suffix(from),
            _ => panic!("Got unexpected intermediate PaxosSequence::None in get_suffix"),
        }
    }

    pub fn get_promise(&self) -> R {
        self.paxos_state.get_promise()
    }

    pub fn stopped(&self) -> bool {
        match self.sequence {
            PaxosSequence::Active(ref s) => s.stopped(),
            PaxosSequence::Stopped(_) => true,
            _ => panic!("Got unexpected intermediate PaxosSequence::None in stopped()"),
        }
    }

    pub fn stop_and_get_sequence(&mut self) -> Arc<S> {
        let a = std::mem::replace(&mut self.sequence, PaxosSequence::None);
        match a {
            PaxosSequence::Active(s) => {
                let arc_s = Arc::from(s);
                self.sequence = PaxosSequence::Stopped(arc_s.clone());
                arc_s
            }
            _ => panic!("Storage should already have been stopped!"),
        }
    }

    pub fn get_sequence(&self) -> Vec<Entry<R>> {
        match &self.sequence {
            PaxosSequence::Active(s) => s.get_sequence(),
            PaxosSequence::Stopped(s) => s.get_sequence(),
            _ => panic!("Got unexpected intermediate PaxosSequence::None in get_sequence"),
        }
    }
}
