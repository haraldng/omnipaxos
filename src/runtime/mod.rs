use kompact::prelude::*;
use crate::core::leader_election::ballot_leader_election::*;
use crate::core::sequence_paxos::*;
use crate::core::storage::*;

#[derive(ComponentDefinition)]
pub(crate) struct SequencePaxosComp<T: Entry, S: Snapshot<T>, B: Storage<T, S>>
{
    ctx: ComponentContext<Self>,
    seq_paxos: SequencePaxos<T, S, B>
}

#[derive(ComponentDefinition)]
pub(crate) struct BLEComp {
    ctx: ComponentContext<Self>,
    ble: BallotLeaderElection
}

