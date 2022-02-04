use std::fmt::Debug;
use kompact::prelude::*;
use crate::core::storage::{Snapshot, Storage};

pub struct OmniPaxos
{
    system: KompactSystem,
    pid: u64,
    comp: OmniPaxosComp

}

#[derive(ComponentDefinition)]
struct OmniPaxosComp {
}

struct SequencePaxosComp<T, S, B>
where
    T: Clone + Debug,
    S: Snapshot<T>,
    B: Storage<T, S>
{

}