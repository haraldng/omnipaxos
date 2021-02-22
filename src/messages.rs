use crate::storage::{Entry};
use crate::leader_election::Round;

#[derive(Clone, Debug)]
pub struct Prepare<R> where R: Round {
    pub n: R,
    pub ld: u64,
    pub n_accepted: R,
}

impl<R> Prepare<R> where R: Round {
    pub fn with(n: R, ld: u64, n_accepted: R) -> Self {
        Prepare { n, ld, n_accepted }
    }
}

#[derive(Clone, Debug)]
pub struct Promise<R> where R: Round {
    pub n: R,
    pub n_accepted: R,
    pub sfx: Vec<Entry<R>>,
    pub ld: u64,
}

impl<R> Promise<R> where R: Round {
    pub fn with(n: R, n_accepted: R, sfx: Vec<Entry<R>>, ld: u64) -> Self {
        Promise {
            n,
            n_accepted,
            sfx,
            ld,
        }
    }
}

#[derive(Clone, Debug)]
pub struct AcceptSync<R> where R: Round  {
    pub n: R,
    pub entries: Vec<Entry<R>>,
    pub ld: u64,
    pub sync: bool, // true -> append on prefix(ld), false -> append
}

impl<R> AcceptSync<R> where R: Round {
    pub fn with(n: R, sfx: Vec<Entry<R>>, ld: u64, sync: bool) -> Self {
        AcceptSync {
            n,
            entries: sfx,
            ld,
            sync,
        }
    }
}

#[derive(Clone, Debug)]
pub struct FirstAccept<R> where R: Round {
    pub n: R,
    pub entries: Vec<Entry<R>>,
}

impl<R> FirstAccept<R> where R: Round {
    pub fn with(n: R, entries: Vec<Entry<R>>) -> Self {
        FirstAccept { n, entries }
    }
}

#[derive(Clone, Debug)]
pub struct AcceptDecide<R> where R: Round {
    pub n: R,
    pub ld: u64,
    pub entries: Vec<Entry<R>>,
}

impl<R> AcceptDecide<R> where R: Round {
    pub fn with(n: R, ld: u64, entries: Vec<Entry<R>>) -> Self {
        AcceptDecide { n, ld, entries }
    }
}

#[derive(Clone, Debug)]
pub struct Accepted<R> where R: Round {
    pub n: R,
    pub la: u64,
}

impl<R> Accepted<R> where R: Round {
    pub fn with(n: R, la: u64) -> Self {
        Accepted { n, la }
    }
}

#[derive(Clone, Debug)]
pub struct Decide<R> where R: Round {
    pub ld: u64,
    pub n: R,
}

impl<R> Decide<R> where R: Round {
    pub fn with(ld: u64, n: R) -> Self {
        Decide { ld, n }
    }
}

#[derive(Clone, Debug)]
pub enum PaxosMsg<R> where R: Round {
    PrepareReq,
    Prepare(Prepare<R>),
    Promise(Promise<R>),
    AcceptSync(AcceptSync<R>),
    FirstAccept(FirstAccept<R>),
    AcceptDecide(AcceptDecide<R>),
    Accepted(Accepted<R>),
    Decide(Decide<R>),
    ProposalForward(Vec<Entry<R>>),
}

#[derive(Clone, Debug)]
pub struct Message<R> where R: Round {
    pub from: u64,
    pub to: u64,
    pub msg: PaxosMsg<R>,
}

impl<R> Message<R> where R: Round {
    pub fn with(from: u64, to: u64, msg: PaxosMsg<R>) -> Self {
        Message { from, to, msg }
    }
}