// https://omnipaxos.com/docs/omnipaxos/omnipaxos/
use omnipaxos::macros::Entry;
use serde::{Deserialize, Serialize};

// Original: https://omnipaxos.com/docs/omnipaxos/omnipaxos/
#[derive(Clone, Debug, Serialize, Deserialize, Entry)]
pub struct KeyValue {
    pub key: String,
    pub value: u64,
}
