// https://omnipaxos.com/docs/omnipaxos/omnipaxos/
use omnipaxos::macros::Entry;

#[derive(Clone, Debug, Entry)] // Clone and Debug are required traits.
pub struct KeyValue {
    pub key: String,
    pub value: u64,
}
