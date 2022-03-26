use std::collections::HashMap;
use omnipaxos_core::{
    ballot_leader_election::messages::{BLEMessage},
    messages::Message,
    storage::{Snapshot},};

use serde::{Deserialize, Serialize};
use structopt::StructOpt;

/// An wrap structure for network message between different nodes
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Package {
    pub types: Types,
    pub msg: Msg,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum Types {
    BLE,
    SP,
    CMD,
}

/// An enum for all the different Network message types.
#[allow(missing_docs)]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum Msg {

    BLE(BLEMessage),
    SP(Message<KeyValue, KVSnapshot>),
    CMD(CMDMessage),
}

#[allow(missing_docs)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct CMDMessage {
    pub operation: Operaiton,
    pub kv: KeyValue,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum Operaiton {
    Read,
    Write,
    Snap,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct KeyValue {
    pub key: String,
    pub value: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct KVSnapshot {
    pub snapshotted: HashMap<String, u64>,
}

impl Snapshot<KeyValue> for KVSnapshot {
    fn create(entries: &[KeyValue]) -> Self {
        let mut snapshotted = HashMap::new();
        for e in entries {
            let KeyValue { key, value } = e;
            snapshotted.insert(key.clone(), *value);
        }
        Self { snapshotted }
    }

    fn merge(&mut self, delta: Self) {
        for (k, v) in delta.snapshotted {
            self.snapshotted.insert(k, v);
        }
    }

    fn use_snapshots() -> bool {
        true
    }
}

#[derive(Debug, StructOpt, Serialize, Deserialize)]
pub (crate) struct Node {

    #[structopt(long)]
    pub id: u64,
    
    #[structopt(long)]
    pub peers: Vec<u64>,

}