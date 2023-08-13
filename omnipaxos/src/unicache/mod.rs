pub mod lru_cache;

use std::fmt::{Debug, Formatter};
// use crate::sequence_paxos::Role;
use crate::storage::Entry;
// TODO use num_traits::PrimInt;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(not(feature = "serde"))]
pub trait Encoded: Clone + Debug {}
#[cfg(not(feature = "serde"))]
impl<T: Clone + Debug> Encoded for T {}
#[cfg(feature = "serde")]
pub trait Encoded: Clone + Debug + Serialize + for<'a> Deserialize<'a> {}
#[cfg(feature = "serde")]
impl<T: Clone + Debug + Serialize + for<'a> Deserialize<'a>> Encoded for T {}

#[cfg(not(feature = "serde"))]
pub trait Encodable: Clone + Debug {}
#[cfg(not(feature = "serde"))]
impl<T: Clone + Debug> Encodable for T {}
#[cfg(feature = "serde")]
pub trait Encodable: Clone + Debug + Serialize + for<'a> Deserialize<'a> {}
#[cfg(feature = "serde")]
impl<T: Clone + Debug + Serialize + for<'a> Deserialize<'a>> Encodable for T {}

#[cfg(not(feature = "serde"))]
pub trait NotEncodable: Clone + Debug {}
#[cfg(not(feature = "serde"))]
impl<T: Clone + Debug> NotEncodable for T {}
#[cfg(feature = "serde")]
pub trait NotEncodable: Clone + Debug + Serialize + for<'a> Deserialize<'a> {}
#[cfg(feature = "serde")]
impl<T: Clone + Debug + Serialize + for<'a> Deserialize<'a>> NotEncodable for T {}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
enum MaybeEncodedData<T: Entry> {
    Encoded(T::Encoded),
    NotEncoded(T::Encodable),
}

pub struct PreProcessedEntry<T: Entry> {
    pub encodable: Vec<T::Encodable>,
    pub not_encodable: Vec<T::NotEncodable>,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct ProcessedEntry<T: Entry> {
    maybe_encoded: Vec<MaybeEncodedData<T>>,
    not_encodable: Vec<T::NotEncodable>,
}

impl<T: Entry> Clone for ProcessedEntry<T> {
    fn clone(&self) -> Self {
        Self {
            maybe_encoded: self.maybe_encoded.clone(),
            not_encodable: self.not_encodable.clone(),
        }
    }
}

impl<T: Entry> Debug for ProcessedEntry<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessedEntry")
            .field("maybe_encoded", &self.maybe_encoded)
            .field("not_encodable", &self.not_encodable)
            .finish()
    }
}

pub trait UniCache<T: Entry>: Clone + Debug {
    fn new(size: usize) -> Self;

    fn try_encode(&mut self, pre_processed: PreProcessedEntry<T>) -> ProcessedEntry<T>;

    fn decode(&mut self, processed: ProcessedEntry<T>) -> PreProcessedEntry<T>;
}
