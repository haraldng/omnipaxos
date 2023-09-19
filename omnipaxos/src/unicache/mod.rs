#[cfg(feature = "unicache")]
pub mod lfu_cache;
#[cfg(feature = "unicache")]
pub mod lru_cache;

use crate::storage::Entry;
use num_traits::One;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Debug, Formatter},
    hash::Hash,
    marker::PhantomData,
    num::NonZeroUsize,
    ops::Add,
};

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
pub enum MaybeEncodedData<T: Entry> {
    Encoded(T::Encoded),
    NotEncoded(T::Encodable),
}

pub trait UniCache: Clone + Debug {
    type T: Entry;

    fn new() -> Self;

    fn try_encode(&mut self, entry: &Self::T) -> <Self::T as Entry>::EncodeResult;

    fn decode(&mut self, processed: <Self::T as Entry>::EncodeResult) -> Self::T;
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum MaybeEncoded<Encodable, Encoded> {
    NotEncoded(Encodable),
    Encoded(Encoded),
}

pub trait IncrementByOne: Default + Clone + One + Add<Output = Self> {}
impl<T: Default + Clone + One + Add<Output = Self>> IncrementByOne for T {}

#[cfg(not(feature = "serde"))]
pub trait DefaultEncodable: Clone + Hash + Eq + PartialEq {}
#[cfg(not(feature = "serde"))]
impl<T: Clone + Hash + Eq + PartialEq> DefaultEncodable for T {}
#[cfg(feature = "serde")]
pub trait DefaultEncodable:
    Clone + Hash + Eq + PartialEq + Serialize + for<'a> Deserialize<'a>
{
}
#[cfg(feature = "serde")]
impl<T: Clone + Hash + Eq + PartialEq + Serialize + for<'a> Deserialize<'a>> DefaultEncodable
    for T
{
}

pub trait DefaultEncoded: Clone + IncrementByOne + DefaultEncodable {}
impl<T: Clone + IncrementByOne + DefaultEncodable> DefaultEncoded for T {}

pub trait FieldCache<Encodable, Encoded> {
    fn new(size: usize) -> Self;

    fn try_encode(&mut self, field: &Encodable) -> MaybeEncoded<Encodable, Encoded>;

    fn decode(&mut self, result: MaybeEncoded<Encodable, Encoded>) -> Encodable;
}
