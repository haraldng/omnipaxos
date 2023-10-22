#[cfg(feature = "unicache")]
/// UniCache with LFU eviction policy
pub mod lfu_cache;
#[cfg(feature = "unicache")]
/// UniCache with LRU eviction policy
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
/// The encoded type of a field. If there is a cache hit in UniCache, the field will be replaced and get sent over the network as this type.
pub trait Encoded: Clone + Debug {}
#[cfg(not(feature = "serde"))]
impl<T: Clone + Debug> Encoded for T {}
#[cfg(feature = "serde")]
/// The encoded type of a field. If there is a cache hit in UniCache, the field will be replaced and get sent over the network as this type.
pub trait Encoded: Clone + Debug + Serialize + for<'a> Deserialize<'a> {}
#[cfg(feature = "serde")]
impl<T: Clone + Debug + Serialize + for<'a> Deserialize<'a>> Encoded for T {}

#[cfg(not(feature = "serde"))]
/// The encodable type of a field i.e., a field in Entry that should be considered by UniCache.
pub trait Encodable: Clone + Debug {}
#[cfg(not(feature = "serde"))]
impl<T: Clone + Debug> Encodable for T {}
#[cfg(feature = "serde")]
/// The encodable type of a field i.e., a field in Entry that should be considered by UniCache.
pub trait Encodable: Clone + Debug + Serialize + for<'a> Deserialize<'a> {}
#[cfg(feature = "serde")]
impl<T: Clone + Debug + Serialize + for<'a> Deserialize<'a>> Encodable for T {}

#[cfg(not(feature = "serde"))]
/// Type for those fields in Entry that should not be considered by UniCache.
pub trait NotEncodable: Clone + Debug {}
#[cfg(not(feature = "serde"))]
impl<T: Clone + Debug> NotEncodable for T {}
#[cfg(feature = "serde")]
/// Type for those fields in Entry that should not be considered by UniCache.
pub trait NotEncodable: Clone + Debug + Serialize + for<'a> Deserialize<'a> {}
#[cfg(feature = "serde")]
impl<T: Clone + Debug + Serialize + for<'a> Deserialize<'a>> NotEncodable for T {}

/// The UniCache trait. Implement this trait for your own UniCache implementation.
pub trait UniCache: Clone + Debug {
    /// The type of the entry that the UniCache will be used for.
    type T: Entry;

    /// Create a new UniCache instance.
    fn new() -> Self;

    /// Try to encode an entry by checking if the fields in the entry are in the cache.
    fn try_encode(&mut self, entry: &Self::T) -> <Self::T as Entry>::EncodeResult;

    /// Decode an entry by replacing the encoded values in the entry with the real cached values.
    fn decode(&mut self, processed: <Self::T as Entry>::EncodeResult) -> Self::T;
}

/// The result of an trying to encode a field.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum MaybeEncoded<Encodable, Encoded> {
    /// Cache hit. The field is replaced with the encoded representation.
    Encoded(Encoded),
    /// Cache miss. The field is not replaced and holds the original value.
    NotEncoded(Encodable),
}

/// Trait for incrementing a value by one. It is used to generate the encoding values in UniCache.
pub trait IncrementByOne: Default + Clone + One + Add<Output = Self> {}
impl<T: Default + Clone + One + Add<Output = Self>> IncrementByOne for T {}

#[cfg(not(feature = "serde"))]
/// Blanket implementation for types that are encodable by default.
pub trait DefaultEncodable: Clone + Hash + Eq + PartialEq {}
#[cfg(not(feature = "serde"))]
impl<T: Clone + Hash + Eq + PartialEq> DefaultEncodable for T {}
#[cfg(feature = "serde")]
/// Blanket implementation for types that are encodable by default.
pub trait DefaultEncodable:
    Clone + Hash + Eq + PartialEq + Serialize + for<'a> Deserialize<'a>
{
}
#[cfg(feature = "serde")]
impl<T: Clone + Hash + Eq + PartialEq + Serialize + for<'a> Deserialize<'a>> DefaultEncodable
    for T
{
}

/// Blanket implementation for types that can be used as Encoded type and can increment by one by default.
pub trait DefaultEncoded: Clone + IncrementByOne + DefaultEncodable {}
impl<T: Clone + IncrementByOne + DefaultEncodable> DefaultEncoded for T {}

/// The UniCache of an individual field of an entry.
pub trait FieldCache<Encodable, Encoded> {
    /// Create a new UniCache instance.
    fn new(size: usize) -> Self;

    /// Try to encode the field of an entry by checking if it exists in the cache.
    fn try_encode(&mut self, field: &Encodable) -> MaybeEncoded<Encodable, Encoded>;

    /// Decode the encoded representation of a field by checking the cache.
    fn decode(&mut self, result: MaybeEncoded<Encodable, Encoded>) -> Encodable;
}
