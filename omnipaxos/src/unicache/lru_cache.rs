use crate::{
    storage::Entry,
    unicache::{MaybeEncodedData, UniCache},
};
use lru::LruCache;
use num_traits::identities::One;
#[cfg(feature = "serde")]
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{
    fmt::{Debug, Formatter},
    hash::Hash,
    marker::PhantomData,
    num::NonZeroUsize,
    ops::Add,
};

use super::MaybeEncoded;

pub trait IncrementByOne: Default + Clone + One + Add<Output = Self> {}
impl<T: Default + Clone + One + Add<Output = Self>> IncrementByOne for T {}

#[cfg(not(feature = "serde"))]
pub trait LRUEncodable: Clone + Hash + Eq + PartialEq {}
#[cfg(not(feature = "serde"))]
impl<T: Clone + Hash + Eq + PartialEq> LRUEncodable for T {}
#[cfg(feature = "serde")]
pub trait LRUEncodable:
    Clone + Hash + Eq + PartialEq + Serialize + for<'a> Deserialize<'a>
{
}
#[cfg(feature = "serde")]
impl<T: Clone + Hash + Eq + PartialEq + Serialize + for<'a> Deserialize<'a>> LRUEncodable for T {}

pub trait LRUEncoded: Clone + IncrementByOne + LRUEncodable {}
impl<T: Clone + IncrementByOne + LRUEncodable> LRUEncoded for T {}

/// Wrapper to implement serde for LruCache
struct LruWrapper<Encodable, Encoded>(LruCache<Encodable, Encoded>);

impl<Encodable, Encoded> std::ops::Deref for LruWrapper<Encodable, Encoded> {
    type Target = LruCache<Encodable, Encoded>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<Encodable, Encoded> std::ops::DerefMut for LruWrapper<Encodable, Encoded> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub trait FieldCache<Encodable, Encoded> {
    fn new(size: usize) -> Self;

    fn try_encode(&mut self, field: &Encodable) -> MaybeEncoded<Encodable, Encoded>;

    fn decode(&mut self, result: MaybeEncoded<Encodable, Encoded>) -> Encodable;
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[serde(bound(deserialize = ""))]
pub struct LRUniCache<Encodable, Encoded>
where
    Encodable: LRUEncodable,
    Encoded: LRUEncoded,
{
    lru_cache_encoder: LruWrapper<Encodable, Encoded>,
    lru_cache_decoder: LruWrapper<Encoded, Encodable>,
    encoding: Encoded,
    size: usize,
}

impl<Encodable, Encoded> Clone for LRUniCache<Encodable, Encoded>
where
    Encodable: LRUEncodable,
    Encoded: LRUEncoded,
{
    fn clone(&self) -> Self {
        let mut new = Self::new(self.size);
        self.lru_cache_encoder
            .0
            .iter()
            .rev()
            .for_each(|(encodable, encoded)| {
                new.lru_cache_encoder
                    .push(encodable.clone(), encoded.clone());
                new.lru_cache_decoder
                    .push(encoded.clone(), encodable.clone());
            });
        new
    }
}

impl<Encodable, Encoded> Debug for LRUniCache<Encodable, Encoded>
where
    Encodable: LRUEncodable,
    Encoded: LRUEncoded,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("LRUnicache") // todo
    }
}

impl<Encodable, Encoded> FieldCache<Encodable, Encoded> for LRUniCache<Encodable, Encoded>
where
    Encodable: LRUEncodable,
    Encoded: LRUEncoded,
{
    fn new(size: usize) -> Self {
        let s: NonZeroUsize = NonZeroUsize::new(size).unwrap();
        Self {
            lru_cache_encoder: LruWrapper(LruCache::new(s)),
            lru_cache_decoder: LruWrapper(LruCache::new(s)),
            encoding: Encoded::default(),
            size,
        }
    }

    fn try_encode(&mut self, field: &Encodable) -> MaybeEncoded<Encodable, Encoded> {
        match self.lru_cache_encoder.get(&field) {
            Some(encoding) => MaybeEncoded::<Encodable, Encoded>::Encoded(encoding.clone()),
            None => {
                if self.lru_cache_encoder.len() == self.size {
                    // cache is full, replace LRU with new item
                    let (_, popped_encoding) = self.lru_cache_encoder.pop_lru().unwrap();
                    self.lru_cache_encoder.push(field.clone(), popped_encoding);
                } else {
                    let one = Encoded::one();
                    let enc = std::mem::take(&mut self.encoding);
                    let added = enc.add(one);
                    self.lru_cache_encoder.push(field.clone(), added.clone());
                    self.encoding = added;
                }
                MaybeEncoded::NotEncoded(field.clone())
            }
        }
    }

    fn decode(&mut self, result: MaybeEncoded<Encodable, Encoded>) -> Encodable {
        match result {
            MaybeEncoded::Encoded(encoding) => {
                self.lru_cache_decoder.get(&encoding).unwrap().clone()
            }
            MaybeEncoded::NotEncoded(not_encodable) => {
                let one = Encoded::one();
                let enc = std::mem::take(&mut self.encoding);
                let added = enc.add(one);
                self.lru_cache_decoder
                    .push(added.clone(), not_encodable.clone());
                self.encoding = added;
                not_encodable
            }
        }
    }
}

#[cfg(feature = "serde")]
mod serialization {
    use super::*;
    use serde::{
        de::{SeqAccess, Visitor},
        ser::SerializeSeq,
    };

    impl<Encodable: LRUEncodable, Encoded: LRUEncodable> Serialize for LruWrapper<Encodable, Encoded> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let len = self.0.len();
            let mut seq = serializer.serialize_seq(Some(len))?;
            let _ = self.0.iter().rev().for_each(|item| {
                seq.serialize_element(&item).unwrap();
            });
            seq.end()
        }
    }

    impl<'de, Encodable: LRUEncodable, Encoded: LRUEncodable> Deserialize<'de>
        for LruWrapper<Encodable, Encoded>
    {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_seq(LruWrapperVisitor::new())
        }
    }

    struct LruWrapperVisitor<Encodable, Encoded> {
        _k: PhantomData<Encodable>,
        _v: PhantomData<Encoded>,
    }

    impl<Encodable, Encoded> LruWrapperVisitor<Encodable, Encoded> {
        fn new() -> Self {
            Self {
                _k: PhantomData::default(),
                _v: PhantomData::default(),
            }
        }
    }

    impl<'de, Encodable, Encoded> Visitor<'de> for LruWrapperVisitor<Encodable, Encoded>
    where
        Encodable: LRUEncodable,
        Encoded: LRUEncodable,
    {
        type Value = LruWrapper<Encodable, Encoded>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a sequence of key-value pairs")
        }

        fn visit_seq<S>(self, mut seq: S) -> Result<Self::Value, S::Error>
        where
            S: SeqAccess<'de>,
        {
            let size = seq.size_hint().unwrap();
            let mut lru = LruCache::new(NonZeroUsize::new(size).unwrap());
            while let Some((key, value)) = seq.next_element::<(Encodable, Encoded)>()? {
                lru.push(key, value);
            }
            // Wrap the LruCache in the LruWrapper
            Ok(LruWrapper(lru))
        }
    }
}
