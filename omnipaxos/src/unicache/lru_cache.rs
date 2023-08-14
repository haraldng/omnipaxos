use crate::{
    storage::Entry,
    unicache::{MaybeEncodedData, PreProcessedEntry, ProcessedEntry, UniCache},
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

trait IncrementByOne: Default + Clone + One + Add<Output = Self> {}
impl<T: Default + Clone + One + Add<Output = Self>> IncrementByOne for T {}

#[cfg(not(feature = "serde"))]
trait LRUEncodable: Hash + Eq + PartialEq {}
#[cfg(not(feature = "serde"))]
impl<T: Hash + Eq + PartialEq> LRUEncodable for T {}
#[cfg(feature = "serde")]
trait LRUEncodable: Hash + Eq + PartialEq + Serialize + for<'a> Deserialize<'a> {}
#[cfg(feature = "serde")]
impl<T: Hash + Eq + PartialEq + Serialize + for<'a> Deserialize<'a>> LRUEncodable for T {}

trait LRUEncoded: IncrementByOne + LRUEncodable {}
impl<T: IncrementByOne + LRUEncodable> LRUEncoded for T {}

/// Wrapper to implement serde for LruCache
struct LruWrapper<K, V>(LruCache<K, V>);

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct LRUniCache<T: Entry>
where
    T::Encodable: Hash + Eq,
    T::Encoded: Hash + Eq,
{
    lru_cache_encoder: LruWrapper<T::Encodable, T::Encoded>,
    lru_cache_decoder: LruWrapper<T::Encoded, T::Encodable>,
    encoding: T::Encoded,
    size: usize,
}

impl<T> Clone for LRUniCache<T>
where
    T: Entry,
    T::Encoded: LRUEncoded,
    T::Encodable: LRUEncodable,
{
    fn clone(&self) -> Self {
        let mut new = Self::new(self.size);
        self.lru_cache_encoder
            .0
            .iter()
            .rev()
            .for_each(|(encodable, encoded)| {
                new.lru_cache_encoder
                    .0
                    .push(encodable.clone(), encoded.clone());
                new.lru_cache_decoder
                    .0
                    .push(encoded.clone(), encodable.clone());
            });
        new
    }
}

impl<T> Debug for LRUniCache<T>
where
    T: Entry,
    T::Encoded: LRUEncoded,
    T::Encodable: LRUEncodable,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("LRUnicache") // todo
    }
}

impl<T> UniCache<T> for LRUniCache<T>
where
    T: Entry,
    T::Encoded: LRUEncoded,
    T::Encodable: LRUEncodable,
{
    fn new(size: usize) -> Self {
        let s: NonZeroUsize = NonZeroUsize::new(size).unwrap();
        Self {
            lru_cache_encoder: LruWrapper(LruCache::new(s)),
            lru_cache_decoder: LruWrapper(LruCache::new(s)),
            encoding: T::Encoded::default(),
            size,
        }
    }

    fn try_encode(&mut self, pre_processed: PreProcessedEntry<T>) -> ProcessedEntry<T> {
        let PreProcessedEntry {
            encodable,
            not_encodable,
        } = pre_processed;
        let maybe_encoded = encodable
            .into_iter()
            .map(|e| {
                match self.lru_cache_encoder.0.get(&e) {
                    Some(encoding) => MaybeEncodedData::Encoded(encoding.clone()),
                    None => {
                        if self.lru_cache_encoder.0.len() == self.size {
                            // cache is full, replace LRU with new item
                            let (_, popped_encoding) = self.lru_cache_encoder.0.pop_lru().unwrap();
                            self.lru_cache_encoder.0.push(e.clone(), popped_encoding);
                        } else {
                            let one = T::Encoded::one();
                            let enc = std::mem::take(&mut self.encoding);
                            let added = enc.add(one);
                            self.lru_cache_encoder.0.push(e.clone(), added.clone());
                            self.encoding = added;
                        }
                        MaybeEncodedData::NotEncoded(e)
                    }
                }
            })
            .collect();
        ProcessedEntry {
            maybe_encoded,
            not_encodable,
        }
    }

    fn decode(&mut self, processed: ProcessedEntry<T>) -> PreProcessedEntry<T> {
        let ProcessedEntry {
            maybe_encoded,
            not_encodable,
        } = processed;
        let encodable = maybe_encoded
            .into_iter()
            .map(|e| match e {
                MaybeEncodedData::Encoded(encoding) => {
                    self.lru_cache_decoder.0.get(&encoding).unwrap().clone()
                }
                MaybeEncodedData::NotEncoded(not_encodable) => not_encodable,
            })
            .collect();
        PreProcessedEntry {
            encodable,
            not_encodable,
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

    impl<K: LRUEncodable, V: LRUEncodable> Serialize for LruWrapper<K, V> {
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

    impl<'de, K: LRUEncodable, V: LRUEncodable> Deserialize<'de> for LruWrapper<K, V> {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_seq(LruWrapperVisitor::new())
        }
    }

    struct LruWrapperVisitor<K, V> {
        _k: PhantomData<K>,
        _v: PhantomData<V>,
    }

    impl<K, V> LruWrapperVisitor<K, V> {
        fn new() -> Self {
            Self {
                _k: PhantomData::default(),
                _v: PhantomData::default(),
            }
        }
    }

    impl<'de, K, V> Visitor<'de> for LruWrapperVisitor<K, V>
    where
        K: LRUEncodable,
        V: LRUEncodable,
    {
        type Value = LruWrapper<K, V>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a sequence of key-value pairs")
        }

        fn visit_seq<S>(self, mut seq: S) -> Result<Self::Value, S::Error>
        where
            S: SeqAccess<'de>,
        {
            let size = seq.size_hint().unwrap();
            let mut lru = LruCache::new(NonZeroUsize::new(size).unwrap());
            while let Some((key, value)) = seq.next_element::<(K, V)>()? {
                lru.push(key, value);
            }
            // Wrap the LruCache in the LruWrapper
            Ok(LruWrapper(lru))
        }
    }
}
