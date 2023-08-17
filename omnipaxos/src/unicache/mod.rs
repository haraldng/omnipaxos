#[cfg(feature = "unicache")]
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
pub enum MaybeEncodedData<T: Entry> {
    Encoded(T::Encoded),
    NotEncoded(T::Encodable),
}

// pub struct PreProcessedEntry<T: Entry> {
//     pub encodable: Vec<T::Encodable>,
//     pub not_encodable: Vec<T::NotEncodable>,
// }

// #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
// pub struct ProcessedEntry<T: Entry> {
//     maybe_encoded: Vec<MaybeEncodedData<T>>,
//     not_encodable: Vec<T::NotEncodable>,
// }

// impl<T: Entry> Clone for ProcessedEntry<T> {
//     fn clone(&self) -> Self {
//         Self {
//             maybe_encoded: self.maybe_encoded.clone(),
//             not_encodable: self.not_encodable.clone(),
//         }
//     }
// }
//
// impl<T: Entry> Debug for ProcessedEntry<T> {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         f.debug_struct("ProcessedEntry")
//             .field("maybe_encoded", &self.maybe_encoded)
//             .field("not_encodable", &self.not_encodable)
//             .finish()
//     }
// }

pub trait UniCache: Clone + Debug {
    type T: Entry;

    fn new(size: usize) -> Self;

    fn try_encode(&mut self, entry: &Self::T) -> <Self::T as Entry>::EncodeResult;

    fn decode(&mut self, processed: <Self::T as Entry>::EncodeResult) -> Self::T;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MaybeEncoded<Encodable, Encoded> {
    NotEncoded(Encodable),
    Encoded(Encoded),
}

// #[cfg(test)]
mod unicache_macro_test {
    use omnipaxos_macros::UniCacheEntry;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Serialize, Deserialize, UniCacheEntry)]
    struct T {
        #[unicache(encoding(u8), size(100))]
        f0: String,
        f1: i32,
        #[unicache(encoding(u128))]
        f2: u64,
        #[unicache(size(20), cache(LRUniCache), encoding(u64))]
        f3: i64,
    }

    #[derive(Clone, Debug, PartialEq)]
    struct S {
        f0: String,
        f1: i32,
        f2: u64,
    }

    use std::num::NonZeroUsize;

    use lru::LruCache;

    use crate::storage::{Entry, NoSnapshot};

    use super::{lru_cache::FieldCache, MaybeEncoded, UniCache};
    use crate::unicache::lru_cache::LRUniCache;

    impl Entry for S {
        type Snapshot = NoSnapshot;
        type Encoded = (u8, u8);
        type Encodable = (String, u64);
        type NotEncodable = (i32,);
        type EncodeResult = (MaybeEncoded<String, u8>, i32, MaybeEncoded<u64, u8>);
        type UniCache = CacheS;
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct CacheS {
        f0: LRUniCache<String, u8>,
        f2: LRUniCache<u64, u8>,
    }

    impl UniCache for CacheS {
        type T = S;
        fn new(size: usize) -> Self {
            Self {
                f0: LRUniCache::new(size),
                f2: LRUniCache::new(size),
            }
        }

        fn try_encode(&mut self, entry: &Self::T) -> <Self::T as Entry>::EncodeResult {
            let f0_result = self.f0.try_encode(&entry.f0);
            let f2_result = self.f2.try_encode(&entry.f2);
            (f0_result, entry.f1.clone(), f2_result)
        }

        fn decode(
            &mut self,
            (f0_result, f1, f2_result): <Self::T as Entry>::EncodeResult,
        ) -> Self::T {
            let f0 = self.f0.decode(f0_result);
            let f2 = self.f2.decode(f2_result);
            S { f0, f1, f2 }
        }
    }
}
