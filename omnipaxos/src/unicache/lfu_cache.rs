use crate::unicache::*;
use lfu::LFUCache;
use std::fmt::format;

#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct LFUniCache<Encodable, Encoded>
where
    Encodable: Hash + Eq + Clone,
    Encoded: Hash + Eq + Clone,
{
    lfu_cache_encoder: LFUCache<Encodable, Encoded>,
    lfu_cache_decoder: LFUCache<Encoded, Encodable>,
    encoding: Encoded,
    size: usize,
}

impl<Encodable, Encoded> Clone for LFUniCache<Encodable, Encoded>
    where
        Encodable: DefaultEncodable,
        Encoded: DefaultEncoded,
{
    fn clone(&self) -> Self {
        let mut lfu_cache_decoder = LFUCache::with_capacity(self.size);
        self.lfu_cache_encoder
            .iter()
            .for_each(|(encodable, encoded)| {
                lfu_cache_decoder
                    .set(encoded.clone(), encodable.clone());
            });
        Self {
            lfu_cache_encoder: LFUCache::with_capacity(1),
            lfu_cache_decoder,
            encoding: self.encoding.clone(),
            size: self.size,
        }
    }
}

impl<Encodable, Encoded> FieldCache<Encodable, Encoded> for LFUniCache<Encodable, Encoded>
where
    Encodable: DefaultEncodable,
    Encoded: DefaultEncoded,
{
    fn new(size: usize) -> Self {
        Self {
            lfu_cache_encoder: LFUCache::with_capacity(size),
            lfu_cache_decoder: LFUCache::with_capacity(size),
            encoding: Encoded::default(),
            size,
        }
    }

    fn try_encode(&mut self, field: &Encodable) -> MaybeEncoded<Encodable, Encoded> {
        match self.lfu_cache_encoder.get(&field) {
            Some(encoding) => MaybeEncoded::<Encodable, Encoded>::Encoded(encoding.clone()),
            None => {
                if self.lfu_cache_encoder.len() == self.size {
                    // cache is full, replace LRU with new item
                    let popped_encoding = self.lfu_cache_encoder.evict_and_return_value().unwrap();
                    self.lfu_cache_encoder.set(field.clone(), popped_encoding);
                } else {
                    let one = Encoded::one();
                    let enc = std::mem::take(&mut self.encoding);
                    let added = enc.add(one);
                    self.lfu_cache_encoder.set(field.clone(), added.clone());
                    self.encoding = added;
                }
                MaybeEncoded::NotEncoded(field.clone())
            }
        }
    }

    fn decode(&mut self, result: MaybeEncoded<Encodable, Encoded>) -> Encodable {
        match result {
            MaybeEncoded::Encoded(encoding) => {
                self.lfu_cache_decoder.get(&encoding).unwrap().clone()
            }
            MaybeEncoded::NotEncoded(not_encodable) => {
                if self.lfu_cache_decoder.len() == self.size {
                    // cache is full, replace LRU with new item
                    let popped_encoded = self.lfu_cache_decoder.evict_and_return_key().unwrap();
                    self.lfu_cache_decoder
                        .set(popped_encoded, not_encodable.clone());
                } else {
                    let one = Encoded::one();
                    let enc = std::mem::take(&mut self.encoding);
                    let added = enc.add(one);
                    self.lfu_cache_decoder
                        .set(added.clone(), not_encodable.clone());
                    self.encoding = added;
                }
                not_encodable
            }
        }
    }
}
