//! LFU cache implementation based on the [lfu](https://crates.io/crates/lfu) crate. It has been modified to to support the required operations for UniCache in OmniPaxos.

#[cfg(feature = "serde")]
mod serialization;

use linked_hash_set::LinkedHashSet;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::{
    collections::{
        hash_map::{IntoIter, Iter},
        HashMap,
    },
    fmt::Debug,
    hash::Hash,
    ops::Index,
};

#[derive(Clone, Debug)]
struct LinkedHashSetWrapper<K: Hash + Eq + Clone>(LinkedHashSet<K>);

impl<K: Hash + Eq + Clone> Default for LinkedHashSetWrapper<K> {
    fn default() -> Self {
        LinkedHashSetWrapper(LinkedHashSet::default())
    }
}

#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct LFUCache<K: Hash + Eq + Clone, V> {
    values: HashMap<K, ValueCounter<V>>,
    frequency_bin: HashMap<usize, LinkedHashSetWrapper<K>>,
    capacity: usize,
    min_frequency: usize,
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
struct ValueCounter<V> {
    value: V,
    count: usize,
}

impl<V> ValueCounter<V> {
    fn inc(&mut self) {
        self.count += 1;
    }
}

impl<K: Hash + Eq + Clone, V> LFUCache<K, V> {
    pub fn with_capacity(capacity: usize) -> LFUCache<K, V> {
        if capacity == 0 {
            panic!("Unable to create cache: capacity is {:?}", capacity);
        }
        LFUCache {
            values: HashMap::new(),
            frequency_bin: HashMap::new(),
            capacity,
            min_frequency: 0,
        }
    }

    pub fn contains(&self, key: &K) -> bool {
        self.values.contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn remove(&mut self, key: K) -> Option<V> {
        if let Some(value_counter) = self.values.get(&key) {
            let count = value_counter.count;
            self.frequency_bin.entry(count).or_default().0.remove(&key);
            self.values.remove(&key).map(|x| x.value)
        } else {
            None
        }
    }

    /// Returns the value associated with the given key (if it still exists)
    /// Method marked as mutable because it internally updates the frequency of the accessed key
    pub fn get(&mut self, key: &K) -> Option<&V> {
        self.update_frequency_bin(key);
        self.values.get(key).map(|x| &x.value)
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.update_frequency_bin(key);
        self.values.get_mut(key).map(|x| &mut x.value)
    }

    fn update_frequency_bin(&mut self, key: &K) {
        if let Some(value_counter) = self.values.get_mut(key) {
            let bin = self.frequency_bin.get_mut(&value_counter.count).unwrap();
            bin.0.remove(key);
            let count = value_counter.count;
            value_counter.inc();
            if count == self.min_frequency && bin.0.is_empty() {
                self.min_frequency += 1;
            }
            self.frequency_bin
                .entry(count + 1)
                .or_default()
                .0
                .insert(key.clone());
        }
    }

    fn evict(&mut self) {
        let least_frequently_used_keys = self.frequency_bin.get_mut(&self.min_frequency).unwrap();
        let least_recently_used = least_frequently_used_keys.0.pop_front().unwrap();
        self.values.remove(&least_recently_used);
    }

    pub fn evict_and_return_key(&mut self) -> Option<K> {
        match self.frequency_bin.get_mut(&self.min_frequency) {
            Some(least_frequently_used_keys) => {
                let least_recently_used = least_frequently_used_keys.0.pop_front().unwrap();
                self.values.remove(&least_recently_used);
                Some(least_recently_used)
            }
            None => None,
        }
    }

    pub fn evict_and_return_value(&mut self) -> Option<V> {
        match self.frequency_bin.get_mut(&self.min_frequency) {
            Some(least_frequently_used_keys) => {
                let least_recently_used = least_frequently_used_keys.0.pop_front().unwrap();
                self.values.remove(&least_recently_used).map(|x| x.value)
            }
            None => None,
        }
    }

    pub fn iter(&self) -> LfuIterator<K, V> {
        LfuIterator {
            values: self.values.iter(),
        }
    }

    pub fn set(&mut self, key: K, value: V) {
        if let Some(value_counter) = self.values.get_mut(&key) {
            value_counter.value = value;
            self.update_frequency_bin(&key);
            return;
        }
        if self.len() >= self.capacity {
            self.evict();
        }
        self.values
            .insert(key.clone(), ValueCounter { value, count: 1 });
        self.min_frequency = 1;
        self.frequency_bin
            .entry(self.min_frequency)
            .or_default()
            .0
            .insert(key);
    }
}

pub struct LfuIterator<'a, K, V> {
    values: Iter<'a, K, ValueCounter<V>>,
}

pub struct LfuConsumer<K, V> {
    values: IntoIter<K, ValueCounter<V>>,
}

impl<K, V> Iterator for LfuConsumer<K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        self.values.next().map(|(k, v)| (k, v.value))
    }
}

impl<K: Eq + Hash + Clone, V> IntoIterator for LFUCache<K, V> {
    type Item = (K, V);
    type IntoIter = LfuConsumer<K, V>;

    fn into_iter(self) -> Self::IntoIter {
        LfuConsumer {
            values: self.values.into_iter(),
        }
    }
}

impl<'a, K: Hash + Eq + Clone, V> Iterator for LfuIterator<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        self.values.next().map(|(key, vc)| (key, &vc.value))
    }
}

impl<'a, K: Hash + Eq + Clone, V> IntoIterator for &'a LFUCache<K, V> {
    type Item = (&'a K, &'a V);

    type IntoIter = LfuIterator<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        return self.iter();
    }
}

impl<K: Hash + Eq + Clone, V> Index<K> for LFUCache<K, V> {
    type Output = V;
    fn index(&self, index: K) -> &Self::Output {
        return self.values.get(&index).map(|x| &x.value).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub fn check_equality<K: Clone + Eq + Hash + Debug, V: Eq + Debug>(
        lfu: &LFUCache<K, V>,
        lfu2: &LFUCache<K, V>,
    ) {
        for (key, vc) in lfu.values.iter() {
            let (_key2, vc2) = lfu2.values.get_key_value(key).unwrap();
            assert_eq!(vc.count, vc2.count);
            assert_eq!(vc.value, vc2.value);
        }
        for (fb, fb2) in lfu.frequency_bin.iter().zip(lfu2.frequency_bin.iter()) {
            assert_eq!(fb.0, fb2.0);
            assert_eq!(fb.1 .0, fb2.1 .0);
        }
    }

    #[test]
    fn it_works() {
        let mut lfu = LFUCache::with_capacity(20);
        lfu.set(10, 10);
        lfu.set(20, 30);
        assert_eq!(lfu.get(&10).unwrap(), &10);
        assert_eq!(lfu.get(&30), None);
    }

    #[test]
    fn test_lru_eviction() {
        let mut lfu = LFUCache::with_capacity(2);
        lfu.set(1, 1);
        lfu.set(2, 2);
        lfu.set(3, 3);
        assert_eq!(lfu.get(&1), None)
    }

    #[test]
    fn test_key_frequency_update() {
        let mut lfu = LFUCache::with_capacity(2);
        lfu.set(1, 1);
        lfu.set(2, 2);
        lfu.set(1, 3);
        lfu.set(10, 10);
        assert_eq!(lfu.get(&2), None);
        assert_eq!(lfu[10], 10);
    }

    #[test]
    fn test_lfu_indexing() {
        let mut lfu: LFUCache<i32, i32> = LFUCache::with_capacity(2);
        lfu.set(1, 1);
        assert_eq!(lfu[1], 1);
    }

    #[test]
    fn test_lfu_deletion() {
        let mut lfu = LFUCache::with_capacity(2);
        lfu.set(1, 1);
        lfu.set(2, 2);
        lfu.remove(1);
        assert_eq!(lfu.get(&1), None);
        lfu.set(3, 3);
        lfu.set(4, 4);
        assert_eq!(lfu.get(&2), None);
        assert_eq!(lfu.get(&3), Some(&3));
    }

    #[test]
    fn test_duplicates() {
        let mut lfu = LFUCache::with_capacity(2);
        lfu.set(1, 1);
        lfu.set(1, 2);
        lfu.set(1, 3);
        {
            lfu.set(5, 20);
        }

        assert_eq!(lfu[1], 3);
    }

    #[test]
    fn test_lfu_consumption() {
        let mut lfu = LFUCache::with_capacity(1);
        lfu.set(&1, 1);
        for (_, v) in lfu {
            assert_eq!(v, 1);
        }
    }

    #[test]
    fn test_lfu_iter() {
        let mut lfu = LFUCache::with_capacity(2);
        lfu.set(&1, 1);
        lfu.set(&2, 2);
        for (key, v) in lfu.iter() {
            match *key {
                1 => {
                    assert_eq!(v, &1);
                }
                2 => {
                    assert_eq!(v, &2);
                }
                _ => {}
            }
        }
    }

    #[test]
    fn clone_test() {
        let mut lfu = LFUCache::with_capacity(2);
        lfu.set(1, 1);
        lfu.set(2, 2);
        lfu.set(3, 3);

        let lfu2 = lfu.clone();
        check_equality(&lfu, &lfu2);
    }

    #[test]
    fn evict_and_return_value_test() {
        let mut lfu = LFUCache::with_capacity(2);
        lfu.set(1, 1);
        lfu.set(2, 2);

        let _ = lfu.get(&1);
        let evicted = lfu.evict_and_return_value();
        assert_eq!(evicted, Some(2));

        lfu.set(3, 3);
        assert_eq!(lfu.get(&2), None);
    }
}
