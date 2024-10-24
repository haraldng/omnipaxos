use super::LinkedHashSetWrapper;
use linked_hash_set::LinkedHashSet;
use serde::{de::Visitor, ser::SerializeSeq, Deserialize, Deserializer, Serialize, Serializer};
use std::{fmt, hash::Hash};

impl<T> Serialize for LinkedHashSetWrapper<T>
where
    T: Serialize + PartialEq + Eq + Hash + Clone,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let len = self.0.len();
        let mut seq = serializer.serialize_seq(Some(len))?;
        self.0.iter().for_each(|item| {
            seq.serialize_element(&item).unwrap();
        });
        seq.end()
    }
}

impl<'de, T> Deserialize<'de> for LinkedHashSetWrapper<T>
where
    T: Deserialize<'de> + Eq + Hash + Clone,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct LinkedHashSetVisitor<T> {
            marker: std::marker::PhantomData<T>,
        }

        impl<'de, T> Visitor<'de> for LinkedHashSetVisitor<T>
        where
            T: Deserialize<'de> + Eq + Hash + Clone,
        {
            type Value = LinkedHashSetWrapper<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a sequence of elements")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let size = seq.size_hint().unwrap_or(u8::MAX as usize);
                let mut lsh = LinkedHashSet::with_capacity(size);
                while let Some(value) = seq.next_element()? {
                    lsh.insert(value);
                }
                Ok(LinkedHashSetWrapper(lsh))
            }
        }

        deserializer.deserialize_seq(LinkedHashSetVisitor {
            marker: std::marker::PhantomData,
        })
    }
}

#[cfg(test)]
mod tests {

    use super::super::{tests::check_equality, *};

    #[test]
    fn ser_test() {
        let mut lfu = LFUCache::with_capacity(2);
        lfu.set(1, 1);
        lfu.set(2, 2);
        lfu.set(3, 3);
        lfu.set(4, 4);

        let ser = serde_json::to_string(&lfu).unwrap();
        let lfu2: LFUCache<i32, i32> = serde_json::from_str(&ser).unwrap();
        check_equality(&lfu2, &lfu);
    }

    #[test]
    fn clone_ser_test() {
        let mut lfu = LFUCache::with_capacity(2);
        lfu.set(1, 1);
        lfu.set(2, 2);
        lfu.set(3, 3);
        lfu.set(4, 4);
        let lfu2 = lfu.clone();

        let ser = serde_json::to_string(&lfu2).unwrap();
        let lfu3: LFUCache<i32, i32> = serde_json::from_str(&ser).unwrap();
        check_equality(&lfu3, &lfu);
    }
}
