Consensus protocols typically treat the data being replicated as black boxes. However, real-world applications may have skewed workloads that can result in redundant replication. As an example, consider a distributed database that stores the customers of a store. The database uses OmniPaxos to replicate the customers:

```rust
use omnipaxos_macros::Entry;

#[derive(Clone, Debug, Entry)]
struct Customer {
    id: u64,
    first_name: String,
    last_name: String,
    email: String,
    profession: String,
}
```

Each customer will have unique `id` and `email`, but the `first_name`, `last_name`, and `profession` are fields that may repeat for different customers. 

TODO: add figure showing redundancy here

To avoid redundant replication of repeated data, we can use the UniCache feature in OmniPaxos. UniCache learns from the replication history and constructs a cache of popular data at each server. **The cache is embedded into OmniPaxos** such that when cached data is encountered again, it gets transmitted as a compressed encoding (e.g., an integer) instead. In this way, OmniPaxos can exploit the skew to reduce the amount of data transferred over network.

TODO: UniCache figure

```rust
use omnipaxos_macros::UniCacheEntry;

#[derive(Clone, Debug, UniCacheEntry)]
struct Customer {
    id: u64,
    #[unicache(encoding(u16), size(1000))]
    first_name: String,
    #[unicache(encoding(u8))]
    last_name: String,
    email: String,
    #[unicache(encoding(u8), cache(lru))]
    profession: String,
}
```

Here, we use the `unicache` macro to annotate the fields that should be cached. We also define the type that the field should be encoded as.

TODO example of encoding and decoding.



