# OmniPaxos Runtime
Async runtime actor layer for [OmniPaxos](https://omnipaxos.com/). Spawns an `OmniPaxos`
instance as a background actor and returns a cloneable `OmniPaxosHandle` with `async`
methods to append entries, subscribe to decided entries, and observe leader/reconfiguration
events -- no manual driving of `tick()`, `send_msg()`, or `outgoing_messages()` needed.

The runtime is generic over the async executor via the `AsyncRuntime` trait; a ready-to-use
`TokioRuntime` is provided behind the `tokio_runtime` feature. See
`examples/kv_store_async` for a full usage example.
