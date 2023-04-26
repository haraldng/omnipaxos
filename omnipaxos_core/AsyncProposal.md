# Async API Proposal
Current ideas on the `async` api/feature for omnipaxos.

### Callbacks to handle messages
Instead of making the user poll for messages, we require them to
provide a callback function that handles message transport.
The signature of the callback might look like this:
```rust
dyn async fn(omnipaxos::Message)
```

### Notification on decide
The `append` function could return a future that resolves once the
entry has been decided. This would probably require the `Eq` trait on `Entry`.

### Handling timers internally
We don't need a timer to poll for messages anymore, but we do still need the
election timeout. We could ask the user to provide a `std::time::Duration` that
we then use internally for that.

### Async storage
This would be amazing, but we have to figure out what we actually can do without
compromising correctness.



## The shadow-log
The shadow-log will be used to keep track of which entries have been decided or will never be
decided, in order to guarantee that futures for proposed entries will eventually be resolved or cancelled.

TO FIGURE OUT: can we use another storage instance for the shadow-log or do we need something else?
Specifically, does the shadow-log need to store pending, unreplicated shadow entries or is it enough to have these in the waiting futures?

### Entry
The replicated part of a shadow-log entry is:
 - the proposer's PID
 - a locally unique proposal counter
 - the proposer's promised ballot
 - None, or the entry's index in the log?

A shadow-log entry is pending and has no index, until it has been replicated back to its proposer.
Then its index will be the same as the corresponding entry's log index.

### Resolving the future
The future for a shadow-log entry `x` can be resolved if it is waiting and the entry corresponding to `x` has been decided.

### Canceling the future
The future for a shadow-log entry `x` can be cancelled if it is waiting and:
 - either an AccSync message with a higher ballot has been received
 - or it is still pending and an entry with shadow-log entry `y` with `y.PID == x.PID == self.PID && y.counter > x.counter` has been decided
Both of these conditions can be checked within the futures when receiving on the broadcast channel.
So the broadcast must handle 2 types of messages:
 - "decided" shadow-log entries
 - notification of AccSync with new ballot

TODO: on AccSync, some futures might resolve, while others cancel. What are the exact rules around this?

### Cleanup
The shadow-log does not need to be persisted indefinitely.
A shadow-log entry can be cleaned up if:
 - it has been replicated to its proposer (or the ballot increased?), if the proposer is different from the local process
 - or the future has been resolved or canceled, if the proposer is the local process

This cleanup will not be part of the first implementation, but can be added later on.
