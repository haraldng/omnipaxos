# Logging

OmniPaxos uses the [slog](https://crates.io/crates/slog) crate to provide system wide logging facilities. Logging works out of the box with a default asynchronous console and file logger implementation that roughly corresponds to the following setup code:

```rust,edition2018,no_run,noplaypen
let path = std::path::Path::new(file_path);
let prefix = path.parent().unwrap();
std::fs::create_dir_all(prefix).unwrap();

let file = OpenOptions::new()
    .create(true)
    .write(true)
    .truncate(true)
    .open(file_path)
    .unwrap();

let term_decorator = slog_term::TermDecorator::new().build();
let file_decorator = slog_term::PlainSyncDecorator::new(file);

let term_fuse = slog_term::FullFormat::new(term_decorator).build().fuse();
let file_fuse = slog_term::FullFormat::new(file_decorator).build().fuse();

let both = Mutex::new(slog::Duplicate::new(term_fuse, file_fuse)).fuse();
let both = slog_async::Async::new(both).build().fuse();
let logger = slog::Logger::root(both, o!());
```

The actual logging levels are controlled via build features. The default features correspond to `max_level_trace` and `release_max_level_info`, that is in debug builds all levels are shown, while in the release profile only `info` and more severe message are shown.

## Custom Logger

Sometimes the default logging configuration is not sufficient for a particular application. For example, you might need a larger queue size in the `Async` drain, or you may want to write to a file instead of the terminal.

In the following example we replace the default terminal logger with a file logger, logging to `/tmp/myloggingfile` instead. We also increase the queue size in the `Async` drain to 2048, so that it fits the 2048 logging events we are sending it short succession later. In order to replace the default logger, we give the logger as an argument to the OmniPaxos constructor.

```rust,edition2018,no_run,noplaypen
let logger = {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(FILE_NAME)
        .expect("logging file");

    // create logger
    let decorator = slog_term::PlainSyncDecorator::new(file);
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).chan_size(2048).build().fuse();
    slog::Logger::root_typed(
        Arc::new(drain),
        o!(
        "location" => slog::PushFnValue(|r: &slog::Record<'_>, ser: slog::PushFnValueSerializer<'_>| {
            ser.emit(format_args!("{}:{}", r.file(), r.line()))
        })),
    )
};

let replica = OmniPaxos::with(
    1,
    pid,
    peer_pids,
    Storage::with(
        MemorySequence::<Ballot>::new(),
        MemoryState::<Ballot>::new(),
    ),
    None,
    logger,
    None,
),
```

> **Note:** The custom logger can be created for the [**Ballot Leader Election**](../ble/index.md) module as well.
