#[cfg(feature = "logging")]
use slog::{o, Drain, Logger};
#[cfg(feature = "logging")]
use std::{fs::OpenOptions, sync::Mutex};

#[cfg(feature = "logging")]
/// Creates an asynchronous logger which outputs to both the terminal and a specified file_path.
pub fn create_logger(file_path: &str) -> Logger {
    let path = std::path::Path::new(file_path);
    let prefix = path.parent().unwrap(); // todo change unwrap
    std::fs::create_dir_all(prefix).unwrap(); // todo change unwrap

    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(file_path)
        .unwrap(); // todo change unwrap

    let term_decorator = slog_term::TermDecorator::new().build();
    let file_decorator = slog_term::PlainSyncDecorator::new(file);

    let term_fuse = slog_term::FullFormat::new(term_decorator).build().fuse();
    let file_fuse = slog_term::FullFormat::new(file_decorator).build().fuse();

    let both = Mutex::new(slog::Duplicate::new(term_fuse, file_fuse)).fuse();
    let both = slog_async::Async::new(both).build().fuse();
    slog::Logger::root(both, o!())
}
