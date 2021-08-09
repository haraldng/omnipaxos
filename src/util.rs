use crate::leader_election::Round;
use slog::{o, Drain, Logger};
use std::cmp::Ordering;
use std::fs::OpenOptions;
use std::sync::Mutex;

#[derive(Debug, Clone)]
/// Promise without the suffix
pub(crate) struct PromiseMetaData<R>
where
    R: Round,
{
    pub n: R,
    pub la: u64,
    pub pid: u64,
}

impl<R> PromiseMetaData<R>
where
    R: Round,
{
    pub fn with(n: R, la: u64, pid: u64) -> Self {
        Self { n, la, pid }
    }
}

impl<R> Ord for PromiseMetaData<R>
where
    R: Round,
{
    fn cmp(&self, other: &Self) -> Ordering {
        if self.n == other.n && self.la == other.la && self.pid == other.pid {
            Ordering::Equal
        } else if self.n > other.n && self.la > other.la {
            Ordering::Greater
        } else {
            Ordering::Less
        }
    }
}

impl<R> PartialOrd for PromiseMetaData<R>
where
    R: Round,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let ordering = if self.n == other.n && self.la == other.la && self.pid == other.pid {
            Ordering::Equal
        } else if self.n > other.n || (self.n == other.n && self.la > other.la) {
            Ordering::Greater
        } else {
            Ordering::Less
        };
        Some(ordering)
    }
}

impl<R> PartialEq for PromiseMetaData<R>
where
    R: Round,
{
    fn eq(&self, other: &Self) -> bool {
        self.n == other.n && self.la == other.la && self.pid == other.pid
    }
}

impl<R> Eq for PromiseMetaData<R> where R: Round {}

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
