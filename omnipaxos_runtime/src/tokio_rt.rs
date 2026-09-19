use core::{future::Future, time::Duration};

use super::traits::AsyncRuntime;

/// [`AsyncRuntime`] impl backed by [`tokio`]. Enabled by the `tokio_runtime` feature.
pub struct TokioRuntime;

impl AsyncRuntime for TokioRuntime {
    type JoinHandle = tokio::task::JoinHandle<()>;

    fn spawn<F>(future: F) -> Self::JoinHandle
    where
        F: Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(future)
    }

    fn sleep(duration: Duration) -> impl Future<Output = ()> + Send + 'static {
        tokio::time::sleep(duration)
    }
}
