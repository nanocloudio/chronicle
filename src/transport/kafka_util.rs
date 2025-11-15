#![forbid(unsafe_code)]

use std::future::Future;
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;

/// Sleeps for the provided duration unless the shutdown token is cancelled.
/// Returns `true` if shutdown was requested before the delay elapsed.
pub async fn wait_backoff(delay: Duration, shutdown: &CancellationToken) -> bool {
    tokio::select! {
        _ = shutdown.cancelled() => true,
        _ = sleep(delay) => false,
    }
}

/// Executes the provided commit future and invokes `on_error` if it fails.
pub async fn commit_with_logging<F, Fut, E, OnError>(commit: F, on_error: OnError)
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<(), E>>,
    OnError: FnOnce(E),
{
    if let Err(err) = commit().await {
        on_error(err);
    }
}
