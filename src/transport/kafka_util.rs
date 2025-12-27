#![forbid(unsafe_code)]

use crate::transport::runtime::sleep_with_shutdown;
use std::future::Future;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Sleeps for the provided duration unless the shutdown token is cancelled.
/// Returns `true` if shutdown was requested before the delay elapsed.
///
/// This is a thin wrapper around [`sleep_with_shutdown`] for backward compatibility.
#[inline]
pub async fn wait_backoff(delay: Duration, shutdown: &CancellationToken) -> bool {
    sleep_with_shutdown(delay, shutdown).await
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
