#![forbid(unsafe_code)]

use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;

/// Sleeps for a duration but aborts early if the shutdown token fires.
/// Returns `true` if shutdown occurred during the wait.
pub async fn sleep_with_shutdown(duration: Duration, shutdown: &CancellationToken) -> bool {
    tokio::select! {
        _ = shutdown.cancelled() => true,
        _ = sleep(duration) => false,
    }
}
