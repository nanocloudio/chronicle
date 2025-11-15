use crate::chronicle::trigger_common::RetrySettings;
use crate::transport::runtime::sleep_with_shutdown;
use async_trait::async_trait;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[async_trait]
pub trait RetryContext {
    type Item;
    type Error;

    async fn poll(&mut self) -> Result<Option<Self::Item>, Self::Error>;
    async fn handle_item(&mut self, item: Self::Item);
    async fn report_error(&mut self, error: &Self::Error, delay: Duration);
}

pub async fn run_retry_loop<C>(
    shutdown: CancellationToken,
    settings: RetrySettings,
    idle_delay: Duration,
    context: &mut C,
) where
    C: RetryContext + Send,
{
    let mut current = settings.initial();

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            result = context.poll() => match result {
                Ok(Some(item)) => {
                    current = settings.initial();
                    context.handle_item(item).await;
                }
                Ok(None) => {
                    if sleep_with_shutdown(idle_delay, &shutdown).await {
                        break;
                    }
                }
                Err(err) => {
                    let delay = backoff_delay(current, &settings);
                    current = next_backoff(current, &settings);
                    context.report_error(&err, delay).await;
                    if sleep_with_shutdown(delay, &shutdown).await {
                        break;
                    }
                }
            }
        }
    }
}

fn backoff_delay(current: Duration, _settings: &RetrySettings) -> Duration {
    current.max(Duration::from_millis(50))
}

fn next_backoff(current: Duration, settings: &RetrySettings) -> Duration {
    let delay = backoff_delay(current, settings);
    let next = (delay.as_millis() as f64 * settings.multiplier()).round();
    let capped = next.min(settings.max().as_millis() as f64);
    let next_duration = Duration::from_millis(capped as u64);
    std::cmp::min(next_duration, settings.max())
}
