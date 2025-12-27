use chronicle::chronicle::retry_runner::{run_retry_loop, RetryContext};
use chronicle::chronicle::trigger_common::RetrySettings;
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use std::collections::VecDeque;
use std::time::Duration;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn build_retry_settings(initial_ms: u64, max_ms: u64, multiplier: f64) -> RetrySettings {
    let mut extras = JsonMap::new();
    extras.insert(
        "retry_initial_ms".to_string(),
        JsonValue::Number(initial_ms.into()),
    );
    extras.insert("retry_max_ms".to_string(), JsonValue::Number(max_ms.into()));
    // from_f64 returns None for NaN/Inf, but 2.0 is always valid
    extras.insert(
        "retry_multiplier".to_string(),
        JsonValue::Number(JsonNumber::from_f64(multiplier).unwrap_or_else(|| JsonNumber::from(2))),
    );
    RetrySettings::from_extras(&extras, &JsonMap::new())
}

struct ErrorSequenceContext {
    polls: VecDeque<Result<Option<u32>, &'static str>>,
    delays: Vec<Duration>,
    shutdown: CancellationToken,
}

impl ErrorSequenceContext {
    fn new(shutdown: CancellationToken) -> Self {
        let polls = VecDeque::from(vec![Err("first"), Err("second"), Err("third")]);
        Self {
            polls,
            delays: Vec::new(),
            shutdown,
        }
    }
}

#[async_trait::async_trait]
impl RetryContext for ErrorSequenceContext {
    type Item = u32;
    type Error = &'static str;

    async fn poll(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        self.polls.pop_front().unwrap_or(Ok(None))
    }

    async fn handle_item(&mut self, item: Self::Item) {
        // This scenario only errors, so receiving an item is a test failure
        let _ = item; // Acknowledge unused
        std::process::abort();
    }

    async fn report_error(&mut self, _error: &Self::Error, delay: Duration) {
        self.delays.push(delay);
        if self.delays.len() >= 3 {
            self.shutdown.cancel();
        }
    }
}

struct IdleContext;

#[async_trait::async_trait]
impl RetryContext for IdleContext {
    type Item = ();
    type Error = &'static str;

    async fn poll(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        Ok(None)
    }

    async fn handle_item(&mut self, item: Self::Item) {
        // Idle context returns Ok(None) from poll, so items are never received
        let _ = item;
        std::process::abort();
    }

    async fn report_error(&mut self, error: &Self::Error, delay: Duration) {
        // Idle context returns Ok(None) from poll, so errors never occur
        let _ = (error, delay);
        std::process::abort();
    }
}

#[tokio::test]
async fn retry_runner_backoff_increases_and_caps() {
    let settings = build_retry_settings(5, 400, 2.0);
    let shutdown = CancellationToken::new();
    let mut context = ErrorSequenceContext::new(shutdown.clone());

    run_retry_loop(
        shutdown.clone(),
        settings.clone(),
        Duration::from_millis(0),
        &mut context,
    )
    .await;

    assert_eq!(context.delays.len(), 3);
    assert!(context
        .delays
        .windows(2)
        .all(|window| window[0] <= window[1]));
    assert!(context.delays.iter().all(|delay| *delay <= settings.max()));
}

#[tokio::test]
async fn retry_runner_cancels_when_shutdown_fires() -> TestResult {
    let settings = build_retry_settings(100, 1_000, 2.0);
    let shutdown = CancellationToken::new();
    let cancel = shutdown.clone();
    tokio::spawn(async move {
        sleep(Duration::from_millis(10)).await;
        cancel.cancel();
    });

    let mut context = IdleContext;
    tokio::time::timeout(
        Duration::from_secs(1),
        run_retry_loop(shutdown, settings, Duration::from_millis(20), &mut context),
    )
    .await
    .map_err(|_| "runner should exit on cancellation within timeout")?;
    Ok(())
}
