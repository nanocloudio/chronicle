use chronicle::config::integration::{JitterMode, RetryBudget};
use chronicle::retry::retry_after_seconds_from_budget;
use std::time::Duration;

#[test]
fn retry_after_respects_elapsed_and_backoff() {
    let budget = RetryBudget {
        max_attempts: Some(3),
        max_elapsed: Some(Duration::from_secs(2)),
        base_backoff: Some(Duration::from_secs(5)),
        max_backoff: Some(Duration::from_secs(10)),
        jitter: Some(JitterMode::None),
    };
    assert_eq!(
        retry_after_seconds_from_budget(Some(&budget)),
        2,
        "retry-after should clamp to max_elapsed"
    );
}

#[test]
fn retry_after_defaults_to_one_second() {
    assert_eq!(retry_after_seconds_from_budget(None), 1);
}
