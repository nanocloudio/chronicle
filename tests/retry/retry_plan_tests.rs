//! Tests for retry plan logic.

use chronicle::config::integration::{BackoffPolicy, DeliveryPolicy, JitterMode, RetryBudget};
use chronicle::retry::RetryPlan;
use std::time::Duration;

#[test]
fn retries_respect_tightest_budget() {
    let policy = DeliveryPolicy {
        retries: Some(5),
        backoff: Some(BackoffPolicy {
            min: Duration::from_millis(10),
            max: Duration::from_millis(10),
        }),
        idempotent: None,
    };
    let budget = RetryBudget {
        max_attempts: Some(3),
        max_elapsed: None,
        base_backoff: Some(Duration::from_millis(5)),
        max_backoff: Some(Duration::from_millis(5)),
        jitter: Some(JitterMode::None),
    };
    let plan = RetryPlan::new(Some(&policy), Some(&budget));
    assert!(plan.next_delay(0, Duration::ZERO).is_some());
    assert!(plan.next_delay(1, Duration::ZERO).is_some());
    assert!(plan.next_delay(2, Duration::ZERO).is_none());
}

#[test]
fn elapsed_window_enforced() {
    let policy = DeliveryPolicy::default();
    let budget = RetryBudget {
        max_attempts: Some(2),
        max_elapsed: Some(Duration::from_millis(50)),
        base_backoff: Some(Duration::from_millis(40)),
        max_backoff: Some(Duration::from_millis(40)),
        jitter: Some(JitterMode::None),
    };
    let plan = RetryPlan::new(Some(&policy), Some(&budget));
    assert!(plan.next_delay(0, Duration::from_millis(10)).is_some());
    assert!(plan.next_delay(0, Duration::from_millis(60)).is_none());
}

#[test]
fn backoff_exponential_growth() {
    let budget = RetryBudget {
        max_attempts: Some(10),
        max_elapsed: None,
        base_backoff: Some(Duration::from_millis(100)),
        max_backoff: Some(Duration::from_secs(60)),
        jitter: Some(JitterMode::None),
    };
    let plan = RetryPlan::new(None, Some(&budget));

    // Attempt 1: base_backoff
    assert_eq!(plan.backoff_for(1), Duration::from_millis(100));
    // Attempt 2: 2x base
    assert_eq!(plan.backoff_for(2), Duration::from_millis(200));
    // Attempt 3: 4x base
    assert_eq!(plan.backoff_for(3), Duration::from_millis(400));
    // Attempt 4: 8x base
    assert_eq!(plan.backoff_for(4), Duration::from_millis(800));
}

#[test]
fn backoff_respects_max_backoff() {
    let budget = RetryBudget {
        max_attempts: Some(10),
        max_elapsed: None,
        base_backoff: Some(Duration::from_millis(100)),
        max_backoff: Some(Duration::from_millis(500)),
        jitter: Some(JitterMode::None),
    };
    let plan = RetryPlan::new(None, Some(&budget));

    // After max_backoff is reached, it should stay capped
    assert_eq!(plan.backoff_for(5), Duration::from_millis(500)); // Would be 1600, but capped
    assert_eq!(plan.backoff_for(6), Duration::from_millis(500));
}

#[test]
fn no_retries_when_max_attempts_is_one() {
    let budget = RetryBudget {
        max_attempts: Some(1),
        max_elapsed: None,
        base_backoff: Some(Duration::from_millis(100)),
        max_backoff: Some(Duration::from_secs(10)),
        jitter: Some(JitterMode::None),
    };
    let plan = RetryPlan::new(None, Some(&budget));

    // With max_attempts=1, no retries allowed (only the initial attempt)
    assert!(plan.next_delay(0, Duration::ZERO).is_none());
}

#[test]
fn policy_retries_zero_means_one_attempt() {
    let policy = DeliveryPolicy {
        retries: Some(0),
        backoff: None,
        idempotent: None,
    };
    let plan = RetryPlan::new(Some(&policy), None);

    // retries=0 means 1 total attempt, so no retry on first failure
    assert!(plan.next_delay(0, Duration::ZERO).is_none());
}
