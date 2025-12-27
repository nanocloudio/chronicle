use crate::config::integration::{DeliveryPolicy, JitterMode, RetryBudget};
use rand::Rng;
use std::cmp::{max, min};
use std::time::Duration;

/// A retry plan that computes backoff delays for delivery attempts.
///
/// This encapsulates the logic for merging a delivery policy with a retry budget
/// to determine how many retries are allowed and what backoff delays to use.
///
/// Used internally by `ActionDispatcher`. Exposed for integration testing.
#[derive(Debug, Clone)]
pub struct RetryPlan {
    max_retries: u32,
    max_elapsed: Option<Duration>,
    base_backoff: Duration,
    max_backoff: Duration,
    jitter: JitterMode,
}

impl RetryPlan {
    /// Create a new retry plan by merging an optional delivery policy with an optional retry budget.
    ///
    /// The plan uses the most restrictive settings from both sources:
    /// - Minimum of policy retries and budget max_attempts
    /// - Maximum of policy min_backoff and budget base_backoff
    /// - Minimum of policy max_backoff and budget max_backoff
    pub fn new(policy: Option<&DeliveryPolicy>, budget: Option<&RetryBudget>) -> Self {
        let policy_attempts = policy
            .and_then(|p| p.retries)
            .unwrap_or(u32::MAX)
            .saturating_add(1);
        let budget_attempts = budget
            .and_then(|b| b.max_attempts)
            .unwrap_or(u32::MAX)
            .max(1);
        let max_attempts = policy_attempts.min(budget_attempts).max(1);

        let mut base_backoff = policy
            .and_then(|p| p.backoff.as_ref().map(|b| b.min))
            .or_else(|| budget.and_then(|b| b.base_backoff))
            .unwrap_or(Duration::from_millis(0));
        if let Some(envelope) = budget.and_then(|b| b.base_backoff) {
            if base_backoff < envelope {
                base_backoff = envelope;
            }
        }

        let mut max_backoff = policy
            .and_then(|p| p.backoff.as_ref().map(|b| b.max))
            .or_else(|| budget.and_then(|b| b.max_backoff))
            .unwrap_or(base_backoff);
        if let Some(envelope) = budget.and_then(|b| b.max_backoff) {
            if max_backoff > envelope {
                max_backoff = envelope;
            }
        }
        if max_backoff < base_backoff {
            max_backoff = base_backoff;
        }

        Self {
            max_retries: max_attempts.saturating_sub(1),
            max_elapsed: budget.and_then(|b| b.max_elapsed),
            base_backoff,
            max_backoff,
            jitter: budget.and_then(|b| b.jitter).unwrap_or(JitterMode::None),
        }
    }

    /// Compute the next delay for a retry, if one is allowed.
    ///
    /// Returns `None` if:
    /// - The attempt count has exceeded max_retries
    /// - The elapsed time has exceeded max_elapsed
    /// - The remaining time within max_elapsed is less than the computed delay
    pub fn next_delay(&self, attempt: u32, elapsed: Duration) -> Option<Duration> {
        if attempt >= self.max_retries {
            return None;
        }

        let mut delay = self.backoff_for(attempt + 1);
        delay = match self.jitter {
            JitterMode::None => delay,
            JitterMode::Equal => jitter_between(delay.mul_f64(0.5), delay),
            JitterMode::Full => jitter_between(Duration::from_secs(0), delay),
        };

        if let Some(limit) = self.max_elapsed {
            if elapsed >= limit {
                return None;
            }
            let remaining = limit - elapsed;
            if remaining < delay {
                return None;
            }
        }

        Some(delay)
    }

    /// Compute the backoff duration for a given attempt index.
    ///
    /// Uses exponential backoff starting from base_backoff and capped at max_backoff.
    pub fn backoff_for(&self, attempt_index: u32) -> Duration {
        if self.base_backoff.is_zero() {
            return Duration::from_secs(0);
        }

        let exponent = attempt_index.saturating_sub(1).min(16);
        let factor = 1u32 << exponent;
        let mut delay = self.base_backoff.mul_f64(factor as f64);
        if delay > self.max_backoff {
            delay = self.max_backoff;
        }
        delay
    }
}

pub fn merge_retry_budgets<'a, I>(budgets: I) -> Option<RetryBudget>
where
    I: IntoIterator<Item = Option<&'a RetryBudget>>,
{
    let mut merged = RetryBudget::default();
    let mut seen = false;

    for budget in budgets.into_iter().flatten() {
        seen = true;
        merged.max_attempts = min_opt(merged.max_attempts, budget.max_attempts);
        merged.max_elapsed = min_duration_opt(merged.max_elapsed, budget.max_elapsed);
        merged.base_backoff = max_duration_opt(merged.base_backoff, budget.base_backoff);
        merged.max_backoff = min_duration_opt(merged.max_backoff, budget.max_backoff);
        merged.jitter = merge_jitter(merged.jitter, budget.jitter);
    }

    if seen {
        Some(merged)
    } else {
        None
    }
}

fn min_opt<T: Ord>(current: Option<T>, candidate: Option<T>) -> Option<T> {
    match (current, candidate) {
        (Some(lhs), Some(rhs)) => Some(min(lhs, rhs)),
        (Some(lhs), None) => Some(lhs),
        (None, Some(rhs)) => Some(rhs),
        (None, None) => None,
    }
}

fn max_duration_opt(current: Option<Duration>, candidate: Option<Duration>) -> Option<Duration> {
    match (current, candidate) {
        (Some(lhs), Some(rhs)) => Some(max(lhs, rhs)),
        (Some(lhs), None) => Some(lhs),
        (None, Some(rhs)) => Some(rhs),
        (None, None) => None,
    }
}

fn min_duration_opt(current: Option<Duration>, candidate: Option<Duration>) -> Option<Duration> {
    match (current, candidate) {
        (Some(lhs), Some(rhs)) => Some(min(lhs, rhs)),
        (Some(lhs), None) => Some(lhs),
        (None, Some(rhs)) => Some(rhs),
        (None, None) => None,
    }
}

fn merge_jitter(current: Option<JitterMode>, candidate: Option<JitterMode>) -> Option<JitterMode> {
    match (current, candidate) {
        (Some(lhs), Some(rhs)) => {
            if jitter_rank(rhs) >= jitter_rank(lhs) {
                Some(rhs)
            } else {
                Some(lhs)
            }
        }
        (Some(lhs), None) => Some(lhs),
        (None, Some(rhs)) => Some(rhs),
        (None, None) => None,
    }
}

const fn jitter_rank(mode: JitterMode) -> u8 {
    match mode {
        JitterMode::None => 0,
        JitterMode::Equal => 1,
        JitterMode::Full => 2,
    }
}

pub fn jitter_between(min: Duration, max: Duration) -> Duration {
    if max <= min {
        return min;
    }
    let mut rng = rand::thread_rng();
    let min_secs = min.as_secs_f64();
    let span = max.as_secs_f64() - min_secs;
    let sample = rng.gen::<f64>() * span + min_secs;
    Duration::from_secs_f64(sample)
}

pub fn retry_after_seconds_from_budget(budget: Option<&RetryBudget>) -> u64 {
    match budget {
        Some(budget) => {
            let mut delay = budget.base_backoff.unwrap_or(Duration::from_secs(1));
            if let Some(max_backoff) = budget.max_backoff {
                if delay > max_backoff {
                    delay = max_backoff;
                }
            }
            if let Some(max_elapsed) = budget.max_elapsed {
                if delay > max_elapsed {
                    delay = max_elapsed;
                }
            }
            duration_to_seconds(delay)
        }
        None => 1,
    }
}

fn duration_to_seconds(duration: Duration) -> u64 {
    let secs = duration.as_secs();
    if duration.subsec_nanos() == 0 {
        secs.max(1)
    } else {
        secs.saturating_add(1).max(1)
    }
}
