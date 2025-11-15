use crate::config::integration::{JitterMode, RetryBudget};
use rand::Rng;
use std::cmp::{max, min};
use std::time::Duration;

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

#[cfg(test)]
mod tests {
    use super::*;

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
}
