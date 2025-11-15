use humantime::parse_duration;
use serde::Deserialize;
use serde_yaml::Value as YamlValue;
use std::time::Duration;

pub(crate) const KNOWN_FEATURE_FLAGS: &[&str] = &[
    "rabbitmq",
    "mqtt",
    "mongodb",
    "redis",
    "parallel_phase",
    "serialize_phase",
];

pub fn known_feature_flags() -> &'static [&'static str] {
    KNOWN_FEATURE_FLAGS
}

pub fn default_app_retry_budget() -> RetryBudget {
    RetryBudget {
        max_attempts: Some(5),
        max_elapsed: Some(Duration::from_secs(30)),
        base_backoff: Some(Duration::from_millis(50)),
        max_backoff: Some(Duration::from_secs(5)),
        jitter: Some(JitterMode::Full),
    }
}

fn merge_retry_budget_with_defaults(
    defaults: &RetryBudget,
    overrides: Option<RetryBudget>,
) -> RetryBudget {
    let mut merged = defaults.clone();
    if let Some(override_budget) = overrides {
        if override_budget.max_attempts.is_some() {
            merged.max_attempts = override_budget.max_attempts;
        }
        if override_budget.max_elapsed.is_some() {
            merged.max_elapsed = override_budget.max_elapsed;
        }
        if override_budget.base_backoff.is_some() {
            merged.base_backoff = override_budget.base_backoff;
        }
        if override_budget.max_backoff.is_some() {
            merged.max_backoff = override_budget.max_backoff;
        }
        if override_budget.jitter.is_some() {
            merged.jitter = override_budget.jitter;
        }
    }
    merged
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub min_ready_routes: MinReadyRoutes,
    pub half_open_counts_as_ready: HalfOpenPolicy,
    pub warmup_timeout: Duration,
    pub readiness_cache: Duration,
    pub drain_timeout: Duration,
    pub limits: AppLimits,
    pub retry_budget: Option<RetryBudget>,
    pub feature_flags: Vec<String>,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            min_ready_routes: MinReadyRoutes::All,
            half_open_counts_as_ready: HalfOpenPolicy::Route,
            warmup_timeout: Duration::from_secs(30),
            readiness_cache: Duration::from_millis(250),
            drain_timeout: Duration::from_secs(30),
            limits: AppLimits::default(),
            retry_budget: Some(default_app_retry_budget()),
            feature_flags: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum MinReadyRoutes {
    #[default]
    All,
    Count(u32),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HalfOpenPolicy {
    Never,
    Endpoint,
    #[default]
    Route,
}

#[derive(Debug, Clone)]
pub struct AppLimits {
    pub routes: Option<RouteLimits>,
    pub http: Option<HttpLimits>,
    pub kafka: Option<KafkaLimits>,
}

impl Default for AppLimits {
    fn default() -> Self {
        Self {
            routes: Some(default_route_limits()),
            http: Some(default_http_limits()),
            kafka: Some(default_kafka_limits()),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RouteLimits {
    pub max_inflight: Option<u32>,
    pub overflow_policy: Option<OverflowPolicy>,
    pub max_queue_depth: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowPolicy {
    Reject,
    Queue,
    Shed,
}

#[derive(Debug, Clone, Default)]
pub struct HttpLimits {
    pub max_concurrency: Option<u32>,
    pub max_payload_bytes: Option<u64>,
}

#[derive(Debug, Clone, Default)]
pub struct KafkaLimits {
    pub max_inflight_per_partition: Option<u32>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct RetryBudget {
    pub max_attempts: Option<u32>,
    pub max_elapsed: Option<Duration>,
    pub base_backoff: Option<Duration>,
    pub max_backoff: Option<Duration>,
    pub jitter: Option<JitterMode>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JitterMode {
    None,
    Equal,
    Full,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawAppSection {
    #[serde(default)]
    pub(crate) min_ready_routes: Option<YamlValue>,
    #[serde(default)]
    pub(crate) half_open_counts_as_ready: Option<String>,
    #[serde(default)]
    pub(crate) warmup_timeout: Option<String>,
    #[serde(default)]
    pub(crate) readiness_cache: Option<String>,
    #[serde(default)]
    pub(crate) drain_timeout: Option<String>,
    #[serde(default)]
    pub(crate) limits: Option<RawAppLimits>,
    #[serde(default)]
    pub(crate) retry_budget: Option<RawRetryBudget>,
    #[serde(default)]
    pub(crate) feature_flags: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawAppLimits {
    #[serde(default)]
    pub(crate) routes: Option<RawRouteLimits>,
    #[serde(default)]
    pub(crate) http: Option<RawHttpLimits>,
    #[serde(default)]
    pub(crate) kafka: Option<RawKafkaLimits>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawRouteLimits {
    #[serde(default)]
    pub(crate) max_inflight: Option<u32>,
    #[serde(default)]
    pub(crate) overflow_policy: Option<String>,
    #[serde(default)]
    pub(crate) max_queue_depth: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawHttpLimits {
    #[serde(default)]
    pub(crate) max_concurrency: Option<u32>,
    #[serde(default)]
    pub(crate) max_payload_bytes: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawKafkaLimits {
    #[serde(default)]
    pub(crate) max_inflight_per_partition: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawRetryBudget {
    #[serde(default)]
    pub(crate) max_attempts: Option<u32>,
    #[serde(default)]
    pub(crate) max_elapsed: Option<String>,
    #[serde(default)]
    pub(crate) base_backoff: Option<String>,
    #[serde(default)]
    pub(crate) max_backoff: Option<String>,
    #[serde(default)]
    pub(crate) jitter: Option<String>,
}

pub(crate) fn parse_app_config(raw: Option<RawAppSection>, errors: &mut Vec<String>) -> AppConfig {
    let raw = raw.unwrap_or_default();
    let mut config = AppConfig::default();

    if let Some(value) = raw.min_ready_routes {
        match parse_min_ready_routes(value) {
            Ok(parsed) => config.min_ready_routes = parsed,
            Err(message) => errors.push(message),
        }
    }

    if let Some(value) = raw.half_open_counts_as_ready {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            errors.push("app.half_open_counts_as_ready must be one of `never`, `endpoint`, or `route` (got ``)".to_string());
        } else if let Some(policy) = parse_half_open_policy(trimmed) {
            config.half_open_counts_as_ready = policy;
        } else {
            errors.push(format!(
                "app.half_open_counts_as_ready must be one of `never`, `endpoint`, or `route` (got `{trimmed}`)"
            ));
        }
    }

    let warmup_timeout = parse_duration_optional("app.warmup_timeout", raw.warmup_timeout, errors)
        .and_then(|dur| ensure_positive_duration(dur, "app.warmup_timeout", errors));
    if let Some(duration) = warmup_timeout {
        config.warmup_timeout = duration;
    } else {
        config.warmup_timeout = Duration::from_secs(30);
    }

    let readiness_cache =
        parse_duration_optional("app.readiness_cache", raw.readiness_cache, errors);
    config.readiness_cache = Duration::from_millis(250);
    if let Some(duration) = readiness_cache {
        if duration < Duration::from_millis(50) {
            errors.push("app.readiness_cache must be at least 50ms".to_string());
        } else {
            config.readiness_cache = duration;
        }
    }

    let drain_timeout = parse_duration_optional("app.drain_timeout", raw.drain_timeout, errors)
        .and_then(|dur| ensure_positive_duration(dur, "app.drain_timeout", errors));
    if let Some(duration) = drain_timeout {
        config.drain_timeout = duration;
    } else {
        config.drain_timeout = Duration::from_secs(30);
    }

    config.limits = parse_app_limits(raw.limits, errors);
    let parsed_budget = parse_retry_budget(raw.retry_budget, errors, "app.retry_budget");
    config.retry_budget = Some(merge_retry_budget_with_defaults(
        &default_app_retry_budget(),
        parsed_budget,
    ));
    config.feature_flags = raw
        .feature_flags
        .into_iter()
        .map(|flag| flag.trim().to_string())
        .filter(|flag| !flag.is_empty())
        .collect();
    validate_feature_flags(&config.feature_flags, errors);

    config
}

pub(crate) fn parse_route_limits(
    raw: Option<RawRouteLimits>,
    errors: &mut Vec<String>,
    context: &str,
) -> Option<RouteLimits> {
    let route_limits = raw?;

    let mut parsed = RouteLimits {
        max_inflight: route_limits.max_inflight,
        max_queue_depth: route_limits.max_queue_depth,
        ..RouteLimits::default()
    };

    if matches!(parsed.max_inflight, Some(0)) {
        errors.push(format!(
            "{context}.max_inflight must be greater than zero when provided"
        ));
    }
    if matches!(parsed.max_queue_depth, Some(0)) {
        errors.push(format!(
            "{context}.max_queue_depth must be greater than zero when provided"
        ));
    }

    if let Some(policy_raw) = route_limits.overflow_policy {
        let trimmed = policy_raw.trim();
        let field_label = format!("{context}.overflow_policy");
        if trimmed.is_empty() {
            errors.push(format!(
                "{field_label} must be one of `reject`, `queue`, or `shed` (got ``)"
            ));
        } else if let Some(policy) = parse_overflow_policy(trimmed) {
            parsed.overflow_policy = Some(policy);
        } else {
            errors.push(format!(
                "{field_label} must be one of `reject`, `queue`, or `shed` (got `{trimmed}`)"
            ));
        }
    }

    if matches!(parsed.overflow_policy, Some(OverflowPolicy::Queue))
        && parsed.max_queue_depth.is_none()
    {
        errors.push(format!(
            "{context}.max_queue_depth must be provided when overflow_policy is set to `queue`"
        ));
    }

    if parsed.max_inflight.is_some()
        || parsed.overflow_policy.is_some()
        || parsed.max_queue_depth.is_some()
    {
        Some(parsed)
    } else {
        None
    }
}

pub(crate) fn parse_retry_budget(
    raw: Option<RawRetryBudget>,
    errors: &mut Vec<String>,
    context_label: &str,
) -> Option<RetryBudget> {
    let raw_budget = raw?;

    let mut budget = RetryBudget {
        max_attempts: raw_budget.max_attempts,
        ..RetryBudget::default()
    };
    let max_elapsed_label = format!("{context_label}.max_elapsed");
    budget.max_elapsed =
        parse_duration_optional(&max_elapsed_label, raw_budget.max_elapsed, errors)
            .and_then(|duration| ensure_positive_duration(duration, &max_elapsed_label, errors));

    let base_backoff_label = format!("{context_label}.base_backoff");
    budget.base_backoff =
        parse_duration_optional(&base_backoff_label, raw_budget.base_backoff, errors)
            .and_then(|duration| ensure_positive_duration(duration, &base_backoff_label, errors));

    let max_backoff_label = format!("{context_label}.max_backoff");
    budget.max_backoff =
        parse_duration_optional(&max_backoff_label, raw_budget.max_backoff, errors)
            .and_then(|duration| ensure_positive_duration(duration, &max_backoff_label, errors));

    if let Some(jitter_raw) = raw_budget.jitter {
        let trimmed = jitter_raw.trim();
        let jitter_label = format!("{context_label}.jitter");
        if trimmed.is_empty() {
            errors.push(format!(
                "{jitter_label} must be one of `none`, `equal`, or `full` (got ``)"
            ));
        } else if let Some(mode) = parse_jitter_mode(trimmed) {
            budget.jitter = Some(mode);
        } else {
            errors.push(format!(
                "{jitter_label} must be one of `none`, `equal`, or `full` (got `{trimmed}`)"
            ));
        }
    }

    if let Some(attempts) = budget.max_attempts {
        if attempts == 0 {
            errors.push(format!(
                "{context_label}.max_attempts must be greater than zero"
            ));
        }
    }

    if let (Some(base), Some(maximum)) = (budget.base_backoff, budget.max_backoff) {
        if maximum < base {
            errors.push(format!(
                "{context_label}.max_backoff must be greater than or equal to {context_label}.base_backoff"
            ));
        }
    }

    if let (Some(max_backoff), Some(max_elapsed)) = (budget.max_backoff, budget.max_elapsed) {
        if max_backoff > max_elapsed {
            errors.push(format!(
                "{context_label}.max_backoff must be less than or equal to {context_label}.max_elapsed"
            ));
        }
    }

    if budget.max_attempts.is_none()
        && budget.max_elapsed.is_none()
        && budget.base_backoff.is_none()
        && budget.max_backoff.is_none()
        && budget.jitter.is_none()
    {
        None
    } else {
        Some(budget)
    }
}

pub(crate) fn parse_half_open_policy(value: &str) -> Option<HalfOpenPolicy> {
    match value.to_ascii_lowercase().as_str() {
        "never" => Some(HalfOpenPolicy::Never),
        "endpoint" => Some(HalfOpenPolicy::Endpoint),
        "route" => Some(HalfOpenPolicy::Route),
        _ => None,
    }
}

pub(crate) fn parse_duration_value(
    field_label: &str,
    raw: Option<String>,
    errors: &mut Vec<String>,
) -> Option<Duration> {
    let raw_value = raw?;

    let trimmed = raw_value.trim();
    if trimmed.is_empty() {
        errors.push(format!("{field_label} must be a non-empty duration string"));
        return None;
    }

    match parse_duration(trimmed) {
        Ok(duration) => Some(duration),
        Err(_) => {
            errors.push(format!(
                "{field_label} must be a valid duration (got `{trimmed}`)"
            ));
            None
        }
    }
}

pub(crate) fn ensure_positive_duration(
    duration: Duration,
    label: &str,
    errors: &mut Vec<String>,
) -> Option<Duration> {
    if duration.is_zero() {
        errors.push(format!("{label} must be greater than zero"));
        None
    } else {
        Some(duration)
    }
}

pub(crate) fn value_to_string(value: &YamlValue) -> String {
    match value {
        YamlValue::Null => "null".to_string(),
        YamlValue::Bool(inner) => inner.to_string(),
        YamlValue::Number(inner) => inner.to_string(),
        YamlValue::String(inner) => inner.clone(),
        YamlValue::Sequence(items) => format!(
            "[{}]",
            items
                .iter()
                .map(value_to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        YamlValue::Mapping(map) => format!(
            "{{{}}}",
            map.iter()
                .map(|(key, val)| {
                    let key_str = value_to_string(key);
                    let val_str = value_to_string(val);
                    format!("{key_str}: {val_str}")
                })
                .collect::<Vec<_>>()
                .join(", ")
        ),
        YamlValue::Tagged(tagged) => value_to_string(&tagged.value),
    }
}

fn parse_duration_optional(
    field_label: &str,
    raw: Option<String>,
    errors: &mut Vec<String>,
) -> Option<Duration> {
    parse_duration_value(field_label, raw, errors)
}

fn parse_min_ready_routes(value: YamlValue) -> Result<MinReadyRoutes, String> {
    match value {
        YamlValue::String(text) => {
            let trimmed = text.trim();
            if trimmed.eq_ignore_ascii_case("all") || trimmed.is_empty() {
                Ok(MinReadyRoutes::All)
            } else {
                match trimmed.parse::<u32>() {
                    Ok(count) if count > 0 => Ok(MinReadyRoutes::Count(count)),
                    _ => Err(format!(
                        "app.min_ready_routes must be `all` or a positive integer (got `{trimmed}`)"
                    )),
                }
            }
        }
        YamlValue::Number(number) => {
            let raw_display = number
                .as_u64()
                .map(|value| value.to_string())
                .or_else(|| number.as_i64().map(|value| value.to_string()))
                .or_else(|| number.as_f64().map(|value| value.to_string()))
                .unwrap_or_else(|| "NaN".to_string());

            match number.as_u64() {
                Some(0) | None => Err(format!(
                    "app.min_ready_routes must be `all` or a positive integer (got `{raw_display}`)"
                )),
                Some(value) if value <= u32::MAX as u64 => Ok(MinReadyRoutes::Count(value as u32)),
                Some(value) => Err(format!(
                    "app.min_ready_routes value `{value}` exceeds supported range"
                )),
            }
        }
        other => Err(format!(
            "app.min_ready_routes must be `all` or a positive integer (got `{}`)",
            value_to_string(&other)
        )),
    }
}

fn parse_app_limits(raw: Option<RawAppLimits>, errors: &mut Vec<String>) -> AppLimits {
    let mut limits = AppLimits::default();
    let Some(raw_limits) = raw else {
        return limits;
    };

    if let Some(route_limits) = parse_route_limits(raw_limits.routes, errors, "app.limits.routes") {
        limits.routes = Some(merge_route_defaults(limits.routes.as_ref(), &route_limits));
    }

    if let Some(http_limits) = raw_limits.http {
        let parsed = HttpLimits {
            max_concurrency: http_limits.max_concurrency,
            max_payload_bytes: http_limits.max_payload_bytes,
        };

        if matches!(parsed.max_concurrency, Some(0)) {
            errors.push(
                "app.limits.http.max_concurrency must be greater than zero when provided"
                    .to_string(),
            );
        }
        if matches!(parsed.max_payload_bytes, Some(0)) {
            errors.push(
                "app.limits.http.max_payload_bytes must be greater than zero when provided"
                    .to_string(),
            );
        }

        if parsed.max_concurrency.is_some() || parsed.max_payload_bytes.is_some() {
            limits.http = Some(merge_http_limits(limits.http.as_ref(), &parsed));
        }
    }

    if let Some(kafka_limits) = raw_limits.kafka {
        let parsed = KafkaLimits {
            max_inflight_per_partition: kafka_limits.max_inflight_per_partition,
        };
        if matches!(parsed.max_inflight_per_partition, Some(0)) {
            errors.push(
                "app.limits.kafka.max_inflight_per_partition must be greater than zero when provided"
                    .to_string(),
            );
        }
        if parsed.max_inflight_per_partition.is_some() {
            limits.kafka = Some(merge_kafka_limits(limits.kafka.as_ref(), &parsed));
        }
    }

    limits
}

fn parse_overflow_policy(value: &str) -> Option<OverflowPolicy> {
    match value.to_ascii_lowercase().as_str() {
        "reject" => Some(OverflowPolicy::Reject),
        "queue" => Some(OverflowPolicy::Queue),
        "shed" => Some(OverflowPolicy::Shed),
        _ => None,
    }
}

fn parse_jitter_mode(value: &str) -> Option<JitterMode> {
    match value.to_ascii_lowercase().as_str() {
        "none" => Some(JitterMode::None),
        "equal" => Some(JitterMode::Equal),
        "full" => Some(JitterMode::Full),
        _ => None,
    }
}

fn merge_route_defaults(base: Option<&RouteLimits>, overrides: &RouteLimits) -> RouteLimits {
    RouteLimits {
        max_inflight: overrides
            .max_inflight
            .or_else(|| base.and_then(|limits| limits.max_inflight)),
        overflow_policy: overrides
            .overflow_policy
            .or_else(|| base.and_then(|limits| limits.overflow_policy)),
        max_queue_depth: overrides
            .max_queue_depth
            .or_else(|| base.and_then(|limits| limits.max_queue_depth)),
    }
}

fn merge_http_limits(base: Option<&HttpLimits>, overrides: &HttpLimits) -> HttpLimits {
    HttpLimits {
        max_concurrency: overrides
            .max_concurrency
            .or_else(|| base.and_then(|limits| limits.max_concurrency)),
        max_payload_bytes: overrides
            .max_payload_bytes
            .or_else(|| base.and_then(|limits| limits.max_payload_bytes)),
    }
}

fn merge_kafka_limits(base: Option<&KafkaLimits>, overrides: &KafkaLimits) -> KafkaLimits {
    KafkaLimits {
        max_inflight_per_partition: overrides
            .max_inflight_per_partition
            .or_else(|| base.and_then(|limits| limits.max_inflight_per_partition)),
    }
}

fn validate_feature_flags(flags: &[String], errors: &mut Vec<String>) {
    for flag in flags {
        if !KNOWN_FEATURE_FLAGS.iter().any(|known| known == flag) {
            let supported = KNOWN_FEATURE_FLAGS.join(", ");
            errors.push(format!(
                "app.feature_flags contains unsupported gate `{flag}` (supported gates: {supported})"
            ));
        }
    }
}
fn default_route_limits() -> RouteLimits {
    RouteLimits {
        max_inflight: Some(1024),
        overflow_policy: Some(OverflowPolicy::Reject),
        max_queue_depth: Some(1024),
    }
}

fn default_http_limits() -> HttpLimits {
    HttpLimits {
        max_concurrency: Some(512),
        max_payload_bytes: Some(1_048_576),
    }
}

fn default_kafka_limits() -> KafkaLimits {
    KafkaLimits {
        max_inflight_per_partition: Some(32),
    }
}
