use crate::error::Result;
use chrono::{SecondsFormat, Utc};
use std::collections::{BTreeMap, HashMap};
use std::fmt::{self as stdfmt};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Duration;
use tracing::field::{Field, Visit};
use tracing::Event;
use tracing::Subscriber;
use tracing_subscriber::fmt::writer::MakeWriterExt;
use tracing_subscriber::fmt::{
    self as fmt_subscriber, format::Writer, FmtContext, FormatEvent, FormatFields,
};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::EnvFilter;

const SERVICE_NAME: &str = "chronicle";
const HTTP_DURATION_BUCKETS: [f64; 10] = [0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];

pub fn init_tracing() -> Result<()> {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("chronicle=info,info"));

    let stdout = std::io::stdout;
    let stderr = std::io::stderr;

    let writer = stdout
        .with_max_level(tracing::Level::INFO)
        .or_else(stderr.with_min_level(tracing::Level::WARN));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .with_level(false)
        .with_ansi(false)
        .event_format(KeyValueFormatter::new())
        .fmt_fields(fmt_subscriber::format::DefaultFields::new())
        .with_writer(writer)
        .try_init()
        .map_err(|err| crate::err!("failed to initialise tracing subscriber: {err}"))
}

struct KeyValueFormatter {
    service_name: &'static str,
}

impl KeyValueFormatter {
    const fn new() -> Self {
        Self {
            service_name: SERVICE_NAME,
        }
    }
}

impl<S, N> FormatEvent<S, N> for KeyValueFormatter
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    N: for<'writer> FormatFields<'writer> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> stdfmt::Result {
        let timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
        let pid = std::process::id().to_string();
        let metadata = event.metadata();
        let component = metadata.target();

        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);

        let message = visitor
            .message
            .take()
            .unwrap_or_else(|| metadata.name().to_string());

        let mut fields = visitor.fields;
        fields.sort_by(|(lhs, _), (rhs, _)| lhs.cmp(rhs));

        let span_path = current_span_path(ctx);

        let mut line = String::new();
        push_field(&mut line, "ts", &timestamp);
        push_field(&mut line, "level", metadata.level().as_str());
        push_field(&mut line, "service", self.service_name);
        push_field(&mut line, "component", component);
        push_field(&mut line, "pid", &pid);

        if let Some(span_path) = span_path {
            push_field(&mut line, "span", &span_path);
        }

        push_field(&mut line, "msg", &message);

        for (key, value) in fields {
            push_field(&mut line, &key, &value);
        }

        if let Some(file) = metadata.file() {
            push_field(&mut line, "file", file);
        }
        if let Some(line_no) = metadata.line() {
            push_field(&mut line, "line", &line_no.to_string());
        }

        writer.write_str(&line)?;
        writer.write_char('\n')
    }
}

fn current_span_path<S, N>(ctx: &FmtContext<'_, S, N>) -> Option<String>
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    N: for<'writer> FormatFields<'writer> + 'static,
{
    let span = ctx.lookup_current()?;
    let names: Vec<&str> = span.scope().from_root().map(|s| s.name()).collect();
    if names.is_empty() {
        None
    } else {
        Some(names.join("."))
    }
}

#[derive(Default)]
struct FieldVisitor {
    message: Option<String>,
    fields: Vec<(String, String)>,
}

impl FieldVisitor {
    fn record_field(&mut self, field: &Field, value: String) {
        if field.name().is_empty() {
            return;
        }
        if field.name() == "message" {
            self.message = Some(value);
        } else {
            self.fields.push((field.name().to_string(), value));
        }
    }
}

impl Visit for FieldVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.record_field(field, value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn stdfmt::Debug) {
        self.record_field(field, format!("{value:?}"));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.record_field(field, value.to_string());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.record_field(field, value.to_string());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.record_field(field, value.to_string());
    }
}

#[derive(Default)]
pub struct RuntimeCounters {
    rabbitmq_publish_success: AtomicU64,
    rabbitmq_publish_failure: AtomicU64,
    mqtt_publish_success: AtomicU64,
    mqtt_publish_failure: AtomicU64,
    rabbitmq_trigger_inflight: AtomicU64,
    mqtt_trigger_inflight: AtomicU64,
    redis_trigger_inflight: AtomicU64,
    warmup: WarmupRegistry,
    connector_outcomes: ConnectorOutcomeRegistry,
    retry_budget: RetryBudgetRegistry,
    route_queue_depth: RouteQueueDepthRegistry,
    limit_enforcements: LimitEnforcementRegistry,
    route_shed: RouteShedRegistry,
    http_requests: HttpRequestMetrics,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeCountersSnapshot {
    pub rabbitmq_publish_success: u64,
    pub rabbitmq_publish_failure: u64,
    pub mqtt_publish_success: u64,
    pub mqtt_publish_failure: u64,
    pub rabbitmq_trigger_inflight: u64,
    pub mqtt_trigger_inflight: u64,
    pub redis_trigger_inflight: u64,
    pub warmup: Vec<WarmupStatusSnapshot>,
    pub connector_outcomes: Vec<ConnectorOutcomeSnapshot>,
    pub retry_budget_exhausted: Vec<RetryBudgetExhaustionSnapshot>,
    pub route_queue_depth: Vec<RouteQueueDepthSnapshot>,
    pub limit_enforcements: Vec<LimitEnforcementSnapshot>,
    pub route_shed: Vec<RouteShedSnapshot>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum WarmupState {
    #[default]
    Pending,
    Succeeded,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WarmupStatusSnapshot {
    pub connector: String,
    pub state: WarmupState,
    pub attempts: u32,
    pub duration_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpRequestCountSnapshot {
    pub route: String,
    pub status_code: u16,
    pub total: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HttpDurationSnapshot {
    pub route: String,
    pub buckets: Vec<(f64, u64)>,
    pub sum: f64,
    pub count: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HttpMetricsSnapshot {
    pub requests: Vec<HttpRequestCountSnapshot>,
    pub durations: Vec<HttpDurationSnapshot>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectorOutcomeSnapshot {
    pub connector: String,
    pub kind: String,
    pub success: u64,
    pub failure: u64,
    pub failures_by_reason: Vec<(String, u64)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteQueueDepthSnapshot {
    pub route: String,
    pub depth: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LimitEnforcementSnapshot {
    pub route: String,
    pub policy: String,
    pub total: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteShedSnapshot {
    pub route: String,
    pub total: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetryBudgetExhaustionSnapshot {
    pub route: String,
    pub endpoint: Option<String>,
    pub total: u64,
}

#[derive(Default)]
struct HttpRequestMetrics {
    counts: Mutex<HashMap<(String, u16), u64>>,
    durations: Mutex<HashMap<String, HttpDurationBuckets>>,
}

impl HttpRequestMetrics {
    fn record(&self, route: &str, status: u16, duration: Duration) {
        let mut counts = self
            .counts
            .lock()
            .expect("http request counts lock poisoned");
        *counts.entry((route.to_string(), status)).or_insert(0) += 1;
        drop(counts);

        let mut durations = self
            .durations
            .lock()
            .expect("http request durations lock poisoned");
        let entry = durations.entry(route.to_string()).or_default();
        entry.observe(duration.as_secs_f64());
    }

    fn snapshot(&self) -> HttpMetricsSnapshot {
        let counts_guard = self
            .counts
            .lock()
            .expect("http request counts lock poisoned");
        let durations_guard = self
            .durations
            .lock()
            .expect("http request durations lock poisoned");

        let requests = counts_guard
            .iter()
            .map(|((route, status), total)| HttpRequestCountSnapshot {
                route: route.clone(),
                status_code: *status,
                total: *total,
            })
            .collect();

        let durations = durations_guard
            .iter()
            .map(|(route, buckets)| HttpDurationSnapshot {
                route: route.clone(),
                buckets: buckets.histogram(),
                sum: buckets.sum,
                count: buckets.total,
            })
            .collect();

        HttpMetricsSnapshot {
            requests,
            durations,
        }
    }
}

#[derive(Default)]
struct HttpDurationBuckets {
    counts: [u64; HTTP_DURATION_BUCKETS.len()],
    sum: f64,
    total: u64,
}

impl HttpDurationBuckets {
    fn observe(&mut self, duration_secs: f64) {
        for (idx, boundary) in HTTP_DURATION_BUCKETS.iter().enumerate() {
            if duration_secs <= *boundary {
                self.counts[idx] += 1;
            }
        }
        self.sum += duration_secs;
        self.total += 1;
    }

    fn histogram(&self) -> Vec<(f64, u64)> {
        let mut cumulative = 0;
        HTTP_DURATION_BUCKETS
            .iter()
            .enumerate()
            .map(|(idx, boundary)| {
                cumulative += self.counts[idx];
                (*boundary, cumulative)
            })
            .collect()
    }
}

static RUNTIME_COUNTERS: OnceLock<RuntimeCounters> = OnceLock::new();

pub fn runtime_counters() -> &'static RuntimeCounters {
    RUNTIME_COUNTERS.get_or_init(RuntimeCounters::default)
}

impl RuntimeCounters {
    pub fn inc_rabbitmq_publish_success(&self) {
        self.rabbitmq_publish_success
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_rabbitmq_publish_failure(&self) {
        self.rabbitmq_publish_failure
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_mqtt_publish_success(&self) {
        self.mqtt_publish_success.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_mqtt_publish_failure(&self) {
        self.mqtt_publish_failure.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_rabbitmq_trigger_inflight(&self) {
        self.rabbitmq_trigger_inflight
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_rabbitmq_trigger_inflight(&self) {
        let _ = self.rabbitmq_trigger_inflight.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| (current > 0).then_some(current - 1),
        );
    }

    pub fn inc_mqtt_trigger_inflight(&self) {
        self.mqtt_trigger_inflight.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_mqtt_trigger_inflight(&self) {
        let _ = self.mqtt_trigger_inflight.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| (current > 0).then_some(current - 1),
        );
    }

    pub fn inc_redis_trigger_inflight(&self) {
        self.redis_trigger_inflight.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_redis_trigger_inflight(&self) {
        let _ = self.redis_trigger_inflight.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| (current > 0).then_some(current - 1),
        );
    }

    pub fn snapshot(&self) -> RuntimeCountersSnapshot {
        RuntimeCountersSnapshot {
            rabbitmq_publish_success: self.rabbitmq_publish_success.load(Ordering::Relaxed),
            rabbitmq_publish_failure: self.rabbitmq_publish_failure.load(Ordering::Relaxed),
            mqtt_publish_success: self.mqtt_publish_success.load(Ordering::Relaxed),
            mqtt_publish_failure: self.mqtt_publish_failure.load(Ordering::Relaxed),
            rabbitmq_trigger_inflight: self.rabbitmq_trigger_inflight.load(Ordering::Relaxed),
            mqtt_trigger_inflight: self.mqtt_trigger_inflight.load(Ordering::Relaxed),
            redis_trigger_inflight: self.redis_trigger_inflight.load(Ordering::Relaxed),
            warmup: self.warmup.snapshot(),
            connector_outcomes: self.connector_outcomes.snapshot(),
            retry_budget_exhausted: self.retry_budget.snapshot(),
            route_queue_depth: self.route_queue_depth.snapshot(),
            limit_enforcements: self.limit_enforcements.snapshot(),
            route_shed: self.route_shed.snapshot(),
        }
    }

    pub fn register_warmup_target(&self, connector: &str) {
        self.warmup.register(connector);
    }

    pub fn record_warmup_success(&self, connector: &str, attempts: u32, elapsed: Duration) {
        self.warmup.record_success(connector, attempts, elapsed);
    }

    pub fn record_warmup_failure(&self, connector: &str, attempts: u32, elapsed: Duration) {
        self.warmup.record_failure(connector, attempts, elapsed);
    }

    pub fn record_connector_success(&self, kind: &str, connector: &str) {
        self.connector_outcomes.record_success(kind, connector);
    }

    pub fn record_connector_failure(&self, kind: &str, connector: &str, reason: Option<&str>) {
        self.connector_outcomes
            .record_failure(kind, connector, reason);
    }

    pub fn record_http_request(&self, route: &str, status: u16, duration: Duration) {
        self.http_requests.record(route, status, duration);
    }

    pub fn http_metrics_snapshot(&self) -> HttpMetricsSnapshot {
        self.http_requests.snapshot()
    }

    pub fn record_retry_budget_exhausted(&self, route: &str, endpoint: Option<&str>) {
        self.retry_budget.record(route, endpoint);
    }

    pub fn set_route_queue_depth(&self, route: &str, depth: u32) {
        self.route_queue_depth.set(route, depth);
    }

    pub fn record_limit_enforcement(&self, route: &str, policy: &str) {
        self.limit_enforcements.record(route, policy);
    }

    pub fn record_route_shed(&self, route: &str) {
        self.route_shed.record(route);
    }
}

#[derive(Clone, Debug, Default)]
struct WarmupEntry {
    state: WarmupState,
    attempts: u32,
    duration_ms: Option<u64>,
}

#[derive(Default)]
struct WarmupRegistry {
    inner: Mutex<BTreeMap<String, WarmupEntry>>,
}

impl WarmupRegistry {
    fn register(&self, connector: &str) {
        let mut guard = self.inner.lock().expect("warmup registry poisoned");
        guard.entry(connector.to_string()).or_default();
    }

    fn record_success(&self, connector: &str, attempts: u32, elapsed: Duration) {
        let mut guard = self.inner.lock().expect("warmup registry poisoned");
        let entry = guard.entry(connector.to_string()).or_default();
        let millis = elapsed.as_millis();
        let clamped = std::cmp::min(millis, u128::from(u64::MAX)) as u64;
        entry.state = WarmupState::Succeeded;
        entry.attempts = attempts;
        entry.duration_ms = Some(clamped);
    }

    fn record_failure(&self, connector: &str, attempts: u32, elapsed: Duration) {
        let mut guard = self.inner.lock().expect("warmup registry poisoned");
        let entry = guard.entry(connector.to_string()).or_default();
        let millis = elapsed.as_millis();
        let clamped = std::cmp::min(millis, u128::from(u64::MAX)) as u64;
        entry.state = WarmupState::Failed;
        entry.attempts = attempts;
        entry.duration_ms = Some(clamped);
    }

    fn snapshot(&self) -> Vec<WarmupStatusSnapshot> {
        let guard = self.inner.lock().expect("warmup registry poisoned");
        guard
            .iter()
            .map(|(connector, entry)| WarmupStatusSnapshot {
                connector: connector.clone(),
                state: entry.state,
                attempts: entry.attempts,
                duration_ms: entry.duration_ms,
            })
            .collect()
    }
}

#[derive(Clone, Debug, Default)]
struct ConnectorOutcomeEntry {
    success: u64,
    failure: u64,
    failure_reasons: BTreeMap<String, u64>,
}

#[derive(Default)]
struct ConnectorOutcomeRegistry {
    inner: Mutex<BTreeMap<(String, String), ConnectorOutcomeEntry>>,
}

impl ConnectorOutcomeRegistry {
    fn record_success(&self, kind: &str, connector: &str) {
        let mut guard = self
            .inner
            .lock()
            .expect("connector outcome registry poisoned");
        let entry = guard
            .entry((connector.to_string(), kind.to_string()))
            .or_default();
        entry.success = entry.success.saturating_add(1);
    }

    fn record_failure(&self, kind: &str, connector: &str, reason: Option<&str>) {
        let mut guard = self
            .inner
            .lock()
            .expect("connector outcome registry poisoned");
        let entry = guard
            .entry((connector.to_string(), kind.to_string()))
            .or_default();
        entry.failure = entry.failure.saturating_add(1);
        let label = reason.unwrap_or("unknown").to_string();
        *entry.failure_reasons.entry(label).or_insert(0) += 1;
    }

    fn snapshot(&self) -> Vec<ConnectorOutcomeSnapshot> {
        let guard = self
            .inner
            .lock()
            .expect("connector outcome registry poisoned");
        guard
            .iter()
            .map(|((connector, kind), entry)| ConnectorOutcomeSnapshot {
                connector: connector.clone(),
                kind: kind.clone(),
                success: entry.success,
                failure: entry.failure,
                failures_by_reason: entry
                    .failure_reasons
                    .iter()
                    .map(|(reason, count)| (reason.clone(), *count))
                    .collect(),
            })
            .collect()
    }
}

#[derive(Default)]
struct RetryBudgetRegistry {
    inner: Mutex<BTreeMap<(String, Option<String>), u64>>,
}

impl RetryBudgetRegistry {
    fn record(&self, route: &str, endpoint: Option<&str>) {
        let mut guard = self.inner.lock().expect("retry budget registry poisoned");
        let key = (route.to_string(), endpoint.map(|value| value.to_string()));
        let counter = guard.entry(key).or_insert(0);
        *counter = counter.saturating_add(1);
    }

    fn snapshot(&self) -> Vec<RetryBudgetExhaustionSnapshot> {
        let guard = self.inner.lock().expect("retry budget registry poisoned");
        guard
            .iter()
            .map(|((route, endpoint), total)| RetryBudgetExhaustionSnapshot {
                route: route.clone(),
                endpoint: endpoint.clone(),
                total: *total,
            })
            .collect()
    }
}

#[derive(Default)]
struct RouteQueueDepthRegistry {
    inner: Mutex<BTreeMap<String, u32>>,
}

impl RouteQueueDepthRegistry {
    fn set(&self, route: &str, depth: u32) {
        let mut guard = self.inner.lock().expect("queue depth registry poisoned");
        guard.insert(route.to_string(), depth);
    }

    fn snapshot(&self) -> Vec<RouteQueueDepthSnapshot> {
        let guard = self.inner.lock().expect("queue depth registry poisoned");
        guard
            .iter()
            .map(|(route, depth)| RouteQueueDepthSnapshot {
                route: route.clone(),
                depth: *depth,
            })
            .collect()
    }
}

#[derive(Default)]
struct LimitEnforcementRegistry {
    inner: Mutex<BTreeMap<(String, String), u64>>,
}

impl LimitEnforcementRegistry {
    fn record(&self, route: &str, policy: &str) {
        let mut guard = self
            .inner
            .lock()
            .expect("limit enforcement registry poisoned");
        let key = (route.to_string(), policy.to_string());
        *guard.entry(key).or_insert(0) += 1;
    }

    fn snapshot(&self) -> Vec<LimitEnforcementSnapshot> {
        let guard = self
            .inner
            .lock()
            .expect("limit enforcement registry poisoned");
        guard
            .iter()
            .map(|((route, policy), total)| LimitEnforcementSnapshot {
                route: route.clone(),
                policy: policy.clone(),
                total: *total,
            })
            .collect()
    }
}

#[derive(Default)]
struct RouteShedRegistry {
    inner: Mutex<BTreeMap<String, u64>>,
}

impl RouteShedRegistry {
    fn record(&self, route: &str) {
        let mut guard = self.inner.lock().expect("shed registry poisoned");
        *guard.entry(route.to_string()).or_insert(0) += 1;
    }

    fn snapshot(&self) -> Vec<RouteShedSnapshot> {
        let guard = self.inner.lock().expect("shed registry poisoned");
        guard
            .iter()
            .map(|(route, total)| RouteShedSnapshot {
                route: route.clone(),
                total: *total,
            })
            .collect()
    }
}
fn encode_field_value(value: &str) -> String {
    let needs_quotes = value.chars().any(|c| {
        c.is_whitespace()
            || matches!(
                c,
                '"' | '\\' | '=' | '[' | ']' | '{' | '}' | ',' | '\n' | '\r' | '\t'
            )
    });

    if !needs_quotes {
        return value.to_string();
    }

    let mut encoded = String::with_capacity(value.len() + 2);
    encoded.push('"');
    for ch in value.chars() {
        match ch {
            '"' => encoded.push_str("\\\""),
            '\\' => encoded.push_str("\\\\"),
            '\n' => encoded.push_str("\\n"),
            '\r' => encoded.push_str("\\r"),
            '\t' => encoded.push_str("\\t"),
            _ => encoded.push(ch),
        }
    }
    encoded.push('"');
    encoded
}

fn push_field(buffer: &mut String, key: &str, value: &str) {
    if !buffer.is_empty() {
        buffer.push(' ');
    }
    buffer.push_str(key);
    buffer.push('=');
    buffer.push_str(&encode_field_value(value));
}
