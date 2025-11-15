use crate::backpressure::ControllerSnapshot;
use crate::telemetry::{runtime_counters, RuntimeCounters};
use std::sync::OnceLock;
use std::time::Duration;

pub use crate::telemetry::{
    HttpDurationSnapshot, HttpMetricsSnapshot, RuntimeCountersSnapshot, WarmupState,
};

/// Collector that wraps the runtime counter APIs with a single entrypoint.
pub struct MetricsCollector {
    counters: &'static RuntimeCounters,
}

impl MetricsCollector {
    fn new() -> Self {
        Self {
            counters: runtime_counters(),
        }
    }

    pub fn global() -> &'static Self {
        static INSTANCE: OnceLock<MetricsCollector> = OnceLock::new();
        INSTANCE.get_or_init(Self::new)
    }

    pub fn snapshot(&self) -> crate::telemetry::RuntimeCountersSnapshot {
        self.counters.snapshot()
    }

    pub fn http_metrics_snapshot(&self) -> crate::telemetry::HttpMetricsSnapshot {
        self.counters.http_metrics_snapshot()
    }

    pub fn inc_rabbitmq_publish_success(&self) {
        self.counters.inc_rabbitmq_publish_success();
    }

    pub fn inc_rabbitmq_publish_failure(&self) {
        self.counters.inc_rabbitmq_publish_failure();
    }

    pub fn inc_mqtt_publish_success(&self) {
        self.counters.inc_mqtt_publish_success();
    }

    pub fn inc_mqtt_publish_failure(&self) {
        self.counters.inc_mqtt_publish_failure();
    }

    pub fn inc_rabbitmq_trigger_inflight(&self) {
        self.counters.inc_rabbitmq_trigger_inflight();
    }

    pub fn dec_rabbitmq_trigger_inflight(&self) {
        self.counters.dec_rabbitmq_trigger_inflight();
    }

    pub fn inc_mqtt_trigger_inflight(&self) {
        self.counters.inc_mqtt_trigger_inflight();
    }

    pub fn dec_mqtt_trigger_inflight(&self) {
        self.counters.dec_mqtt_trigger_inflight();
    }

    pub fn inc_redis_trigger_inflight(&self) {
        self.counters.inc_redis_trigger_inflight();
    }

    pub fn dec_redis_trigger_inflight(&self) {
        self.counters.dec_redis_trigger_inflight();
    }

    pub fn register_warmup_target(&self, connector: &str) {
        self.counters.register_warmup_target(connector);
    }

    pub fn record_warmup_success(&self, connector: &str, attempts: u32, elapsed: Duration) {
        self.counters
            .record_warmup_success(connector, attempts, elapsed);
    }

    pub fn record_warmup_failure(&self, connector: &str, attempts: u32, elapsed: Duration) {
        self.counters
            .record_warmup_failure(connector, attempts, elapsed);
    }

    pub fn record_connector_success(&self, kind: &str, connector: &str) {
        self.counters.record_connector_success(kind, connector);
    }

    pub fn record_connector_failure(&self, kind: &str, connector: &str, reason: Option<&str>) {
        self.counters
            .record_connector_failure(kind, connector, reason);
    }

    pub fn record_http_request(&self, route: &str, status: u16, duration: Duration) {
        self.counters.record_http_request(route, status, duration);
    }

    pub fn record_retry_budget_exhausted(&self, route: &str, endpoint: Option<&str>) {
        self.counters.record_retry_budget_exhausted(route, endpoint);
    }

    pub fn set_route_queue_depth(&self, route: &str, depth: u32) {
        self.counters.set_route_queue_depth(route, depth);
    }

    pub fn record_limit_enforcement(&self, route: &str, policy: &str) {
        self.counters.record_limit_enforcement(route, policy);
    }

    pub fn record_route_shed(&self, route: &str) {
        self.counters.record_route_shed(route);
    }
}

/// Returns the shared `MetricsCollector` instance.
pub fn metrics() -> &'static MetricsCollector {
    MetricsCollector::global()
}

#[derive(Clone, Copy)]
pub struct BackpressureMetricSet {
    pub limit_metric: &'static str,
    pub limit_help: &'static str,
    pub limit_type: &'static str,
    pub inflight_metric: &'static str,
    pub inflight_help: &'static str,
    pub inflight_type: &'static str,
    pub throttled_metric: &'static str,
    pub throttled_help: &'static str,
    pub throttled_type: &'static str,
}

impl BackpressureMetricSet {
    pub fn render(&self, output: &mut String, snapshot: ControllerSnapshot) {
        output.push_str(&format!(
            "# HELP {} {}\n",
            self.limit_metric, self.limit_help
        ));
        output.push_str(&format!(
            "# TYPE {} {}\n",
            self.limit_metric, self.limit_type
        ));
        output.push_str(&format!(
            "{} {}\n",
            self.limit_metric,
            snapshot.limit.unwrap_or(0)
        ));

        output.push_str(&format!(
            "# HELP {} {}\n",
            self.inflight_metric, self.inflight_help
        ));
        output.push_str(&format!(
            "# TYPE {} {}\n",
            self.inflight_metric, self.inflight_type
        ));
        output.push_str(&format!("{} {}\n", self.inflight_metric, snapshot.inflight));

        output.push_str(&format!(
            "# HELP {} {}\n",
            self.throttled_metric, self.throttled_help
        ));
        output.push_str(&format!(
            "# TYPE {} {}\n",
            self.throttled_metric, self.throttled_type
        ));
        output.push_str(&format!(
            "{} {}\n",
            self.throttled_metric, snapshot.throttled
        ));
    }
}

pub const HTTP_BACKPRESSURE_METRICS: BackpressureMetricSet = BackpressureMetricSet {
    limit_metric: "chronicle_backpressure_http_max_concurrency",
    limit_help: "Configured HTTP max concurrency permits (0 means unlimited)",
    limit_type: "gauge",
    inflight_metric: "chronicle_backpressure_http_inflight",
    inflight_help: "Current in-flight HTTP requests gated by backpressure",
    inflight_type: "gauge",
    throttled_metric: "chronicle_backpressure_http_throttled_total",
    throttled_help: "Number of HTTP requests that waited for a permit",
    throttled_type: "counter",
};

pub const KAFKA_BACKPRESSURE_METRICS: BackpressureMetricSet = BackpressureMetricSet {
    limit_metric: "chronicle_backpressure_kafka_max_inflight",
    limit_help: "Configured Kafka in-flight message permits (0 means unlimited)",
    limit_type: "gauge",
    inflight_metric: "chronicle_backpressure_kafka_inflight",
    inflight_help: "Current in-flight Kafka messages gated by backpressure",
    inflight_type: "gauge",
    throttled_metric: "chronicle_backpressure_kafka_throttled_total",
    throttled_help: "Number of Kafka messages that waited for a permit",
    throttled_type: "counter",
};
