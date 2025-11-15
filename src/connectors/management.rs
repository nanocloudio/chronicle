use crate::app_state::AppState;
use crate::backpressure::ControllerSnapshot;
use crate::codec::http::{
    application_state_label, readiness_endpoints_payload, readiness_routes_payload,
};
use crate::config::integration::{ConnectorKind, ManagementConfig, ManagementEndpointConfig};
use crate::error::{Context, Result};
use crate::metrics::{
    metrics as metrics_collector, HttpMetricsSnapshot, RuntimeCountersSnapshot, WarmupState,
    HTTP_BACKPRESSURE_METRICS, KAFKA_BACKPRESSURE_METRICS,
};
use crate::readiness::{
    retry_after_hint_seconds, ApplicationState as ReadinessApplicationState, EndpointState,
    ReadinessSnapshot, RouteState as ReadinessRouteState,
};
use axum::body::Body;
use axum::http::{
    header::{CONTENT_TYPE, RETRY_AFTER},
    StatusCode,
};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Extension, Json, Router};
use chrono::Utc;
use serde_json::json;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use tokio_util::sync::CancellationToken;

pub struct ManagementServer {
    addr: SocketAddr,
    live: Option<ManagementEndpointConfig>,
    ready: Option<ManagementEndpointConfig>,
    status: Option<ManagementEndpointConfig>,
    metrics: Option<ManagementEndpointConfig>,
}

impl ManagementServer {
    pub fn build(config: &ManagementConfig) -> Result<Option<Self>> {
        let has_live = config.live.is_some();
        let has_ready = config.ready.is_some();
        let has_status = config.status.is_some();
        let has_metrics = config.metrics.is_some();

        if !has_live && !has_ready && !has_status && !has_metrics {
            return Ok(None);
        }

        let addr: SocketAddr = format!("{}:{}", config.host, config.port)
            .parse()
            .with_context(|| {
                format!(
                    "invalid management listen address {}:{}",
                    config.host, config.port
                )
            })?;

        Ok(Some(Self {
            addr,
            live: config.live.clone(),
            ready: config.ready.clone(),
            status: config.status.clone(),
            metrics: config.metrics.clone(),
        }))
    }

    pub async fn serve(self, state: AppState, shutdown: CancellationToken) -> Result<()> {
        let listener = tokio::net::TcpListener::bind(self.addr)
            .await
            .with_context(|| format!("failed to bind management listener on {}", self.addr))?;

        let mut router = Router::new();

        if let Some(endpoint) = &self.live {
            router = router.route(endpoint.path.as_str(), get(live));
        }

        if let Some(endpoint) = &self.ready {
            router = router.route(endpoint.path.as_str(), get(ready));
        }

        if let Some(endpoint) = &self.status {
            router = router.route(endpoint.path.as_str(), get(status_report));
        }

        if let Some(endpoint) = &self.metrics {
            router = router.route(endpoint.path.as_str(), get(metrics));
        }

        router = router.layer(Extension(state));

        tracing::info!("management server listening on {}", self.addr);

        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                shutdown.cancelled().await;
            })
            .await
            .context("management server exited abnormally")?;

        Ok(())
    }
}

async fn live(Extension(state): Extension<AppState>) -> impl IntoResponse {
    let chronicle_count = state
        .integration
        .as_ref()
        .map(|cfg| cfg.chronicles.len())
        .unwrap_or(0);

    (
        StatusCode::OK,
        Json(json!({
            "status": "ok",
            "chronicles_loaded": chronicle_count,
        })),
    )
}

async fn ready(Extension(state): Extension<AppState>) -> impl IntoResponse {
    if let Some(controller) = state.readiness.as_ref() {
        let snapshot = controller
            .cached_snapshot(state.app_policy.readiness_cache)
            .await;
        let payload = Json(json!({
            "state": application_state_label(snapshot.application_state),
            "routes": readiness_routes_payload(&snapshot),
        }));

        match snapshot.application_state {
            ReadinessApplicationState::Ready | ReadinessApplicationState::Degraded => {
                (StatusCode::OK, payload).into_response()
            }
            _ => {
                let mut response = payload.into_response();
                *response.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
                let retry_after = retry_after_hint_seconds(state.app_policy.readiness_cache);
                if let Ok(value) = axum::http::HeaderValue::from_str(&retry_after.to_string()) {
                    response.headers_mut().insert(RETRY_AFTER, value);
                }
                response
            }
        }
    } else {
        (
            StatusCode::OK,
            Json(json!({
                "state": "UNKNOWN",
                "routes": [],
            })),
        )
            .into_response()
    }
}

async fn status_report(Extension(state): Extension<AppState>) -> impl IntoResponse {
    let timestamp = Utc::now().to_rfc3339();

    if let Some(controller) = state.readiness.as_ref() {
        let snapshot = controller
            .cached_snapshot(state.app_policy.readiness_cache)
            .await;
        let payload = Json(json!({
            "app_state": application_state_label(snapshot.application_state),
            "routes": readiness_routes_payload(&snapshot),
            "endpoints": readiness_endpoints_payload(&snapshot),
            "ts": timestamp,
        }));
        (StatusCode::OK, payload).into_response()
    } else {
        (
            StatusCode::OK,
            Json(json!({
                "app_state": "UNKNOWN",
                "routes": [],
                "endpoints": [],
                "ts": timestamp,
            })),
        )
            .into_response()
    }
}

async fn metrics(Extension(state): Extension<AppState>) -> impl IntoResponse {
    let readiness_snapshot = match state.readiness.as_ref() {
        Some(controller) => Some(
            controller
                .cached_snapshot(state.app_policy.readiness_cache)
                .await,
        ),
        None => None,
    };

    let body = metrics_body(&state, readiness_snapshot.as_ref());

    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "text/plain; version=0.0.4")
        .body(Body::from(body))
        .expect("metrics response")
}

fn metrics_body(state: &AppState, readiness: Option<&ReadinessSnapshot>) -> String {
    let chronicle_count = state
        .integration
        .as_ref()
        .map(|cfg| cfg.chronicles.len())
        .unwrap_or(0);

    let connector_metrics = metrics_collector().snapshot();
    let http_metrics = metrics_collector().http_metrics_snapshot();
    let http_snapshot = state.backpressure.http.snapshot();
    let kafka_snapshot = state.backpressure.kafka.snapshot();

    let mut output = String::new();
    append_readiness_metrics(&mut output, readiness);
    append_chronicle_metrics(&mut output, state, chronicle_count);
    append_backpressure_metrics(&mut output, http_snapshot, kafka_snapshot);
    append_http_request_metrics(&mut output, &http_metrics);
    append_trigger_metrics(&mut output, &connector_metrics);
    append_warmup_metrics(&mut output, &connector_metrics);
    append_connector_outcomes(&mut output, &connector_metrics);
    append_retry_budget_exhausted(&mut output, &connector_metrics);
    append_route_queue_depth(&mut output, &connector_metrics);
    append_limit_enforcements(&mut output, &connector_metrics);
    append_route_shed(&mut output, &connector_metrics);

    output
}

fn append_readiness_metrics(output: &mut String, readiness: Option<&ReadinessSnapshot>) {
    if let Some(snapshot) = readiness {
        output.push_str("# HELP chronicle_app_state Application readiness state (0=WARMING_UP,1=NOT_READY,2=READY,3=DEGRADED,4=DRAINING)\n");
        output.push_str("# TYPE chronicle_app_state gauge\n");
        output.push_str(&format!(
            "chronicle_app_state {}\n",
            application_state_gauge(snapshot.application_state)
        ));

        output.push_str("# HELP chronicle_route_state Route readiness state (0=WARMING_UP,1=NOT_READY,2=READY,3=DEGRADED,4=DRAINING)\n");
        output.push_str("# TYPE chronicle_route_state gauge\n");
        for route in &snapshot.routes {
            output.push_str(&format!(
                "chronicle_route_state{{route=\"{}\"}} {}\n",
                route.name,
                route_state_gauge(route.state)
            ));
        }

        output.push_str("# HELP chronicle_endpoint_state Endpoint health state (0=WARMING_UP,1=UNHEALTHY,2=HEALTHY,3=CB_OPEN,4=CB_HALF_OPEN)\n");
        output.push_str("# TYPE chronicle_endpoint_state gauge\n");
        for endpoint in &snapshot.endpoints {
            output.push_str(&format!(
                "chronicle_endpoint_state{{endpoint=\"{}\"}} {}\n",
                endpoint.name,
                endpoint_state_gauge(endpoint.state)
            ));
        }
    }
}

fn append_chronicle_metrics(output: &mut String, state: &AppState, chronicle_count: usize) {
    output.push_str(
        "# HELP chronicle_chronicles_loaded Number of chronicles loaded from integration config\n",
    );
    output.push_str("# TYPE chronicle_chronicles_loaded gauge\n");
    output.push_str(&format!(
        "chronicle_chronicles_loaded {}\n",
        chronicle_count
    ));

    let mut connector_totals: BTreeMap<String, usize> = BTreeMap::new();
    if let Some(cfg) = &state.integration {
        for connector in &cfg.connectors {
            let label = connector.kind.as_str().to_string();
            *connector_totals.entry(label).or_default() += 1;
        }
    }

    output.push_str("# HELP chronicle_connectors_total Number of configured connectors by type\n");
    output.push_str("# TYPE chronicle_connectors_total gauge\n");
    if connector_totals.is_empty() {
        output.push_str("chronicle_connectors_total{type=\"none\"} 0\n");
    } else {
        for (label, count) in connector_totals {
            output.push_str(&format!(
                "chronicle_connectors_total{{type=\"{}\"}} {}\n",
                label, count
            ));
        }
    }

    let db_configured = if state.db.is_some() { 1 } else { 0 };
    output.push_str(
        "# HELP chronicle_database_configured Indicates if a database connection pool is active\n",
    );
    output.push_str("# TYPE chronicle_database_configured gauge\n");
    output.push_str(&format!(
        "chronicle_database_configured {}\n",
        db_configured
    ));

    let kafka_enabled = state
        .integration
        .as_ref()
        .map(|cfg| {
            cfg.connectors
                .iter()
                .any(|connector| matches!(connector.kind, ConnectorKind::Kafka))
        })
        .unwrap_or(false);
    output.push_str("# HELP chronicle_kafka_connectors_present Indicates if any Kafka connectors are configured\n");
    output.push_str("# TYPE chronicle_kafka_connectors_present gauge\n");
    output.push_str(&format!(
        "chronicle_kafka_connectors_present {}\n",
        if kafka_enabled { 1 } else { 0 }
    ));
}

fn append_backpressure_metrics(
    output: &mut String,
    http_snapshot: ControllerSnapshot,
    kafka_snapshot: ControllerSnapshot,
) {
    HTTP_BACKPRESSURE_METRICS.render(output, http_snapshot);
    KAFKA_BACKPRESSURE_METRICS.render(output, kafka_snapshot);

    output.push_str("# HELP chronicle_connector_paused Connectors currently paused due to backpressure saturation\n");
    output.push_str("# TYPE chronicle_connector_paused gauge\n");
    let http_paused = if http_snapshot.paused() { 1 } else { 0 };
    output.push_str(&format!(
        "chronicle_connector_paused{{connector=\"http\"}} {}\n",
        http_paused
    ));
    let kafka_paused = if kafka_snapshot.paused() { 1 } else { 0 };
    output.push_str(&format!(
        "chronicle_connector_paused{{connector=\"kafka\"}} {}\n",
        kafka_paused
    ));
}

fn append_http_request_metrics(output: &mut String, http_metrics: &HttpMetricsSnapshot) {
    output.push_str(
        "# HELP chronicle_http_requests_total HTTP request outcomes by route and status code\n",
    );
    output.push_str("# TYPE chronicle_http_requests_total counter\n");
    if http_metrics.requests.is_empty() {
        output.push_str("chronicle_http_requests_total{route=\"none\",code=\"0\"} 0\n");
    } else {
        for entry in &http_metrics.requests {
            let route = bounded_label(&entry.route);
            output.push_str(&format!(
                "chronicle_http_requests_total{{route=\"{}\",code=\"{}\"}} {}\n",
                route, entry.status_code, entry.total
            ));
        }
    }

    if !http_metrics.durations.is_empty() {
        output.push_str("# HELP chronicle_http_request_duration_seconds HTTP request latency\n");
        output.push_str("# TYPE chronicle_http_request_duration_seconds histogram\n");
        for entry in &http_metrics.durations {
            let route = bounded_label(&entry.route);
            for (boundary, cumulative) in &entry.buckets {
                output.push_str(&format!(
                    "chronicle_http_request_duration_seconds_bucket{{route=\"{}\",le=\"{}\"}} {}\n",
                    route, boundary, cumulative
                ));
            }
            output.push_str(&format!(
                "chronicle_http_request_duration_seconds_bucket{{route=\"{}\",le=\"+Inf\"}} {}\n",
                route, entry.count
            ));
            output.push_str(&format!(
                "chronicle_http_request_duration_seconds_sum{{route=\"{}\"}} {:.6}\n",
                route, entry.sum
            ));
            output.push_str(&format!(
                "chronicle_http_request_duration_seconds_count{{route=\"{}\"}} {}\n",
                route, entry.count
            ));
        }
    }
}

fn append_trigger_metrics(output: &mut String, connector_metrics: &RuntimeCountersSnapshot) {
    output.push_str("# HELP chronicle_rabbitmq_trigger_inflight RabbitMQ trigger deliveries currently in-flight\n");
    output.push_str("# TYPE chronicle_rabbitmq_trigger_inflight gauge\n");
    output.push_str(&format!(
        "chronicle_rabbitmq_trigger_inflight {}\n",
        connector_metrics.rabbitmq_trigger_inflight
    ));

    output.push_str(
        "# HELP chronicle_mqtt_trigger_inflight MQTT trigger messages currently in-flight\n",
    );
    output.push_str("# TYPE chronicle_mqtt_trigger_inflight gauge\n");
    output.push_str(&format!(
        "chronicle_mqtt_trigger_inflight {}\n",
        connector_metrics.mqtt_trigger_inflight
    ));

    output.push_str(
        "# HELP chronicle_redis_trigger_inflight Redis trigger messages currently in-flight\n",
    );
    output.push_str("# TYPE chronicle_redis_trigger_inflight gauge\n");
    output.push_str(&format!(
        "chronicle_redis_trigger_inflight {}\n",
        connector_metrics.redis_trigger_inflight
    ));

    output.push_str("# HELP chronicle_rabbitmq_publish_success_total Successful RabbitMQ publish actions emitted by phases\n");
    output.push_str("# TYPE chronicle_rabbitmq_publish_success_total counter\n");
    output.push_str(&format!(
        "chronicle_rabbitmq_publish_success_total {}\n",
        connector_metrics.rabbitmq_publish_success
    ));

    output.push_str("# HELP chronicle_rabbitmq_publish_failure_total Failed RabbitMQ publish actions emitted by phases\n");
    output.push_str("# TYPE chronicle_rabbitmq_publish_failure_total counter\n");
    output.push_str(&format!(
        "chronicle_rabbitmq_publish_failure_total {}\n",
        connector_metrics.rabbitmq_publish_failure
    ));

    output.push_str("# HELP chronicle_mqtt_publish_success_total Successful MQTT publish actions emitted by phases\n");
    output.push_str("# TYPE chronicle_mqtt_publish_success_total counter\n");
    output.push_str(&format!(
        "chronicle_mqtt_publish_success_total {}\n",
        connector_metrics.mqtt_publish_success
    ));

    output.push_str("# HELP chronicle_mqtt_publish_failure_total Failed MQTT publish actions emitted by phases\n");
    output.push_str("# TYPE chronicle_mqtt_publish_failure_total counter\n");
    output.push_str(&format!(
        "chronicle_mqtt_publish_failure_total {}\n",
        connector_metrics.mqtt_publish_failure
    ));
}

fn append_warmup_metrics(output: &mut String, connector_metrics: &RuntimeCountersSnapshot) {
    if connector_metrics.warmup.is_empty() {
        return;
    }

    let mut totals = BTreeMap::from([("pending", 0u64), ("succeeded", 0u64), ("failed", 0u64)]);

    output.push_str(
        "# HELP chronicle_warmup_state Connector warm-up state (0=PENDING,1=SUCCEEDED,2=FAILED)\n",
    );
    output.push_str("# TYPE chronicle_warmup_state gauge\n");

    output.push_str(
        "# HELP chronicle_warmup_duration_seconds Observed connector warm-up duration in seconds\n",
    );
    output.push_str("# TYPE chronicle_warmup_duration_seconds gauge\n");

    for entry in &connector_metrics.warmup {
        let connector = bounded_label(&entry.connector);
        let label = warmup_state_label(entry.state).to_ascii_lowercase();
        if let Some(counter) = totals.get_mut(label.as_str()) {
            *counter += 1;
        }
        output.push_str(&format!(
            "chronicle_warmup_state{{connector=\"{}\"}} {}\n",
            connector,
            warmup_state_gauge(entry.state)
        ));

        if let Some(duration_ms) = entry.duration_ms {
            let seconds = (duration_ms as f64) / 1000.0;
            output.push_str(&format!(
                "chronicle_warmup_duration_seconds{{connector=\"{}\"}} {:.3}\n",
                connector, seconds
            ));
        } else {
            output.push_str(&format!(
                "chronicle_warmup_duration_seconds{{connector=\"{}\"}} 0\n",
                connector
            ));
        }
    }

    output.push_str(
        "# HELP chronicle_warmup_connectors_total Connectors requiring warm-up by state\n",
    );
    output.push_str("# TYPE chronicle_warmup_connectors_total gauge\n");
    for (state, count) in totals {
        output.push_str(&format!(
            "chronicle_warmup_connectors_total{{state=\"{}\"}} {}\n",
            state, count
        ));
    }
}

fn append_connector_outcomes(output: &mut String, connector_metrics: &RuntimeCountersSnapshot) {
    if connector_metrics.connector_outcomes.is_empty() {
        return;
    }

    output
        .push_str("# HELP chronicle_connector_actions_total Connector action dispatch outcomes\n");
    output.push_str("# TYPE chronicle_connector_actions_total counter\n");
    for entry in &connector_metrics.connector_outcomes {
        let connector = bounded_label(&entry.connector);
        let kind = bounded_label(&entry.kind);
        output.push_str(&format!(
            "chronicle_connector_actions_total{{connector=\"{}\",kind=\"{}\",status=\"success\"}} {}\n",
            connector, kind, entry.success
        ));
        for (reason, total) in &entry.failures_by_reason {
            let reason = bounded_label(reason);
            output.push_str(&format!(
                "chronicle_connector_actions_total{{connector=\"{}\",kind=\"{}\",status=\"failure\",reason=\"{}\"}} {}\n",
                connector, kind, reason, total
            ));
        }
    }
}

fn append_retry_budget_exhausted(output: &mut String, connector_metrics: &RuntimeCountersSnapshot) {
    if connector_metrics.retry_budget_exhausted.is_empty() {
        return;
    }

    output.push_str("# HELP chronicle_retry_budget_exhausted_total Retry budget exhaustion events by route/endpoint\n");
    output.push_str("# TYPE chronicle_retry_budget_exhausted_total counter\n");
    for entry in &connector_metrics.retry_budget_exhausted {
        let route = bounded_label(&entry.route);
        match &entry.endpoint {
            Some(endpoint) => {
                let endpoint = bounded_label(endpoint);
                output.push_str(&format!(
                    "chronicle_retry_budget_exhausted_total{{route=\"{}\",endpoint=\"{}\"}} {}\n",
                    route, endpoint, entry.total
                ));
            }
            None => {
                output.push_str(&format!(
                    "chronicle_retry_budget_exhausted_total{{route=\"{}\"}} {}\n",
                    route, entry.total
                ));
            }
        }
    }
}

fn append_route_queue_depth(output: &mut String, connector_metrics: &RuntimeCountersSnapshot) {
    if connector_metrics.route_queue_depth.is_empty() {
        return;
    }

    output.push_str(
        "# HELP chronicle_route_queue_depth Pending work waiting in route-level queues\n",
    );
    output.push_str("# TYPE chronicle_route_queue_depth gauge\n");
    for entry in &connector_metrics.route_queue_depth {
        let route = bounded_label(&entry.route);
        output.push_str(&format!(
            "chronicle_route_queue_depth{{route=\"{}\"}} {}\n",
            route, entry.depth
        ));
    }
}

fn append_limit_enforcements(output: &mut String, connector_metrics: &RuntimeCountersSnapshot) {
    if connector_metrics.limit_enforcements.is_empty() {
        return;
    }

    output.push_str(
        "# HELP chronicle_limits_enforced_total Route limit enforcement events grouped by policy\n",
    );
    output.push_str("# TYPE chronicle_limits_enforced_total counter\n");
    for entry in &connector_metrics.limit_enforcements {
        let route = bounded_label(&entry.route);
        let policy = bounded_label(&entry.policy);
        output.push_str(&format!(
            "chronicle_limits_enforced_total{{route=\"{}\",policy=\"{}\"}} {}\n",
            route, policy, entry.total
        ));
    }
}

fn append_route_shed(output: &mut String, connector_metrics: &RuntimeCountersSnapshot) {
    if connector_metrics.route_shed.is_empty() {
        return;
    }

    output.push_str("# HELP chronicle_shed_total Shed events per route\n");
    output.push_str("# TYPE chronicle_shed_total counter\n");
    for entry in &connector_metrics.route_shed {
        let route = bounded_label(&entry.route);
        output.push_str(&format!(
            "chronicle_shed_total{{route=\"{}\"}} {}\n",
            route, entry.total
        ));
    }
}

fn application_state_gauge(state: ReadinessApplicationState) -> u8 {
    match state {
        ReadinessApplicationState::WarmingUp => 0,
        ReadinessApplicationState::NotReady => 1,
        ReadinessApplicationState::Ready => 2,
        ReadinessApplicationState::Degraded => 3,
        ReadinessApplicationState::Draining => 4,
    }
}

fn route_state_gauge(state: ReadinessRouteState) -> u8 {
    match state {
        ReadinessRouteState::WarmingUp => 0,
        ReadinessRouteState::NotReady => 1,
        ReadinessRouteState::Ready => 2,
        ReadinessRouteState::Degraded => 3,
        ReadinessRouteState::Draining => 4,
    }
}

fn endpoint_state_gauge(state: EndpointState) -> u8 {
    match state {
        EndpointState::WarmingUp => 0,
        EndpointState::Unhealthy => 1,
        EndpointState::Healthy => 2,
        EndpointState::CircuitOpen => 3,
        EndpointState::CircuitHalfOpen => 4,
    }
}

fn warmup_state_label(state: WarmupState) -> &'static str {
    match state {
        WarmupState::Pending => "PENDING",
        WarmupState::Succeeded => "SUCCEEDED",
        WarmupState::Failed => "FAILED",
    }
}

fn warmup_state_gauge(state: WarmupState) -> u8 {
    match state {
        WarmupState::Pending => 0,
        WarmupState::Succeeded => 1,
        WarmupState::Failed => 2,
    }
}

fn bounded_label(value: &str) -> String {
    const MAX_LEN: usize = 40;
    if value.len() <= MAX_LEN {
        value.to_string()
    } else {
        value.chars().take(MAX_LEN).collect()
    }
}
