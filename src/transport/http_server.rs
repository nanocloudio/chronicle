#![forbid(unsafe_code)]
use crate::backpressure::BackpressureManager;
use crate::codec::http::{
    binary_body_to_json, encode_http_response, headers_to_json, normalise_headers, normalise_path,
    path_to_json, query_to_json,
};
use crate::error::Result;
use crate::readiness::{
    retry_after_hint_seconds, DependencyHealth, ReadinessController, RouteState,
};
use crate::transport::{TaskTransportRuntime, TransportKind, TransportRuntime};
use async_trait::async_trait;
use axum::http::{
    header::{CACHE_CONTROL, CONTENT_TYPE, RETRY_AFTER},
    Method, StatusCode,
};
use axum::routing::any;
use axum::{body::Bytes, Router};
use chronicle_core::chronicle::dispatcher::{
    ActionDispatchError, ActionDispatcher, DeliveryContext,
};
use chronicle_core::chronicle::engine::{
    ChronicleAction, ChronicleEngineError, ChronicleExecution,
};
use chronicle_core::config::integration::{AppConfig, RetryBudget};
use chronicle_core::integration::{
    factory::ConnectorFactoryRegistry, registry::HttpServerHealthConfig,
};
use chronicle_core::retry::retry_after_seconds_from_budget;
use chrono::Utc;
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tower::make::Shared;
use tracing::{error, info, warn};

const DEFAULT_MAX_BODY_BYTES: usize = 1_048_576;

pub struct HttpTriggerRuntime {
    inner: TaskTransportRuntime,
    server_count: usize,
}

struct HttpServerInstance {
    addr: SocketAddr,
    state: Arc<HttpServerState>,
}

struct ServerBindings {
    routes: HashMap<String, Vec<HttpRoute>>,
    health_routes: Vec<HealthRoute>,
}

impl ServerBindings {
    fn new() -> Self {
        Self {
            routes: HashMap::new(),
            health_routes: Vec::new(),
        }
    }
}

struct HttpServerState {
    engine: Arc<chronicle_core::chronicle::engine::ChronicleEngine>,
    dispatcher: Arc<ActionDispatcher>,
    routes: HashMap<String, Vec<HttpRoute>>, // path -> routes per method
    health_routes: Vec<HealthRoute>,
    readiness: Option<ReadinessController>,
    backpressure: BackpressureManager,
    readiness_retry_after: u64,
    max_payload_bytes: usize,
    limit_cache: RwLock<HashMap<String, CachedLimitResponse>>,
    limit_cache_ttl: Duration,
}

struct CachedLimitResponse {
    observed_at: Instant,
    body: Bytes,
    retry_after: u64,
}

#[derive(Clone)]
struct HttpRoute {
    chronicle: String,
    method: Method,
    expects_json: bool,
    retry_budget: Option<RetryBudget>,
}

#[derive(Clone)]
struct HealthRoute {
    path: String,
    method: Method,
}

impl HttpServerState {
    async fn limit_response(
        &self,
        route: &str,
        retry_after: u64,
        detail: Option<&str>,
    ) -> axum::response::Response {
        if self.limit_cache_ttl.is_zero() {
            let body = build_limit_response_body(route, retry_after, detail);
            return build_limit_response_from_body(body, retry_after);
        }

        let now = Instant::now();
        {
            let cache = self.limit_cache.read().await;
            if let Some(entry) = cache.get(route) {
                if now.duration_since(entry.observed_at) <= self.limit_cache_ttl
                    && entry.retry_after == retry_after
                {
                    return build_limit_response_from_body(entry.body.clone(), entry.retry_after);
                }
            }
        }

        let body = build_limit_response_body(route, retry_after, detail);
        {
            let mut cache = self.limit_cache.write().await;
            cache.insert(
                route.to_string(),
                CachedLimitResponse {
                    observed_at: now,
                    body: body.clone(),
                    retry_after,
                },
            );
        }

        build_limit_response_from_body(body, retry_after)
    }
}

impl HttpTriggerRuntime {
    pub fn build(
        config: Arc<chronicle_core::config::IntegrationConfig>,
        registry: Arc<chronicle_core::integration::registry::ConnectorRegistry>,
        engine: Arc<chronicle_core::chronicle::engine::ChronicleEngine>,
        readiness: Option<ReadinessController>,
        dependency_health: Option<DependencyHealth>,
        backpressure: BackpressureManager,
        app_policy: AppConfig,
    ) -> Result<Self> {
        let mut servers: HashMap<SocketAddr, ServerBindings> = HashMap::new();
        let mut processed_connectors: HashSet<String> = HashSet::new();

        for chronicle in &config.chronicles {
            if chronicle.trigger.kafka.is_some() {
                continue;
            }

            let connector_name = &chronicle.trigger.connector;
            let server = match registry.http_server(connector_name) {
                Some(server) => server,
                None => {
                    warn!(%connector_name, "http trigger references missing http server connector");
                    continue;
                }
            };

            let socket_addr = SocketAddr::from_str(&format!("{}:{}", server.host, server.port))
                .map_err(|err| {
                    crate::err!(
                        "invalid listen address `{}:{}` for connector `{}`: {err}",
                        server.host,
                        server.port,
                        connector_name
                    )
                })?;

            let options = chronicle.trigger.options_json();
            let path = options
                .get("path")
                .and_then(|value| value.as_str())
                .map(normalise_path)
                .unwrap_or_else(|| "/".to_string());

            let method = options
                .get("method")
                .and_then(|value| value.as_str())
                .unwrap_or("POST");

            let method = Method::from_bytes(method.as_bytes()).map_err(|err| {
                crate::err!(
                    "invalid http method `{}` for chronicle `{}`: {err}",
                    method,
                    chronicle.name
                )
            })?;

            let expects_json = options
                .get("content_type")
                .and_then(|value| value.as_str())
                .map(|ct| ct.to_ascii_lowercase().contains("json"))
                .unwrap_or(true);

            let retry_budget = engine
                .route_metadata(&chronicle.name)
                .and_then(|meta| meta.retry_budget.clone());

            let route = HttpRoute {
                chronicle: chronicle.name.clone(),
                method,
                expects_json,
                retry_budget,
            };

            let entry = servers
                .entry(socket_addr)
                .or_insert_with(ServerBindings::new);

            if processed_connectors.insert(connector_name.clone()) {
                if let Some(health_cfg) = server.health.as_ref() {
                    let health_route = build_health_route(connector_name, health_cfg)?;
                    register_health_route(entry, connector_name, health_route);
                }
            }

            entry.routes.entry(path).or_default().push(route);
        }

        let factory = Arc::new(ConnectorFactoryRegistry::new(Arc::clone(&registry)));
        let dispatcher = Arc::new(ActionDispatcher::new(
            Arc::clone(&factory),
            readiness.clone(),
            dependency_health,
        ));
        let retry_after = retry_after_hint_seconds(app_policy.readiness_cache);
        let max_payload_bytes = app_policy
            .limits
            .http
            .as_ref()
            .and_then(|limits| limits.max_payload_bytes)
            .map(|limit| limit as usize)
            .unwrap_or(DEFAULT_MAX_BODY_BYTES);

        let instances: Vec<HttpServerInstance> = servers
            .into_iter()
            .map(|(addr, bindings)| {
                let state = HttpServerState {
                    engine: Arc::clone(&engine),
                    dispatcher: Arc::clone(&dispatcher),
                    routes: bindings.routes,
                    health_routes: bindings.health_routes,
                    readiness: readiness.clone(),
                    backpressure: backpressure.clone(),
                    readiness_retry_after: retry_after,
                    max_payload_bytes,
                    limit_cache: RwLock::new(HashMap::new()),
                    limit_cache_ttl: app_policy.readiness_cache,
                };
                HttpServerInstance {
                    addr,
                    state: Arc::new(state),
                }
            })
            .collect();

        let server_count = instances.len();
        let inner = TaskTransportRuntime::new(TransportKind::HttpIn, "http-in", move |shutdown| {
            instances
                .into_iter()
                .map(|instance| spawn_http_server(instance, shutdown.clone()))
                .collect()
        });

        Ok(Self {
            inner,
            server_count,
        })
    }

    pub fn server_count(&self) -> usize {
        self.server_count
    }
}

fn build_health_route(connector: &str, config: &HttpServerHealthConfig) -> Result<HealthRoute> {
    let method_raw = config.method.as_deref().unwrap_or("GET");
    let method = Method::from_bytes(method_raw.as_bytes()).map_err(|err| {
        crate::err!(
            "invalid http_server health.method `{}` for connector `{}`: {err}",
            method_raw,
            connector
        )
    })?;
    let path_raw = config.path.as_deref().unwrap_or("/health");
    let path = normalise_path(path_raw);

    Ok(HealthRoute { path, method })
}

fn register_health_route(entry: &mut ServerBindings, connector: &str, route: HealthRoute) {
    if entry.routes.contains_key(&route.path) {
        warn!(
            connector = connector,
            path = route.path.as_str(),
            "skipping http_server health probe because path conflicts with an existing route"
        );
        return;
    }

    if entry
        .health_routes
        .iter()
        .any(|existing| existing.path == route.path && existing.method == route.method)
    {
        return;
    }

    entry.health_routes.push(route);
}

fn spawn_http_server(instance: HttpServerInstance, shutdown: CancellationToken) -> JoinHandle<()> {
    tokio::spawn(async move {
        let HttpServerInstance { addr, state } = instance;

        let paths: Vec<String> = state.routes.keys().cloned().collect();
        let mut router = Router::new();
        for path in paths {
            let route_state = state.clone();
            router = router.route(
                path.as_str(),
                any(move |request| handle_http_trigger(route_state.clone(), request)),
            );
        }

        for health in &state.health_routes {
            let method = health.method.clone();
            let path = health.path.clone();
            router = router.route(
                path.as_str(),
                any(move |request| handle_health_probe(method.clone(), request)),
            );
        }

        match TcpListener::bind(addr).await {
            Ok(listener) => {
                info!(address = %addr, "http trigger runtime listening");
                let make_service = Shared::new(router.into_service::<axum::body::Body>());
                let server =
                    axum::serve(listener, make_service).with_graceful_shutdown(async move {
                        shutdown.cancelled().await;
                    });

                if let Err(err) = server.await {
                    error!(address = %addr, %err, "http trigger runtime terminated with error");
                }
            }
            Err(err) => {
                error!(address = %addr, %err, "failed to bind http trigger listener");
            }
        }
    })
}

#[async_trait]
impl TransportRuntime for HttpTriggerRuntime {
    fn kind(&self) -> TransportKind {
        self.inner.kind()
    }

    fn name(&self) -> &'static str {
        self.inner.name()
    }

    fn health(&self) -> crate::transport::TransportHealth {
        self.inner.health()
    }

    async fn prepare(&mut self) -> Result<()> {
        self.inner.prepare().await
    }

    async fn start(&mut self, shutdown: CancellationToken) -> Result<()> {
        self.inner.start(shutdown).await
    }

    fn run(&mut self) -> crate::transport::TransportRun {
        self.inner.run()
    }

    async fn shutdown(&mut self) -> Result<()> {
        self.inner.shutdown().await
    }
}

async fn handle_http_trigger(
    state: Arc<HttpServerState>,
    request: axum::http::Request<axum::body::Body>,
) -> std::result::Result<axum::response::Response, StatusCode> {
    let (parts, body) = request.into_parts();
    let path = parts.uri.path().to_string();
    let method = parts.method.clone();

    let route_entry = state.routes.get(&path);
    let routes = match route_entry {
        Some(routes) => routes,
        None => return Err(StatusCode::NOT_FOUND),
    };

    let route = match routes.iter().find(|route| route.method == method) {
        Some(route) => route.clone(),
        None => return Err(StatusCode::METHOD_NOT_ALLOWED),
    };

    let _http_permit = match state.backpressure.http.try_acquire_now() {
        Some(permit) => permit,
        None => {
            let retry_after = retry_after_seconds_from_budget(route.retry_budget.as_ref());
            let response = state
                .limit_response(&route.chronicle, retry_after, Some("HTTP_MAX_CONCURRENCY"))
                .await;
            return Ok(response);
        }
    };

    if let Some(response) = gate_route_readiness(&state, &route).await {
        return Ok(response);
    }

    let bytes = axum::body::to_bytes(body, state.max_payload_bytes)
        .await
        .map_err(|_| StatusCode::PAYLOAD_TOO_LARGE)?;

    let payload = build_trigger_payload(
        &route,
        &path,
        parts.uri.query(),
        &parts.method,
        &parts.headers,
        &bytes,
    )?;

    match state.engine.execute(&route.chronicle, payload) {
        Ok(execution) => {
            let delivery_ctx = DeliveryContext {
                policy: execution.delivery.as_ref(),
                fallback: execution.fallback.as_ref(),
                fallback_actions: execution.fallback_actions.as_ref(),
                allow_partial_delivery: execution.allow_partial_delivery,
                retry_budget: execution.retry_budget.as_ref(),
            };

            match state
                .dispatcher
                .dispatch(&route.chronicle, &execution.actions, delivery_ctx)
                .await
            {
                Ok(()) => {}
                Err(ActionDispatchError::RetryBudgetExhausted { .. }) => {
                    return Ok(build_budget_exhausted_response(&route.chronicle));
                }
                Err(err) => {
                    error!(
                        chronicle = %route.chronicle,
                        error = %err,
                        "failed to dispatch chronicle actions"
                    );
                    return Err(StatusCode::BAD_GATEWAY);
                }
            }

            if let Some(controller) = state.readiness.as_ref() {
                if let Err(err) = controller.reset_retry_budget(&route.chronicle).await {
                    warn!(
                        chronicle = %route.chronicle,
                        error = ?err,
                        "failed to reset retry budget after dispatch"
                    );
                }
            }

            log_actions(&execution.actions);
            execution_to_response(execution)
        }
        Err(err) => match err {
            ChronicleEngineError::RouteBackpressure { .. } => {
                warn!(
                    chronicle = %route.chronicle,
                    "chronicle rejected due to route backpressure"
                );
                let retry_after = retry_after_seconds_from_budget(route.retry_budget.as_ref());
                let response = state
                    .limit_response(&route.chronicle, retry_after, Some("ROUTE_BACKPRESSURE"))
                    .await;
                Ok(response)
            }
            ChronicleEngineError::RouteQueueOverflow { .. } => {
                warn!(
                    chronicle = %route.chronicle,
                    "chronicle queue depth exceeded"
                );
                let retry_after = retry_after_seconds_from_budget(route.retry_budget.as_ref());
                let response = state
                    .limit_response(&route.chronicle, retry_after, Some("ROUTE_QUEUE_OVERFLOW"))
                    .await;
                Ok(response)
            }
            other => {
                error!(
                    chronicle = %route.chronicle,
                    error = %other,
                    "chronicle execution failed"
                );
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        },
    }
}

async fn handle_health_probe(
    expected_method: Method,
    request: axum::http::Request<axum::body::Body>,
) -> std::result::Result<axum::response::Response, StatusCode> {
    if request.method() != expected_method {
        return Err(StatusCode::METHOD_NOT_ALLOWED);
    }

    use axum::body::Body;
    use axum::http::Response;

    let payload = serde_json::json!({
        "status": "ok",
        "ts": Utc::now().to_rfc3339(),
    });

    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/json")
        .header(CACHE_CONTROL, "no-store")
        .body(Body::from(payload.to_string()))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn gate_route_readiness(
    state: &HttpServerState,
    route: &HttpRoute,
) -> Option<axum::response::Response> {
    let controller = state.readiness.as_ref()?;
    let route_state = controller.route_state(&route.chronicle).await?;
    if matches!(route_state, RouteState::Ready | RouteState::Degraded) {
        None
    } else {
        Some(build_not_ready_response(
            &route.chronicle,
            state.readiness_retry_after,
        ))
    }
}

fn build_not_ready_response(route: &str, retry_after: u64) -> axum::response::Response {
    use axum::body::Body;
    use axum::http::Response;
    let now = Utc::now().to_rfc3339();
    let payload = serde_json::json!({
        "error": "TARGET_UNAVAILABLE",
        "route": route,
        "ts": now
    });
    Response::builder()
        .status(StatusCode::SERVICE_UNAVAILABLE)
        .header(CONTENT_TYPE, "application/json")
        .header(CACHE_CONTROL, "no-store")
        .header(RETRY_AFTER, retry_after.to_string())
        .body(Body::from(payload.to_string()))
        .expect("http readiness response")
}

fn build_budget_exhausted_response(route: &str) -> axum::response::Response {
    use axum::body::Body;
    use axum::http::Response;
    let now = Utc::now().to_rfc3339();
    let payload = serde_json::json!({
        "error": "BUDGET_EXHAUSTED",
        "route": route,
        "ts": now
    });
    Response::builder()
        .status(StatusCode::SERVICE_UNAVAILABLE)
        .header(CONTENT_TYPE, "application/json")
        .header(CACHE_CONTROL, "no-store")
        .body(Body::from(payload.to_string()))
        .expect("budget exhausted response")
}

fn build_limit_response_body(route: &str, retry_after: u64, detail: Option<&str>) -> Bytes {
    let now = Utc::now().to_rfc3339();
    let mut payload = serde_json::json!({
        "error": "LIMITS_ENFORCED",
        "route": route,
        "retry_after_seconds": retry_after,
        "ts": now
    });
    if let (Some(value), Some(detail)) = (payload.as_object_mut(), detail) {
        value.insert("detail".to_string(), JsonValue::String(detail.to_string()));
    }
    Bytes::from(payload.to_string())
}

fn build_limit_response_from_body(body: Bytes, retry_after: u64) -> axum::response::Response {
    use axum::body::Body;
    use axum::http::Response;
    Response::builder()
        .status(StatusCode::TOO_MANY_REQUESTS)
        .header(CONTENT_TYPE, "application/json")
        .header(CACHE_CONTROL, "no-store")
        .header(RETRY_AFTER, retry_after.to_string())
        .body(Body::from(body))
        .expect("http limit response")
}

fn execution_to_response(
    execution: ChronicleExecution,
) -> std::result::Result<axum::response::Response, StatusCode> {
    use axum::body::Body;
    use axum::http::header::CONTENT_TYPE;
    use axum::http::Response;

    if let Some(resp) = execution.response {
        let encoded = encode_http_response(&resp).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let status =
            StatusCode::from_u16(encoded.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

        let mut builder = Response::builder().status(status);
        if let Some(content_type) = encoded.content_type.as_ref() {
            builder = builder.header(CONTENT_TYPE, content_type);
        }

        builder
            .body(Body::from(encoded.body))
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
    } else {
        axum::http::Response::builder()
            .status(StatusCode::ACCEPTED)
            .body(Body::from(Bytes::new()))
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
    }
}

fn build_trigger_payload(
    route: &HttpRoute,
    path: &str,
    query: Option<&str>,
    method: &Method,
    headers: &axum::http::HeaderMap,
    body: &[u8],
) -> std::result::Result<JsonValue, StatusCode> {
    let header_pairs = normalise_headers(headers);
    let header_json = headers_to_json(&header_pairs);
    let query_json = query_to_json(query);
    let path_json = path_to_json(path);

    let normalised_path = normalise_path(path);

    let body_json = if route.expects_json {
        if body.is_empty() {
            JsonValue::Null
        } else {
            serde_json::from_slice(body).map_err(|_| StatusCode::BAD_REQUEST)?
        }
    } else {
        binary_body_to_json(body)
    };

    let mut metadata = JsonMap::new();
    metadata.insert("method".to_string(), JsonValue::String(method.to_string()));
    metadata.insert(
        "path".to_string(),
        JsonValue::String(normalised_path.clone()),
    );
    if let Some(q) = query {
        if !q.is_empty() {
            metadata.insert("query".to_string(), JsonValue::String(q.to_string()));
        }
    }
    metadata.insert(
        "received_at".to_string(),
        JsonValue::String(Utc::now().to_rfc3339()),
    );

    let mut legacy_request = JsonMap::new();
    legacy_request.insert("method".to_string(), JsonValue::String(method.to_string()));
    legacy_request.insert(
        "path".to_string(),
        JsonValue::String(normalised_path.clone()),
    );
    if let Some(q) = query {
        if !q.is_empty() {
            legacy_request.insert("query".to_string(), JsonValue::String(q.to_string()));
        }
    }

    let mut root = JsonMap::new();
    root.insert(
        "headers".to_string(),
        JsonValue::Object(header_json.clone()),
    );
    root.insert("header".to_string(), JsonValue::Object(header_json));
    root.insert("query".to_string(), JsonValue::Object(query_json.clone()));
    root.insert("request".to_string(), JsonValue::Object(legacy_request));
    root.insert("path".to_string(), JsonValue::Object(path_json));
    root.insert("body".to_string(), body_json);
    root.insert("metadata".to_string(), JsonValue::Object(metadata));

    Ok(JsonValue::Object(root))
}

fn log_actions(actions: &[chronicle_core::chronicle::engine::ChronicleAction]) {
    for action in actions {
        match action {
            ChronicleAction::KafkaPublish {
                connector,
                topic,
                trace_id,
                record_id,
                ..
            } => {
                info!(
                    connector,
                    topic,
                    trace_id = trace_id.as_deref().unwrap_or_default(),
                    record_id = record_id.as_deref().unwrap_or_default(),
                    "kafka publish action emitted"
                );
            }
            ChronicleAction::HttpRequest {
                connector,
                path,
                method,
                ..
            } => {
                info!(connector, path, method, "http request action emitted");
            }
            ChronicleAction::GrpcRequest {
                connector,
                service,
                method,
                ..
            } => {
                info!(connector, service, method, "grpc request action emitted");
            }
            ChronicleAction::MqttPublish {
                connector, topic, ..
            } => {
                info!(connector, topic, "mqtt publish action emitted");
            }
            ChronicleAction::RabbitmqPublish {
                connector,
                routing_key,
                ..
            } => {
                info!(
                    connector,
                    routing_key = routing_key.as_deref().unwrap_or_default(),
                    "rabbitmq publish action emitted"
                );
            }
            ChronicleAction::SmtpEmail {
                connector, host, ..
            } => {
                info!(connector, host, "smtp email action emitted");
            }
            ChronicleAction::MariadbInsert { connector, .. } => {
                info!(connector, "mariadb insert action emitted");
            }
            ChronicleAction::PostgresQuery { connector, .. } => {
                info!(connector, "postgres query action emitted");
            }
            ChronicleAction::MongodbCommand {
                connector,
                collection,
                operation,
                ..
            } => {
                info!(
                    connector,
                    collection, operation, "mongodb command action emitted"
                );
            }
            ChronicleAction::RedisCommand {
                connector, command, ..
            } => {
                info!(connector, command, "redis command action emitted");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn health_route_defaults_to_get_health() {
        let cfg = HttpServerHealthConfig {
            method: None,
            path: None,
            extra: BTreeMap::new(),
        };

        let route = build_health_route("http_in", &cfg).expect("health route");
        assert_eq!(route.method, Method::GET);
        assert_eq!(route.path, "/health");
    }

    #[test]
    fn register_health_route_skips_conflicts() {
        let mut bindings = ServerBindings::new();
        bindings.routes.insert("/health".to_string(), Vec::new());

        let route = HealthRoute {
            path: "/health".to_string(),
            method: Method::GET,
        };

        register_health_route(&mut bindings, "http_in", route);
        assert!(
            bindings.health_routes.is_empty(),
            "conflicting path should be ignored"
        );
    }
}
