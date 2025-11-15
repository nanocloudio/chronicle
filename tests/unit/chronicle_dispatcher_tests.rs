use chronicle::chronicle::dispatcher::{
    extract_email_body, extract_timeout_ms, quote_mysql_identifier, resolve_http_url,
    ActionDispatcher, ActionError,
};
use chronicle::chronicle::engine::ChronicleAction;
use chronicle::config::integration::IntegrationConfig;
use chronicle::integration::factory::{ConnectorFactoryError, ConnectorFactoryRegistry};
use chronicle::integration::registry::ConnectorRegistry;
use chronicle::telemetry::{runtime_counters, RuntimeCountersSnapshot};
use serde_json::{json, Value as JsonValue};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};
use tracing::{
    callsite,
    field::{Field, Visit},
    metadata::LevelFilter,
    subscriber::Interest,
    Event, Metadata,
};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::Registry;

static TELEMETRY_TEST_GUARD: OnceLock<Mutex<()>> = OnceLock::new();

fn telemetry_test_lock() -> &'static Mutex<()> {
    TELEMETRY_TEST_GUARD.get_or_init(|| Mutex::new(()))
}

#[derive(Clone, Debug, Default)]
struct CapturedEvent {
    fields: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default)]
struct RecordingLayer {
    events: Arc<Mutex<Vec<CapturedEvent>>>,
}

impl RecordingLayer {
    fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn take_events(&self) -> Vec<CapturedEvent> {
        let mut guard = self.events.lock().expect("recording layer poisoned");
        let events = guard.clone();
        guard.clear();
        events
    }
}

impl<S> Layer<S> for RecordingLayer
where
    S: tracing::Subscriber,
{
    fn register_callsite(&self, _metadata: &Metadata<'_>) -> Interest {
        Interest::always()
    }

    fn enabled(&self, _metadata: &Metadata<'_>, _ctx: Context<'_, S>) -> bool {
        true
    }

    fn max_level_hint(&self) -> Option<LevelFilter> {
        Some(LevelFilter::TRACE)
    }

    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut visitor = RecordingVisitor::default();
        event.record(&mut visitor);
        let mut fields = visitor.fields;
        if let Some(message) = visitor.message {
            fields.insert("message".to_string(), message);
        }

        let record = CapturedEvent { fields };

        self.events
            .lock()
            .expect("recording layer poisoned")
            .push(record);
    }
}

#[derive(Default)]
struct RecordingVisitor {
    message: Option<String>,
    fields: BTreeMap<String, String>,
}

impl Visit for RecordingVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = Some(value.to_string());
        } else {
            self.fields
                .insert(field.name().to_string(), value.to_string());
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.fields
            .insert(field.name().to_string(), format!("{value:?}"));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }
}

fn connector_failure_count(
    snapshot: &RuntimeCountersSnapshot,
    connector: &str,
    kind: &str,
) -> u64 {
    snapshot
        .connector_outcomes
        .iter()
        .find(|entry| entry.connector == connector && entry.kind == kind)
        .map(|entry| entry.failure)
        .unwrap_or(0)
}

#[test]
fn telemetry_logs_include_trace_and_record_ids_on_failure() {
    let _guard = telemetry_test_lock().lock().expect("lock poisoned");

    let registry = Arc::new(ConnectorRegistry::default());
    let factory = Arc::new(ConnectorFactoryRegistry::new(registry));
    let dispatcher = ActionDispatcher::new(factory, None, None);

    let action = ChronicleAction::HttpRequest {
        connector: "missing_http".to_string(),
        base_url: "https://example.invalid".to_string(),
        method: "GET".to_string(),
        path: "/health".to_string(),
        headers: BTreeMap::new(),
        body: JsonValue::Null,
        content_type: None,
        extra: JsonValue::Null,
        trace_id: Some("trace-telemetry".to_string()),
        record_id: Some("record-telemetry".to_string()),
    };

    let before = runtime_counters().snapshot();
    let recorder = RecordingLayer::new();
    let subscriber = Registry::default().with(recorder.clone());

    tracing::subscriber::with_default(subscriber, || {
        callsite::rebuild_interest_cache();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let result = dispatcher.dispatch_action(&action).await;
            assert!(result.is_err(), "dispatcher should surface error");
        });
    });

    let events = recorder.take_events();
    assert!(!events.is_empty(), "expected at least one telemetry event");
    let failure_event = events
        .iter()
        .find(|event| {
            event.fields.get("connector").map(String::as_str) == Some("missing_http")
                && event.fields.contains_key("trace_id")
        })
        .expect("action_failed event recorded");

    assert_eq!(
        failure_event.fields.get("connector").map(String::as_str),
        Some("missing_http"),
    );
    assert_eq!(
        failure_event.fields.get("trace_id").map(String::as_str),
        Some("trace-telemetry"),
    );
    assert_eq!(
        failure_event.fields.get("record_id").map(String::as_str),
        Some("record-telemetry"),
    );

    let after = runtime_counters().snapshot();
    let before_failures = connector_failure_count(&before, "missing_http", "http_request");
    let after_failures = connector_failure_count(&after, "missing_http", "http_request");
    assert_eq!(after_failures, before_failures + 1);
}

#[test]
fn dispatcher_surface_connector_error_with_context() {
    let registry = Arc::new(ConnectorRegistry::default());
    let factory = Arc::new(ConnectorFactoryRegistry::new(registry));
    let dispatcher = ActionDispatcher::new(factory, None, None);

    let action = ChronicleAction::HttpRequest {
        connector: "missing_http".to_string(),
        base_url: "https://example.invalid".to_string(),
        method: "GET".to_string(),
        path: "/health".to_string(),
        headers: BTreeMap::new(),
        body: JsonValue::Null,
        content_type: None,
        extra: JsonValue::Null,
        trace_id: None,
        record_id: None,
    };

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let error = runtime
        .block_on(async { dispatcher.dispatch_action(&action).await })
        .expect_err("missing connector should fail");

    match error {
        ActionError::Connector { connector, source } => {
            assert_eq!(connector, "missing_http");
            match source {
                ConnectorFactoryError::MissingConnector { name, expected } => {
                    assert_eq!(name, "missing_http");
                    assert_eq!(expected, "http_client");
                }
                other => panic!("unexpected connector factory error: {other:?}"),
            }
        }
        other => panic!("unexpected action error: {other:?}"),
    }
}

#[test]
fn http_url_resolution_handles_relative_paths() {
    let url = resolve_http_url("https://example.com/api", "/v1/items").unwrap();
    assert_eq!(url.as_str(), "https://example.com/v1/items");

    let absolute =
        resolve_http_url("https://ignored", "https://service.internal/health").unwrap();
    assert_eq!(absolute.as_str(), "https://service.internal/health");
}

#[test]
fn timeout_extraction_supports_numeric_and_duration() {
    let value = json!({ "timeout_ms": 2500 });
    assert_eq!(extract_timeout_ms(&value).unwrap(), Some(2500));

    let value = json!({ "timeout": "1500ms" });
    assert_eq!(extract_timeout_ms(&value).unwrap(), Some(1500));

    let value = json!({});
    assert_eq!(extract_timeout_ms(&value).unwrap(), None);
}

#[test]
fn mysql_identifier_quoting_preserves_schema_and_table() {
    let quoted = quote_mysql_identifier("analytics.records").unwrap();
    assert_eq!(quoted, "`analytics`.`records`");
    assert!(quote_mysql_identifier("invalid-name").is_err());
}

#[test]
fn parse_mailboxes_from_string_and_array() {
    let list = parse_mailboxes(Some(&JsonValue::String(
        "alpha@example.com, beta@example.com".to_string(),
    )))
    .unwrap();
    assert_eq!(list.len(), 2);

    let list = parse_mailboxes(Some(&json!(["gamma@example.com"]))).unwrap();
    assert_eq!(list.len(), 1);
}

#[test]
fn email_body_serialises_objects() {
    let body = extract_email_body(Some(&json!({"foo": 1}))).unwrap();
    assert_eq!(body, "{\"foo\":1}");
    let empty = extract_email_body(None).unwrap();
    assert!(empty.is_empty());
}

#[derive(Default)]
struct RecordingVisitor {
    message: Option<String>,
    fields: BTreeMap<String, String>,
}
