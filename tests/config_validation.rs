#[path = "common/mod.rs"]
mod common;

use chronicle::config::integration::{ConnectorKind, PhaseKind};
use chronicle::config::IntegrationConfig;
use serde_json::json;

#[test]
fn integration_config_matches_spec_schema() {
    let config = common::load_integration_config();

    assert_eq!(config.connectors.len(), 6);
    assert_eq!(config.chronicles.len(), 5);

    let http_server = config
        .connectors
        .iter()
        .find(|connector| matches!(connector.kind, ConnectorKind::HttpServer))
        .expect("http server connector present");
    let http_options_json = http_server.options_json();
    let http_options = http_options_json
        .as_object()
        .expect("http server options object");
    assert_eq!(http_options.get("port"), Some(&json!(8443)));
    let tls = http_options
        .get("tls")
        .and_then(|value| value.as_object())
        .expect("tls block present");
    assert_eq!(tls.get("key"), Some(&json!("key.pem")));
    assert_eq!(tls.get("cert"), Some(&json!("cert.pem")));
    assert_eq!(tls.get("ca"), Some(&json!("ca.pem")));

    let http_client = config
        .connectors
        .iter()
        .find(|connector| matches!(connector.kind, ConnectorKind::HttpClient))
        .expect("http client connector present");
    let client_options_json = http_client.options_json();
    let client_options = client_options_json
        .as_object()
        .expect("http client options object");
    assert_eq!(
        client_options.get("base_url"),
        Some(&json!("https://processor.example.com"))
    );

    let grpc_connector = config
        .connectors
        .iter()
        .find(|connector| matches!(connector.kind, ConnectorKind::Grpc))
        .expect("grpc connector present");
    let grpc_options_json = grpc_connector.options_json();
    let grpc_options = grpc_options_json.as_object().expect("grpc options object");
    assert_eq!(
        grpc_options.get("endpoint"),
        Some(&json!("https://grpc.example.com:443"))
    );

    let smtp_connector = config
        .connectors
        .iter()
        .find(|connector| matches!(connector.kind, ConnectorKind::Smtp))
        .expect("smtp connector present");
    let smtp_options_json = smtp_connector.options_json();
    let smtp_options = smtp_options_json.as_object().expect("smtp options object");
    assert_eq!(smtp_options.get("host"), Some(&json!("smtp.example.com")));
    assert_eq!(smtp_options.get("from"), Some(&json!("alerts@example.com")));

    let collect_record = config
        .chronicles
        .iter()
        .find(|chronicle| chronicle.name == "collect_record")
        .expect("collect_record chronicle present");
    assert_eq!(collect_record.trigger.connector, "sample_ingest_service");
    assert_eq!(collect_record.phases.len(), 3);
    assert_eq!(collect_record.phases[0].kind, PhaseKind::Transform);
    assert_eq!(collect_record.phases[1].kind, PhaseKind::KafkaProducer);

    let relay_record = config
        .chronicles
        .iter()
        .find(|chronicle| chronicle.name == "relay_record")
        .expect("relay_record chronicle present");
    let relay_trigger = relay_record
        .trigger
        .kafka_json()
        .and_then(|json| json.as_object().cloned())
        .expect("relay_record uses kafka trigger");
    assert_eq!(relay_trigger.get("topic"), Some(&json!("records.samples")));
    assert_eq!(relay_trigger.get("group_id"), Some(&json!("relay-service")));
}

#[test]
fn app_section_rejects_unknown_keys() {
    let yaml = r#"
app:
  warmup_timeout: 10s
  unexpected_flag: true
connectors: []
chronicles: []
"#;

    let error = IntegrationConfig::from_reader(yaml.as_bytes())
        .expect_err("unknown app key should fail validation");

    let message = format!("{error}");
    assert!(
        message.contains("unknown field") && message.contains("unexpected_flag"),
        "error `{message}` did not mention the unexpected flag"
    );
}

#[test]
fn connector_section_rejects_unknown_keys() {
    let yaml = r#"
connectors:
  - name: inbound
    type: http_server
    bogus: true
chronicles: []
"#;

    let error = IntegrationConfig::from_reader(yaml.as_bytes())
        .expect_err("unknown connector key should fail validation");

    let message = format!("{error}");
    assert!(
        message.contains("unknown field") && message.contains("bogus"),
        "error `{message}` did not mention the bogus connector field"
    );
}

#[test]
fn chronicle_section_rejects_unknown_keys() {
    let yaml = r#"
connectors:
  - name: inbound
    type: http_server
chronicles:
  - name: sample
    trigger:
      connector: inbound
      options: {}
    phases: []
    unexpected: true
"#;

    let error = IntegrationConfig::from_reader(yaml.as_bytes())
        .expect_err("unknown chronicle key should fail validation");

    let message = format!("{error}");
    assert!(
        message.contains("unknown field") && message.contains("unexpected"),
        "error `{message}` did not mention the unexpected chronicle field"
    );
}

#[test]
fn management_section_rejects_unknown_keys() {
    let yaml = r#"
management:
  host: 0.0.0.0
  port: 9100
  live:
    path: /livez
  extra: true
connectors: []
chronicles: []
"#;

    let error = IntegrationConfig::from_reader(yaml.as_bytes())
        .expect_err("unknown management key should fail validation");

    let message = format!("{error}");
    assert!(
        message.contains("unknown field") && message.contains("extra"),
        "error `{message}` did not mention the management extra field"
    );
}

#[test]
fn delivery_retries_cannot_exceed_budget() {
    let yaml = r#"
api_version: v1
app:
  retry_budget:
    max_attempts: 2
connectors:
  - name: http-in
    type: http_server
    options:
      host: 0.0.0.0
      port: 8080
  - name: http-out
    type: http_client
    options:
      base_url: https://processor.example.com
chronicles:
  - name: sample
    trigger:
      connector: http-in
      options:
        method: POST
        path: /ingest
    phases:
      - name: call
        type: http_client
        options:
          connector: http-out
          method: POST
          path: /process
    policy:
      delivery:
        retries: 5
"#;

    let error = IntegrationConfig::from_reader(yaml.as_bytes())
        .expect_err("delivery retries above retry budget should fail");

    let message = format!("{error}");
    assert!(
        message.contains("policy.delivery.retries"),
        "error `{message}` did not mention delivery retries exceeding budget"
    );
}

#[test]
fn app_warmup_timeout_must_be_positive() {
    let yaml = r#"
api_version: v1
app:
  warmup_timeout: 0s
connectors: []
chronicles: []
"#;

    let error = IntegrationConfig::from_reader(yaml.as_bytes())
        .expect_err("zero warmup timeout should fail");

    assert!(format!("{error}").contains("app.warmup_timeout must be greater than zero"));
}

#[test]
fn app_retry_budget_durations_must_be_positive() {
    let yaml = r#"
api_version: v1
app:
  retry_budget:
    max_attempts: 0
    max_elapsed: 0s
    base_backoff: 0s
    max_backoff: 0s
connectors: []
chronicles: []
"#;

    let error = IntegrationConfig::from_reader(yaml.as_bytes())
        .expect_err("invalid retry budget should fail");

    let message = format!("{error}");
    assert!(message.contains("app.retry_budget.max_attempts must be greater than zero"));
    assert!(message.contains("app.retry_budget.max_elapsed must be greater than zero"));
    assert!(message.contains("app.retry_budget.base_backoff must be greater than zero"));
    assert!(message.contains("app.retry_budget.max_backoff must be greater than zero"));
}

#[test]
fn connector_warmup_timeout_must_be_positive() {
    let yaml = r#"
api_version: v1
connectors:
  - name: inbound
    type: http
    warmup: true
    warmup_timeout: 0s
    options:
      role: server
      host: 0.0.0.0
      port: 8080
chronicles: []
"#;

    let error = IntegrationConfig::from_reader(yaml.as_bytes())
        .expect_err("connector warmup timeout should fail");

    assert!(
        format!("{error}").contains("connector `inbound` warmup_timeout must be greater than zero")
    );
}

#[test]
fn rabbitmq_connector_requires_feature_flag() {
    let yaml = r#"
api_version: v1
connectors:
  - name: rabbit
    type: rabbitmq
    options:
      url: amqps://guest:guest@localhost:5671/%2f
chronicles: []
"#;

    let error = IntegrationConfig::from_reader(yaml.as_bytes())
        .expect_err("rabbitmq connector should require flag");

    assert!(format!("{error}")
        .contains("connector `rabbit` of type `rabbitmq` requires feature flag `rabbitmq`"));
}

#[test]
fn parallel_phase_requires_feature_flag() {
    let yaml = r#"
api_version: v1
connectors:
  - name: http_ingest
    type: http
    options:
      role: server
      host: 0.0.0.0
      port: 8080
  - name: outbound
    type: http
    options:
      role: client
      base_url: https://api.example.com
      tls.ca: ca.pem
chronicles:
  - name: fanout
    trigger:
      connector: http_ingest
      options:
        method: POST
        path: /fanout
    phases:
      - name: fanout_parallel
        type: parallel
        options: {}
"#;

    let error = IntegrationConfig::from_reader(yaml.as_bytes())
        .expect_err("parallel phases should require flag");

    assert!(format!("{error}")
        .contains("phase `fanout_parallel` (index 0) requires feature flag `parallel_phase`"));
}

#[test]
fn unknown_feature_flags_fail_validation() {
    let yaml = r#"
api_version: v1
app:
  feature_flags:
    - rabbitmq
    - typo_feature
connectors: []
chronicles: []
"#;

    let error = IntegrationConfig::from_reader(yaml.as_bytes())
        .expect_err("unknown feature flag should fail validation");

    let message = format!("{error}");
    assert!(message.contains("unsupported gate `typo_feature`"));
    assert!(message.contains("supported gates"));
}

#[test]
fn queue_overflow_requires_depth_and_positive_limits() {
    let yaml = r#"
api_version: v1
app:
  limits:
    routes:
      max_inflight: 0
      overflow_policy: queue
connectors: []
chronicles: []
"#;

    let error = IntegrationConfig::from_reader(yaml.as_bytes())
        .expect_err("queue overflow without depth should fail");

    let message = format!("{error}");
    assert!(message.contains("app.limits.routes.max_inflight must be greater than zero"));
    assert!(message.contains("app.limits.routes.max_queue_depth must be provided"));
}

#[test]
fn chronicle_policy_limit_errors_reference_chronicle() {
    let yaml = r#"
api_version: v1
connectors:
  - name: inbound
    type: http_server
    options:
      port: 8080
chronicles:
  - name: degrade
    trigger:
      connector: inbound
      options:
        path: /
    phases: []
    policy:
      limits:
        overflow_policy: queue
"#;

    let error = IntegrationConfig::from_reader(yaml.as_bytes())
        .expect_err("chronicle policy limits should be validated");

    let message = format!("{error}");
    assert!(message.contains("chronicle `degrade` policy.limits.max_queue_depth"));
}

#[test]
fn http_trigger_requires_server_connector_role() {
    let yaml = r#"
api_version: v1
connectors:
  - name: outbound
    type: http
    options:
      role: client
      base_url: https://api.example.com
      tls:
        ca: ca.pem
chronicles:
  - name: invalid
    trigger:
      connector: outbound
      options:
        path: /forward
    phases: []
"#;

    let error = IntegrationConfig::from_reader(yaml.as_bytes())
        .expect_err("http trigger should reject client connectors");

    assert!(format!("{error}")
        .contains("trigger references HTTP connector `outbound` configured as a client"));
}

#[test]
fn connector_circuit_breaker_validation_runs() {
    let yaml = r#"
api_version: v1
connectors:
  - name: api
    type: http
    options:
      role: client
      base_url: https://api.example.com
      tls:
        ca: ca.pem
    cb:
      failure_rate: 1.5
      window: 0s
      open_base: 1s
      open_max: 500ms
chronicles: []
"#;

    let error = IntegrationConfig::from_reader(yaml.as_bytes())
        .expect_err("invalid circuit breaker should fail");

    let message = format!("{error}");
    assert!(message.contains("cb.failure_rate must be greater than 0 and at most 1.0"));
    assert!(message.contains("cb.window must be greater than zero"));
    assert!(message.contains("cb.open_max must be greater than or equal to cb.open_base"));
}
