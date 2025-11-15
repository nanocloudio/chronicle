use chronicle::config::IntegrationConfig;
use insta::assert_snapshot;

#[path = "support/mod.rs"]
mod support;

fn expect_validation_error(yaml: &str) -> String {
    let decorated = support::feature_flags::enable_optional_feature_flags(yaml);
    match IntegrationConfig::from_reader(decorated.as_bytes()) {
        Ok(_) => panic!("validation should fail"),
        Err(err) => err.to_string(),
    }
}

#[test]
fn http_connector_requires_role_and_supported_version() {
    let yaml = r#"
api_version: v2
connectors:
  - name: ingress
    type: http
    options:
      host: 0.0.0.0
      port: 8080
chronicles: []
"#;

    let error = expect_validation_error(yaml);
    assert_snapshot!("http_connector_role_and_version", error);
}

#[test]
fn chronicle_rejects_unknown_trigger_connector() {
    let yaml = r#"
api_version: v1
connectors:
  - name: http_ingest
    type: http
    options:
      role: server
      host: 0.0.0.0
      port: 8080
chronicles:
  - name: orphan_route
    trigger:
      connector: missing
    phases: []
"#;

    let error = expect_validation_error(yaml);
    assert_snapshot!("unknown_trigger_connector", error);
}

#[test]
fn phase_connector_type_mismatch_is_reported() {
    let yaml = r#"
api_version: v1
connectors:
  - name: http_client
    type: http
    options:
      role: client
      base_url: https://example.com
  - name: amqp_bus
    type: rabbitmq
    options:
      url: amqp://guest:guest@localhost:5672/%2f
chronicles:
  - name: invalid_publish
    trigger:
      connector: amqp_bus
      options:
        queue: events
    phases:
      - name: publish_invalid
        type: rabbitmq
        options:
          connector: http_client
"#;

    let error = expect_validation_error(yaml);
    assert_snapshot!("phase_connector_type_mismatch", error);
}
