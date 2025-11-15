use chronicle::config::IntegrationConfig;
use chronicle::error::Error as ChronicleError;
use chronicle::integration::factory::{ConnectorFactoryError, ConnectorFactoryRegistry};
use chronicle::integration::registry::ConnectorRegistry;
use chronicle::readiness::{
    warmup_connectors, EndpointState, ReadinessController, RouteState, WarmupCoordinator,
};
use std::path::Path;
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread")]
async fn warmup_http_client_succeeds() {
    let yaml = r#"
api_version: v1
app:
  min_ready_routes: 1
connectors:
  - name: http-out
    type: http_client
    warmup: true
    options:
      base_url: http://localhost:8080
chronicles: []
"#;

    let config = IntegrationConfig::from_reader(yaml.as_bytes()).expect("config must parse");
    let registry =
        ConnectorRegistry::build(&config, Path::new(".")).expect("registry build succeeds");
    let factory = ConnectorFactoryRegistry::new(Arc::new(registry));

    let result = warmup_connectors(&config.app, &config.connectors, &factory).await;
    assert!(result.is_ok(), "warm-up should succeed: {result:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn warmup_failure_is_reported() {
    let yaml = r#"
api_version: v1
app:
  min_ready_routes: 1
connectors:
  - name: db
    type: postgres
    warmup: true
    options:
      url: invalid-url
chronicles: []
"#;

    let config = IntegrationConfig::from_reader(yaml.as_bytes()).expect("config must parse");
    let registry =
        ConnectorRegistry::build(&config, Path::new(".")).expect("registry build succeeds");
    let factory = ConnectorFactoryRegistry::new(Arc::new(registry));

    let result = warmup_connectors(&config.app, &config.connectors, &factory).await;
    let error = result.expect_err("warm-up should fail");
    assert_eq!(error.failures.len(), 1);
    assert_eq!(error.failures[0].connector, "db");

    match &error.failures[0].error {
        ChronicleError::ConnectorFactory(ConnectorFactoryError::Postgres { name, .. }) => {
            assert_eq!(name, "db");
        }
        other => panic!("unexpected error variant: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn warmup_failure_records_attempts_and_display_context() {
    let yaml = r#"
api_version: v1
app:
  min_ready_routes: 1
connectors:
  - name: db
    type: postgres
    warmup: true
    options:
      url: invalid-url
chronicles: []
"#;

    let config = IntegrationConfig::from_reader(yaml.as_bytes()).expect("config must parse");
    let registry =
        ConnectorRegistry::build(&config, Path::new(".")).expect("registry build succeeds");
    let factory = ConnectorFactoryRegistry::new(Arc::new(registry));

    let error = warmup_connectors(&config.app, &config.connectors, &factory)
        .await
        .expect_err("warm-up should fail");

    let failure = &error.failures[0];
    assert_eq!(failure.connector, "db");
    assert_eq!(failure.attempts, 3, "warm-up should exhaust retry budget");
    match &failure.error {
        ChronicleError::ConnectorFactory(ConnectorFactoryError::Postgres { name, .. }) => {
            assert_eq!(name, "db");
        }
        other => panic!("unexpected error variant: {other:?}"),
    }

    let display = format!("{error}");
    assert!(
        display.contains("db (attempts: 3)"),
        "aggregate display should include connector attempts: {display}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn warmup_honours_app_retry_budget_attempts() {
    let yaml = r#"
api_version: v1
app:
  min_ready_routes: 1
  retry_budget:
    max_attempts: 2
    base_backoff: 5ms
connectors:
  - name: db
    type: postgres
    warmup: true
    options:
      url: invalid-url
chronicles: []
"#;

    let config = IntegrationConfig::from_reader(yaml.as_bytes()).expect("config must parse");
    let registry =
        ConnectorRegistry::build(&config, Path::new(".")).expect("registry build succeeds");
    let factory = ConnectorFactoryRegistry::new(Arc::new(registry));

    let error = warmup_connectors(&config.app, &config.connectors, &factory)
        .await
        .expect_err("warm-up should fail");

    assert_eq!(error.failures.len(), 1);
    assert_eq!(error.failures[0].attempts, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn warmup_success_marks_readiness_dependencies() {
    let yaml = r#"
api_version: v1
app:
  min_ready_routes: 1
connectors:
  - name: ingest
    type: http_server
    options:
      host: 127.0.0.1
      port: 8080
  - name: relay
    type: http_client
    warmup: true
    options:
      base_url: http://localhost:8081
chronicles:
  - name: example
    trigger:
      connector: ingest
      options:
        path: /ingest
    phases:
      - name: forward
        type: http_client
        options:
          connector: relay
          method: POST
          path: /submit
"#;

    let config = IntegrationConfig::from_reader(yaml.as_bytes()).expect("config must parse");
    let registry =
        ConnectorRegistry::build(&config, Path::new(".")).expect("registry build succeeds");
    let factory = ConnectorFactoryRegistry::new(Arc::new(registry));
    let readiness = ReadinessController::initialise(&config);

    WarmupCoordinator::new(&config.app, &config.connectors)
        .with_readiness(readiness.clone())
        .run(&factory)
        .await
        .expect("warm-up should succeed");

    let routes = readiness.routes_snapshot().await;
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].state, RouteState::Ready);

    let status = readiness.status_snapshot().await;
    let endpoint = status
        .endpoints
        .iter()
        .find(|ep| ep.name == "relay")
        .expect("relay endpoint tracked");
    assert_eq!(endpoint.state, EndpointState::Healthy);
}

#[tokio::test(flavor = "multi_thread")]
async fn warmup_failure_marks_readiness_unhealthy() {
    let yaml = r#"
api_version: v1
app:
  min_ready_routes: 1
connectors:
  - name: ingest
    type: http_server
    options:
      host: 127.0.0.1
      port: 8080
  - name: relay
    type: http_client
    warmup: true
    options:
      base_url: https://localhost:8081
      tls.ca: missing-ca.pem
chronicles:
  - name: example
    trigger:
      connector: ingest
      options:
        path: /ingest
    phases:
      - name: forward
        type: http_client
        options:
          connector: relay
          method: POST
          path: /submit
"#;

    let config = IntegrationConfig::from_reader(yaml.as_bytes()).expect("config must parse");
    let registry =
        ConnectorRegistry::build(&config, Path::new(".")).expect("registry build succeeds");
    let factory = ConnectorFactoryRegistry::new(Arc::new(registry));
    let readiness = ReadinessController::initialise(&config);

    let error = WarmupCoordinator::new(&config.app, &config.connectors)
        .with_readiness(readiness.clone())
        .run(&factory)
        .await
        .expect_err("warm-up should fail due to missing CA");
    assert_eq!(error.failures.len(), 1);

    let routes = readiness.routes_snapshot().await;
    assert_eq!(routes[0].state, RouteState::NotReady);

    let status = readiness.status_snapshot().await;
    let endpoint = status
        .endpoints
        .iter()
        .find(|ep| ep.name == "relay")
        .expect("relay endpoint tracked");
    assert_eq!(endpoint.state, EndpointState::Unhealthy);
}
