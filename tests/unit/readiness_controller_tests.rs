use chronicle::config::IntegrationConfig;
use chronicle::readiness::{
    ApplicationState, EndpointState, ReadinessController, RouteState,
};
use chronicle::transport::readiness_gate::RouteReadinessGate;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

fn sample_config() -> IntegrationConfig {
    let yaml = r#"
api_version: v1
app:
  min_ready_routes: 1
connectors:
  - name: ingest
    type: http_server
    options:
      host: 0.0.0.0
      port: 8080
  - name: kafka-out
    type: kafka
    options:
      brokers: ["broker:9092"]
chronicles:
  - name: example
    trigger:
      connector: ingest
      options:
        path: /events
    phases:
      - name: publish
        type: kafka_producer
        options:
          connector: kafka-out
          topic: events
"#;

    IntegrationConfig::from_reader(yaml.as_bytes()).expect("config must parse")
}

#[tokio::test(flavor = "multi_thread")]
async fn controller_tracks_route_and_application_state() {
    let config = sample_config();
    let controller = ReadinessController::initialise(&config);

    assert_eq!(controller.route_count(), 1);
    assert_eq!(
        controller.application_state().await,
        ApplicationState::WarmingUp
    );

    controller
        .set_route_state("example", RouteState::Ready)
        .await
        .expect("transition succeeds");

    assert_eq!(
        controller.application_state().await,
        ApplicationState::Ready
    );

    let routes = controller.routes_snapshot().await;
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].name, "example");
    assert_eq!(routes[0].state, RouteState::Ready);
}

#[tokio::test(flavor = "multi_thread")]
async fn describe_route_exposes_policy() {
    let config = sample_config();
    let controller = ReadinessController::initialise(&config);

    let summary = controller
        .describe_route("example")
        .expect("summary must exist");
    assert_eq!(summary.name, "example");
    assert_eq!(summary.dependencies.len(), 1);
    assert_eq!(
        summary.dependencies[0].endpoint.name,
        "endpoint.kafka-out"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn readiness_gate_waits_until_route_ready() {
    let config = sample_config();
    let controller = ReadinessController::initialise(&config);
    controller
        .set_route_state("example", RouteState::NotReady)
        .await
        .expect("state set");

    let gate = RouteReadinessGate::new(Some(controller.clone()), "example");
    let shutdown = CancellationToken::new();

    let blocked = tokio::time::timeout(Duration::from_millis(100), gate.wait(&shutdown)).await;
    assert!(
        blocked.is_err(),
        "gate should block while route is not ready"
    );

    controller
        .set_route_state("example", RouteState::Ready)
        .await
        .expect("state set");

    let unblocked =
        tokio::time::timeout(Duration::from_millis(250), gate.wait(&shutdown)).await;
    assert!(matches!(unblocked, Ok(true)));
}

#[tokio::test(flavor = "multi_thread")]
async fn half_open_policy_respected() {
    let yaml = r#"
api_version: v1
app:
  min_ready_routes: 1
  half_open_counts_as_ready: never
connectors:
  - name: ingest
    type: http_server
    options:
      host: 0.0.0.0
      port: 8080
  - name: kafka-out
    type: kafka
    options:
      brokers: ["broker:9092"]
chronicles:
  - name: example
    trigger:
      connector: ingest
      options:
        path: /events
    phases:
      - name: publish
        type: kafka_producer
        options:
          connector: kafka-out
          topic: events
"#;

    let config = IntegrationConfig::from_reader(yaml.as_bytes()).expect("config must parse");
    let controller = ReadinessController::initialise(&config);

    controller
        .set_dependency_state("example", "endpoint.kafka-out", EndpointState::Healthy)
        .await
        .expect("state update");
    assert_eq!(
        controller.route_state("example").await,
        Some(RouteState::Ready)
    );

    controller
        .set_dependency_state(
            "example",
            "endpoint.kafka-out",
            EndpointState::CircuitHalfOpen
        )
        .await
        .expect("state update");
    assert_eq!(
        controller.route_state("example").await,
        Some(RouteState::NotReady)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn half_open_route_override_allows_degraded() {
    let yaml = r#"
api_version: v1
app:
  min_ready_routes: 1
connectors:
  - name: ingest
    type: http_server
    options:
      host: 0.0.0.0
      port: 8080
  - name: kafka-out
    type: kafka
    options:
      brokers: ["broker:9092"]
chronicles:
  - name: example
    trigger:
      connector: ingest
      options:
        path: /events
    policy:
      half_open_counts_as_ready: route
    phases:
      - name: publish
        type: kafka_producer
        options:
          connector: kafka-out
          topic: events
"#;

    let config = IntegrationConfig::from_reader(yaml.as_bytes()).expect("config must parse");
    let controller = ReadinessController::initialise(&config);

    controller
        .set_dependency_state("example", "endpoint.kafka-out", EndpointState::Healthy)
        .await
        .expect("state update");
    assert_eq!(
        controller.route_state("example").await,
        Some(RouteState::Ready)
    );

    controller
        .set_dependency_state(
            "example",
            "endpoint.kafka-out",
            EndpointState::CircuitHalfOpen
        )
        .await
        .expect("state update");
    assert_eq!(
        controller.route_state("example").await,
        Some(RouteState::Degraded)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn half_open_endpoint_policy_does_not_count_route() {
    let yaml = r#"
api_version: v1
app:
  min_ready_routes: 1
connectors:
  - name: ingest
    type: http_server
    options:
      host: 0.0.0.0
      port: 8080
  - name: kafka-out
    type: kafka
    options:
      brokers: ["broker:9092"]
chronicles:
  - name: example
    trigger:
      connector: ingest
      options:
        path: /events
    policy:
      half_open_counts_as_ready: endpoint
    phases:
      - name: publish
        type: kafka_producer
        options:
          connector: kafka-out
          topic: events
"#;

    let config = IntegrationConfig::from_reader(yaml.as_bytes()).expect("config must parse");
    let controller = ReadinessController::initialise(&config);

    controller
        .set_dependency_state("example", "endpoint.kafka-out", EndpointState::Healthy)
        .await
        .expect("state update");
    assert_eq!(
        controller.route_state("example").await,
        Some(RouteState::Ready)
    );

    controller
        .set_dependency_state(
            "example",
            "endpoint.kafka-out",
            EndpointState::CircuitHalfOpen
        )
        .await
        .expect("state update");
    assert_eq!(
        controller.route_state("example").await,
        Some(RouteState::NotReady),
        "endpoint policy should not promote the route to READY/DEGRADED"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn retry_budget_exhaustion_transitions_route() {
    let yaml = r#"
api_version: v1
app:
  min_ready_routes: 1
  retry_budget:
    max_attempts: 3
connectors:
  - name: ingest
    type: http_server
    options:
      host: 0.0.0.0
      port: 8080
chronicles:
  - name: example
    trigger:
      connector: ingest
      options:
        path: /events
    policy:
      retry_budget:
        max_attempts: 2
    phases: []
"#;

    let config = IntegrationConfig::from_reader(yaml.as_bytes()).expect("config must parse");
    let controller = ReadinessController::initialise(&config);

    controller
        .set_route_state("example", RouteState::Ready)
        .await
        .expect("state set");

    assert!(controller.record_retry_failure("example").await.unwrap());
    assert!(controller.record_retry_failure("example").await.unwrap());
    assert!(!controller.record_retry_failure("example").await.unwrap());

    assert_eq!(
        controller.route_state("example").await,
        Some(RouteState::NotReady)
    );
}
