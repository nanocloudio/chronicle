#![cfg(feature = "http-in")]

use anyhow::{Context, Result};
use chronicle::app_state::AppState;
use chronicle::backpressure::BackpressureManager;
use chronicle::chronicle::phase::PhaseExecutor;
use chronicle::config::{BackpressureConfig, IntegrationConfig};
use chronicle::connectors::management::ManagementServer;
use chronicle::metrics::metrics;
use chronicle::readiness::{EndpointState, ReadinessController, RouteState};
use chrono::{DateTime, FixedOffset};
use reqwest::StatusCode;
use serde_json::Value;
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

fn reserve_port() -> std::io::Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

fn build_config(port: u16, readiness_cache: &str) -> Result<IntegrationConfig> {
    let yaml = format!(
        r#"
api_version: v1
app:
  readiness_cache: {readiness_cache}
  warmup_timeout: 1s
  min_ready_routes: all
connectors:
  - name: http_ingest
    type: http
    options:
      role: server
      host: 127.0.0.1
      port: 8080
  - name: http_processing
    type: http
    options:
      role: client
      base_url: http://processor.local:8081
chronicles:
  - name: route_a
    trigger:
      connector: http_ingest
      options:
        method: GET
        path: /ingest
    phases:
      - name: forward
        type: http_client
        options:
          connector: http_processing
          method: POST
          path: /fanout
management:
  host: 127.0.0.1
  port: {port}
  ready:
    path: /ready
  status:
    path: /status
  metrics:
    path: /metrics
"#
    );

    IntegrationConfig::from_reader(yaml.as_bytes())
        .context("failed to parse inline management readiness config")
}

fn build_app_state(config: &IntegrationConfig, controller: ReadinessController) -> AppState {
    AppState {
        db: None,
        phase_executor: PhaseExecutor::new(None),
        integration: Some(Arc::new(config.clone())),
        connectors: None,
        chronicle_engine: None,
        backpressure: BackpressureManager::new(&BackpressureConfig::default(), Some(&config.app)),
        app_policy: config.app.clone(),
        readiness: Some(controller),
        dependency_health: None,
    }
}

async fn start_management_server(
    config: &IntegrationConfig,
    controller: ReadinessController,
) -> Result<(
    String,
    CancellationToken,
    JoinHandle<chronicle::error::Result<()>>,
)> {
    let state = build_app_state(config, controller);
    let management_cfg = config
        .management
        .as_ref()
        .expect("management configuration present");
    let server = ManagementServer::build(management_cfg)
        .context("build management server")?
        .expect("management server configured");

    let shutdown = CancellationToken::new();
    let state_clone = state.clone();
    let shutdown_clone = shutdown.clone();
    let task = tokio::spawn(async move { server.serve(state_clone, shutdown_clone).await });

    // Give the listener a brief moment to come up.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let base_url = format!("http://{}:{}", management_cfg.host, management_cfg.port);

    Ok((base_url, shutdown, task))
}

#[tokio::test(flavor = "multi_thread")]
async fn ready_endpoint_exposes_retry_after_and_honours_cache() -> Result<()> {
    let port = reserve_port().context("reserve management port")?;
    let config = build_config(port, "100ms")?;

    let controller = ReadinessController::initialise(&config);
    controller
        .set_route_state("route_a", RouteState::NotReady)
        .await
        .expect("route_a transitions to NOT_READY");
    controller
        .set_dependency_state(
            "route_a",
            "endpoint.http_processing",
            EndpointState::Unhealthy,
        )
        .await
        .expect("dependency transitions to UNHEALTHY");

    let (base_url, shutdown, server_task) =
        start_management_server(&config, controller.clone()).await?;

    let client = reqwest::Client::new();

    let request_ready = || async {
        client
            .get(format!("{base_url}/ready"))
            .send()
            .await
            .context("perform readiness request")
    };

    let response = request_ready().await?;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let retry_after = response
        .headers()
        .get("retry-after")
        .expect("Retry-After header present")
        .to_str()
        .context("Retry-After header is valid ASCII")?;
    assert_eq!(
        retry_after, "1",
        "sub-second cache should advertise 1 second retry hint"
    );

    controller
        .set_route_state("route_a", RouteState::Ready)
        .await
        .expect("route_a transitions to READY");

    let cached_response = request_ready().await?;
    assert_eq!(
        cached_response.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "cached snapshot should surface until readiness_cache elapses"
    );
    assert!(
        cached_response.headers().contains_key("retry-after"),
        "cached response must continue advertising Retry-After"
    );

    tokio::time::sleep(Duration::from_millis(150)).await;

    let response = request_ready().await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        !response.headers().contains_key("retry-after"),
        "Retry-After should be absent when the application is READY"
    );

    shutdown.cancel();
    let _ = server_task.await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn ready_endpoint_retry_after_matches_cache_duration() -> Result<()> {
    let port = reserve_port().context("reserve management port")?;
    let config = build_config(port, "3s")?;

    let controller = ReadinessController::initialise(&config);
    controller
        .set_route_state("route_a", RouteState::NotReady)
        .await
        .expect("route_a transitions to NOT_READY");

    let (base_url, shutdown, server_task) =
        start_management_server(&config, controller.clone()).await?;

    let client = reqwest::Client::new();
    let response = client
        .get(format!("{base_url}/ready"))
        .send()
        .await
        .context("perform readiness request")?;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let retry_after = response
        .headers()
        .get("retry-after")
        .expect("Retry-After header present")
        .to_str()
        .context("Retry-After header is valid ASCII")?;
    assert_eq!(
        retry_after, "3",
        "Retry-After value should match the configured readiness cache rounded to seconds"
    );

    shutdown.cancel();
    let _ = server_task.await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn status_endpoint_reports_snapshots_and_timestamps() -> Result<()> {
    let port = reserve_port().context("reserve management port")?;
    let config = build_config(port, "200ms")?;

    let controller = ReadinessController::initialise(&config);
    controller
        .set_route_state("route_a", RouteState::NotReady)
        .await
        .expect("route_a transitions to NOT_READY");

    let (base_url, shutdown, server_task) =
        start_management_server(&config, controller.clone()).await?;

    let client = reqwest::Client::new();

    let fetch_status = || async {
        let response = client
            .get(format!("{base_url}/status"))
            .send()
            .await
            .context("perform status request")?;
        response
            .json::<Value>()
            .await
            .context("decode status response")
    };

    let payload = fetch_status().await?;
    assert_eq!(payload["app_state"], "NOT_READY");
    let routes = payload["routes"].as_array().expect("routes array present");
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0]["id"], "route_a");
    assert_eq!(routes[0]["state"], "NOT_READY");
    let unhealthy = routes[0]["unhealthy"]
        .as_array()
        .expect("unhealthy array present");
    assert!(
        unhealthy
            .iter()
            .any(|value| value == "endpoint.http_processing"),
        "unhealthy list should include the endpoint.http_processing dependency"
    );
    assert!(
        payload["endpoints"].is_array(),
        "status payload exposes endpoints array"
    );
    let ts = payload["ts"]
        .as_str()
        .expect("status payload includes timestamp string");
    DateTime::<FixedOffset>::parse_from_rfc3339(ts).expect("timestamp should be RFC3339 formatted");

    controller
        .set_route_state("route_a", RouteState::Ready)
        .await
        .expect("route_a transitions to READY");
    controller
        .set_dependency_state(
            "route_a",
            "endpoint.http_processing",
            EndpointState::Healthy,
        )
        .await
        .expect("dependency transitions to HEALTHY");

    let cached_payload = fetch_status().await?;
    assert_eq!(
        cached_payload["app_state"], "NOT_READY",
        "cached snapshot should remain stale until readiness_cache expires"
    );

    tokio::time::sleep(Duration::from_millis(225)).await;
    let refreshed = fetch_status().await?;
    assert_eq!(refreshed["app_state"], "READY");
    let refreshed_routes = refreshed["routes"]
        .as_array()
        .expect("refreshed routes array present");
    assert_eq!(refreshed_routes[0]["state"], "READY");

    shutdown.cancel();
    let _ = server_task.await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn metrics_endpoint_reports_prometheus_gauges() -> Result<()> {
    let port = reserve_port().context("reserve management port")?;
    let config = build_config(port, "250ms")?;

    let controller = ReadinessController::initialise(&config);
    controller
        .set_route_state("route_a", RouteState::NotReady)
        .await
        .expect("route_a transitions to NOT_READY");

    let (base_url, shutdown, server_task) =
        start_management_server(&config, controller.clone()).await?;
    let client = reqwest::Client::new();

    let counters = metrics();
    counters.register_warmup_target("warmup-metrics");
    counters.record_warmup_success("warmup-metrics", 1, Duration::from_millis(400));
    counters.record_connector_success("http_client", "http_processing");
    counters.record_connector_failure("http_client", "http_processing", None);
    counters.record_retry_budget_exhausted("route_a", Some("endpoint.http_processing"));

    let body = client
        .get(format!("{base_url}/metrics"))
        .send()
        .await
        .context("perform metrics request")?
        .text()
        .await
        .context("read metrics body")?;

    assert!(
        body.contains("chronicle_app_state 1"),
        "metrics should expose application state gauge for NOT_READY"
    );
    assert!(
        body.contains("chronicle_route_state{route=\"route_a\"} 1"),
        "route-specific gauge expected"
    );
    assert!(
        body.contains("chronicle_chronicles_loaded 1"),
        "chronicle count gauge expected"
    );
    assert!(
        body.contains("chronicle_connectors_total{type=\"http_server\"} 1"),
        "connector totals should include http_server count"
    );
    assert!(
        body.contains("chronicle_connectors_total{type=\"http_client\"} 1"),
        "connector totals should include http_client count"
    );
    assert!(
        body.contains("chronicle_backpressure_http_max_concurrency 512"),
        "HTTP backpressure metrics should reflect app.limits.http defaults"
    );
    assert!(
        body.contains("chronicle_warmup_state{connector=\"warmup-metrics\"} 1"),
        "warm-up gauge should reflect recorded success"
    );
    assert!(
        body.contains("chronicle_warmup_duration_seconds{connector=\"warmup-metrics\"} 0.400"),
        "warm-up duration should be reported"
    );
    assert!(
        body.contains("chronicle_connector_actions_total{connector=\"http_processing\",kind=\"http_client\",status=\"success\"} 1"),
        "connector action success counter should be present"
    );
    assert!(
        body.contains("chronicle_connector_actions_total{connector=\"http_processing\",kind=\"http_client\",status=\"failure\",reason=\"unknown\"} 1"),
        "connector action failure counter should be present with reason label"
    );
    assert!(
        body.contains("chronicle_retry_budget_exhausted_total{route=\"route_a\",endpoint=\"endpoint.http_processing\"} 1"),
        "retry budget exhaustion counter should include endpoint label"
    );

    shutdown.cancel();
    let _ = server_task.await;
    Ok(())
}
