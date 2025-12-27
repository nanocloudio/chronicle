//! Tests for health route configuration.

use axum::http::Method;
use chronicle::codec::http::RouteTemplate;
use chronicle::integration::registry::HttpServerHealthConfig;
use chronicle::transport::health::{HealthRoute, HttpRoute, ServerBindings};
use std::collections::BTreeMap;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[test]
fn health_route_defaults_to_get_health() -> TestResult {
    let cfg = HttpServerHealthConfig {
        method: None,
        path: None,
        extra: BTreeMap::new(),
    };

    let route = HealthRoute::from_config("http_in", &cfg)?;
    assert_eq!(route.method, Method::GET);
    assert_eq!(route.path, "/health");
    Ok(())
}

#[test]
fn register_health_route_skips_conflicts() -> TestResult {
    let mut bindings = ServerBindings::new();

    // Add an existing exact route at /health to simulate a conflict
    let existing_route = HttpRoute {
        chronicle: "test_chronicle".to_string(),
        method: Method::GET,
        expects_json: true,
        retry_budget: None,
        template: RouteTemplate::parse("/health")?,
    };
    bindings.routes.push(existing_route);

    let route = HealthRoute {
        path: "/health".to_string(),
        method: Method::GET,
    };

    bindings.register_health_route("http_in", route);
    assert!(
        bindings.health_routes.is_empty(),
        "conflicting path should be ignored"
    );
    Ok(())
}

#[test]
fn register_health_route_skips_duplicates() {
    let mut bindings = ServerBindings::new();

    let route1 = HealthRoute {
        path: "/health".to_string(),
        method: Method::GET,
    };
    let route2 = HealthRoute {
        path: "/health".to_string(),
        method: Method::GET,
    };

    bindings.register_health_route("http_in", route1);
    bindings.register_health_route("http_in", route2);

    assert_eq!(
        bindings.health_routes.len(),
        1,
        "duplicate routes should not be added"
    );
}

#[test]
fn health_route_custom_method_and_path() -> TestResult {
    let cfg = HttpServerHealthConfig {
        method: Some("HEAD".to_string()),
        path: Some("/ready".to_string()),
        extra: BTreeMap::new(),
    };

    let route = HealthRoute::from_config("http_in", &cfg)?;
    assert_eq!(route.method, Method::HEAD);
    assert_eq!(route.path, "/ready");
    Ok(())
}
