//! Health route configuration for HTTP servers.
//!
//! This module provides types and functions for configuring health probe endpoints
//! on HTTP server connectors. Health routes are used for liveness and readiness checks.
//!
//! # Visibility
//!
//! Types in this module are `pub` for integration testing but are not part of
//! the stable public API. External consumers should use the `IntegrationConfig`
//! API rather than constructing these types directly.

use crate::codec::http::{normalise_path, RouteTemplate};
use crate::error::Result;
use crate::integration::registry::HttpServerHealthConfig;
use axum::http::Method;

/// A configured health route for an HTTP server.
///
/// Used internally for HTTP server configuration. Exposed for integration testing.
#[derive(Clone, Debug)]
pub struct HealthRoute {
    /// The path at which the health endpoint is served.
    pub path: String,
    /// The HTTP method for the health endpoint.
    pub method: Method,
}

impl HealthRoute {
    /// Build a health route from connector configuration.
    ///
    /// Uses defaults of `GET /health` if method or path are not specified.
    pub fn from_config(connector: &str, config: &HttpServerHealthConfig) -> Result<Self> {
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

        Ok(Self { path, method })
    }
}

/// Server bindings aggregate routes for an HTTP server instance.
///
/// Used internally by `HttpTriggerRuntime`. Exposed for integration testing.
#[derive(Debug)]
pub struct ServerBindings {
    /// Chronicle-triggered routes.
    pub routes: Vec<HttpRoute>,
    /// Health probe routes.
    pub health_routes: Vec<HealthRoute>,
}

impl ServerBindings {
    /// Create an empty set of server bindings.
    pub fn new() -> Self {
        Self {
            routes: Vec::new(),
            health_routes: Vec::new(),
        }
    }

    /// Register a health route, checking for conflicts with existing routes.
    ///
    /// The route is silently skipped if:
    /// - It conflicts with an existing exact-match chronicle route
    /// - An identical health route already exists
    pub fn register_health_route(&mut self, connector: &str, route: HealthRoute) {
        // Check if any route template would conflict with the health route path
        let conflicts = self
            .routes
            .iter()
            .any(|r| r.template.is_exact() && r.template.match_path(&route.path).is_some());

        if conflicts {
            tracing::warn!(
                connector = connector,
                path = route.path.as_str(),
                "skipping http_server health probe because path conflicts with an existing route"
            );
            return;
        }

        if self
            .health_routes
            .iter()
            .any(|existing| existing.path == route.path && existing.method == route.method)
        {
            return;
        }

        self.health_routes.push(route);
    }
}

impl Default for ServerBindings {
    fn default() -> Self {
        Self::new()
    }
}

/// An HTTP route bound to a chronicle.
///
/// Used internally by `HttpTriggerRuntime`. Exposed for integration testing.
#[derive(Clone, Debug)]
pub struct HttpRoute {
    /// The name of the chronicle this route triggers.
    pub chronicle: String,
    /// The HTTP method for this route.
    pub method: Method,
    /// Whether the route expects JSON payloads.
    pub expects_json: bool,
    /// Optional retry budget for this route.
    pub retry_budget: Option<crate::config::integration::RetryBudget>,
    /// The route template for path matching.
    pub template: RouteTemplate,
}
