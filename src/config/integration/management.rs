use super::connectors::default_http_host;
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct ManagementConfig {
    pub host: String,
    pub port: u16,
    pub live: Option<ManagementEndpointConfig>,
    pub ready: Option<ManagementEndpointConfig>,
    pub status: Option<ManagementEndpointConfig>,
    pub metrics: Option<ManagementEndpointConfig>,
}

#[derive(Debug, Clone)]
pub struct ManagementEndpointConfig {
    pub path: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawManagementSection {
    #[serde(default)]
    pub(crate) host: Option<String>,
    #[serde(default)]
    pub(crate) port: Option<u16>,
    #[serde(default)]
    pub(crate) live: Option<RawManagementEndpoint>,
    #[serde(default)]
    pub(crate) ready: Option<RawManagementEndpoint>,
    #[serde(default)]
    pub(crate) status: Option<RawManagementEndpoint>,
    #[serde(default)]
    pub(crate) metrics: Option<RawManagementEndpoint>,
    #[serde(default)]
    pub(crate) health: Option<RawManagementEndpoint>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawManagementEndpoint {
    #[serde(default)]
    pub(crate) path: Option<String>,
}

pub(crate) fn resolve_management(
    section: RawManagementSection,
    errors: &mut Vec<String>,
) -> Option<ManagementConfig> {
    let port = match section.port {
        Some(port) => port,
        None => {
            errors.push("management section requires `port` when present".to_string());
            return None;
        }
    };

    let host = section
        .host
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(default_http_host);

    let live_source = section.live.or(section.health);
    let live = live_source.map(|endpoint| ManagementEndpointConfig {
        path: normalise_endpoint_path(endpoint.path, "/live"),
    });

    let ready = section.ready.map(|endpoint| ManagementEndpointConfig {
        path: normalise_endpoint_path(endpoint.path, "/ready"),
    });

    let status = section.status.map(|endpoint| ManagementEndpointConfig {
        path: normalise_endpoint_path(endpoint.path, "/status"),
    });

    let metrics = section.metrics.map(|endpoint| ManagementEndpointConfig {
        path: normalise_endpoint_path(endpoint.path, "/metrics"),
    });

    if live.is_none() && ready.is_none() && status.is_none() && metrics.is_none() {
        tracing::debug!(
            "management section provided without endpoints; management server disabled"
        );
        return None;
    }

    Some(ManagementConfig {
        host,
        port,
        live,
        ready,
        status,
        metrics,
    })
}

fn normalise_endpoint_path(path: Option<String>, default: &str) -> String {
    let mut resolved = path
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| default.to_string());

    if !resolved.starts_with('/') {
        resolved.insert(0, '/');
    }

    resolved
}
