#![forbid(unsafe_code)]

use crate::chronicle::engine::HttpResponse;
use crate::codec::payload::EncodedPayload;
use crate::error::{ChronicleError, Context, Result};
use crate::readiness::{
    ApplicationState as ReadinessApplicationState, EndpointState, ReadinessSnapshot,
    RouteState as ReadinessRouteState,
};
#[cfg(feature = "http-in")]
use axum::http::HeaderMap as AxumHeaderMap;
use base64::engine::general_purpose::STANDARD as BASE64_ENGINE;
use base64::Engine;
use bytes::Bytes;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, CONTENT_TYPE};
use reqwest::{Method, RequestBuilder, Url};
use serde_json::{json, Map as JsonMap, Value as JsonValue};
use std::collections::{BTreeMap, HashMap};
use url::form_urlencoded;

pub fn resolve_url(base: &str, path: &str) -> Result<Url> {
    if path.starts_with("http://") || path.starts_with("https://") {
        return Url::parse(path).map_err(|err| crate::err!("invalid request url `{path}`: {err}"));
    }

    let base_url =
        Url::parse(base).with_context(|| format!("invalid connector base url `{base}`"))?;

    if path.is_empty() {
        Ok(base_url)
    } else {
        base_url
            .join(path)
            .map_err(|err| crate::err!("failed to resolve path `{path}` against `{base}`: {err}"))
    }
}

pub enum HttpBody<'a> {
    Empty,
    Json(&'a JsonValue),
    Bytes {
        data: &'a [u8],
        content_type: Option<&'a str>,
    },
}

pub fn build_request(
    client: &reqwest::Client,
    method: Method,
    url: Url,
    headers: &[(String, String)],
    body: HttpBody<'_>,
    content_type: Option<&str>,
) -> Result<RequestBuilder> {
    let mut request = client.request(method, url);

    for (name, value) in headers {
        let header_name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|err| crate::err!("invalid header name `{name}`: {err}"))?;
        let header_value = HeaderValue::from_str(value)
            .map_err(|err| crate::err!("invalid header value for `{name}`: {err}"))?;
        request = request.header(header_name, header_value);
    }

    let apply_content_type =
        |builder: RequestBuilder, ct: &str| -> Result<RequestBuilder, ChronicleError> {
            let header_value = HeaderValue::from_str(ct)
                .map_err(|err| crate::err!("invalid content-type `{ct}`: {err}"))?;
            Ok(builder.header(CONTENT_TYPE, header_value))
        };

    match body {
        HttpBody::Empty => {}
        HttpBody::Json(body_value) => {
            if !body_value.is_null() {
                if let Some(ct) = content_type {
                    request = apply_content_type(request, ct)?;
                } else {
                    request = apply_content_type(request, "application/json")?;
                }

                request = match body_value {
                    JsonValue::String(text) => request.body(text.clone()),
                    JsonValue::Null => request,
                    other => request.json(other),
                };
            }
        }
        HttpBody::Bytes {
            data,
            content_type: body_ct,
        } => {
            if let Some(ct) = body_ct.or(content_type) {
                request = apply_content_type(request, ct)?;
            }
            if !data.is_empty() {
                request = request.body(Bytes::copy_from_slice(data));
            }
        }
    }

    Ok(request)
}
pub struct ConnectorRequest<'a> {
    pub method: &'a str,
    pub base_url: &'a str,
    pub path: &'a str,
    pub default_headers: &'a [(String, String)],
    pub headers: &'a [(String, String)],
    pub body: Option<&'a JsonValue>,
    pub content_type: Option<&'a str>,
}

pub fn build_connector_request(
    client: &reqwest::Client,
    request: ConnectorRequest<'_>,
) -> Result<RequestBuilder> {
    let method = Method::from_bytes(request.method.as_bytes())
        .map_err(|err| crate::err!("invalid HTTP method `{}`: {err}", request.method))?;
    let url = resolve_url(request.base_url, request.path)?;

    let mut header_pairs = request.default_headers.to_vec();
    header_pairs.extend_from_slice(request.headers);

    let encoded_payload = request.body.and_then(EncodedPayload::from_json);
    let http_body = if let Some(payload) = encoded_payload.as_ref() {
        HttpBody::Bytes {
            data: payload.data(),
            content_type: payload.content_type(),
        }
    } else if let Some(value) = request.body {
        if value.is_null() {
            HttpBody::Empty
        } else {
            HttpBody::Json(value)
        }
    } else {
        HttpBody::Empty
    };

    build_request(
        client,
        method,
        url,
        &header_pairs,
        http_body,
        request.content_type,
    )
}

pub fn flatten_response_headers(headers: &HeaderMap) -> JsonMap<String, JsonValue> {
    let mut map = JsonMap::new();
    for (name, value) in headers.iter() {
        let key = name.as_str().to_string();
        let entry = map
            .entry(key)
            .or_insert_with(|| JsonValue::Array(Vec::new()));
        if let JsonValue::Array(values) = entry {
            let string_value = value
                .to_str()
                .map(|s| JsonValue::String(s.to_string()))
                .unwrap_or_else(|_| JsonValue::String(BASE64_ENGINE.encode(value.as_bytes())));
            values.push(string_value);
        }
    }
    map
}

pub fn decode_body(bytes: &[u8]) -> Option<JsonValue> {
    if bytes.is_empty() {
        return Some(JsonValue::Null);
    }

    if let Ok(json) = serde_json::from_slice::<JsonValue>(bytes) {
        return Some(json);
    }

    match std::str::from_utf8(bytes) {
        Ok(text) => Some(JsonValue::String(text.to_string())),
        Err(_) => Some(JsonValue::String(BASE64_ENGINE.encode(bytes))),
    }
}

#[cfg(feature = "http-in")]
pub fn normalise_headers(headers: &AxumHeaderMap) -> Vec<(String, String)> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            let value = value.to_str().ok()?;
            Some((name.as_str().to_string(), value.to_string()))
        })
        .collect()
}

pub fn headers_to_json(headers: &[(String, String)]) -> JsonMap<String, JsonValue> {
    let mut map = JsonMap::new();
    for (key, value) in headers {
        let normalised = key.to_ascii_lowercase().replace('-', "_");
        merge_json_value(&mut map, &normalised, JsonValue::String(value.to_string()));
    }
    map
}

pub fn query_to_json(query: Option<&str>) -> JsonMap<String, JsonValue> {
    let mut map = JsonMap::new();
    if let Some(q) = query {
        for (key, value) in form_urlencoded::parse(q.as_bytes()) {
            merge_json_value(
                &mut map,
                key.as_ref(),
                JsonValue::String(value.into_owned()),
            );
        }
    }
    map
}

pub fn path_to_json(path: &str) -> JsonMap<String, JsonValue> {
    let mut map = JsonMap::new();
    map.insert("value".to_string(), JsonValue::String(normalise_path(path)));
    map
}

pub fn normalise_path(path: &str) -> String {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        "/".to_string()
    } else if trimmed.starts_with('/') {
        trimmed.to_string()
    } else {
        format!("/{trimmed}")
    }
}

pub fn binary_body_to_json(body: &[u8]) -> JsonValue {
    if body.is_empty() {
        return JsonValue::Null;
    }

    let mut map = JsonMap::new();
    map.insert(
        "base64".to_string(),
        JsonValue::String(BASE64_ENGINE.encode(body)),
    );

    if let Ok(text) = std::str::from_utf8(body) {
        map.insert("text".to_string(), JsonValue::String(text.to_string()));
        if let Ok(json_value) = serde_json::from_str::<JsonValue>(text) {
            map.insert("json".to_string(), json_value);
        }
    }

    JsonValue::Object(map)
}

pub fn map_headers(headers: &BTreeMap<String, String>) -> Vec<(String, String)> {
    headers
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

pub struct EncodedHttpResponse {
    pub status: u16,
    pub content_type: Option<String>,
    pub body: Bytes,
    pub headers: std::collections::BTreeMap<String, String>,
}

pub fn encode_http_response(
    response: &HttpResponse,
) -> Result<EncodedHttpResponse, serde_json::Error> {
    // Priority: body_b64 > body_raw_json > body
    let body = if let Some(b64) = &response.body_b64 {
        // Decode base64 to raw bytes
        BASE64_ENGINE
            .decode(b64)
            .map(Bytes::from)
            .unwrap_or_else(|_| Bytes::new())
    } else if let Some(raw_json) = &response.body_raw_json {
        // Send raw JSON string as-is (not double-encoded)
        Bytes::from(raw_json.clone())
    } else {
        encode_body(&response.body)?
    };

    Ok(EncodedHttpResponse {
        status: response.status,
        content_type: response.content_type.clone(),
        body,
        headers: response.headers.clone(),
    })
}

pub fn encode_body(body: &JsonValue) -> Result<Bytes, serde_json::Error> {
    match body {
        JsonValue::Null => Ok(Bytes::new()),
        JsonValue::String(text) => Ok(Bytes::from(text.clone())),
        other => serde_json::to_vec(other).map(Bytes::from),
    }
}

pub fn readiness_routes_payload(snapshot: &ReadinessSnapshot) -> Vec<JsonValue> {
    snapshot
        .routes
        .iter()
        .map(|route| {
            json!({
                "id": route.name,
                "state": route_state_label(route.state),
                "requires": route.dependencies,
                "unhealthy": route.unhealthy,
            })
        })
        .collect()
}

pub fn readiness_endpoints_payload(snapshot: &ReadinessSnapshot) -> Vec<JsonValue> {
    snapshot
        .endpoints
        .iter()
        .map(|endpoint| {
            json!({
                "id": endpoint.name,
                "type": endpoint.kind.as_str(),
                "state": endpoint_state_label(endpoint.state),
            })
        })
        .collect()
}

pub fn application_state_label(state: ReadinessApplicationState) -> &'static str {
    match state {
        ReadinessApplicationState::WarmingUp => "WARMING_UP",
        ReadinessApplicationState::Ready => "READY",
        ReadinessApplicationState::NotReady => "NOT_READY",
        ReadinessApplicationState::Degraded => "DEGRADED",
        ReadinessApplicationState::Draining => "DRAINING",
    }
}

pub fn route_state_label(state: ReadinessRouteState) -> &'static str {
    match state {
        ReadinessRouteState::WarmingUp => "WARMING_UP",
        ReadinessRouteState::Ready => "READY",
        ReadinessRouteState::NotReady => "NOT_READY",
        ReadinessRouteState::Degraded => "DEGRADED",
        ReadinessRouteState::Draining => "DRAINING",
    }
}

pub fn endpoint_state_label(state: EndpointState) -> &'static str {
    match state {
        EndpointState::WarmingUp => "WARMING_UP",
        EndpointState::Healthy => "HEALTHY",
        EndpointState::Unhealthy => "UNHEALTHY",
        EndpointState::CircuitOpen => "CB_OPEN",
        EndpointState::CircuitHalfOpen => "CB_HALF_OPEN",
    }
}

fn merge_json_value(map: &mut JsonMap<String, JsonValue>, key: &str, value: JsonValue) {
    match map.get_mut(key) {
        Some(JsonValue::Array(existing)) => existing.push(value),
        Some(existing) => {
            let current = existing.clone();
            *existing = JsonValue::Array(vec![current, value]);
        }
        None => {
            map.insert(key.to_string(), value);
        }
    }
}

// --- Route Template Support ---

/// A compiled route template that can match paths and extract parameters.
#[derive(Debug, Clone)]
pub struct RouteTemplate {
    segments: Vec<TemplateSegment>,
    original: String,
}

#[derive(Debug, Clone)]
enum TemplateSegment {
    /// A literal path segment (e.g., "v2" in "/v2/...")
    Literal(String),
    /// A parameter segment (e.g., "{namespace}" captures into "namespace")
    Param(String),
}

impl RouteTemplate {
    /// Parse a route template string like "/v2/{namespace}/{name}/manifests/{reference}".
    pub fn parse(template: &str) -> std::result::Result<Self, RouteParseError> {
        let normalized = normalise_path(template);
        let mut segments = Vec::new();

        for part in normalized.split('/') {
            if part.is_empty() {
                continue;
            }

            if part.starts_with('{') && part.ends_with('}') {
                let param_name = &part[1..part.len() - 1];
                if param_name.is_empty() {
                    return Err(RouteParseError::EmptyParam {
                        template: template.to_string(),
                    });
                }
                if !is_valid_param_name(param_name) {
                    return Err(RouteParseError::InvalidParamName {
                        template: template.to_string(),
                        param: param_name.to_string(),
                    });
                }
                segments.push(TemplateSegment::Param(param_name.to_string()));
            } else if part.contains('{') || part.contains('}') {
                return Err(RouteParseError::MalformedSegment {
                    template: template.to_string(),
                    segment: part.to_string(),
                });
            } else {
                segments.push(TemplateSegment::Literal(part.to_string()));
            }
        }

        Ok(RouteTemplate {
            segments,
            original: template.to_string(),
        })
    }

    /// Try to match a request path against this template.
    /// Returns the extracted path parameters if the match succeeds.
    pub fn match_path(&self, path: &str) -> Option<HashMap<String, String>> {
        let normalized = normalise_path(path);
        let parts: Vec<&str> = normalized.split('/').filter(|s| !s.is_empty()).collect();

        if parts.len() != self.segments.len() {
            return None;
        }

        let mut params = HashMap::new();

        for (segment, part) in self.segments.iter().zip(parts.iter()) {
            match segment {
                TemplateSegment::Literal(expected) => {
                    if *part != expected {
                        return None;
                    }
                }
                TemplateSegment::Param(name) => {
                    // URL-decode the parameter value
                    let decoded = percent_decode_path(part);
                    params.insert(name.clone(), decoded);
                }
            }
        }

        Some(params)
    }

    /// Check if this template represents an exact path (no parameters).
    pub fn is_exact(&self) -> bool {
        self.segments
            .iter()
            .all(|s| matches!(s, TemplateSegment::Literal(_)))
    }

    /// Get the original template string.
    pub fn original(&self) -> &str {
        &self.original
    }

    /// Convert to an axum-compatible route pattern.
    /// e.g., "/v2/{namespace}/{name}/manifests/{reference}" -> "/v2/:namespace/:name/manifests/:reference"
    pub fn to_axum_pattern(&self) -> String {
        let mut pattern = String::new();
        for segment in &self.segments {
            pattern.push('/');
            match segment {
                TemplateSegment::Literal(lit) => pattern.push_str(lit),
                TemplateSegment::Param(name) => {
                    pattern.push(':');
                    pattern.push_str(name);
                }
            }
        }
        if pattern.is_empty() {
            "/".to_string()
        } else {
            pattern
        }
    }
}

fn is_valid_param_name(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }
    let first = name.chars().next().unwrap();
    if !first.is_ascii_alphabetic() && first != '_' {
        return false;
    }
    name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn percent_decode_path(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '%' {
            let mut hex = String::with_capacity(2);
            if let Some(&h1) = chars.peek() {
                if h1.is_ascii_hexdigit() {
                    hex.push(chars.next().unwrap());
                    if let Some(&h2) = chars.peek() {
                        if h2.is_ascii_hexdigit() {
                            hex.push(chars.next().unwrap());
                        }
                    }
                }
            }
            if hex.len() == 2 {
                if let Ok(byte) = u8::from_str_radix(&hex, 16) {
                    if byte.is_ascii() {
                        output.push(byte as char);
                        continue;
                    }
                }
            }
            // Failed to decode, keep original
            output.push('%');
            output.push_str(&hex);
        } else if ch == '+' {
            output.push(' ');
        } else {
            output.push(ch);
        }
    }

    output
}

#[derive(Debug, Clone)]
pub enum RouteParseError {
    EmptyParam { template: String },
    InvalidParamName { template: String, param: String },
    MalformedSegment { template: String, segment: String },
}

impl std::fmt::Display for RouteParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RouteParseError::EmptyParam { template } => {
                write!(f, "empty parameter `{{}}` in route template `{}`", template)
            }
            RouteParseError::InvalidParamName { template, param } => {
                write!(
                    f,
                    "invalid parameter name `{}` in route template `{}`",
                    param, template
                )
            }
            RouteParseError::MalformedSegment { template, segment } => {
                write!(
                    f,
                    "malformed segment `{}` in route template `{}`",
                    segment, template
                )
            }
        }
    }
}

impl std::error::Error for RouteParseError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_template() {
        let tmpl = RouteTemplate::parse("/v2/").unwrap();
        assert_eq!(tmpl.segments.len(), 1);
        assert!(tmpl.is_exact());
    }

    #[test]
    fn parse_template_with_params() {
        let tmpl = RouteTemplate::parse("/v2/{namespace}/{name}/manifests/{reference}").unwrap();
        assert_eq!(tmpl.segments.len(), 5);
        assert!(!tmpl.is_exact());
    }

    #[test]
    fn match_exact_path() {
        let tmpl = RouteTemplate::parse("/v2/").unwrap();
        let params = tmpl.match_path("/v2/").unwrap();
        assert!(params.is_empty());
    }

    #[test]
    fn match_path_with_params() {
        let tmpl = RouteTemplate::parse("/v2/{namespace}/{name}/manifests/{reference}").unwrap();
        let params = tmpl
            .match_path("/v2/library/alpine/manifests/latest")
            .unwrap();
        assert_eq!(params.get("namespace").unwrap(), "library");
        assert_eq!(params.get("name").unwrap(), "alpine");
        assert_eq!(params.get("reference").unwrap(), "latest");
    }

    #[test]
    fn match_path_mismatch() {
        let tmpl = RouteTemplate::parse("/v2/{namespace}/{name}/manifests/{reference}").unwrap();
        assert!(tmpl
            .match_path("/v2/library/alpine/blobs/sha256:abc")
            .is_none());
    }

    #[test]
    fn match_path_url_encoded() {
        let tmpl = RouteTemplate::parse("/v2/{namespace}/{name}/manifests/{reference}").unwrap();
        let params = tmpl
            .match_path("/v2/my%2Fnamespace/alpine/manifests/v1.0")
            .unwrap();
        assert_eq!(params.get("namespace").unwrap(), "my/namespace");
    }

    #[test]
    fn to_axum_pattern() {
        let tmpl = RouteTemplate::parse("/v2/{namespace}/{name}/manifests/{reference}").unwrap();
        assert_eq!(
            tmpl.to_axum_pattern(),
            "/v2/:namespace/:name/manifests/:reference"
        );
    }
}
