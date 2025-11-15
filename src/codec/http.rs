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
use std::collections::BTreeMap;
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
}

pub fn encode_http_response(
    response: &HttpResponse,
) -> Result<EncodedHttpResponse, serde_json::Error> {
    let body = encode_body(&response.body)?;
    Ok(EncodedHttpResponse {
        status: response.status,
        content_type: response.content_type.clone(),
        body,
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
