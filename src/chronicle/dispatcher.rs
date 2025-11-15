use crate::chronicle::engine::ChronicleAction;
use crate::chronicle_event;
#[cfg(feature = "http-out")]
use crate::codec::http::{
    build_connector_request, map_headers as map_http_headers, ConnectorRequest,
};
#[cfg(any(feature = "http-out", feature = "mqtt"))]
use crate::codec::payload::EncodedPayload;
#[cfg(feature = "smtp")]
use crate::config::integration::SmtpTlsMode;
use crate::config::integration::{DeliveryPolicy, JitterMode, RetryBudget};
#[cfg(any(feature = "http-out", feature = "grpc"))]
use crate::error::Context;
use crate::error::{ChronicleError, Error as ErrorKind, Result};
#[cfg(feature = "smtp")]
use crate::integration::factory::SmtpHandle;
use crate::integration::factory::{ConnectorFactoryError, ConnectorFactoryRegistry};
use crate::metrics::metrics;
use crate::readiness::{DependencyHealth, ReadinessController};
use crate::retry::jitter_between;
#[cfg(all(not(feature = "grpc"), any(feature = "http-out", feature = "mqtt")))]
use bytes::Bytes;
#[cfg(feature = "grpc")]
use bytes::{Buf, BufMut, Bytes};
#[cfg(feature = "grpc")]
use http::uri::PathAndQuery;
#[cfg(feature = "http-out")]
use humantime::parse_duration;
#[cfg(feature = "smtp")]
use lettre::message::Mailbox;
#[cfg(feature = "smtp")]
use lettre::transport::smtp::authentication::Credentials as SmtpCredentials;
#[cfg(feature = "smtp")]
use lettre::AsyncSmtpTransport;
#[cfg(feature = "smtp")]
use lettre::AsyncTransport;
#[cfg(feature = "smtp")]
use lettre::Tokio1Executor;
#[cfg(feature = "grpc")]
use prost_reflect::{prost::Message as ProstMessage, DescriptorPool, DynamicMessage};
#[cfg(feature = "grpc")]
use serde_json::Deserializer as JsonDeserializer;
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, HashMap};
use std::io::ErrorKind as IoErrorKind;
#[cfg(feature = "grpc")]
use std::path::{Path, PathBuf};
#[cfg(feature = "grpc")]
use std::result::Result as StdResult;
#[cfg(feature = "grpc")]
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use thiserror::Error;
#[cfg(feature = "grpc")]
use tokio::fs;
use tokio::time::sleep;
#[cfg(any(feature = "http-out", feature = "db-postgres", feature = "db-mariadb"))]
use tokio::time::timeout;
#[cfg(feature = "grpc")]
use tonic::client::Grpc;
#[cfg(feature = "grpc")]
use tonic::codec::{Codec, DecodeBuf, Decoder as TonicDecoder, EncodeBuf, Encoder as TonicEncoder};
#[cfg(feature = "grpc")]
use tonic::metadata::{AsciiMetadataKey, MetadataValue};
#[cfg(feature = "grpc")]
use tonic::Request;
#[cfg(feature = "grpc")]
use tonic::Status;

#[cfg(feature = "kafka")]
use rdkafka::producer::FutureRecord;

#[cfg(feature = "mqtt")]
use crate::transport::mqtt;
#[cfg(feature = "rabbitmq")]
use crate::transport::rabbitmq;

/// Executes chronicle actions in-order using cached connector handles.
#[derive(Clone)]
pub struct ActionDispatcher {
    factory: Arc<ConnectorFactoryRegistry>,
    readiness: Option<ReadinessController>,
    dependency_health: Option<DependencyHealth>,
    connector_health: Arc<Mutex<HashMap<String, ConnectorConnectionState>>>,
    #[cfg(feature = "grpc")]
    grpc_descriptors: Arc<GrpcDescriptorCache>,
}

struct ConnectorConnectionState {
    disconnected: bool,
}

impl ConnectorConnectionState {
    fn new() -> Self {
        Self {
            disconnected: false,
        }
    }
}

#[derive(Clone, Copy, Default)]
pub struct DeliveryContext<'a> {
    pub policy: Option<&'a DeliveryPolicy>,
    pub fallback: Option<&'a BTreeMap<String, String>>,
    pub fallback_actions: Option<&'a BTreeMap<String, Vec<ChronicleAction>>>,
    pub allow_partial_delivery: bool,
    pub retry_budget: Option<&'a RetryBudget>,
}

#[cfg(feature = "grpc")]
#[derive(Clone, Default)]
struct BytesCodec;

#[cfg(feature = "grpc")]
#[derive(Clone, Default)]
struct BytesEncoder;

#[cfg(feature = "grpc")]
#[derive(Clone, Default)]
struct BytesDecoder;

#[cfg(feature = "grpc")]
impl TonicEncoder for BytesEncoder {
    type Item = Bytes;
    type Error = Status;

    fn encode(&mut self, item: Self::Item, dst: &mut EncodeBuf<'_>) -> StdResult<(), Self::Error> {
        dst.put(item);
        Ok(())
    }
}

#[cfg(feature = "grpc")]
impl TonicDecoder for BytesDecoder {
    type Item = Bytes;
    type Error = tonic::Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> StdResult<Option<Self::Item>, Self::Error> {
        if src.remaining() == 0 {
            return Ok(None);
        }
        let bytes = src.copy_to_bytes(src.remaining());
        Ok(Some(bytes))
    }
}

#[cfg(feature = "grpc")]
impl Codec for BytesCodec {
    type Encode = Bytes;
    type Decode = Bytes;
    type Encoder = BytesEncoder;
    type Decoder = BytesDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        BytesEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        BytesDecoder
    }
}

#[cfg(feature = "http-out")]
fn extract_timeout_ms(extra: &JsonValue) -> Result<Option<u64>> {
    let map = match extra {
        JsonValue::Object(map) => map,
        _ => return Ok(None),
    };

    if let Some(value) = map.get("timeout_ms").or_else(|| map.get("timeoutMs")) {
        return parse_timeout_value("timeout_ms", value);
    }

    if let Some(value) = map.get("timeout") {
        return parse_timeout_value("timeout", value);
    }

    Ok(None)
}

#[cfg(feature = "http-out")]
fn parse_timeout_value(label: &str, value: &JsonValue) -> Result<Option<u64>> {
    match value {
        JsonValue::Null => Ok(None),
        JsonValue::Number(num) => num
            .as_u64()
            .ok_or_else(|| crate::err!("{label} must be a positive integer"))
            .map(Some),
        JsonValue::String(text) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }

            if let Ok(ms) = trimmed.parse::<u64>() {
                return Ok(Some(ms));
            }

            let duration = parse_duration(trimmed)
                .with_context(|| format!("failed to parse `{label}` value `{trimmed}`"))?;
            let millis = duration.as_millis();
            if millis > u64::MAX as u128 {
                Err(crate::err!(
                    "{label} `{trimmed}` exceeds maximum supported duration"
                ))
            } else {
                Ok(Some(millis as u64))
            }
        }
        _ => Err(crate::err!(
            "{label} must be specified as integer milliseconds or a duration string"
        )),
    }
}

#[cfg(feature = "grpc")]
fn insert_metadata(
    map: &mut tonic::metadata::MetadataMap,
    entries: &[(String, String)],
) -> Result<()> {
    for (key, value) in entries {
        let metadata_key = key
            .parse::<AsciiMetadataKey>()
            .map_err(|err| crate::err!("invalid metadata key `{key}`: {err}"))?;
        let metadata_value = MetadataValue::<tonic::metadata::Ascii>::from_str(value)
            .map_err(|err| crate::err!("invalid metadata value for `{key}`: {err}"))?;
        map.insert(metadata_key, metadata_value);
    }
    Ok(())
}

#[cfg(feature = "grpc")]
#[derive(Default)]
struct GrpcDescriptorCache {
    pools: Mutex<HashMap<PathBuf, Arc<DescriptorPool>>>,
}

#[cfg(feature = "grpc")]
impl GrpcDescriptorCache {
    async fn get_or_load(&self, path: &Path) -> Result<Arc<DescriptorPool>> {
        if let Some(pool) = self
            .pools
            .lock()
            .expect("descriptor cache lock")
            .get(path)
            .cloned()
        {
            return Ok(pool);
        }

        let bytes = fs::read(path)
            .await
            .with_context(|| format!("failed to read descriptor set `{}`", path.display()))?;
        let pool = DescriptorPool::decode(bytes.as_slice()).map_err(|err| {
            crate::err!(
                "failed to decode descriptor set `{}`: {err}",
                path.display()
            )
        })?;
        let pool = Arc::new(pool);

        self.pools
            .lock()
            .expect("descriptor cache lock")
            .insert(path.to_path_buf(), pool.clone());

        Ok(pool)
    }
}

#[cfg(feature = "db-mariadb")]
fn quote_mysql_identifier(identifier: &str) -> Result<String> {
    if identifier.is_empty() {
        return Err(crate::err!("identifier cannot be empty"));
    }

    let mut quoted = String::new();
    for (index, part) in identifier.split('.').enumerate() {
        if part.is_empty() {
            return Err(crate::err!("invalid identifier `{identifier}`"));
        }
        if !part
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '$')
        {
            return Err(crate::err!(
                "identifier `{identifier}` contains unsupported characters"
            ));
        }
        if index > 0 {
            quoted.push('.');
        }
        quoted.push('`');
        for ch in part.chars() {
            if ch == '`' {
                quoted.push('`');
            }
            quoted.push(ch);
        }
        quoted.push('`');
    }
    Ok(quoted)
}

#[cfg(feature = "smtp")]
fn parse_mailboxes(value: Option<&JsonValue>) -> Result<Vec<Mailbox>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };

    match value {
        JsonValue::Null => Ok(Vec::new()),
        JsonValue::String(text) => text
            .split([',', ';'])
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(parse_mailbox)
            .collect(),
        JsonValue::Array(items) => {
            let mut mailboxes = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    JsonValue::Null => {}
                    JsonValue::String(text) => mailboxes.push(parse_mailbox(text.trim())?),
                    other => {
                        return Err(crate::err!(
                            "smtp recipient entries must be strings, found {other:?}"
                        ))
                    }
                }
            }
            Ok(mailboxes)
        }
        other => Err(crate::err!(
            "smtp recipients must be strings or arrays, found {other:?}"
        )),
    }
}

#[cfg(feature = "smtp")]
fn parse_mailbox(value: &str) -> Result<Mailbox> {
    value
        .trim()
        .parse::<Mailbox>()
        .map_err(|err| crate::err!("invalid email address `{value}`: {err}"))
}

#[cfg(feature = "smtp")]
fn extract_email_body(value: Option<&JsonValue>) -> Result<String> {
    match value {
        None | Some(JsonValue::Null) => Ok(String::new()),
        Some(JsonValue::String(text)) => Ok(text.to_string()),
        Some(other) => serde_json::to_string(other)
            .map_err(|err| crate::err!("failed to serialise email body: {err}")),
    }
}

#[cfg(feature = "smtp")]
async fn build_smtp_transport(
    handle: &SmtpHandle,
    timeout_secs: Option<u64>,
) -> Result<AsyncSmtpTransport<Tokio1Executor>> {
    let mode = handle
        .tls()
        .map(|tls| tls.mode().clone())
        .unwrap_or(SmtpTlsMode::None);

    let builder = match mode {
        SmtpTlsMode::Starttls => {
            AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(handle.host())
                .map_err(|err| crate::err!("failed to configure STARTTLS transport: {err}"))?
        }
        SmtpTlsMode::Tls => AsyncSmtpTransport::<Tokio1Executor>::relay(handle.host())
            .map_err(|err| crate::err!("failed to configure TLS transport: {err}"))?,
        SmtpTlsMode::None => AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(handle.host()),
    };

    let mut builder = builder.port(handle.port());

    if let Some(timeout) = timeout_secs.or(handle.timeout_secs()) {
        builder = builder.timeout(Some(Duration::from_secs(timeout)));
    }

    if let Some(auth) = handle.auth() {
        let credentials =
            SmtpCredentials::new(auth.username().to_string(), auth.password().to_string());
        builder = builder.credentials(credentials);
    }

    Ok(builder.build())
}

#[cfg(feature = "http-out")]
struct HttpDispatchRequest<'a> {
    connector: &'a str,
    route: &'a str,
    method: &'a str,
    path: &'a str,
    headers: &'a BTreeMap<String, String>,
    body: &'a JsonValue,
    content_type: Option<&'a str>,
    timeout_ms: Option<u64>,
    extra: &'a JsonValue,
}

#[cfg(feature = "mqtt")]
struct MqttPublishRequest<'a> {
    connector: &'a str,
    url: &'a str,
    topic: &'a str,
    qos: u8,
    retain: bool,
    payload_encoding: &'a str,
    encoded_payload: &'a str,
    packet_id: Option<u64>,
    published_at: &'a str,
    timeout_ms: Option<u64>,
    trace_id: Option<&'a str>,
    record_id: Option<&'a str>,
}

#[cfg_attr(not(feature = "rabbitmq"), allow(dead_code))]
struct RabbitmqPublishRequest<'a> {
    connector: &'a str,
    url: &'a str,
    exchange: Option<&'a str>,
    routing_key: Option<&'a str>,
    payload: &'a JsonValue,
    headers: &'a JsonValue,
    properties: &'a JsonValue,
    mandatory: Option<bool>,
    confirm: Option<bool>,
    timeout_ms: Option<u64>,
    trace_id: Option<&'a str>,
    record_id: Option<&'a str>,
}

#[cfg(feature = "grpc")]
struct GrpcDispatchRequest<'a> {
    connector: &'a str,
    service: &'a str,
    method: &'a str,
    descriptor_path: &'a Path,
    payload: &'a JsonValue,
    metadata: &'a [(String, String)],
    timeout_ms: Option<u64>,
}

impl ActionDispatcher {
    pub fn new(
        factory: Arc<ConnectorFactoryRegistry>,
        readiness: Option<ReadinessController>,
        dependency_health: Option<DependencyHealth>,
    ) -> Self {
        Self {
            factory,
            readiness,
            dependency_health,
            connector_health: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(feature = "grpc")]
            grpc_descriptors: Arc::new(GrpcDescriptorCache::default()),
        }
    }

    fn record_connector_failure(&self, connector: &str, kind: &'static str, err: &ActionError) {
        let mut health = self
            .connector_health
            .lock()
            .expect("connector health lock poisoned");
        let entry = health
            .entry(connector.to_string())
            .or_insert_with(ConnectorConnectionState::new);
        if !entry.disconnected {
            entry.disconnected = true;
            tracing::warn!(
                target: "chronicle::dispatcher",
                event = "transport_disconnected",
                connector = connector,
                kind,
                error = %err
            );
        }
    }

    fn record_connector_success(&self, connector: &str, kind: &'static str) {
        let mut health = self
            .connector_health
            .lock()
            .expect("connector health lock poisoned");
        let entry = health
            .entry(connector.to_string())
            .or_insert_with(ConnectorConnectionState::new);
        if entry.disconnected {
            entry.disconnected = false;
            tracing::info!(
                target: "chronicle::dispatcher",
                event = "transport_reconnected",
                connector = connector,
                kind
            );
        }
    }

    #[cfg(feature = "http-out")]
    async fn dispatch_http_request(
        &self,
        request: HttpDispatchRequest<'_>,
    ) -> Result<(), ActionError> {
        let connector_name = request.connector.to_string();
        let handle =
            self.factory
                .http_client(request.connector)
                .map_err(|err| ActionError::Connector {
                    connector: connector_name.clone(),
                    source: err,
                })?;

        let header_pairs = map_http_headers(request.headers);
        let http_request = build_connector_request(
            handle.client(),
            ConnectorRequest {
                method: request.method,
                base_url: handle.base_url(),
                path: request.path,
                default_headers: handle.default_headers(),
                headers: &header_pairs,
                body: Some(request.body),
                content_type: request.content_type,
            },
        )
        .map_err(|err| ActionError::Http {
            connector: connector_name.clone(),
            source: err,
        })?;

        let timeout_ms = if let Some(ms) = request.timeout_ms {
            Some(ms)
        } else {
            extract_timeout_ms(request.extra).map_err(|err| ActionError::Http {
                connector: connector_name.clone(),
                source: err,
            })?
        };

        let started = Instant::now();
        let send_future = http_request.send();
        let response = if let Some(ms) = timeout_ms {
            match timeout(Duration::from_millis(ms), send_future).await {
                Ok(result) => result,
                Err(_) => {
                    metrics().record_http_request(request.route, 0, started.elapsed());
                    return Err(ActionError::Http {
                        connector: connector_name.clone(),
                        source: crate::err!("request timed out after {ms}ms"),
                    });
                }
            }
        } else {
            send_future.await
        }
        .map_err(|err| {
            metrics().record_http_request(request.route, 0, started.elapsed());
            ActionError::Http {
                connector: connector_name.clone(),
                source: ChronicleError::new(err),
            }
        })?;

        let status = response.status().as_u16();
        let response = response.error_for_status().map_err(|err| {
            metrics().record_http_request(request.route, status, started.elapsed());
            ActionError::Http {
                connector: connector_name.clone(),
                source: ChronicleError::new(err),
            }
        })?;

        response
            .bytes()
            .await
            .map(|_| {
                metrics().record_http_request(request.route, status, started.elapsed());
            })
            .map_err(|err| {
                metrics().record_http_request(request.route, status, started.elapsed());
                ActionError::Http {
                    connector: connector_name,
                    source: ChronicleError::new(err),
                }
            })
    }

    #[cfg(feature = "grpc")]
    #[cfg(feature = "grpc")]
    async fn dispatch_grpc_request(
        &self,
        request: GrpcDispatchRequest<'_>,
    ) -> Result<(), ActionError> {
        let connector_name = request.connector.to_string();
        let service_name = request.service.to_string();
        let method_name = request.method.to_string();

        let handle =
            self.factory
                .grpc_client(request.connector)
                .map_err(|err| ActionError::Connector {
                    connector: connector_name.clone(),
                    source: err,
                })?;

        let descriptor_pool = self
            .grpc_descriptors
            .get_or_load(request.descriptor_path)
            .await
            .map_err(|err| ActionError::Grpc {
                connector: connector_name.clone(),
                service: service_name.clone(),
                method: method_name.clone(),
                source: err,
            })?;

        let service_descriptor = descriptor_pool
            .get_service_by_name(request.service)
            .ok_or_else(|| ActionError::Grpc {
                connector: connector_name.clone(),
                service: service_name.clone(),
                method: method_name.clone(),
                source: crate::err!(
                    "service `{}` not present in descriptor set",
                    request.service
                ),
            })?;

        let method_descriptor = service_descriptor
            .methods()
            .find(|m| m.name() == request.method)
            .ok_or_else(|| ActionError::Grpc {
                connector: connector_name.clone(),
                service: service_name.clone(),
                method: method_name.clone(),
                source: crate::err!(
                    "method `{}` not present on service `{}`",
                    request.method,
                    request.service
                ),
            })?;

        let input_descriptor = method_descriptor.input();
        let request_json =
            serde_json::to_string(request.payload).map_err(|err| ActionError::Grpc {
                connector: connector_name.clone(),
                service: service_name.clone(),
                method: method_name.clone(),
                source: ChronicleError::new(err),
            })?;

        let mut deserializer = JsonDeserializer::from_str(&request_json);
        let dynamic_request = DynamicMessage::deserialize(input_descriptor, &mut deserializer)
            .map_err(|err| ActionError::Grpc {
                connector: connector_name.clone(),
                service: service_name.clone(),
                method: method_name.clone(),
                source: crate::err!("failed to serialise gRPC request body: {err}"),
            })?;
        deserializer.end().map_err(|err| ActionError::Grpc {
            connector: connector_name.clone(),
            service: service_name.clone(),
            method: method_name.clone(),
            source: crate::err!("unexpected trailing data in gRPC request JSON: {err}"),
        })?;
        let payload = Bytes::from(dynamic_request.encode_to_vec());
        let timeout_ms = request.timeout_ms;

        let path = format!("/{}/{}", request.service, request.method);
        let path = path
            .parse::<PathAndQuery>()
            .map_err(|err| ActionError::Grpc {
                connector: connector_name.clone(),
                service: service_name.clone(),
                method: method_name.clone(),
                source: ChronicleError::new(err),
            })?;

        let mut client = Grpc::new(handle.channel());
        let mut grpc_request = Request::new(payload);

        if let Some(ms) = timeout_ms {
            grpc_request.set_timeout(Duration::from_millis(ms));
        }

        insert_metadata(grpc_request.metadata_mut(), handle.default_metadata()).map_err(|err| {
            ActionError::Grpc {
                connector: connector_name.clone(),
                service: service_name.clone(),
                method: method_name.clone(),
                source: err,
            }
        })?;
        insert_metadata(grpc_request.metadata_mut(), request.metadata).map_err(|err| {
            ActionError::Grpc {
                connector: connector_name.clone(),
                service: service_name.clone(),
                method: method_name.clone(),
                source: err,
            }
        })?;

        client
            .unary(grpc_request, path, BytesCodec)
            .await
            .map(|_| ())
            .map_err(|status: Status| ActionError::Grpc {
                connector: connector_name,
                service: service_name,
                method: method_name,
                source: crate::err!(status),
            })
    }

    #[cfg(feature = "db-postgres")]
    async fn dispatch_postgres_query(
        &self,
        connector: &str,
        sql: &str,
        _parameters: &JsonValue,
        timeout_ms: Option<u64>,
    ) -> Result<(), ActionError> {
        let connector_name = connector.to_string();
        let handle =
            self.factory
                .postgres_pool(connector)
                .map_err(|err| ActionError::Connector {
                    connector: connector_name.clone(),
                    source: err,
                })?;

        let pool = handle.pool().clone();
        let execute = sqlx::query(sql).execute(&pool);

        if let Some(ms) = timeout_ms {
            timeout(Duration::from_millis(ms), execute)
                .await
                .map_err(|_| ActionError::Postgres {
                    connector: connector_name.clone(),
                    source: crate::err!("query timed out after {ms}ms"),
                })?
                .map(|_| ())
                .map_err(|err| ActionError::Postgres {
                    connector: connector_name,
                    source: ChronicleError::new(err),
                })
        } else {
            execute
                .await
                .map(|_| ())
                .map_err(|err| ActionError::Postgres {
                    connector: connector_name,
                    source: ChronicleError::new(err),
                })
        }
    }

    #[cfg(feature = "db-mariadb")]
    async fn dispatch_mariadb_insert(
        &self,
        connector: &str,
        key: &JsonValue,
        values: &JsonValue,
        timeout_ms: Option<u64>,
    ) -> Result<(), ActionError> {
        let connector_name = connector.to_string();
        let handle =
            self.factory
                .mariadb_pool(connector)
                .map_err(|err| ActionError::Connector {
                    connector: connector_name.clone(),
                    source: err,
                })?;

        let extra = handle.extra();

        let table_name = extra
            .get("table")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| ActionError::Mariadb {
                connector: connector_name.clone(),
                source: crate::err!(
                    "mariadb insert requires `table` entry in connector extra configuration"
                ),
            })?;

        let key_column = extra
            .get("key_column")
            .and_then(JsonValue::as_str)
            .unwrap_or("record_key");
        let value_column = extra
            .get("value_column")
            .and_then(JsonValue::as_str)
            .unwrap_or("payload");

        let table_ident =
            quote_mysql_identifier(table_name).map_err(|err| ActionError::Mariadb {
                connector: connector_name.clone(),
                source: err,
            })?;
        let key_ident = quote_mysql_identifier(key_column).map_err(|err| ActionError::Mariadb {
            connector: connector_name.clone(),
            source: err,
        })?;
        let value_ident =
            quote_mysql_identifier(value_column).map_err(|err| ActionError::Mariadb {
                connector: connector_name.clone(),
                source: err,
            })?;

        let sql = format!(
            "INSERT INTO {table} ({key}, {value}) VALUES (?, ?) \
             ON DUPLICATE KEY UPDATE {value} = VALUES({value})",
            table = table_ident,
            key = key_ident,
            value = value_ident
        );

        let key_repr = match key {
            JsonValue::String(text) => text.clone(),
            other => serde_json::to_string(other).map_err(|err| ActionError::Mariadb {
                connector: connector_name.clone(),
                source: ChronicleError::new(err),
            })?,
        };
        let values_repr = serde_json::to_string(values).map_err(|err| ActionError::Mariadb {
            connector: connector_name.clone(),
            source: ChronicleError::new(err),
        })?;

        let pool = handle.pool().clone();
        let execute = sqlx::query(&sql)
            .bind(key_repr)
            .bind(values_repr)
            .execute(&pool);

        if let Some(ms) = timeout_ms {
            timeout(Duration::from_millis(ms), execute)
                .await
                .map_err(|_| ActionError::Mariadb {
                    connector: connector_name.clone(),
                    source: crate::err!("query timed out after {ms}ms"),
                })?
                .map(|_| ())
                .map_err(|err| ActionError::Mariadb {
                    connector: connector_name,
                    source: ChronicleError::new(err),
                })
        } else {
            execute
                .await
                .map(|_| ())
                .map_err(|err| ActionError::Mariadb {
                    connector: connector_name,
                    source: ChronicleError::new(err),
                })
        }
    }

    #[cfg(feature = "smtp")]
    async fn dispatch_smtp_email(
        &self,
        connector: &str,
        message: &JsonValue,
        timeout_secs: Option<u64>,
    ) -> Result<(), ActionError> {
        let connector_name = connector.to_string();
        let handle = self
            .factory
            .smtp_mailer(connector)
            .map_err(|err| ActionError::Connector {
                connector: connector_name.clone(),
                source: err,
            })?;

        let message_map = message.as_object().ok_or_else(|| ActionError::Smtp {
            connector: connector_name.clone(),
            source: crate::err!("smtp message payload must be an object"),
        })?;

        let from_addr = message_map
            .get("from")
            .and_then(JsonValue::as_str)
            .or_else(|| handle.default_from());

        let from = from_addr.ok_or_else(|| ActionError::Smtp {
            connector: connector_name.clone(),
            source: crate::err!("smtp message requires `from` address"),
        })?;

        let to = parse_mailboxes(message_map.get("to")).map_err(|err| ActionError::Smtp {
            connector: connector_name.clone(),
            source: err,
        })?;
        let cc = parse_mailboxes(message_map.get("cc")).map_err(|err| ActionError::Smtp {
            connector: connector_name.clone(),
            source: err,
        })?;
        let bcc = parse_mailboxes(message_map.get("bcc")).map_err(|err| ActionError::Smtp {
            connector: connector_name.clone(),
            source: err,
        })?;

        if to.is_empty() && cc.is_empty() && bcc.is_empty() {
            return Err(ActionError::Smtp {
                connector: connector_name.clone(),
                source: crate::err!("smtp message requires at least one recipient"),
            });
        }

        let subject = message_map
            .get("subject")
            .and_then(JsonValue::as_str)
            .unwrap_or_default();

        let reply_to = message_map
            .get("reply_to")
            .and_then(JsonValue::as_str)
            .map(|value| value.to_string());

        let body_text =
            extract_email_body(message_map.get("body")).map_err(|err| ActionError::Smtp {
                connector: connector_name.clone(),
                source: err,
            })?;

        let mut builder = lettre::Message::builder()
            .from(parse_mailbox(from).map_err(|err| ActionError::Smtp {
                connector: connector_name.clone(),
                source: err,
            })?)
            .subject(subject);

        for mailbox in to {
            builder = builder.to(mailbox);
        }
        for mailbox in cc {
            builder = builder.cc(mailbox);
        }
        for mailbox in bcc {
            builder = builder.bcc(mailbox);
        }

        if let Some(reply) = reply_to {
            builder = builder.reply_to(parse_mailbox(&reply).map_err(|err| ActionError::Smtp {
                connector: connector_name.clone(),
                source: err,
            })?);
        }

        let email = builder.body(body_text).map_err(|err| ActionError::Smtp {
            connector: connector_name.clone(),
            source: ChronicleError::new(err),
        })?;

        let transport = build_smtp_transport(handle.as_ref(), timeout_secs)
            .await
            .map_err(|err| ActionError::Smtp {
                connector: connector_name.clone(),
                source: err,
            })?;

        transport
            .send(email)
            .await
            .map(|_| ())
            .map_err(|err| ActionError::Smtp {
                connector: connector_name,
                source: ChronicleError::new(err),
            })
    }

    pub async fn dispatch(
        &self,
        route: &str,
        actions: &[ChronicleAction],
        context: DeliveryContext<'_>,
    ) -> Result<(), ActionDispatchError> {
        self.dispatch_with_fallback(route, actions, context).await
    }

    async fn dispatch_with_fallback(
        &self,
        route: &str,
        actions: &[ChronicleAction],
        context: DeliveryContext<'_>,
    ) -> Result<(), ActionDispatchError> {
        for (index, action) in actions.iter().enumerate() {
            let kind = action_kind(action);
            let connector_name = action_connector(action).map(|name| name.to_string());
            let mut attempt: u32 = 0;
            let retry_plan = DeliveryRetryPlan::new(context.policy, context.retry_budget);
            let attempt_window = Instant::now();

            loop {
                match self.dispatch_action(route, action).await {
                    Ok(()) => {
                        record_dependency_success(self, connector_name.as_deref()).await;
                        break;
                    }
                    Err(source) => {
                        record_dependency_failure(self, connector_name.as_deref()).await;

                        if let Some(delay) =
                            retry_plan.next_delay(attempt, attempt_window.elapsed())
                        {
                            attempt += 1;
                            if !delay.is_zero() {
                                sleep(delay).await;
                            }
                            continue;
                        }

                        let mut handled_by_fallback = false;
                        if let Some(connector) = connector_name.as_deref() {
                            if let Some(target) =
                                context.fallback.and_then(|map| map.get(connector))
                            {
                                if let Some(extra_actions) = context
                                    .fallback_actions
                                    .and_then(|actions| actions.get(target))
                                {
                                    tracing::warn!(
                                        connector,
                                        fallback = %target,
                                        route,
                                        kind,
                                        index,
                                        "dispatching fallback actions after primary failure"
                                    );
                                    let fallback_context = DeliveryContext {
                                        policy: context.policy,
                                        fallback: None,
                                        fallback_actions: None,
                                        allow_partial_delivery: false,
                                        retry_budget: context.retry_budget,
                                    };
                                    match self
                                        .dispatch_without_fallback(
                                            route,
                                            extra_actions,
                                            fallback_context,
                                        )
                                        .await
                                    {
                                        Ok(()) => handled_by_fallback = true,
                                        Err(err) => {
                                            tracing::error!(
                                                connector,
                                                fallback = %target,
                                                route,
                                                error = %err,
                                                "fallback dispatch failed"
                                            );
                                            return Err(err);
                                        }
                                    }
                                } else {
                                    tracing::warn!(
                                        connector,
                                        fallback = %target,
                                        route,
                                        "fallback actions unavailable; propagating failure"
                                    );
                                }
                            }
                        }

                        if handled_by_fallback {
                            break;
                        }

                        if !record_retry_failure(self, route).await {
                            return Err(ActionDispatchError::RetryBudgetExhausted {
                                route: route.to_string(),
                                index,
                                kind,
                            });
                        }

                        return Err(ActionDispatchError::ActionFailed {
                            index,
                            kind,
                            source: Box::new(source),
                        });
                    }
                }
            }
        }

        Ok(())
    }

    async fn dispatch_without_fallback(
        &self,
        route: &str,
        actions: &[ChronicleAction],
        context: DeliveryContext<'_>,
    ) -> Result<(), ActionDispatchError> {
        for (index, action) in actions.iter().enumerate() {
            let kind = action_kind(action);
            let connector_name = action_connector(action).map(|name| name.to_string());
            let mut attempt: u32 = 0;
            let retry_plan = DeliveryRetryPlan::new(context.policy, context.retry_budget);
            let attempt_window = Instant::now();

            loop {
                match self.dispatch_action(route, action).await {
                    Ok(()) => {
                        record_dependency_success(self, connector_name.as_deref()).await;
                        break;
                    }
                    Err(source) => {
                        record_dependency_failure(self, connector_name.as_deref()).await;

                        if let Some(delay) =
                            retry_plan.next_delay(attempt, attempt_window.elapsed())
                        {
                            attempt += 1;
                            if !delay.is_zero() {
                                sleep(delay).await;
                            }
                            continue;
                        }

                        if !record_retry_failure(self, route).await {
                            return Err(ActionDispatchError::RetryBudgetExhausted {
                                route: route.to_string(),
                                index,
                                kind,
                            });
                        }

                        return Err(ActionDispatchError::ActionFailed {
                            index,
                            kind,
                            source: Box::new(source),
                        });
                    }
                }
            }
        }

        Ok(())
    }

    async fn dispatch_action(
        &self,
        route: &str,
        action: &ChronicleAction,
    ) -> Result<(), ActionError> {
        let kind = action_kind(action);
        let connector_label = action_connector(action).map(|name| name.to_string());
        let trace_label = action_trace_id(action).map(|value| value.to_string());
        let record_label = action_record_id(action).map(|value| value.to_string());

        let result = match action {
            ChronicleAction::KafkaPublish {
                connector,
                topic,
                payload,
                ..
            } => self.dispatch_kafka_publish(connector, topic, payload).await,
            ChronicleAction::MqttPublish {
                connector,
                url,
                topic,
                qos,
                retain,
                payload_encoding,
                encoded_payload,
                packet_id,
                published_at,
                timeout_ms,
                trace_id,
                record_id,
                ..
            } => {
                #[cfg(feature = "mqtt")]
                {
                    self.dispatch_mqtt_publish(MqttPublishRequest {
                        connector,
                        url,
                        topic,
                        qos: *qos,
                        retain: *retain,
                        payload_encoding,
                        encoded_payload,
                        packet_id: *packet_id,
                        published_at,
                        timeout_ms: *timeout_ms,
                        trace_id: trace_id.as_deref(),
                        record_id: record_id.as_deref(),
                    })
                    .await
                }
                #[cfg(not(feature = "mqtt"))]
                {
                    let _ = (
                        connector,
                        url,
                        topic,
                        qos,
                        retain,
                        payload_encoding,
                        encoded_payload,
                        packet_id,
                        published_at,
                        timeout_ms,
                        trace_id,
                        record_id,
                    );
                    Err(ActionError::FeatureDisabled {
                        action: "mqtt_publish",
                        feature: "mqtt",
                    })
                }
            }
            #[cfg(feature = "http-out")]
            ChronicleAction::HttpRequest {
                connector,
                method,
                path,
                headers,
                body,
                content_type,
                timeout_ms,
                extra,
                ..
            } => {
                self.dispatch_http_request(HttpDispatchRequest {
                    connector,
                    route,
                    method,
                    path,
                    headers,
                    body,
                    content_type: content_type.as_deref(),
                    timeout_ms: *timeout_ms,
                    extra,
                })
                .await
            }
            #[cfg(not(feature = "http-out"))]
            ChronicleAction::HttpRequest { .. } => Err(ActionError::FeatureDisabled {
                action: "http_request",
                feature: "http-out",
            }),
            #[cfg(feature = "grpc")]
            ChronicleAction::GrpcRequest {
                connector,
                service,
                method,
                descriptor_path,
                request,
                metadata,
                timeout_ms,
                ..
            } => {
                self.dispatch_grpc_request(GrpcDispatchRequest {
                    connector,
                    service,
                    method,
                    descriptor_path: descriptor_path.as_path(),
                    payload: request,
                    metadata,
                    timeout_ms: *timeout_ms,
                })
                .await
            }
            #[cfg(not(feature = "grpc"))]
            ChronicleAction::GrpcRequest { .. } => Err(ActionError::FeatureDisabled {
                action: "grpc_request",
                feature: "grpc",
            }),
            ChronicleAction::RabbitmqPublish {
                connector,
                url,
                exchange,
                routing_key,
                payload,
                headers,
                properties,
                mandatory,
                confirm,
                timeout_ms,
                trace_id,
                record_id,
                ..
            } => {
                self.dispatch_rabbitmq_publish(RabbitmqPublishRequest {
                    connector,
                    url,
                    exchange: exchange.as_deref(),
                    routing_key: routing_key.as_deref(),
                    payload,
                    headers,
                    properties,
                    mandatory: *mandatory,
                    confirm: *confirm,
                    timeout_ms: *timeout_ms,
                    trace_id: trace_id.as_deref(),
                    record_id: record_id.as_deref(),
                })
                .await
            }
            #[cfg(feature = "smtp")]
            ChronicleAction::SmtpEmail {
                connector,
                message,
                timeout_secs,
                ..
            } => {
                self.dispatch_smtp_email(connector, message, *timeout_secs)
                    .await
            }
            #[cfg(not(feature = "smtp"))]
            ChronicleAction::SmtpEmail { .. } => Err(ActionError::FeatureDisabled {
                action: "smtp_email",
                feature: "smtp",
            }),
            #[cfg(feature = "db-mariadb")]
            ChronicleAction::MariadbInsert {
                connector,
                key,
                values,
                timeout_ms,
                ..
            } => {
                self.dispatch_mariadb_insert(connector, key, values, *timeout_ms)
                    .await
            }
            #[cfg(not(feature = "db-mariadb"))]
            ChronicleAction::MariadbInsert { .. } => Err(ActionError::FeatureDisabled {
                action: "mariadb_insert",
                feature: "db-mariadb",
            }),
            #[cfg(feature = "db-postgres")]
            ChronicleAction::PostgresQuery {
                connector,
                sql,
                parameters,
                timeout_ms,
                ..
            } => {
                self.dispatch_postgres_query(connector, sql, parameters, *timeout_ms)
                    .await
            }
            #[cfg(not(feature = "db-postgres"))]
            ChronicleAction::PostgresQuery { .. } => Err(ActionError::FeatureDisabled {
                action: "postgres_query",
                feature: "db-postgres",
            }),
            ChronicleAction::MongodbCommand { .. } => Err(ActionError::FeatureDisabled {
                action: "mongodb_command",
                feature: "mongodb",
            }),
            ChronicleAction::RedisCommand { .. } => Err(ActionError::FeatureDisabled {
                action: "redis_command",
                feature: "redis",
            }),
        };

        match result {
            Ok(()) => {
                let counters = metrics();
                if let Some(connector) = connector_label.as_deref() {
                    self.record_connector_success(connector, kind);
                    counters.record_connector_success(kind, connector);
                }
                let connector_field = connector_label.as_deref().unwrap_or("unknown");
                chronicle_event!(
                    info,
                    "chronicle::dispatcher",
                    "action_success",
                    connector = connector_field,
                    chronicle = route,
                    kind = kind,
                    trace_id = trace_label.as_deref().unwrap_or(""),
                    record_id = record_label.as_deref().unwrap_or("")
                );
                Ok(())
            }
            Err(err) => {
                let counters = metrics();
                let reason = connector_failure_reason(&err);
                if let Some(connector) = connector_label.as_deref() {
                    self.record_connector_failure(connector, kind, &err);
                    counters.record_connector_failure(
                        kind,
                        connector,
                        reason.map(|value| value.as_str()),
                    );
                }
                let connector_field = connector_label.as_deref().unwrap_or("unknown");
                if let Some(reason) = reason {
                    chronicle_event!(
                        error,
                        "chronicle::dispatcher",
                        "action_failed",
                        connector = connector_field,
                        chronicle = route,
                        kind = kind,
                        trace_id = trace_label.as_deref().unwrap_or(""),
                        record_id = record_label.as_deref().unwrap_or(""),
                        error_code = "CONNECTOR_FAILURE",
                        reason = reason.as_str(),
                        error = err
                    );
                } else {
                    chronicle_event!(
                        error,
                        "chronicle::dispatcher",
                        "action_failed",
                        connector = connector_field,
                        chronicle = route,
                        kind = kind,
                        trace_id = trace_label.as_deref().unwrap_or(""),
                        record_id = record_label.as_deref().unwrap_or(""),
                        error = err
                    );
                }
                Err(err)
            }
        }
    }

    #[cfg(feature = "kafka")]
    async fn dispatch_kafka_publish(
        &self,
        connector: &str,
        topic: &str,
        payload: &JsonValue,
    ) -> Result<(), ActionError> {
        let connector_name = connector.to_string();
        let topic_name = topic.to_string();

        let handle =
            self.factory
                .kafka_producer(connector)
                .map_err(|err| ActionError::Connector {
                    connector: connector_name.clone(),
                    source: err,
                })?;

        let request_timeout = handle
            .timeouts()
            .request()
            .unwrap_or(Duration::from_secs(5));

        let payload_bytes = payload_bytes(payload).map_err(|err| ActionError::Kafka {
            connector: connector_name.clone(),
            topic: topic_name.clone(),
            source: err,
        })?;

        let publish = match payload_bytes {
            Some(bytes) => {
                handle
                    .producer()
                    .send(
                        FutureRecord::<(), [u8]>::to(&topic_name).payload(bytes.as_slice()),
                        request_timeout,
                    )
                    .await
            }
            None => {
                handle
                    .producer()
                    .send(FutureRecord::<(), [u8]>::to(&topic_name), request_timeout)
                    .await
            }
        };

        publish.map_err(|(err, _)| ActionError::Kafka {
            connector: connector_name,
            topic: topic_name,
            source: crate::err!(err),
        })?;

        Ok(())
    }

    #[cfg(not(feature = "kafka"))]
    async fn dispatch_kafka_publish(
        &self,
        connector: &str,
        topic: &str,
        payload: &JsonValue,
    ) -> Result<(), ActionError> {
        let _ = (connector, topic, payload);
        Err(ActionError::FeatureDisabled {
            action: "kafka_publish",
            feature: "kafka",
        })
    }

    #[cfg(feature = "mqtt")]
    async fn dispatch_mqtt_publish(
        &self,
        request: MqttPublishRequest<'_>,
    ) -> Result<(), ActionError> {
        let MqttPublishRequest {
            connector,
            url,
            topic,
            qos,
            retain,
            payload_encoding,
            encoded_payload,
            packet_id,
            published_at,
            timeout_ms,
            trace_id,
            record_id,
        } = request;

        let counters = metrics();
        let result = mqtt::dispatch_mqtt_publish(
            self.factory.as_ref(),
            mqtt::MqttPublishParams {
                connector,
                topic,
                qos,
                retain,
                payload_encoding,
                encoded_payload,
                timeout_ms,
            },
        )
        .await;

        match result {
            Ok(()) => {
                counters.inc_mqtt_publish_success();
                tracing::info!(
                    target: "chronicle::dispatcher",
                    event = "mqtt_publish_success",
                    connector,
                    url,
                    topic,
                    qos,
                    retain,
                    packet_id = packet_id.unwrap_or(0),
                    published_at,
                    trace_id,
                    record_id
                );
                Ok(())
            }
            Err(err) => {
                counters.inc_mqtt_publish_failure();
                tracing::error!(
                    target: "chronicle::dispatcher",
                    event = "mqtt_publish_failed",
                    connector,
                    url,
                    topic,
                    qos,
                    retain,
                    packet_id = packet_id.unwrap_or(0),
                    published_at,
                    trace_id,
                    record_id,
                    error = %err
                );
                Err(ActionError::Mqtt {
                    connector: connector.to_string(),
                    source: err,
                })
            }
        }
    }

    async fn dispatch_rabbitmq_publish(
        &self,
        request: RabbitmqPublishRequest<'_>,
    ) -> Result<(), ActionError> {
        #[cfg(feature = "rabbitmq")]
        {
            let RabbitmqPublishRequest {
                connector,
                url,
                exchange,
                routing_key,
                payload,
                headers,
                properties,
                mandatory,
                confirm,
                timeout_ms,
                trace_id,
                record_id,
            } = request;

            let counters = metrics();
            let result = rabbitmq::dispatch_rabbitmq_publish(
                self.factory.as_ref(),
                rabbitmq::RabbitmqPublishParams {
                    connector,
                    exchange,
                    routing_key,
                    payload,
                    headers,
                    properties,
                    mandatory,
                    confirm,
                    timeout_ms,
                },
            )
            .await;

            match result {
                Ok(()) => {
                    counters.inc_rabbitmq_publish_success();
                    tracing::info!(
                        target: "chronicle::dispatcher",
                        event = "rabbitmq_publish_success",
                        connector,
                        url,
                        exchange,
                        routing_key,
                        mandatory = mandatory.unwrap_or(false),
                        confirm = confirm.unwrap_or(true),
                        timeout_ms,
                        trace_id,
                        record_id
                    );
                    Ok(())
                }
                Err(err) => {
                    counters.inc_rabbitmq_publish_failure();
                    tracing::error!(
                        target: "chronicle::dispatcher",
                        event = "rabbitmq_publish_failed",
                        connector,
                        url,
                        exchange,
                        routing_key,
                        mandatory = mandatory.unwrap_or(false),
                        confirm = confirm.unwrap_or(true),
                        timeout_ms,
                        trace_id,
                        record_id,
                        error = %err
                    );
                    Err(ActionError::Rabbitmq {
                        connector: connector.to_string(),
                        source: err,
                    })
                }
            }
        }

        #[cfg(not(feature = "rabbitmq"))]
        {
            let _ = (self.factory.as_ref(), request);
            Err(ActionError::FeatureDisabled {
                action: "rabbitmq_publish",
                feature: "rabbitmq",
            })
        }
    }
}

#[derive(Debug, Error)]
pub enum ActionDispatchError {
    #[error("action {index} ({kind}) failed: {source}")]
    ActionFailed {
        index: usize,
        kind: &'static str,
        #[source]
        source: Box<ActionError>,
    },
    #[error("route `{route}` exhausted retry budget while dispatching action {index} ({kind})")]
    RetryBudgetExhausted {
        route: String,
        index: usize,
        kind: &'static str,
    },
}

#[derive(Debug, Error)]
pub enum ActionError {
    #[error("connector `{connector}` unavailable: {source}")]
    Connector {
        connector: String,
        #[source]
        source: ConnectorFactoryError,
    },
    #[error("kafka publish failed on `{connector}` topic `{topic}`: {source}")]
    Kafka {
        connector: String,
        topic: String,
        #[source]
        source: ChronicleError,
    },
    #[cfg(feature = "rabbitmq")]
    #[error("rabbitmq publish failed on `{connector}`: {source}")]
    Rabbitmq {
        connector: String,
        #[source]
        source: ChronicleError,
    },
    #[cfg(feature = "mqtt")]
    #[error("mqtt publish failed on `{connector}`: {source}")]
    Mqtt {
        connector: String,
        #[source]
        source: ChronicleError,
    },
    #[error("http request failed on `{connector}`: {source}")]
    Http {
        connector: String,
        #[source]
        source: ChronicleError,
    },
    #[error(
        "grpc request failed on `{connector}` service `{service}` method `{method}`: {source}"
    )]
    Grpc {
        connector: String,
        service: String,
        method: String,
        #[source]
        source: ChronicleError,
    },
    #[error("smtp send failed on `{connector}`: {source}")]
    Smtp {
        connector: String,
        #[source]
        source: ChronicleError,
    },
    #[error("mariadb insert failed on `{connector}`: {source}")]
    Mariadb {
        connector: String,
        #[source]
        source: ChronicleError,
    },
    #[error("postgres query failed on `{connector}`: {source}")]
    Postgres {
        connector: String,
        #[source]
        source: ChronicleError,
    },
    #[error("action `{action}` requires feature `{feature}`")]
    FeatureDisabled {
        action: &'static str,
        feature: &'static str,
    },
    #[error("unsupported action `{kind}`")]
    Unsupported { kind: &'static str },
}

fn action_kind(action: &ChronicleAction) -> &'static str {
    match action {
        ChronicleAction::KafkaPublish { .. } => "kafka_publish",
        ChronicleAction::HttpRequest { .. } => "http_request",
        ChronicleAction::GrpcRequest { .. } => "grpc_request",
        ChronicleAction::MqttPublish { .. } => "mqtt_publish",
        ChronicleAction::RabbitmqPublish { .. } => "rabbitmq_publish",
        ChronicleAction::SmtpEmail { .. } => "smtp_email",
        ChronicleAction::MariadbInsert { .. } => "mariadb_insert",
        ChronicleAction::PostgresQuery { .. } => "postgres_query",
        ChronicleAction::MongodbCommand { .. } => "mongodb_command",
        ChronicleAction::RedisCommand { .. } => "redis_command",
    }
}

fn action_connector(action: &ChronicleAction) -> Option<&str> {
    match action {
        ChronicleAction::KafkaPublish { connector, .. }
        | ChronicleAction::HttpRequest { connector, .. }
        | ChronicleAction::GrpcRequest { connector, .. }
        | ChronicleAction::MqttPublish { connector, .. }
        | ChronicleAction::RabbitmqPublish { connector, .. }
        | ChronicleAction::SmtpEmail { connector, .. }
        | ChronicleAction::MariadbInsert { connector, .. }
        | ChronicleAction::PostgresQuery { connector, .. }
        | ChronicleAction::MongodbCommand { connector, .. }
        | ChronicleAction::RedisCommand { connector, .. } => Some(connector.as_str()),
    }
}

fn action_trace_id(action: &ChronicleAction) -> Option<&str> {
    match action {
        ChronicleAction::KafkaPublish { trace_id, .. }
        | ChronicleAction::HttpRequest { trace_id, .. }
        | ChronicleAction::GrpcRequest { trace_id, .. }
        | ChronicleAction::MqttPublish { trace_id, .. }
        | ChronicleAction::RabbitmqPublish { trace_id, .. }
        | ChronicleAction::SmtpEmail { trace_id, .. }
        | ChronicleAction::MariadbInsert { trace_id, .. }
        | ChronicleAction::PostgresQuery { trace_id, .. }
        | ChronicleAction::MongodbCommand { trace_id, .. }
        | ChronicleAction::RedisCommand { trace_id, .. } => trace_id.as_deref(),
    }
}

fn action_record_id(action: &ChronicleAction) -> Option<&str> {
    match action {
        ChronicleAction::KafkaPublish { record_id, .. }
        | ChronicleAction::HttpRequest { record_id, .. }
        | ChronicleAction::GrpcRequest { record_id, .. }
        | ChronicleAction::MqttPublish { record_id, .. }
        | ChronicleAction::RabbitmqPublish { record_id, .. }
        | ChronicleAction::SmtpEmail { record_id, .. }
        | ChronicleAction::MariadbInsert { record_id, .. }
        | ChronicleAction::PostgresQuery { record_id, .. }
        | ChronicleAction::MongodbCommand { record_id, .. }
        | ChronicleAction::RedisCommand { record_id, .. } => record_id.as_deref(),
    }
}

async fn record_dependency_outcome(
    dispatcher: &ActionDispatcher,
    connector: Option<&str>,
    success: bool,
) {
    if let (Some(health), Some(name)) = (dispatcher.dependency_health.as_ref(), connector) {
        health.report_outcome(name, success).await;
    }
}

async fn record_dependency_success(dispatcher: &ActionDispatcher, connector: Option<&str>) {
    record_dependency_outcome(dispatcher, connector, true).await;
}

async fn record_dependency_failure(dispatcher: &ActionDispatcher, connector: Option<&str>) {
    record_dependency_outcome(dispatcher, connector, false).await;
}

async fn record_retry_failure(dispatcher: &ActionDispatcher, route: &str) -> bool {
    let Some(controller) = dispatcher.readiness.as_ref() else {
        return true;
    };

    match controller.record_retry_failure(route).await {
        Ok(allowed) => allowed,
        Err(err) => {
            tracing::warn!(
                route = route,
                error = ?err,
                "failed to record retry failure for route"
            );
            true
        }
    }
}

struct DeliveryRetryPlan {
    max_retries: u32,
    max_elapsed: Option<Duration>,
    base_backoff: Duration,
    max_backoff: Duration,
    jitter: JitterMode,
}

impl DeliveryRetryPlan {
    fn new(policy: Option<&DeliveryPolicy>, budget: Option<&RetryBudget>) -> Self {
        let policy_attempts = policy
            .and_then(|p| p.retries)
            .unwrap_or(u32::MAX)
            .saturating_add(1);
        let budget_attempts = budget
            .and_then(|b| b.max_attempts)
            .unwrap_or(u32::MAX)
            .max(1);
        let max_attempts = policy_attempts.min(budget_attempts).max(1);
        let mut base_backoff = policy
            .and_then(|p| p.backoff.as_ref().map(|b| b.min))
            .or_else(|| budget.and_then(|b| b.base_backoff))
            .unwrap_or(Duration::from_millis(0));
        if let Some(envelope) = budget.and_then(|b| b.base_backoff) {
            if base_backoff < envelope {
                base_backoff = envelope;
            }
        }
        let mut max_backoff = policy
            .and_then(|p| p.backoff.as_ref().map(|b| b.max))
            .or_else(|| budget.and_then(|b| b.max_backoff))
            .unwrap_or(base_backoff);
        if let Some(envelope) = budget.and_then(|b| b.max_backoff) {
            if max_backoff > envelope {
                max_backoff = envelope;
            }
        }
        if max_backoff < base_backoff {
            max_backoff = base_backoff;
        }

        Self {
            max_retries: max_attempts.saturating_sub(1),
            max_elapsed: budget.and_then(|b| b.max_elapsed),
            base_backoff,
            max_backoff,
            jitter: budget.and_then(|b| b.jitter).unwrap_or(JitterMode::None),
        }
    }

    fn next_delay(&self, attempt: u32, elapsed: Duration) -> Option<Duration> {
        if attempt >= self.max_retries {
            return None;
        }

        let mut delay = self.backoff_for(attempt + 1);
        delay = match self.jitter {
            JitterMode::None => delay,
            JitterMode::Equal => jitter_between(delay.mul_f64(0.5), delay),
            JitterMode::Full => jitter_between(Duration::from_secs(0), delay),
        };

        if let Some(limit) = self.max_elapsed {
            if elapsed >= limit {
                return None;
            }
            let remaining = limit - elapsed;
            if remaining < delay {
                return None;
            }
        }

        Some(delay)
    }

    fn backoff_for(&self, attempt_index: u32) -> Duration {
        if self.base_backoff.is_zero() {
            return Duration::from_secs(0);
        }

        let exponent = attempt_index.saturating_sub(1).min(16);
        let factor = 1u32 << exponent;
        let mut delay = self.base_backoff.mul_f64(factor as f64);
        if delay > self.max_backoff {
            delay = self.max_backoff;
        }
        delay
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::integration::{BackoffPolicy, DeliveryPolicy, RetryBudget};

    #[test]
    fn retries_respect_tightest_budget() {
        let policy = DeliveryPolicy {
            retries: Some(5),
            backoff: Some(BackoffPolicy {
                min: Duration::from_millis(10),
                max: Duration::from_millis(10),
            }),
            idempotent: None,
        };
        let budget = RetryBudget {
            max_attempts: Some(3),
            max_elapsed: None,
            base_backoff: Some(Duration::from_millis(5)),
            max_backoff: Some(Duration::from_millis(5)),
            jitter: Some(JitterMode::None),
        };
        let plan = DeliveryRetryPlan::new(Some(&policy), Some(&budget));
        assert!(plan.next_delay(0, Duration::ZERO).is_some());
        assert!(plan.next_delay(1, Duration::ZERO).is_some());
        assert!(plan.next_delay(2, Duration::ZERO).is_none());
    }

    #[test]
    fn elapsed_window_enforced() {
        let policy = DeliveryPolicy::default();
        let budget = RetryBudget {
            max_attempts: Some(2),
            max_elapsed: Some(Duration::from_millis(50)),
            base_backoff: Some(Duration::from_millis(40)),
            max_backoff: Some(Duration::from_millis(40)),
            jitter: Some(JitterMode::None),
        };
        let plan = DeliveryRetryPlan::new(Some(&policy), Some(&budget));
        assert!(plan.next_delay(0, Duration::from_millis(10)).is_some());
        assert!(plan.next_delay(0, Duration::from_millis(60)).is_none());
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConnectorFailureReason {
    Timeout,
    Protocol,
    Panic,
    Unknown,
}

impl ConnectorFailureReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::Protocol => "protocol",
            Self::Panic => "panic",
            Self::Unknown => "unknown",
        }
    }
}

fn connector_failure_reason(err: &ActionError) -> Option<ConnectorFailureReason> {
    match err {
        ActionError::Kafka { source, .. }
        | ActionError::Http { source, .. }
        | ActionError::Grpc { source, .. }
        | ActionError::Smtp { source, .. }
        | ActionError::Mariadb { source, .. }
        | ActionError::Postgres { source, .. } => Some(classify_chronicle_error(source)),
        #[cfg(feature = "rabbitmq")]
        ActionError::Rabbitmq { source, .. } => Some(classify_chronicle_error(source)),
        #[cfg(feature = "mqtt")]
        ActionError::Mqtt { source, .. } => Some(classify_chronicle_error(source)),
        ActionError::Connector { source, .. } => Some(classify_connector_factory_error(source)),
        ActionError::FeatureDisabled { .. } | ActionError::Unsupported { .. } => None,
    }
}

fn classify_connector_factory_error(err: &ConnectorFactoryError) -> ConnectorFailureReason {
    match err {
        ConnectorFactoryError::HttpClient { source, .. }
        | ConnectorFactoryError::MariaDb { source, .. }
        | ConnectorFactoryError::Postgres { source, .. }
        | ConnectorFactoryError::Grpc { source, .. }
        | ConnectorFactoryError::KafkaProducer { source, .. }
        | ConnectorFactoryError::Smtp { source, .. }
        | ConnectorFactoryError::Rabbitmq { source, .. }
        | ConnectorFactoryError::Mqtt { source, .. } => classify_chronicle_error(source.as_ref()),
        _ => ConnectorFailureReason::Unknown,
    }
}

fn classify_chronicle_error(err: &ChronicleError) -> ConnectorFailureReason {
    match err {
        ErrorKind::Context { source, .. } => classify_chronicle_error(source),
        ErrorKind::Message(message) => {
            if message_indicates_timeout(message) {
                ConnectorFailureReason::Timeout
            } else {
                ConnectorFailureReason::Unknown
            }
        }
        ErrorKind::Reqwest(inner) => {
            if inner.is_timeout() {
                ConnectorFailureReason::Timeout
            } else if inner.status().is_some() {
                ConnectorFailureReason::Protocol
            } else {
                ConnectorFailureReason::Unknown
            }
        }
        #[cfg(feature = "grpc")]
        ErrorKind::GrpcStatus(status) => classify_grpc_status(status),
        #[cfg(feature = "grpc")]
        ErrorKind::GrpcTransport(inner) => {
            let display = inner.to_string();
            if message_indicates_timeout(&display) {
                ConnectorFailureReason::Timeout
            } else {
                ConnectorFailureReason::Unknown
            }
        }
        ErrorKind::Io(inner) => classify_io_error(inner),
        ErrorKind::Sqlx(inner) => classify_sqlx_error(inner),
        #[cfg(feature = "kafka")]
        ErrorKind::Kafka(_) => ConnectorFailureReason::Protocol,
        #[cfg(feature = "rabbitmq")]
        ErrorKind::RabbitmqTrigger(_) => ConnectorFailureReason::Protocol,
        #[cfg(feature = "mqtt")]
        ErrorKind::MqttTrigger(_) => ConnectorFailureReason::Protocol,
        #[cfg(feature = "db-redis")]
        ErrorKind::RedisTrigger(_) => ConnectorFailureReason::Protocol,
        #[cfg(feature = "db-redis")]
        ErrorKind::Redis(_) => ConnectorFailureReason::Protocol,
        #[cfg(feature = "db-mongodb")]
        ErrorKind::MongodbTrigger(_) => ConnectorFailureReason::Protocol,
        #[cfg(feature = "db-mongodb")]
        ErrorKind::Mongo(_) => ConnectorFailureReason::Protocol,
        ErrorKind::ConnectorFactory(source) => classify_connector_factory_error(source),
        ErrorKind::Join(_) => ConnectorFailureReason::Panic,
        _ => ConnectorFailureReason::Unknown,
    }
}

fn classify_io_error(err: &std::io::Error) -> ConnectorFailureReason {
    match err.kind() {
        IoErrorKind::TimedOut => ConnectorFailureReason::Timeout,
        _ => ConnectorFailureReason::Unknown,
    }
}

fn classify_sqlx_error(err: &sqlx::Error) -> ConnectorFailureReason {
    match err {
        sqlx::Error::PoolTimedOut => ConnectorFailureReason::Timeout,
        sqlx::Error::Io(inner) => classify_io_error(inner),
        sqlx::Error::Database(_) | sqlx::Error::Protocol(_) => ConnectorFailureReason::Protocol,
        sqlx::Error::RowNotFound
        | sqlx::Error::ColumnNotFound(_)
        | sqlx::Error::ColumnDecode { .. }
        | sqlx::Error::TypeNotFound { .. }
        | sqlx::Error::PoolClosed => ConnectorFailureReason::Protocol,
        sqlx::Error::WorkerCrashed => ConnectorFailureReason::Panic,
        _ => ConnectorFailureReason::Unknown,
    }
}

#[cfg(feature = "grpc")]
fn classify_grpc_status(status: &Status) -> ConnectorFailureReason {
    use tonic::Code;
    match status.code() {
        Code::DeadlineExceeded => ConnectorFailureReason::Timeout,
        _ => ConnectorFailureReason::Protocol,
    }
}

fn message_indicates_timeout(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("timed out") || lower.contains("timeout")
}

#[cfg(feature = "kafka")]
fn payload_bytes(payload: &JsonValue) -> Result<Option<Vec<u8>>, ChronicleError> {
    if let Some(encoded) = EncodedPayload::from_json(payload) {
        return Ok(Some(encoded.into_data()));
    }

    match payload {
        JsonValue::Null => Ok(None),
        JsonValue::String(text) => Ok(Some(text.as_bytes().to_vec())),
        other => serde_json::to_vec(other)
            .map(Some)
            .map_err(|err| crate::err!("failed to serialise kafka payload: {err}")),
    }
}
