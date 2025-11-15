#![forbid(unsafe_code)]

use crate::chronicle::retry_runner::{run_retry_loop, RetryContext};
use crate::error::Result;
use crate::readiness::DependencyHealth;
use crate::readiness::ReadinessController;
use crate::transport::readiness_gate::RouteReadinessGate;
use crate::transport::{TaskTransportRuntime, TransportKind, TransportRuntime};
use async_trait::async_trait;
use chronicle_core::chronicle::engine::ChronicleEngine;
use chronicle_core::chronicle::trigger_common::{map_from_btree, parse_binary, RetrySettings};
use chronicle_core::config::integration::{ChronicleDefinition, IntegrationConfig, ScalarValue};
use chronicle_core::integration::registry::{ConnectorRegistry, RedisConnector};
use chronicle_core::metrics::metrics;
use chrono::Utc;
use futures_util::StreamExt;
use redis::aio::{ConnectionManager, PubSub};
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{
    AsyncCommands, Client, ClientTlsConfig, ConnectionInfo, RedisError, TlsCertificates,
    Value as RedisValueRaw,
};
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use std::collections::HashMap;
use std::result::Result as StdResult;
use std::str;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RedisTriggerMode {
    PubSub,
    Stream,
}

#[derive(Clone, Debug)]
pub struct RedisTriggerOptions {
    mode: RedisTriggerMode,
    pub channels: Vec<String>,
    pub stream: Option<String>,
    pub group: Option<String>,
    extra: JsonMap<String, JsonValue>,
}

impl RedisTriggerOptions {
    fn from_trigger(chronicle: &ChronicleDefinition) -> Result<Self, RedisTriggerError> {
        let raw = chronicle.trigger.options_json();
        let mut options =
            raw.as_object()
                .cloned()
                .ok_or_else(|| RedisTriggerError::InvalidTriggerOptions {
                    chronicle: chronicle.name.clone(),
                })?;

        let mode = options
            .remove("mode")
            .and_then(|value| value.as_str().map(|v| v.to_ascii_lowercase()))
            .map(|value| match value.as_str() {
                "pubsub" => Ok(RedisTriggerMode::PubSub),
                "stream" => Ok(RedisTriggerMode::Stream),
                other => Err(RedisTriggerError::UnsupportedMode {
                    chronicle: chronicle.name.clone(),
                    mode: other.to_string(),
                }),
            })
            .transpose()?
            .unwrap_or(RedisTriggerMode::PubSub);

        let channels = options
            .remove("channels")
            .map(|value| match value {
                JsonValue::Array(values) => {
                    let mut channels = Vec::new();
                    for channel in values {
                        if let Some(text) = channel.as_str() {
                            channels.push(text.to_string());
                        }
                    }
                    channels
                }
                JsonValue::String(text) => vec![text],
                _ => Vec::new(),
            })
            .unwrap_or_default();

        let stream = options
            .remove("stream")
            .and_then(|value| value.as_str().map(|v| v.to_string()));

        let group = options
            .remove("group")
            .and_then(|value| value.as_str().map(|v| v.to_string()));

        match mode {
            RedisTriggerMode::PubSub if channels.is_empty() => {
                return Err(RedisTriggerError::MissingChannels {
                    chronicle: chronicle.name.clone(),
                });
            }
            RedisTriggerMode::Stream if stream.is_none() => {
                return Err(RedisTriggerError::MissingStream {
                    chronicle: chronicle.name.clone(),
                });
            }
            _ => {}
        }

        Ok(Self {
            mode,
            channels,
            stream,
            group,
            extra: options,
        })
    }
}

#[derive(Clone, Debug)]
pub struct RedisTriggerConfig {
    pub chronicle: String,
    pub connector: RedisConnector,
    pub options: RedisTriggerOptions,
    pub retry: RetrySettings,
}

pub struct RedisTriggerRuntime {
    inner: TaskTransportRuntime,
    listener_count: usize,
}

impl RedisTriggerRuntime {
    pub async fn build_with<F, Fut>(
        config: Arc<IntegrationConfig>,
        registry: Arc<ConnectorRegistry>,
        engine: Arc<ChronicleEngine>,
        readiness: Option<ReadinessController>,
        dependency_health: Option<DependencyHealth>,
        mut factory: F,
    ) -> Result<Self, RedisTriggerError>
    where
        F: FnMut(RedisTriggerConfig) -> Fut,
        Fut: std::future::Future<
            Output = StdResult<Box<dyn RedisTriggerDriver + Send>, RedisDriverError>,
        >,
    {
        let mut instances = Vec::new();

        for chronicle in &config.chronicles {
            let declared_type = chronicle
                .trigger
                .option("type")
                .and_then(ScalarValue::as_str)
                .map(|value| value.to_ascii_lowercase());

            let connector_handle = registry.redis(&chronicle.trigger.connector);

            let is_redis = match declared_type.as_deref() {
                Some("redis") => true,
                Some(_) => false,
                None => connector_handle.is_some(),
            };

            if !is_redis {
                continue;
            }

            let connector =
                connector_handle
                    .cloned()
                    .ok_or_else(|| RedisTriggerError::MissingConnector {
                        chronicle: chronicle.name.clone(),
                        connector: chronicle.trigger.connector.clone(),
                    })?;

            let options = RedisTriggerOptions::from_trigger(chronicle)?;
            let trigger_extra = options.extra.clone();
            let connector_extra = map_from_btree(&connector.extra);
            let retry = RetrySettings::from_extras(&trigger_extra, &connector_extra);

            let config = RedisTriggerConfig {
                chronicle: chronicle.name.clone(),
                connector: connector.clone(),
                options,
                retry,
            };

            let driver =
                factory(config.clone())
                    .await
                    .map_err(|err| RedisTriggerError::DriverBuild {
                        chronicle: config.chronicle.clone(),
                        reason: err.to_string(),
                    })?;

            instances.push(RedisTriggerInstance {
                chronicle: config.chronicle,
                connector: connector.name,
                driver,
                mode: config.options.mode.clone(),
                retry: config.retry,
                dependency_health: dependency_health.clone(),
                gate: RouteReadinessGate::new(readiness.clone(), &chronicle.name),
                disconnected: false,
            });
        }

        let listener_count = instances.len();
        let engine_shared = Arc::clone(&engine);
        let inner = TaskTransportRuntime::new(TransportKind::RedisIn, "redis", move |shutdown| {
            instances
                .into_iter()
                .map(|instance| {
                    let engine = Arc::clone(&engine_shared);
                    let shutdown = shutdown.clone();
                    tokio::spawn(async move {
                        instance.run(engine, shutdown).await;
                    })
                })
                .collect()
        });

        Ok(Self {
            inner,
            listener_count,
        })
    }

    pub async fn build(
        config: Arc<IntegrationConfig>,
        registry: Arc<ConnectorRegistry>,
        engine: Arc<ChronicleEngine>,
        readiness: Option<ReadinessController>,
        dependency_health: Option<DependencyHealth>,
    ) -> Result<Self, RedisTriggerError> {
        Self::build_with(
            Arc::clone(&config),
            Arc::clone(&registry),
            Arc::clone(&engine),
            readiness,
            dependency_health,
            |cfg| async move {
                RedisDriver::connect(cfg)
                    .await
                    .map(|driver| Box::new(driver) as _)
            },
        )
        .await
    }

    pub fn listener_count(&self) -> usize {
        self.listener_count
    }
}

struct RedisTriggerInstance {
    chronicle: String,
    connector: String,
    driver: Box<dyn RedisTriggerDriver + Send>,
    mode: RedisTriggerMode,
    retry: RetrySettings,
    dependency_health: Option<DependencyHealth>,
    gate: RouteReadinessGate,
    disconnected: bool,
}

impl RedisTriggerInstance {
    async fn run(self, engine: Arc<ChronicleEngine>, shutdown: CancellationToken) {
        let retry = self.retry.clone();
        let context_shutdown = shutdown.clone();
        let mut context = RedisRetryContext {
            instance: self,
            engine,
            shutdown: context_shutdown,
        };

        run_retry_loop(shutdown, retry, Duration::from_millis(50), &mut context).await;
    }

    fn mark_connected(&mut self) {
        if self.disconnected {
            tracing::info!(
                target: "chronicle::redis",
                event = "transport_reconnected",
                chronicle = %self.chronicle,
                connector = %self.connector,
                mode = %format_mode(&self.mode)
            );
            self.disconnected = false;
        }
    }

    fn mark_disconnected(&mut self, err: &RedisDriverError) {
        if !self.disconnected {
            tracing::warn!(
                target: "chronicle::redis",
                event = "transport_disconnected",
                chronicle = %self.chronicle,
                connector = %self.connector,
                mode = %format_mode(&self.mode),
                error = %err
            );
            self.disconnected = true;
        }
    }

    async fn handle_delivery(
        &mut self,
        engine: &Arc<ChronicleEngine>,
        delivery: RedisDelivery,
        shutdown: &CancellationToken,
    ) {
        if !self.gate.wait(shutdown).await {
            return;
        }

        let counters = metrics();
        counters.inc_redis_trigger_inflight();

        let payload = match build_trigger_payload(&delivery) {
            Ok(payload) => payload,
            Err(err) => {
                tracing::warn!(
                    target: "chronicle::redis",
                    event = "payload_build_failed",
                    chronicle = %self.chronicle,
                    connector = %self.connector,
                    mode = %format_mode(&self.mode),
                    error = %err
                );
                counters.dec_redis_trigger_inflight();
                return;
            }
        };

        tracing::info!(
            target: "chronicle::redis",
            event = "trigger_received",
            chronicle = %self.chronicle,
            connector = %self.connector,
            mode = %format_mode(&self.mode)
        );

        match engine.execute(&self.chronicle, payload) {
            Ok(_) => {
                tracing::info!(
                    target: "chronicle::redis",
                    event = "trigger_completed",
                    chronicle = %self.chronicle,
                    connector = %self.connector,
                    mode = %format_mode(&self.mode)
                );

                if let Err(err) = self.driver.ack(&delivery).await {
                    tracing::error!(
                        target: "chronicle::redis",
                        event = "ack_failed",
                        chronicle = %self.chronicle,
                        connector = %self.connector,
                        mode = %format_mode(&self.mode),
                        error = %err
                    );
                }
            }
            Err(err) => {
                tracing::error!(
                    target: "chronicle::redis",
                    event = "trigger_failed",
                    chronicle = %self.chronicle,
                    connector = %self.connector,
                    mode = %format_mode(&self.mode),
                    error = %err
                );
            }
        }

        counters.dec_redis_trigger_inflight();
    }
}

struct RedisRetryContext {
    instance: RedisTriggerInstance,
    engine: Arc<ChronicleEngine>,
    shutdown: CancellationToken,
}

#[async_trait]
impl RetryContext for RedisRetryContext {
    type Item = RedisDelivery;
    type Error = RedisDriverError;

    async fn poll(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        self.instance.driver.next_delivery().await
    }

    async fn handle_item(&mut self, item: Self::Item) {
        let dependency_health = self.instance.dependency_health.clone();
        let connector = self.instance.connector.clone();
        if let Some(health) = dependency_health {
            health.report_outcome(&connector, true).await;
        }
        self.instance.mark_connected();
        self.instance
            .handle_delivery(&self.engine, item, &self.shutdown)
            .await;
    }

    async fn report_error(&mut self, error: &Self::Error, _delay: Duration) {
        let dependency_health = self.instance.dependency_health.clone();
        let connector = self.instance.connector.clone();
        if let Some(health) = dependency_health {
            health.report_outcome(&connector, false).await;
        }
        self.instance.mark_disconnected(error);
        tracing::error!(
            target: "chronicle::redis",
            event = "driver_receive_failed",
            chronicle = %self.instance.chronicle,
            connector = %self.instance.connector,
            mode = %format_mode(&self.instance.mode),
            error = %error
        );

        if let Err(reconnect_err) = self.instance.driver.reconnect().await {
            tracing::error!(
                target: "chronicle::redis",
                event = "driver_reconnect_failed",
                chronicle = %self.instance.chronicle,
                connector = %self.instance.connector,
                mode = %format_mode(&self.instance.mode),
                error = %reconnect_err
            );
        } else {
            self.instance.mark_connected();
        }
    }
}

#[async_trait]
impl TransportRuntime for RedisTriggerRuntime {
    fn kind(&self) -> TransportKind {
        self.inner.kind()
    }

    fn name(&self) -> &'static str {
        self.inner.name()
    }

    fn health(&self) -> crate::transport::TransportHealth {
        self.inner.health()
    }

    async fn prepare(&mut self) -> Result<()> {
        self.inner.prepare().await
    }

    async fn start(&mut self, shutdown: CancellationToken) -> Result<()> {
        self.inner.start(shutdown).await
    }

    fn run(&mut self) -> crate::transport::TransportRun {
        self.inner.run()
    }

    async fn shutdown(&mut self) -> Result<()> {
        self.inner.shutdown().await
    }
}

fn format_mode(mode: &RedisTriggerMode) -> &'static str {
    match mode {
        RedisTriggerMode::PubSub => "pubsub",
        RedisTriggerMode::Stream => "stream",
    }
}

#[derive(Clone, Debug)]
pub enum RedisDelivery {
    PubSub {
        channel: String,
        payload: Vec<u8>,
    },
    Stream {
        stream: String,
        id: String,
        fields: HashMap<String, Vec<u8>>,
    },
}

#[async_trait]
pub trait RedisTriggerDriver: Send {
    async fn next_delivery(&mut self) -> StdResult<Option<RedisDelivery>, RedisDriverError>;
    async fn ack(&mut self, delivery: &RedisDelivery) -> StdResult<(), RedisDriverError>;
    async fn reconnect(&mut self) -> StdResult<(), RedisDriverError>;
}

pub struct RedisDriver {
    config: RedisTriggerConfig,
    client: Client,
    state: RedisDriverState,
    block_ms: u64,
}

enum RedisDriverState {
    PubSub {
        subscription: PubSub,
    },
    Stream {
        connection: ConnectionManager,
        group: Option<String>,
        consumer: String,
        stream: String,
        last_id: Option<String>,
    },
}

impl RedisDriver {
    async fn connect(config: RedisTriggerConfig) -> StdResult<Self, RedisDriverError> {
        let client = build_redis_client(&config.connector)?;
        let block_ms = 1_000;
        let state = match config.options.mode {
            RedisTriggerMode::PubSub => {
                let subscription =
                    build_pubsub(&client, &config.connector, &config.options.channels).await?;
                RedisDriverState::PubSub { subscription }
            }
            RedisTriggerMode::Stream => {
                let stream = config
                    .options
                    .stream
                    .as_ref()
                    .expect("stream name validated at parse");
                let group = config.options.group.clone();
                let connection =
                    build_stream_connection(&client, &config.connector, stream, group.as_deref())
                        .await?;
                let consumer = format!("{}-{}", config.chronicle, Uuid::new_v4());
                RedisDriverState::Stream {
                    connection,
                    group,
                    consumer,
                    stream: stream.clone(),
                    last_id: None,
                }
            }
        };

        Ok(Self {
            config,
            client,
            state,
            block_ms,
        })
    }
}

#[async_trait]
impl RedisTriggerDriver for RedisDriver {
    async fn next_delivery(&mut self) -> StdResult<Option<RedisDelivery>, RedisDriverError> {
        match &mut self.state {
            RedisDriverState::PubSub { subscription } => {
                let mut stream = subscription.on_message();
                match stream.next().await {
                    Some(message) => {
                        let channel = message.get_channel_name().to_string();
                        let payload = message.get_payload_bytes().to_vec();
                        Ok(Some(RedisDelivery::PubSub { channel, payload }))
                    }
                    None => Ok(None),
                }
            }
            RedisDriverState::Stream {
                connection,
                group,
                consumer,
                stream,
                last_id,
            } => {
                let block_ms = if self.block_ms > usize::MAX as u64 {
                    usize::MAX
                } else {
                    self.block_ms as usize
                };
                let mut options = StreamReadOptions::default().count(1).block(block_ms);

                let ids: Vec<String>;

                if let Some(group) = group.as_ref() {
                    options = options.group(group.as_str(), consumer.as_str());
                    ids = vec![">".to_string()];
                } else {
                    let id = last_id.clone().unwrap_or_else(|| "$".to_string());
                    ids = vec![id];
                }

                let keys = vec![stream.clone()];
                let reply: StreamReadReply = connection
                    .xread_options(&keys, &ids, &options)
                    .await
                    .map_err(RedisDriverError::Read)?;

                if reply.keys.is_empty() {
                    return Ok(None);
                }

                let key = &reply.keys[0];
                if key.ids.is_empty() {
                    return Ok(None);
                }

                let entry = &key.ids[0];
                if group.is_none() {
                    *last_id = Some(entry.id.clone());
                }

                let mut fields = HashMap::new();
                for (field, value) in &entry.map {
                    fields.insert(field.clone(), redis_value_to_bytes(value));
                }

                Ok(Some(RedisDelivery::Stream {
                    stream: key.key.clone(),
                    id: entry.id.clone(),
                    fields,
                }))
            }
        }
    }

    async fn ack(&mut self, delivery: &RedisDelivery) -> StdResult<(), RedisDriverError> {
        match (&mut self.state, delivery) {
            (
                RedisDriverState::Stream {
                    connection,
                    group: Some(group),
                    stream,
                    ..
                },
                RedisDelivery::Stream { id, .. },
            ) => {
                let group_name = group.clone();
                let stream_name = stream.clone();
                let entry_id = id.clone();
                redis::cmd("XACK")
                    .arg(&stream_name)
                    .arg(&group_name)
                    .arg(&entry_id)
                    .query_async::<_, i64>(connection)
                    .await
                    .map_err(|err| RedisDriverError::Ack {
                        connector: self.config.connector.name.clone(),
                        stream: stream_name,
                        group: group_name,
                        id: entry_id,
                        source: err,
                    })?;
                Ok(())
            }
            _ => Ok(()),
        }
    }

    async fn reconnect(&mut self) -> StdResult<(), RedisDriverError> {
        self.state = match self.config.options.mode {
            RedisTriggerMode::PubSub => {
                let subscription = build_pubsub(
                    &self.client,
                    &self.config.connector,
                    &self.config.options.channels,
                )
                .await?;
                RedisDriverState::PubSub { subscription }
            }
            RedisTriggerMode::Stream => {
                let stream = self.config.options.stream.as_ref().expect("stream checked");
                let state = match &self.state {
                    RedisDriverState::Stream {
                        group,
                        consumer,
                        last_id,
                        ..
                    } => {
                        let connection = build_stream_connection(
                            &self.client,
                            &self.config.connector,
                            stream,
                            group.as_deref(),
                        )
                        .await?;
                        RedisDriverState::Stream {
                            connection,
                            group: group.clone(),
                            consumer: consumer.clone(),
                            stream: stream.clone(),
                            last_id: last_id.clone(),
                        }
                    }
                    _ => {
                        let group = self.config.options.group.clone();
                        let connection = build_stream_connection(
                            &self.client,
                            &self.config.connector,
                            stream,
                            group.as_deref(),
                        )
                        .await?;
                        let consumer = format!("{}-{}", self.config.chronicle, Uuid::new_v4());
                        RedisDriverState::Stream {
                            connection,
                            group,
                            consumer,
                            stream: stream.clone(),
                            last_id: None,
                        }
                    }
                };
                state
            }
        };
        Ok(())
    }
}

fn build_redis_client(connector: &RedisConnector) -> StdResult<Client, RedisDriverError> {
    let mut info: ConnectionInfo = connector.url.parse().map_err(|err| {
        RedisDriverError::Connect(Box::new(crate::err!(
            "invalid redis url `{}`: {}",
            connector.url,
            err
        )))
    })?;

    if let Some(username) = connector
        .extra
        .get("username")
        .and_then(|value| value.as_str())
    {
        info.redis.username = Some(username.to_string());
    }
    if let Some(password) = connector
        .extra
        .get("password")
        .and_then(|value| value.as_str())
    {
        info.redis.password = Some(password.to_string());
    }

    if let Some(tls) = &connector.tls {
        let certs = load_connector_tls(tls)?;
        Client::build_with_tls(info, certs).map_err(|err| {
            RedisDriverError::Tls(Box::new(crate::err!(
                "failed to create redis TLS client for `{}`: {}",
                connector.url,
                err
            )))
        })
    } else {
        Client::open(info).map_err(|err| {
            RedisDriverError::Connect(Box::new(crate::err!(
                "failed to create redis client for `{}`: {}",
                connector.url,
                err
            )))
        })
    }
}

async fn build_pubsub(
    client: &Client,
    connector: &RedisConnector,
    channels: &[String],
) -> StdResult<PubSub, RedisDriverError> {
    let connection = client.get_async_connection().await.map_err(|err| {
        RedisDriverError::Connect(Box::new(crate::err!(
            "failed to open redis connection for `{}`: {}",
            connector.name,
            err
        )))
    })?;
    let mut pubsub = connection.into_pubsub();
    for channel in channels {
        pubsub
            .subscribe(channel)
            .await
            .map_err(|err| RedisDriverError::Subscribe {
                connector: connector.name.clone(),
                channel: channel.clone(),
                source: err,
            })?;
    }
    Ok(pubsub)
}

async fn build_stream_connection(
    client: &Client,
    connector: &RedisConnector,
    stream: &str,
    group: Option<&str>,
) -> StdResult<ConnectionManager, RedisDriverError> {
    let mut connection = ConnectionManager::new(client.clone())
        .await
        .map_err(|err| {
            RedisDriverError::Connect(Box::new(crate::err!(
                "failed to establish redis connection for `{}`: {}",
                connector.name,
                err
            )))
        })?;

    if let Some(group) = group {
        let _: RedisValueRaw = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(stream)
            .arg(group)
            .arg("$")
            .arg("MKSTREAM")
            .query_async(&mut connection)
            .await
            .or_else(|err| {
                if let Some(code) = err.code() {
                    if code == "BUSYGROUP" {
                        return Ok(redis::Value::Nil);
                    }
                }
                Err(RedisDriverError::CreateGroup {
                    connector: connector.name.clone(),
                    stream: stream.to_string(),
                    group: group.to_string(),
                    source: err,
                })
            })?;
    }

    Ok(connection)
}

fn build_trigger_payload(delivery: &RedisDelivery) -> Result<JsonValue> {
    let now = Utc::now().timestamp_millis();
    match delivery {
        RedisDelivery::PubSub { channel, payload } => {
            let mut root = JsonMap::new();
            root.insert("channel".to_string(), JsonValue::String(channel.clone()));
            root.insert(
                "payload".to_string(),
                JsonValue::Object(binary_to_json_map(payload)),
            );
            root.insert("attributes".to_string(), JsonValue::Object(JsonMap::new()));
            root.insert(
                "metadata".to_string(),
                JsonValue::Object(metadata_with_timestamp(now)),
            );

            Ok(JsonValue::Object(root))
        }
        RedisDelivery::Stream { stream, id, fields } => {
            let mut root = JsonMap::new();
            root.insert("stream".to_string(), JsonValue::String(stream.clone()));
            root.insert("id".to_string(), JsonValue::String(id.clone()));

            let mut attributes = JsonMap::new();
            let mut payload_value: Option<JsonValue> = None;
            let mut metadata = metadata_with_timestamp(now);

            for (field, value) in fields {
                let value_json = JsonValue::Object(binary_to_json_map(value));
                if field == "payload" {
                    payload_value = Some(value_json.clone());
                    continue;
                }

                if field == "timestamp" {
                    if let Ok(text) = str::from_utf8(value) {
                        if let Ok(parsed_ts) = text.parse::<i64>() {
                            metadata.insert(
                                "timestamp".to_string(),
                                JsonValue::Number(JsonNumber::from(parsed_ts)),
                            );
                        }
                    }
                }

                attributes.insert(field.clone(), value_json);
            }

            root.insert(
                "payload".to_string(),
                payload_value.unwrap_or_else(|| JsonValue::Object(binary_to_json_map(&[]))),
            );

            root.insert("attributes".to_string(), JsonValue::Object(attributes));
            root.insert("metadata".to_string(), JsonValue::Object(metadata));

            Ok(JsonValue::Object(root))
        }
    }
}

fn binary_to_json_map(bytes: &[u8]) -> JsonMap<String, JsonValue> {
    let parsed = parse_binary(bytes);
    let mut map = JsonMap::new();
    map.insert("base64".to_string(), JsonValue::String(parsed.base64));
    if let Some(text) = parsed.text {
        map.insert("text".to_string(), JsonValue::String(text));
    }
    if let Some(json_value) = parsed.json {
        map.insert("json".to_string(), json_value);
    }
    map
}

fn metadata_with_timestamp(ts: i64) -> JsonMap<String, JsonValue> {
    let mut metadata = JsonMap::new();
    metadata.insert(
        "timestamp".to_string(),
        JsonValue::Number(JsonNumber::from(ts)),
    );
    metadata
}

fn redis_value_to_bytes(value: &RedisValueRaw) -> Vec<u8> {
    match value {
        RedisValueRaw::Data(bytes) => bytes.clone(),
        RedisValueRaw::Bulk(values) => values
            .iter()
            .flat_map(redis_value_to_bytes)
            .collect::<Vec<_>>(),
        RedisValueRaw::Int(number) => number.to_string().into_bytes(),
        RedisValueRaw::Status(text) => text.as_bytes().to_vec(),
        RedisValueRaw::Okay => b"OK".to_vec(),
        RedisValueRaw::Nil => Vec::new(),
    }
}

fn load_connector_tls(
    tls: &chronicle_core::integration::registry::RedisTlsFiles,
) -> StdResult<TlsCertificates, RedisDriverError> {
    let ca = tls
        .ca
        .as_ref()
        .map(|path| read_file(path.as_path()))
        .transpose()?
        .flatten();
    let cert = tls
        .cert
        .as_ref()
        .map(|path| read_file(path.as_path()))
        .transpose()?
        .flatten();
    let key = tls
        .key
        .as_ref()
        .map(|path| read_file(path.as_path()))
        .transpose()?
        .flatten();

    let client_tls = match (cert, key) {
        (Some(cert), Some(key)) => Some(ClientTlsConfig {
            client_cert: cert,
            client_key: key,
        }),
        (None, None) => None,
        _ => {
            return Err(RedisDriverError::Tls(Box::new(crate::err!(
                "redis TLS configuration requires both `tls.cert` and `tls.key`"
            ))))
        }
    };

    Ok(TlsCertificates {
        client_tls,
        root_cert: ca,
    })
}

fn read_file(path: &std::path::Path) -> StdResult<Option<Vec<u8>>, RedisDriverError> {
    std::fs::read(path)
        .map(Some)
        .map_err(|err| RedisDriverError::Io {
            path: path.display().to_string(),
            source: err,
        })
}

#[derive(Debug, Error)]
pub enum RedisTriggerError {
    #[error("chronicle `{chronicle}` trigger is missing connector `{connector}`")]
    MissingConnector {
        chronicle: String,
        connector: String,
    },
    #[error("chronicle `{chronicle}` trigger options are invalid")]
    InvalidTriggerOptions { chronicle: String },
    #[error("chronicle `{chronicle}` trigger uses unsupported redis mode `{mode}`")]
    UnsupportedMode { chronicle: String, mode: String },
    #[error("chronicle `{chronicle}` redis trigger requires `channels` to be specified")]
    MissingChannels { chronicle: String },
    #[error("chronicle `{chronicle}` redis trigger requires `stream` to be specified")]
    MissingStream { chronicle: String },
    #[error("chronicle `{chronicle}` failed to construct redis driver: {reason}")]
    DriverBuild { chronicle: String, reason: String },
}

#[derive(Debug, Error)]
pub enum RedisDriverError {
    #[error("redis connection error: {0}")]
    Connect(#[source] Box<crate::error::Error>),
    #[error("redis TLS error: {0}")]
    Tls(#[source] Box<crate::error::Error>),
    #[error("failed to subscribe to channel `{channel}` on connector `{connector}`: {source}")]
    Subscribe {
        connector: String,
        channel: String,
        #[source]
        source: RedisError,
    },
    #[error("failed to create group `{group}` on stream `{stream}` for connector `{connector}`: {source}")]
    CreateGroup {
        connector: String,
        stream: String,
        group: String,
        #[source]
        source: RedisError,
    },
    #[error("failed to acknowledge entry `{id}` on stream `{stream}` (group `{group}`) for connector `{connector}`: {source}")]
    Ack {
        connector: String,
        stream: String,
        group: String,
        id: String,
        #[source]
        source: RedisError,
    },
    #[error("redis read failed: {0}")]
    Read(#[source] RedisError),
    #[error("failed to read TLS material `{path}`: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
}
