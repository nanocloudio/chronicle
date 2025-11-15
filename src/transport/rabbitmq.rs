#![forbid(unsafe_code)]

use crate::chronicle::retry_runner::{run_retry_loop, RetryContext};
use crate::error::Result;
use crate::readiness::DependencyHealth;
use crate::readiness::ReadinessController;
use crate::transport::broker::{payload_to_bytes, publish_with_timeout};
use crate::transport::readiness_gate::RouteReadinessGate;
use crate::transport::{TaskTransportRuntime, TransportKind, TransportRuntime};
use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use chronicle_core::chronicle::engine::ChronicleEngine;
use chronicle_core::chronicle::trigger_common::{map_from_btree, parse_binary, RetrySettings};
use chronicle_core::config::integration::{ChronicleDefinition, IntegrationConfig, ScalarValue};
use chronicle_core::integration::factory::ConnectorFactoryRegistry;
use chronicle_core::integration::registry::{ConnectorRegistry, RabbitmqConnector};
use chronicle_core::metrics::metrics;
use futures_util::StreamExt;
use lapin::types::Timestamp;
use lapin::{
    message::Delivery,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions,
        BasicQosOptions,
    },
    types::{AMQPValue, FieldArray, FieldTable, LongString, ShortString},
    BasicProperties, Channel, Connection, ConnectionProperties, Consumer,
};
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::future::Future;
use std::result::Result as StdResult;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
#[cfg(feature = "rabbitmq")]
use tokio_executor_trait::Tokio as TokioExecutor;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Runtime that binds RabbitMQ queue consumers to chronicle executions.
pub struct RabbitmqTriggerRuntime {
    inner: TaskTransportRuntime,
    consumer_count: usize,
}

impl RabbitmqTriggerRuntime {
    pub async fn build_with<C, F, Fut>(
        config: Arc<IntegrationConfig>,
        registry: Arc<ConnectorRegistry>,
        engine: Arc<ChronicleEngine>,
        readiness: Option<ReadinessController>,
        dependency_health: Option<DependencyHealth>,
        mut factory: F,
    ) -> Result<Self, RabbitmqTriggerError>
    where
        C: RabbitmqConsumer + Send,
        F: FnMut(RabbitmqConsumerConfig) -> Fut,
        Fut: Future<Output = StdResult<C, RabbitmqConsumerError>>,
    {
        let mut consumers = Vec::new();

        for chronicle in &config.chronicles {
            let trigger_type = chronicle
                .trigger
                .option("type")
                .and_then(ScalarValue::as_str)
                .map(|value| value.to_ascii_lowercase());

            let connector_handle = registry.rabbitmq(&chronicle.trigger.connector);

            let is_rabbitmq = match trigger_type.as_deref() {
                Some("rabbitmq") => true,
                Some(_) => false,
                None => connector_handle.is_some(),
            };

            if !is_rabbitmq {
                continue;
            }

            let connector = connector_handle.cloned().ok_or_else(|| {
                RabbitmqTriggerError::missing_connector(
                    &chronicle.name,
                    &chronicle.trigger.connector,
                )
            })?;

            let options = RabbitmqTriggerOptions::from_trigger(chronicle)?;
            let consumer_config =
                RabbitmqConsumerConfig::new(&chronicle.name, &connector, &options)?;

            let retry_settings = consumer_config.retry.clone();

            let mut consumer = factory(consumer_config.clone()).await.map_err(|err| {
                RabbitmqTriggerError::ConsumerBuild {
                    chronicle: chronicle.name.clone(),
                    reason: err.to_string(),
                }
            })?;

            if let Some(prefetch) = consumer_config.prefetch {
                if let Err(err) = consumer.configure_prefetch(prefetch).await {
                    return Err(RabbitmqTriggerError::ConfigurePrefetch {
                        chronicle: chronicle.name.clone(),
                        reason: err.to_string(),
                    });
                }
            }

            consumers.push(RabbitmqTriggerInstance {
                chronicle: chronicle.name.clone(),
                connector: connector.name.clone(),
                queue: options.queue.clone(),
                consumer,
                ack_mode: options.ack_mode,
                retry: retry_settings,
                dependency_health: dependency_health.clone(),
                gate: RouteReadinessGate::new(readiness.clone(), &chronicle.name),
                disconnected: false,
            });
        }

        let consumer_count = consumers.len();
        let engine_shared = Arc::clone(&engine);

        let inner =
            TaskTransportRuntime::new(TransportKind::RabbitmqIn, "rabbitmq", move |shutdown| {
                consumers
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
            consumer_count,
        })
    }

    pub fn consumer_count(&self) -> usize {
        self.consumer_count
    }
}

#[async_trait]
pub trait RabbitmqConsumer: Send + 'static {
    async fn configure_prefetch(&mut self, _prefetch: u16) -> StdResult<(), RabbitmqConsumerError> {
        Ok(())
    }

    async fn next_delivery(&mut self)
        -> StdResult<Option<RabbitmqDelivery>, RabbitmqConsumerError>;

    async fn ack(&mut self, delivery_tag: u64) -> StdResult<(), RabbitmqConsumerError>;

    async fn nack(
        &mut self,
        delivery_tag: u64,
        requeue: bool,
    ) -> StdResult<(), RabbitmqConsumerError>;
}

pub struct LapinRabbitmqConsumer {
    config: RabbitmqConsumerConfig,
    connection: Connection,
    channel: Channel,
    consumer: Consumer,
}

impl LapinRabbitmqConsumer {
    pub async fn connect(config: RabbitmqConsumerConfig) -> StdResult<Self, RabbitmqConsumerError> {
        let properties = ConnectionProperties::default().with_executor(TokioExecutor::current());
        let connection = Connection::connect(&config.url, properties)
            .await
            .map_err(|err| RabbitmqConsumerError::new(format!("failed to connect: {err}")))?;

        let channel = connection
            .create_channel()
            .await
            .map_err(|err| RabbitmqConsumerError::new(format!("failed to open channel: {err}")))?;

        if let Some(prefetch) = config.prefetch {
            channel
                .basic_qos(prefetch, BasicQosOptions::default())
                .await
                .map_err(|err| {
                    RabbitmqConsumerError::new(format!(
                        "failed to configure prefetch (prefetch={prefetch}): {err}"
                    ))
                })?;
        }

        let consumer = Self::start_consumer(&channel, &config).await?;

        Ok(Self {
            config,
            connection,
            channel,
            consumer,
        })
    }

    async fn reconnect(&mut self) -> StdResult<(), RabbitmqConsumerError> {
        let properties = ConnectionProperties::default().with_executor(TokioExecutor::current());
        let connection = Connection::connect(&self.config.url, properties)
            .await
            .map_err(|err| {
                RabbitmqConsumerError::new(format!("failed to reconnect to rabbitmq: {err}"))
            })?;

        let channel = connection
            .create_channel()
            .await
            .map_err(|err| RabbitmqConsumerError::new(format!("failed to open channel: {err}")))?;

        if let Some(prefetch) = self.config.prefetch {
            channel
                .basic_qos(prefetch, BasicQosOptions::default())
                .await
                .map_err(|err| {
                    RabbitmqConsumerError::new(format!(
                        "failed to configure prefetch (prefetch={prefetch}): {err}"
                    ))
                })?;
        }

        let consumer = Self::start_consumer(&channel, &self.config).await?;

        self.connection = connection;
        self.channel = channel;
        self.consumer = consumer;

        Ok(())
    }

    async fn start_consumer(
        channel: &Channel,
        config: &RabbitmqConsumerConfig,
    ) -> StdResult<Consumer, RabbitmqConsumerError> {
        let options = BasicConsumeOptions {
            no_ack: matches!(config.ack_mode, RabbitmqAckMode::Auto),
            ..BasicConsumeOptions::default()
        };

        let consumer_tag = format!("chronicle-{}-{}", config.chronicle, Uuid::new_v4());

        channel
            .basic_consume(&config.queue, &consumer_tag, options, FieldTable::default())
            .await
            .map_err(|err| {
                RabbitmqConsumerError::new(format!(
                    "failed to start consumer on queue `{}`: {err}",
                    config.queue
                ))
            })
    }

    fn convert_delivery(delivery: Delivery) -> RabbitmqDelivery {
        let headers = delivery
            .properties
            .headers()
            .as_ref()
            .map(field_table_to_json)
            .unwrap_or_default();

        let properties = basic_properties_to_json(&delivery.properties);

        let timestamp = delivery
            .properties
            .timestamp()
            .as_ref()
            .map(|value| *value as i64);

        RabbitmqDelivery {
            body: delivery.data,
            routing_key: delivery.routing_key.to_string(),
            exchange: delivery.exchange.to_string(),
            delivery_tag: delivery.delivery_tag,
            redelivered: delivery.redelivered,
            headers,
            properties,
            timestamp,
        }
    }
}

#[async_trait]
impl RabbitmqConsumer for LapinRabbitmqConsumer {
    async fn next_delivery(
        &mut self,
    ) -> StdResult<Option<RabbitmqDelivery>, RabbitmqConsumerError> {
        loop {
            match self.consumer.next().await {
                Some(Ok(delivery)) => {
                    return Ok(Some(Self::convert_delivery(delivery)));
                }
                Some(Err(err)) => {
                    tracing::warn!(
                        target: "chronicle::rabbitmq",
                        error = %err,
                        "rabbitmq consumer error; attempting reconnect"
                    );
                    self.reconnect().await?;
                    continue;
                }
                None => {
                    tracing::warn!(
                        target: "chronicle::rabbitmq",
                        "rabbitmq consumer stream ended; attempting reconnect"
                    );
                    self.reconnect().await?;
                    continue;
                }
            }
        }
    }

    async fn ack(&mut self, delivery_tag: u64) -> StdResult<(), RabbitmqConsumerError> {
        self.channel
            .basic_ack(delivery_tag, BasicAckOptions::default())
            .await
            .map_err(|err| RabbitmqConsumerError::new(format!("ack failed: {err}")))
    }

    async fn nack(
        &mut self,
        delivery_tag: u64,
        requeue: bool,
    ) -> StdResult<(), RabbitmqConsumerError> {
        let options = BasicNackOptions {
            requeue,
            ..BasicNackOptions::default()
        };

        self.channel
            .basic_nack(delivery_tag, options)
            .await
            .map_err(|err| RabbitmqConsumerError::new(format!("nack failed: {err}")))
    }
}

#[derive(Debug, Clone)]
pub struct RabbitmqConsumerError {
    message: String,
}

impl RabbitmqConsumerError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for RabbitmqConsumerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for RabbitmqConsumerError {}

#[derive(Clone, Debug)]
pub struct RabbitmqConsumerConfig {
    pub chronicle: String,
    pub connector: String,
    pub url: String,
    pub queue: String,
    pub ack_mode: RabbitmqAckMode,
    pub prefetch: Option<u16>,
    pub trigger_extra: JsonMap<String, JsonValue>,
    pub connector_extra: JsonMap<String, JsonValue>,
    pub(crate) retry: RetrySettings,
}

impl RabbitmqConsumerConfig {
    fn new(
        chronicle_name: &str,
        connector: &RabbitmqConnector,
        options: &RabbitmqTriggerOptions,
    ) -> Result<Self, RabbitmqTriggerError> {
        let prefetch = if let Some(override_prefetch) = options.prefetch {
            Some(override_prefetch)
        } else if let Some(connector_prefetch) = connector.prefetch {
            Some(u16::try_from(connector_prefetch).map_err(|_| {
                RabbitmqTriggerError::InvalidPrefetch {
                    chronicle: chronicle_name.to_string(),
                    value: connector_prefetch.to_string(),
                }
            })?)
        } else {
            None
        };

        let trigger_extra = options.extra.clone();
        let connector_extra = map_from_btree(&connector.extra);
        let retry = RetrySettings::from_extras(&trigger_extra, &connector_extra);

        Ok(Self {
            chronicle: chronicle_name.to_string(),
            connector: connector.name.clone(),
            url: connector.url.clone(),
            queue: options.queue.clone(),
            ack_mode: options.ack_mode,
            prefetch,
            trigger_extra,
            connector_extra,
            retry,
        })
    }
}

struct RabbitmqTriggerInstance<C>
where
    C: RabbitmqConsumer + Send,
{
    chronicle: String,
    connector: String,
    queue: String,
    consumer: C,
    ack_mode: RabbitmqAckMode,
    retry: RetrySettings,
    dependency_health: Option<DependencyHealth>,
    gate: RouteReadinessGate,
    disconnected: bool,
}

impl<C> RabbitmqTriggerInstance<C>
where
    C: RabbitmqConsumer + Send,
{
    async fn run(self, engine: Arc<ChronicleEngine>, shutdown: CancellationToken) {
        let retry = self.retry.clone();
        let context_shutdown = shutdown.clone();
        let mut context = RabbitmqRetryContext {
            instance: self,
            engine,
            shutdown: context_shutdown,
        };

        run_retry_loop(shutdown, retry, Duration::from_millis(50), &mut context).await;
    }

    fn mark_connected(&mut self) {
        if self.disconnected {
            tracing::info!(
                target: "chronicle::rabbitmq",
                event = "transport_reconnected",
                chronicle = %self.chronicle,
                connector = %self.connector,
                queue = %self.queue
            );
            self.disconnected = false;
        }
    }

    fn mark_disconnected(&mut self, err: &RabbitmqConsumerError) {
        if !self.disconnected {
            tracing::warn!(
                target: "chronicle::rabbitmq",
                event = "transport_disconnected",
                chronicle = %self.chronicle,
                connector = %self.connector,
                queue = %self.queue,
                error = %err
            );
            self.disconnected = true;
        }
    }

    async fn handle_delivery(
        &mut self,
        engine: &Arc<ChronicleEngine>,
        delivery: RabbitmqDelivery,
        shutdown: &CancellationToken,
    ) {
        if !self.gate.wait(shutdown).await {
            return;
        }

        let counters = metrics();
        counters.inc_rabbitmq_trigger_inflight();

        let payload = build_trigger_payload(&delivery);

        tracing::info!(
            target: "chronicle::rabbitmq",
            event = "trigger_received",
            chronicle = %self.chronicle,
            connector = %self.connector,
            queue = %self.queue,
            delivery_tag = delivery.delivery_tag,
            exchange = %delivery.exchange,
            routing_key = %delivery.routing_key
        );

        let delivery_tag = delivery.delivery_tag;

        match engine.execute(&self.chronicle, payload) {
            Ok(_) => {
                tracing::info!(
                    target: "chronicle::rabbitmq",
                    event = "trigger_completed",
                    chronicle = %self.chronicle,
                    connector = %self.connector,
                    queue = %self.queue,
                    delivery_tag = delivery_tag
                );

                if matches!(self.ack_mode, RabbitmqAckMode::Manual) {
                    if let Err(err) = self.consumer.ack(delivery_tag).await {
                        tracing::error!(
                            target: "chronicle::rabbitmq",
                            event = "ack_failed",
                            chronicle = %self.chronicle,
                            connector = %self.connector,
                            queue = %self.queue,
                            delivery_tag = delivery_tag,
                            error = %err
                        );
                    }
                }
            }
            Err(err) => {
                tracing::error!(
                    target: "chronicle::rabbitmq",
                    event = "trigger_failed",
                    chronicle = %self.chronicle,
                    connector = %self.connector,
                    queue = %self.queue,
                    delivery_tag = delivery_tag,
                    error = %err
                );

                if matches!(self.ack_mode, RabbitmqAckMode::Manual) {
                    if let Err(nack_err) = self.consumer.nack(delivery_tag, true).await {
                        tracing::error!(
                            target: "chronicle::rabbitmq",
                            event = "nack_failed",
                            chronicle = %self.chronicle,
                            connector = %self.connector,
                            queue = %self.queue,
                            delivery_tag = delivery_tag,
                            error = %nack_err
                        );
                    }
                }
            }
        }

        counters.dec_rabbitmq_trigger_inflight();
    }
}

struct RabbitmqRetryContext<C>
where
    C: RabbitmqConsumer + Send,
{
    instance: RabbitmqTriggerInstance<C>,
    engine: Arc<ChronicleEngine>,
    shutdown: CancellationToken,
}

#[async_trait]
impl<C> RetryContext for RabbitmqRetryContext<C>
where
    C: RabbitmqConsumer + Send,
{
    type Item = RabbitmqDelivery;
    type Error = RabbitmqConsumerError;

    async fn poll(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        self.instance.consumer.next_delivery().await
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
            target: "chronicle::rabbitmq",
            event = "consumer_receive_failed",
            chronicle = %self.instance.chronicle,
            connector = %self.instance.connector,
            queue = %self.instance.queue,
            error = %error
        );
    }
}

#[async_trait]
impl TransportRuntime for RabbitmqTriggerRuntime {
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

#[derive(Debug, Clone)]
pub struct RabbitmqDelivery {
    pub body: Vec<u8>,
    pub routing_key: String,
    pub exchange: String,
    pub delivery_tag: u64,
    pub redelivered: bool,
    pub headers: JsonMap<String, JsonValue>,
    pub properties: JsonMap<String, JsonValue>,
    pub timestamp: Option<i64>,
}

#[derive(Clone, Copy, Debug)]
pub enum RabbitmqAckMode {
    Auto,
    Manual,
}

impl RabbitmqAckMode {
    fn parse(chronicle: &str, value: Option<&str>) -> Result<Self, RabbitmqTriggerError> {
        match value {
            None => Ok(RabbitmqAckMode::Manual),
            Some(raw) => match raw.to_ascii_lowercase().as_str() {
                "auto" => Ok(RabbitmqAckMode::Auto),
                "manual" => Ok(RabbitmqAckMode::Manual),
                other => Err(RabbitmqTriggerError::InvalidAckMode {
                    chronicle: chronicle.to_string(),
                    value: other.to_string(),
                }),
            },
        }
    }
}

#[derive(Clone, Debug)]
struct RabbitmqTriggerOptions {
    queue: String,
    ack_mode: RabbitmqAckMode,
    prefetch: Option<u16>,
    extra: JsonMap<String, JsonValue>,
}

impl RabbitmqTriggerOptions {
    fn from_trigger(chronicle: &ChronicleDefinition) -> Result<Self, RabbitmqTriggerError> {
        let raw = chronicle.trigger.options_json();
        let mut options = raw.as_object().cloned().ok_or_else(|| {
            RabbitmqTriggerError::InvalidTriggerOptions {
                chronicle: chronicle.name.clone(),
            }
        })?;

        let queue_value =
            options
                .remove("queue")
                .ok_or_else(|| RabbitmqTriggerError::MissingTriggerOption {
                    chronicle: chronicle.name.clone(),
                    option: "queue".to_string(),
                })?;

        let queue = queue_value
            .as_str()
            .ok_or_else(|| RabbitmqTriggerError::InvalidTriggerOptions {
                chronicle: chronicle.name.clone(),
            })?
            .to_string();

        let ack_mode_value = options.remove("ack_mode");
        let ack_mode = RabbitmqAckMode::parse(
            &chronicle.name,
            ack_mode_value
                .as_ref()
                .and_then(JsonValue::as_str)
                .map(str::trim),
        )?;

        let prefetch = match options.remove("prefetch") {
            Some(value) => Some(parse_prefetch(&chronicle.name, value)?),
            None => None,
        };

        Ok(Self {
            queue,
            ack_mode,
            prefetch,
            extra: options,
        })
    }
}

fn parse_prefetch(chronicle: &str, value: JsonValue) -> Result<u16, RabbitmqTriggerError> {
    let number = match value {
        JsonValue::Number(num) => {
            num.as_u64()
                .ok_or_else(|| RabbitmqTriggerError::InvalidPrefetch {
                    chronicle: chronicle.to_string(),
                    value: num.to_string(),
                })?
        }
        JsonValue::String(text) => {
            text.parse::<u64>()
                .map_err(|_| RabbitmqTriggerError::InvalidPrefetch {
                    chronicle: chronicle.to_string(),
                    value: text,
                })?
        }
        other => {
            return Err(RabbitmqTriggerError::InvalidPrefetch {
                chronicle: chronicle.to_string(),
                value: other.to_string(),
            });
        }
    };

    let count = u16::try_from(number).map_err(|_| RabbitmqTriggerError::InvalidPrefetch {
        chronicle: chronicle.to_string(),
        value: number.to_string(),
    })?;

    if count == 0 {
        return Err(RabbitmqTriggerError::InvalidPrefetch {
            chronicle: chronicle.to_string(),
            value: "0".to_string(),
        });
    }

    Ok(count)
}

fn build_trigger_payload(delivery: &RabbitmqDelivery) -> JsonValue {
    let mut root = JsonMap::new();

    let parsed = parse_binary(&delivery.body);
    let mut body_map = JsonMap::new();
    body_map.insert(
        "base64".to_string(),
        JsonValue::String(parsed.base64.clone()),
    );

    if let Some(text) = parsed.text.as_ref() {
        body_map.insert("text".to_string(), JsonValue::String(text.clone()));
    }

    if let Some(json_value) = parsed.json.clone() {
        body_map.insert("json".to_string(), json_value);
    }

    root.insert("body".to_string(), JsonValue::Object(body_map));
    root.insert(
        "exchange".to_string(),
        JsonValue::String(delivery.exchange.clone()),
    );
    root.insert(
        "routing_key".to_string(),
        JsonValue::String(delivery.routing_key.clone()),
    );
    root.insert(
        "delivery_tag".to_string(),
        JsonValue::Number(JsonNumber::from(delivery.delivery_tag)),
    );
    root.insert(
        "redelivered".to_string(),
        JsonValue::Bool(delivery.redelivered),
    );

    if !delivery.headers.is_empty() {
        root.insert(
            "headers".to_string(),
            JsonValue::Object(delivery.headers.clone()),
        );
    } else {
        root.insert("headers".to_string(), JsonValue::Object(JsonMap::new()));
    }

    if !delivery.properties.is_empty() {
        root.insert(
            "properties".to_string(),
            JsonValue::Object(delivery.properties.clone()),
        );
    } else {
        root.insert("properties".to_string(), JsonValue::Object(JsonMap::new()));
    }

    if let Some(ts) = delivery.timestamp {
        let mut metadata = JsonMap::new();
        metadata.insert(
            "timestamp".to_string(),
            JsonValue::Number(JsonNumber::from(ts)),
        );
        root.insert("metadata".to_string(), JsonValue::Object(metadata));
    }

    JsonValue::Object(root)
}

fn field_table_to_json(table: &FieldTable) -> JsonMap<String, JsonValue> {
    let mut map = JsonMap::new();
    for (key, value) in table {
        map.insert(key.to_string(), amqp_value_to_json(value));
    }
    map
}

fn amqp_value_to_json(value: &AMQPValue) -> JsonValue {
    match value {
        AMQPValue::Boolean(b) => JsonValue::Bool(*b),
        AMQPValue::ShortShortInt(v) => JsonValue::Number((*v as i64).into()),
        AMQPValue::ShortShortUInt(v) => JsonValue::Number((*v as u64).into()),
        AMQPValue::ShortInt(v) => JsonValue::Number((*v as i64).into()),
        AMQPValue::ShortUInt(v) => JsonValue::Number((*v as u64).into()),
        AMQPValue::LongInt(v) => JsonValue::Number((*v as i64).into()),
        AMQPValue::LongUInt(v) => JsonValue::Number((*v as u64).into()),
        AMQPValue::LongLongInt(v) => JsonValue::Number((*v).into()),
        AMQPValue::Float(v) => JsonNumber::from_f64(*v as f64)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        AMQPValue::Double(v) => JsonNumber::from_f64(*v)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        AMQPValue::DecimalValue(decimal) => {
            let value = decimal.value as f64 / 10_f64.powi(decimal.scale as i32);
            JsonNumber::from_f64(value)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null)
        }
        AMQPValue::ShortString(text) => JsonValue::String(text.to_string()),
        AMQPValue::LongString(text) => {
            JsonValue::String(String::from_utf8_lossy(text.as_bytes()).into())
        }
        AMQPValue::FieldArray(array) => JsonValue::Array(
            array
                .as_slice()
                .iter()
                .map(amqp_value_to_json)
                .collect::<Vec<_>>(),
        ),
        AMQPValue::Timestamp(ts) => JsonValue::Number(JsonNumber::from(*ts)),
        AMQPValue::FieldTable(table) => JsonValue::Object(field_table_to_json(table)),
        AMQPValue::ByteArray(bytes) => JsonValue::String(BASE64_STANDARD.encode(bytes.as_slice())),
        AMQPValue::Void => JsonValue::Null,
    }
}

fn basic_properties_to_json(properties: &BasicProperties) -> JsonMap<String, JsonValue> {
    let mut map = JsonMap::new();

    if let Some(value) = properties.content_type().as_ref() {
        map.insert(
            "content_type".to_string(),
            JsonValue::String(value.to_string()),
        );
    }
    if let Some(value) = properties.content_encoding().as_ref() {
        map.insert(
            "content_encoding".to_string(),
            JsonValue::String(value.to_string()),
        );
    }
    if let Some(value) = properties.delivery_mode().as_ref() {
        map.insert(
            "delivery_mode".to_string(),
            JsonValue::Number(JsonNumber::from(u64::from(*value))),
        );
    }
    if let Some(value) = properties.priority().as_ref() {
        map.insert(
            "priority".to_string(),
            JsonValue::Number(JsonNumber::from(*value as u64)),
        );
    }
    if let Some(value) = properties.correlation_id().as_ref() {
        map.insert(
            "correlation_id".to_string(),
            JsonValue::String(value.to_string()),
        );
    }
    if let Some(value) = properties.reply_to().as_ref() {
        map.insert("reply_to".to_string(), JsonValue::String(value.to_string()));
    }
    if let Some(value) = properties.expiration().as_ref() {
        map.insert(
            "expiration".to_string(),
            JsonValue::String(value.to_string()),
        );
    }
    if let Some(value) = properties.message_id().as_ref() {
        map.insert(
            "message_id".to_string(),
            JsonValue::String(value.to_string()),
        );
    }
    if let Some(value) = properties.user_id().as_ref() {
        map.insert("user_id".to_string(), JsonValue::String(value.to_string()));
    }
    if let Some(value) = properties.app_id().as_ref() {
        map.insert("app_id".to_string(), JsonValue::String(value.to_string()));
    }
    if let Some(value) = properties.cluster_id().as_ref() {
        map.insert(
            "cluster_id".to_string(),
            JsonValue::String(value.to_string()),
        );
    }
    if let Some(value) = properties.kind().as_ref() {
        map.insert("type".to_string(), JsonValue::String(value.to_string()));
    }

    if let Some(value) = properties.timestamp().as_ref() {
        map.insert(
            "timestamp".to_string(),
            JsonValue::Number(JsonNumber::from(*value)),
        );
    }

    if let Some(headers) = properties.headers().as_ref() {
        map.insert(
            "headers".to_string(),
            JsonValue::Object(field_table_to_json(headers)),
        );
    }

    map
}

fn json_to_field_table(value: &JsonValue) -> Result<FieldTable> {
    match value {
        JsonValue::Null => Ok(FieldTable::default()),
        JsonValue::Object(map) => {
            let mut inner = BTreeMap::new();
            for (key, value) in map {
                inner.insert(
                    ShortString::from(key.as_str()),
                    json_value_to_amqp_value(value)?,
                );
            }
            Ok(FieldTable::from(inner))
        }
        other => Err(crate::err!(
            "rabbitmq headers must be an object (got {other})"
        )),
    }
}

fn json_value_to_amqp_value(value: &JsonValue) -> Result<AMQPValue> {
    match value {
        JsonValue::Null => Ok(AMQPValue::Void),
        JsonValue::Bool(b) => Ok(AMQPValue::Boolean(*b)),
        JsonValue::Number(num) => {
            if let Some(v) = num.as_i64() {
                Ok(AMQPValue::LongLongInt(v))
            } else if let Some(v) = num.as_u64() {
                if v <= u32::MAX as u64 {
                    Ok(AMQPValue::LongUInt(v as u32))
                } else if v <= i64::MAX as u64 {
                    Ok(AMQPValue::LongLongInt(v as i64))
                } else {
                    Err(crate::err!(
                        "rabbitmq header value `{}` exceeds supported range",
                        v
                    ))
                }
            } else if let Some(v) = num.as_f64() {
                if v.is_finite() {
                    Ok(AMQPValue::Double(v))
                } else {
                    Err(crate::err!("invalid float value for rabbitmq header: {v}"))
                }
            } else {
                Err(crate::err!(
                    "invalid numeric value for rabbitmq header: {num}"
                ))
            }
        }
        JsonValue::String(text) => Ok(AMQPValue::LongString(LongString::from(text.clone()))),
        JsonValue::Array(values) => {
            let mut items = Vec::with_capacity(values.len());
            for item in values {
                items.push(json_value_to_amqp_value(item)?);
            }
            Ok(AMQPValue::FieldArray(FieldArray::from(items)))
        }
        JsonValue::Object(_) => Ok(AMQPValue::FieldTable(json_to_field_table(value)?)),
    }
}

fn json_to_basic_properties(value: &JsonValue) -> Result<BasicProperties> {
    let mut properties = BasicProperties::default();

    let map = match value {
        JsonValue::Null => return Ok(properties),
        JsonValue::Object(map) => map,
        other => {
            return Err(crate::err!(
                "rabbitmq properties must be an object (got {other})"
            ))
        }
    };

    if let Some(content_type) = map.get("content_type").and_then(JsonValue::as_str) {
        properties = properties.with_content_type(ShortString::from(content_type));
    }

    if let Some(content_encoding) = map.get("content_encoding").and_then(JsonValue::as_str) {
        properties = properties.with_content_encoding(ShortString::from(content_encoding));
    }

    if let Some(delivery_mode) = map.get("delivery_mode") {
        if let Some(mode) = delivery_mode.as_u64() {
            properties = properties.with_delivery_mode(mode as u8);
        }
    }

    if let Some(priority) = map.get("priority").and_then(JsonValue::as_i64) {
        properties = properties.with_priority(priority as u8);
    }

    if let Some(correlation_id) = map.get("correlation_id").and_then(JsonValue::as_str) {
        properties = properties.with_correlation_id(ShortString::from(correlation_id));
    }

    if let Some(reply_to) = map.get("reply_to").and_then(JsonValue::as_str) {
        properties = properties.with_reply_to(ShortString::from(reply_to));
    }

    if let Some(expiration) = map.get("expiration").and_then(JsonValue::as_str) {
        properties = properties.with_expiration(ShortString::from(expiration));
    }

    if let Some(message_id) = map.get("message_id").and_then(JsonValue::as_str) {
        properties = properties.with_message_id(ShortString::from(message_id));
    }

    if let Some(user_id) = map.get("user_id").and_then(JsonValue::as_str) {
        properties = properties.with_user_id(ShortString::from(user_id));
    }

    if let Some(app_id) = map.get("app_id").and_then(JsonValue::as_str) {
        properties = properties.with_app_id(ShortString::from(app_id));
    }

    if let Some(cluster_id) = map.get("cluster_id").and_then(JsonValue::as_str) {
        properties = properties.with_cluster_id(ShortString::from(cluster_id));
    }

    if let Some(type_value) = map.get("type").and_then(JsonValue::as_str) {
        properties = properties.with_type(ShortString::from(type_value));
    }

    if let Some(timestamp) = map.get("timestamp").and_then(JsonValue::as_i64) {
        properties = properties.with_timestamp(timestamp as Timestamp);
    }

    Ok(properties)
}

pub struct RabbitmqPublishParams<'a> {
    pub connector: &'a str,
    pub exchange: Option<&'a str>,
    pub routing_key: Option<&'a str>,
    pub payload: &'a JsonValue,
    pub headers: &'a JsonValue,
    pub properties: &'a JsonValue,
    pub mandatory: Option<bool>,
    pub confirm: Option<bool>,
    pub timeout_ms: Option<u64>,
}

pub async fn dispatch_rabbitmq_publish(
    factory: &ConnectorFactoryRegistry,
    params: RabbitmqPublishParams<'_>,
) -> Result<()> {
    #[cfg(not(feature = "rabbitmq"))]
    {
        let _ = (factory, params);
        return Err(crate::err!("rabbitmq support is disabled in this build"));
    }

    #[cfg(feature = "rabbitmq")]
    {
        let RabbitmqPublishParams {
            connector,
            exchange,
            routing_key,
            payload,
            headers,
            properties,
            mandatory,
            confirm,
            timeout_ms,
        } = params;

        let publisher = factory.rabbitmq_publisher(connector).await.map_err(|err| {
            crate::err!("failed to acquire rabbitmq publisher `{connector}`: {err}")
        })?;

        let mut properties = json_to_basic_properties(properties)?;
        let headers_table = json_to_field_table(headers)?;
        if !headers_table.inner().is_empty() {
            properties = properties.with_headers(headers_table);
        }

        let payload_bytes = payload_to_bytes(payload)?;
        let exchange = exchange.unwrap_or_default();
        let routing_key = routing_key.unwrap_or_default();
        let publish_options = BasicPublishOptions {
            mandatory: mandatory.unwrap_or(false),
            immediate: false,
        };
        publish_with_timeout("rabbitmq", timeout_ms, |timeout| {
            let publisher = Arc::clone(&publisher);
            let payload = payload_bytes.clone();
            async move {
                publisher
                    .publish(
                        exchange,
                        routing_key,
                        &payload,
                        properties,
                        publish_options,
                        confirm.unwrap_or(true),
                        timeout,
                    )
                    .await
            }
        })
        .await
        .map_err(|err| crate::err!("rabbitmq publish failed: {err}"))?;

        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum RabbitmqTriggerError {
    #[error("chronicle `{chronicle}` rabbitmq trigger requires connector `{connector}`")]
    MissingConnector {
        chronicle: String,
        connector: String,
    },
    #[error("chronicle `{chronicle}` rabbitmq trigger options must be an object")]
    InvalidTriggerOptions { chronicle: String },
    #[error("chronicle `{chronicle}` rabbitmq trigger missing option `{option}`")]
    MissingTriggerOption { chronicle: String, option: String },
    #[error("chronicle `{chronicle}` rabbitmq trigger has invalid ack_mode `{value}`")]
    InvalidAckMode { chronicle: String, value: String },
    #[error("chronicle `{chronicle}` rabbitmq trigger has invalid prefetch `{value}`")]
    InvalidPrefetch { chronicle: String, value: String },
    #[error("failed to build rabbitmq consumer for chronicle `{chronicle}`: {reason}")]
    ConsumerBuild { chronicle: String, reason: String },
    #[error("failed to configure prefetch for chronicle `{chronicle}`: {reason}")]
    ConfigurePrefetch { chronicle: String, reason: String },
}

impl RabbitmqTriggerError {
    pub fn missing_connector(chronicle: &str, connector: &str) -> Self {
        RabbitmqTriggerError::MissingConnector {
            chronicle: chronicle.to_string(),
            connector: connector.to_string(),
        }
    }
}

pub fn is_rabbitmq_trigger(chronicle: &ChronicleDefinition, registry: &ConnectorRegistry) -> bool {
    registry.rabbitmq(&chronicle.trigger.connector).is_some() && chronicle.trigger.kafka.is_none()
}
