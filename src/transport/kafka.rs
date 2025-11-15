#![forbid(unsafe_code)]

use crate::chronicle::retry_runner::{run_retry_loop, RetryContext};
use crate::chronicle_event;
use crate::error::Result;
use crate::readiness::DependencyHealth;
use crate::readiness::ReadinessController;
use crate::transport::kafka_util::commit_with_logging;
use crate::transport::readiness_gate::RouteReadinessGate;
use crate::transport::{TaskTransportRuntime, TransportKind, TransportRuntime};
use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use chronicle_core::chronicle::engine::ChronicleEngine;
use chronicle_core::chronicle::trigger_common::RetrySettings;
use chronicle_core::config::integration::{ChronicleDefinition, IntegrationConfig};
use chronicle_core::integration::registry::{ConnectorRegistry, KafkaConnector};
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

pub struct KafkaTriggerRuntime {
    inner: TaskTransportRuntime,
    consumer_count: usize,
}

impl KafkaTriggerRuntime {
    pub async fn build_with<C, F>(
        config: Arc<IntegrationConfig>,
        registry: Arc<ConnectorRegistry>,
        engine: Arc<ChronicleEngine>,
        readiness: Option<ReadinessController>,
        dependency_health: Option<DependencyHealth>,
        mut factory: F,
    ) -> Result<Self, KafkaTriggerError>
    where
        C: KafkaConsumer + Send,
        F: FnMut(KafkaConsumerConfig) -> Result<C, KafkaConsumerError>,
    {
        let mut consumers = Vec::new();

        for chronicle in &config.chronicles {
            if chronicle.trigger.kafka.is_none() {
                continue;
            }

            let connector = registry
                .kafka(&chronicle.trigger.connector)
                .cloned()
                .ok_or_else(|| KafkaTriggerError::MissingConnector {
                    chronicle: chronicle.name.clone(),
                    connector: chronicle.trigger.connector.clone(),
                })?;

            let options = KafkaTriggerOptions::from_trigger(chronicle)?;
            let consumer_config = KafkaConsumerConfig::new(&chronicle.name, &connector, &options);
            let retry = RetrySettings::from_extras(
                &consumer_config.trigger_extra,
                &consumer_config.connector_extra,
            );

            let mut consumer = factory(consumer_config.clone()).map_err(|err| {
                KafkaTriggerError::ConsumerBuild {
                    chronicle: chronicle.name.clone(),
                    reason: err.to_string(),
                }
            })?;

            let topics = vec![options.topic.clone()];
            consumer.subscribe(&topics).await.map_err(|err| {
                KafkaTriggerError::ConsumerSubscribe {
                    chronicle: chronicle.name.clone(),
                    topic: options.topic.clone(),
                    reason: err.to_string(),
                }
            })?;

            consumers.push(KafkaTriggerInstance {
                chronicle: chronicle.name.clone(),
                connector: connector.name.clone(),
                topic: options.topic.clone(),
                consumer,
                retry,
                dependency_health: dependency_health.clone(),
                gate: RouteReadinessGate::new(readiness.clone(), &chronicle.name),
                disconnected: false,
            });
        }

        let consumer_count = consumers.len();
        let engine_shared = Arc::clone(&engine);

        let inner = TaskTransportRuntime::new(TransportKind::KafkaIn, "kafka", move |shutdown| {
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
pub trait KafkaConsumer: Send + 'static {
    async fn subscribe(&mut self, topics: &[String]) -> Result<(), KafkaConsumerError>;
    async fn poll(&mut self) -> Result<Option<KafkaRecord>, KafkaConsumerError>;
    async fn commit(&mut self, record: &KafkaRecord) -> Result<(), KafkaConsumerError>;
}

#[derive(Debug, Clone)]
pub struct KafkaConsumerError {
    message: String,
}

impl std::fmt::Display for KafkaConsumerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for KafkaConsumerError {}

#[derive(Clone, Debug)]
pub struct KafkaConsumerConfig {
    pub chronicle: String,
    pub connector: String,
    pub brokers: Vec<String>,
    pub topic: String,
    pub group_id: Option<String>,
    pub auto_offset_reset: Option<String>,
    pub trigger_extra: JsonMap<String, JsonValue>,
    pub connector_extra: JsonMap<String, JsonValue>,
}

impl KafkaConsumerConfig {
    fn new(
        chronicle_name: &str,
        connector: &KafkaConnector,
        options: &KafkaTriggerOptions,
    ) -> Self {
        Self {
            chronicle: chronicle_name.to_string(),
            connector: connector.name.clone(),
            brokers: connector.brokers.clone(),
            topic: options.topic.clone(),
            group_id: options.group_id.clone(),
            auto_offset_reset: options.auto_offset_reset.clone(),
            trigger_extra: options.extra.clone(),
            connector_extra: map_from_btree(&connector.extra),
        }
    }
}

struct KafkaTriggerInstance<C>
where
    C: KafkaConsumer + Send,
{
    chronicle: String,
    connector: String,
    topic: String,
    consumer: C,
    gate: RouteReadinessGate,
    retry: RetrySettings,
    dependency_health: Option<DependencyHealth>,
    disconnected: bool,
}

impl<C> KafkaTriggerInstance<C>
where
    C: KafkaConsumer + Send,
{
    async fn run(self, engine: Arc<ChronicleEngine>, shutdown: CancellationToken) {
        let retry = self.retry.clone();
        let context_shutdown = shutdown.clone();
        let mut context = KafkaRetryContext {
            instance: self,
            engine,
            shutdown: context_shutdown,
        };

        run_retry_loop(shutdown, retry, Duration::from_millis(50), &mut context).await;
    }

    fn mark_connected(&mut self) {
        if self.disconnected {
            tracing::info!(
                target: "chronicle::kafka",
                event = "transport_reconnected",
                chronicle = self.chronicle,
                connector = self.connector,
                topic = self.topic
            );
            self.disconnected = false;
        }
    }

    fn mark_disconnected(&mut self, err: &KafkaConsumerError) {
        if !self.disconnected {
            tracing::warn!(
                target: "chronicle::kafka",
                event = "transport_disconnected",
                chronicle = self.chronicle,
                connector = self.connector,
                topic = self.topic,
                error = %err
            );
            self.disconnected = true;
        }
    }

    async fn handle_record(
        &mut self,
        engine: &Arc<ChronicleEngine>,
        record: KafkaRecord,
        shutdown: &CancellationToken,
    ) {
        if !self.gate.wait(shutdown).await {
            return;
        }

        let payload = build_trigger_payload(&record);

        tracing::info!(
            target: "chronicle::kafka",
            event = "trigger_received",
            chronicle = self.chronicle,
            topic = record.topic,
            partition = record.partition,
            offset = record.offset
        );

        match engine.execute(&self.chronicle, payload) {
            Ok(_) => {
                chronicle_event!(
                    info,
                    "chronicle::kafka",
                    "trigger_completed",
                    connector = &self.connector,
                    chronicle = &self.chronicle,
                    topic = self.topic,
                    offset = record.offset
                );

                let offset = record.offset;
                commit_with_logging(
                    || self.consumer.commit(&record),
                    |err| {
                        tracing::error!(
                            target: "chronicle::kafka",
                            event = "offset_commit_failed",
                            chronicle = %self.chronicle,
                            topic = %self.topic,
                            offset = offset,
                            error = %err
                        );
                    },
                )
                .await;
            }
            Err(err) => {
                chronicle_event!(
                    error,
                    "chronicle::kafka",
                    "trigger_failed",
                    connector = &self.connector,
                    chronicle = &self.chronicle,
                    topic = self.topic,
                    offset = record.offset,
                    error = err
                );
            }
        }
    }
}

struct KafkaRetryContext<C>
where
    C: KafkaConsumer + Send,
{
    instance: KafkaTriggerInstance<C>,
    engine: Arc<ChronicleEngine>,
    shutdown: CancellationToken,
}

#[async_trait]
impl<C> RetryContext for KafkaRetryContext<C>
where
    C: KafkaConsumer + Send,
{
    type Item = KafkaRecord;
    type Error = KafkaConsumerError;

    async fn poll(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        self.instance.consumer.poll().await
    }

    async fn handle_item(&mut self, item: Self::Item) {
        let dependency_health = self.instance.dependency_health.clone();
        let connector = self.instance.connector.clone();
        if let Some(health) = dependency_health {
            health.report_outcome(&connector, true).await;
        }
        self.instance.mark_connected();
        self.instance
            .handle_record(&self.engine, item, &self.shutdown)
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
            target: "chronicle::kafka",
            event = "consumer_poll_failed",
            chronicle = %self.instance.chronicle,
            topic = %self.instance.topic,
            error = %error
        );
    }
}

#[async_trait]
impl TransportRuntime for KafkaTriggerRuntime {
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

#[derive(Clone, Debug)]
pub struct KafkaRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<i64>,
    pub key: Option<Vec<u8>>,
    pub payload: Option<Vec<u8>>,
    pub headers: Vec<KafkaHeader>,
}

#[derive(Clone, Debug)]
pub struct KafkaHeader {
    pub key: String,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug)]
struct KafkaTriggerOptions {
    topic: String,
    group_id: Option<String>,
    auto_offset_reset: Option<String>,
    extra: JsonMap<String, JsonValue>,
}

impl KafkaTriggerOptions {
    fn from_trigger(chronicle: &ChronicleDefinition) -> Result<Self, KafkaTriggerError> {
        let raw = chronicle.trigger.kafka_json().ok_or_else(|| {
            KafkaTriggerError::InvalidKafkaOptions {
                chronicle: chronicle.name.clone(),
            }
        })?;

        let mut options =
            raw.as_object()
                .cloned()
                .ok_or_else(|| KafkaTriggerError::InvalidKafkaOptions {
                    chronicle: chronicle.name.clone(),
                })?;

        let topic = options
            .get("topic")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| KafkaTriggerError::MissingKafkaOption {
                chronicle: chronicle.name.clone(),
                option: "topic".to_string(),
            })?
            .to_string();

        let group_id = options
            .get("group_id")
            .and_then(JsonValue::as_str)
            .map(|value| value.to_string());

        let auto_offset_reset = options
            .get("auto_offset_reset")
            .and_then(JsonValue::as_str)
            .map(|value| value.to_string());

        options.remove("topic");
        options.remove("group_id");
        options.remove("auto_offset_reset");

        Ok(Self {
            topic,
            group_id,
            auto_offset_reset,
            extra: options,
        })
    }
}

#[derive(Debug, Error)]
pub enum KafkaTriggerError {
    #[error("chronicle `{chronicle}` kafka trigger requires connector `{connector}`")]
    MissingConnector {
        chronicle: String,
        connector: String,
    },
    #[error("chronicle `{chronicle}` kafka trigger options must be an object")]
    InvalidKafkaOptions { chronicle: String },
    #[error("chronicle `{chronicle}` kafka trigger missing option `{option}`")]
    MissingKafkaOption { chronicle: String, option: String },
    #[error("failed to build kafka consumer for chronicle `{chronicle}`: {reason}")]
    ConsumerBuild { chronicle: String, reason: String },
    #[error(
        "consumer for chronicle `{chronicle}` failed to subscribe to topic `{topic}`: {reason}"
    )]
    ConsumerSubscribe {
        chronicle: String,
        topic: String,
        reason: String,
    },
}

fn build_trigger_payload(record: &KafkaRecord) -> JsonValue {
    let mut root = JsonMap::new();
    root.insert("topic".to_string(), JsonValue::String(record.topic.clone()));
    root.insert(
        "partition".to_string(),
        JsonValue::Number(record.partition.into()),
    );
    root.insert(
        "offset".to_string(),
        JsonValue::Number(record.offset.into()),
    );

    if let Some(ts) = record.timestamp {
        root.insert("timestamp".to_string(), JsonValue::Number(ts.into()));
    }

    let mut headers_map = JsonMap::new();
    for header in &record.headers {
        let value = match std::str::from_utf8(&header.value) {
            Ok(text) => JsonValue::String(text.to_string()),
            Err(_) => JsonValue::String(BASE64_STANDARD.encode(&header.value)),
        };
        merge_header_value(&mut headers_map, &header.key, value);
    }

    if let Some(parsed) = record.key.as_ref().map(|bytes| parse_binary(bytes)) {
        let mut key_map = JsonMap::new();
        key_map.insert(
            "base64".to_string(),
            JsonValue::String(parsed.base64.clone()),
        );
        if let Some(text) = parsed.text.clone() {
            key_map.insert("text".to_string(), JsonValue::String(text));
        }
        if let Some(json_value) = parsed.json.clone() {
            key_map.insert("json".to_string(), json_value);
        }
        root.insert("key".to_string(), JsonValue::Object(key_map));
    }

    let mut payload_header_override: Option<JsonMap<String, JsonValue>> = None;

    if let Some(parsed) = record.payload.as_ref().map(|bytes| parse_binary(bytes)) {
        let mut value_map = JsonMap::new();
        value_map.insert(
            "base64".to_string(),
            JsonValue::String(parsed.base64.clone()),
        );

        if let Some(text) = parsed.text.clone() {
            value_map.insert("text".to_string(), JsonValue::String(text.clone()));
        }

        if let Some(json_value) = parsed.json.clone() {
            value_map.insert("json".to_string(), json_value.clone());

            if let Some(body) = json_value
                .as_object()
                .and_then(|map| map.get("body"))
                .cloned()
            {
                root.insert("body".to_string(), body);
            } else {
                root.insert("body".to_string(), json_value.clone());
            }

            if let Some(json_header) = json_value
                .as_object()
                .and_then(|map| map.get("header"))
                .and_then(JsonValue::as_object)
            {
                payload_header_override = Some(json_header.clone());
            }
        } else if let Some(text) = parsed.text.clone() {
            root.insert("body".to_string(), JsonValue::String(text));
        } else {
            root.insert("body".to_string(), JsonValue::Null);
        }

        root.insert("value".to_string(), JsonValue::Object(value_map.clone()));
        root.insert("payload".to_string(), JsonValue::Object(value_map));
    } else {
        root.insert("body".to_string(), JsonValue::Null);
        root.insert("value".to_string(), JsonValue::Null);
        root.insert("payload".to_string(), JsonValue::Null);
    }

    if let Some(mut header_override) = payload_header_override {
        for (key, value) in &headers_map {
            merge_header_value(&mut header_override, key, value.clone());
        }
        let headers_clone = header_override.clone();
        root.insert("headers".to_string(), JsonValue::Object(header_override));
        root.insert("header".to_string(), JsonValue::Object(headers_clone));
    } else if !headers_map.is_empty() {
        let headers_clone = headers_map.clone();
        root.insert("headers".to_string(), JsonValue::Object(headers_map));
        root.insert("header".to_string(), JsonValue::Object(headers_clone));
    }

    JsonValue::Object(root)
}

fn merge_header_value(map: &mut JsonMap<String, JsonValue>, key: &str, value: JsonValue) {
    match map.get_mut(key) {
        Some(JsonValue::Array(values)) => {
            if !values.iter().any(|existing| existing == &value) {
                values.push(value);
            }
        }
        Some(existing) => {
            if *existing == value {
                return;
            }
            let current = existing.clone();
            *existing = JsonValue::Array(vec![current, value]);
        }
        None => {
            map.insert(key.to_string(), value);
        }
    }
}

#[derive(Clone, Debug)]
struct ParsedBinary {
    base64: String,
    text: Option<String>,
    json: Option<JsonValue>,
}

fn parse_binary(bytes: &[u8]) -> ParsedBinary {
    let base64 = BASE64_STANDARD.encode(bytes);

    let mut text = None;
    let mut json_value = None;

    if let Ok(str_value) = std::str::from_utf8(bytes) {
        text = Some(str_value.to_string());
        if let Ok(parsed) = serde_json::from_str::<JsonValue>(str_value) {
            json_value = Some(parsed);
        }
    }

    ParsedBinary {
        base64,
        text,
        json: json_value,
    }
}

fn map_from_btree(map: &BTreeMap<String, JsonValue>) -> JsonMap<String, JsonValue> {
    map.iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}
