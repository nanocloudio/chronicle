#![forbid(unsafe_code)]

use crate::chronicle::retry_runner::{run_retry_loop, RetryContext};
use crate::error::Result;
use crate::integration::factory::ConnectorFactoryRegistry;
use crate::readiness::DependencyHealth;
use crate::readiness::ReadinessController;
use crate::transport::broker::{decode_payload, publish_with_timeout};
use crate::transport::readiness_gate::RouteReadinessGate;
use crate::transport::{TaskTransportRuntime, TransportKind, TransportRuntime};
use async_trait::async_trait;
use chronicle_core::chronicle::engine::ChronicleEngine;
use chronicle_core::chronicle::trigger_common::{map_from_btree, parse_binary, RetrySettings};
use chronicle_core::config::integration::{ChronicleDefinition, IntegrationConfig, ScalarValue};
use chronicle_core::integration::registry::{ConnectorRegistry, MqttConnector};
use chronicle_core::metrics::metrics;
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, Publish, QoS, Transport};
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use std::collections::HashMap;
use std::fs;
use std::future::Future;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::result::Result as StdResult;
use std::sync::Arc;
use thiserror::Error;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

pub struct MqttTriggerRuntime<S>
where
    S: MqttSubscriber + Send,
{
    inner: TaskTransportRuntime,
    subscriber_count: usize,
    _marker: PhantomData<S>,
}

impl<S> MqttTriggerRuntime<S>
where
    S: MqttSubscriber + Send,
{
    pub async fn build_with<F, Fut>(
        config: Arc<IntegrationConfig>,
        registry: Arc<ConnectorRegistry>,
        engine: Arc<ChronicleEngine>,
        readiness: Option<ReadinessController>,
        dependency_health: Option<DependencyHealth>,
        mut factory: F,
    ) -> Result<Self, MqttTriggerError>
    where
        F: FnMut(MqttSubscriberConfig) -> Fut,
        Fut: Future<Output = StdResult<S, MqttSubscriberError>>,
    {
        let mut subscribers = Vec::new();

        for chronicle in &config.chronicles {
            let declared_type = chronicle
                .trigger
                .option("type")
                .and_then(ScalarValue::as_str)
                .map(|value| value.to_ascii_lowercase());

            let connector_handle = registry.mqtt(&chronicle.trigger.connector);

            let is_mqtt = match declared_type.as_deref() {
                Some("mqtt") => true,
                Some(_) => false,
                None => connector_handle.is_some(),
            };

            if !is_mqtt {
                continue;
            }

            let connector = connector_handle.cloned().ok_or_else(|| {
                MqttTriggerError::missing_connector(&chronicle.name, &chronicle.trigger.connector)
            })?;

            let options = MqttTriggerOptions::from_trigger(chronicle)?;
            let subscriber_config =
                MqttSubscriberConfig::new(&chronicle.name, &connector, &options);
            let retry_settings = subscriber_config.retry.clone();

            let mut subscriber = factory(subscriber_config.clone()).await.map_err(|err| {
                MqttTriggerError::SubscriberBuild {
                    chronicle: chronicle.name.clone(),
                    reason: err.to_string(),
                }
            })?;

            subscriber
                .subscribe(&options.topic, options.qos, options.retain_handling)
                .await
                .map_err(|err| MqttTriggerError::Subscribe {
                    chronicle: chronicle.name.clone(),
                    topic: options.topic.clone(),
                    reason: err.to_string(),
                })?;

            subscribers.push(MqttTriggerInstance {
                chronicle: chronicle.name.clone(),
                connector: connector.name.clone(),
                topic: options.topic.clone(),
                qos: options.qos,
                retain_handling: options.retain_handling,
                client_id: connector.client_id.clone(),
                subscriber,
                retry: retry_settings,
                dependency_health: dependency_health.clone(),
                gate: RouteReadinessGate::new(readiness.clone(), &chronicle.name),
                disconnected: false,
            });
        }

        let subscriber_count = subscribers.len();
        let engine_shared = Arc::clone(&engine);
        let inner = TaskTransportRuntime::new(TransportKind::MqttIn, "mqtt", move |shutdown| {
            subscribers
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
            subscriber_count,
            _marker: PhantomData,
        })
    }

    pub fn subscriber_count(&self) -> usize {
        self.subscriber_count
    }
}

#[async_trait]
pub trait MqttSubscriber: Send + 'static {
    async fn subscribe(
        &mut self,
        topic: &str,
        qos: u8,
        retain_handling: RetainHandling,
    ) -> StdResult<(), MqttSubscriberError>;

    async fn next_message(&mut self) -> StdResult<Option<MqttMessage>, MqttSubscriberError>;

    async fn ack(&mut self, _packet_id: Option<u16>) -> StdResult<(), MqttSubscriberError> {
        Ok(())
    }

    async fn reconnect(&mut self) -> StdResult<(), MqttSubscriberError> {
        Ok(())
    }
}

pub struct RumqttcMqttSubscriber {
    config: MqttSubscriberConfig,
    client: AsyncClient,
    eventloop: EventLoop,
    pending: HashMap<u16, Publish>,
}

impl RumqttcMqttSubscriber {
    pub async fn connect(config: MqttSubscriberConfig) -> StdResult<Self, MqttSubscriberError> {
        let manual_acks = config.qos > 0;
        let options = build_mqtt_options_from_config(&config, manual_acks)?;
        let (client, eventloop) = AsyncClient::new(options, 10);

        Ok(Self {
            config,
            client,
            eventloop,
            pending: HashMap::new(),
        })
    }
}

#[async_trait]
impl MqttSubscriber for RumqttcMqttSubscriber {
    async fn subscribe(
        &mut self,
        topic: &str,
        qos: u8,
        _retain_handling: RetainHandling,
    ) -> StdResult<(), MqttSubscriberError> {
        let qos = qos_from_u8(qos)?;
        self.client
            .subscribe(topic.to_string(), qos)
            .await
            .map_err(|err| {
                MqttSubscriberError::new(format!("failed to subscribe to `{topic}`: {err}"))
            })
    }

    async fn next_message(&mut self) -> StdResult<Option<MqttMessage>, MqttSubscriberError> {
        loop {
            match self.eventloop.poll().await {
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    let qos = publish.qos;
                    let packet_id = match qos {
                        QoS::AtMostOnce => None,
                        _ => {
                            if publish.pkid != 0 {
                                self.pending.insert(publish.pkid, publish.clone());
                                Some(publish.pkid)
                            } else {
                                None
                            }
                        }
                    };

                    let message = MqttMessage::new(
                        publish.topic.clone(),
                        publish.payload.to_vec(),
                        qos as u8,
                        publish.retain,
                        packet_id,
                        None,
                    );

                    return Ok(Some(message));
                }
                Ok(Event::Incoming(_)) | Ok(Event::Outgoing(_)) => continue,
                Err(err) => {
                    return Err(MqttSubscriberError::new(format!(
                        "mqtt event loop error: {err}"
                    )))
                }
            }
        }
    }

    async fn ack(&mut self, packet_id: Option<u16>) -> StdResult<(), MqttSubscriberError> {
        if let Some(id) = packet_id {
            if let Some(publish) = self.pending.remove(&id) {
                self.client.ack(&publish).await.map_err(|err| {
                    MqttSubscriberError::new(format!("mqtt ack failed for packet {id}: {err}"))
                })?;
            }
        }

        Ok(())
    }

    async fn reconnect(&mut self) -> StdResult<(), MqttSubscriberError> {
        let manual_acks = self.config.qos > 0;
        let options = build_mqtt_options_from_config(&self.config, manual_acks)?;
        let (client, eventloop) = AsyncClient::new(options, 10);
        let qos = qos_from_u8(self.config.qos)?;
        client
            .subscribe(self.config.topic.clone(), qos)
            .await
            .map_err(|err| MqttSubscriberError::new(format!("mqtt resubscribe failed: {err}")))?;

        self.client = client;
        self.eventloop = eventloop;
        self.pending.clear();

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct MqttSubscriberError {
    message: String,
}

impl MqttSubscriberError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for MqttSubscriberError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for MqttSubscriberError {}

fn build_mqtt_options_from_config(
    config: &MqttSubscriberConfig,
    manual_acks: bool,
) -> StdResult<MqttOptions, MqttSubscriberError> {
    build_mqtt_options(
        &config.url,
        config.client_id.as_deref(),
        config.username.as_deref(),
        config.password.as_deref(),
        config.keep_alive,
        config.tls.as_ref(),
        manual_acks,
    )
}

fn build_mqtt_options(
    url: &str,
    client_id: Option<&str>,
    username: Option<&str>,
    password: Option<&str>,
    keep_alive: Option<u64>,
    tls: Option<&MqttSubscriberTlsConfig>,
    manual_acks: bool,
) -> StdResult<MqttOptions, MqttSubscriberError> {
    let parsed = Url::parse(url)
        .map_err(|err| MqttSubscriberError::new(format!("invalid mqtt url `{url}`: {err}")))?;

    let host = parsed
        .host_str()
        .ok_or_else(|| MqttSubscriberError::new("mqtt url must specify host"))?;

    let scheme = parsed.scheme().to_ascii_lowercase();
    let default_port = match scheme.as_str() {
        "mqtt" | "tcp" => 1883,
        "mqtts" | "ssl" => 8883,
        other => {
            return Err(MqttSubscriberError::new(format!(
                "unsupported mqtt url scheme `{other}`"
            )))
        }
    };

    let port = parsed.port().unwrap_or(default_port);

    let client_id = client_id
        .filter(|id| !id.trim().is_empty())
        .map(|id| id.to_string())
        .unwrap_or_else(|| format!("chronicle-{}", Uuid::new_v4()));

    let mut options = MqttOptions::new(client_id, host, port);

    if let Some(seconds) = keep_alive {
        options.set_keep_alive(Duration::from_secs(seconds));
    }

    if let Some(user) = username {
        let pass = password.unwrap_or("");
        options.set_credentials(user, pass);
    }

    if matches!(scheme.as_str(), "mqtts" | "ssl") || tls.is_some() {
        let transport = build_transport_from_tls(tls)?;
        options.set_transport(transport);
    }

    options.set_manual_acks(manual_acks);

    Ok(options)
}

fn build_transport_from_tls(
    tls: Option<&MqttSubscriberTlsConfig>,
) -> StdResult<Transport, MqttSubscriberError> {
    if let Some(tls) = tls {
        let ca_bytes = match &tls.ca {
            Some(path) => Some(read_file(path).map_err(|err| {
                MqttSubscriberError::new(format!(
                    "failed to read mqtt tls ca `{}`: {err}",
                    path.display()
                ))
            })?),
            None => None,
        };

        let client_auth = match (&tls.cert, &tls.key) {
            (Some(cert), Some(key)) => {
                let cert_bytes = read_file(cert).map_err(|err| {
                    MqttSubscriberError::new(format!(
                        "failed to read mqtt tls cert `{}`: {err}",
                        cert.display()
                    ))
                })?;
                let key_bytes = read_file(key).map_err(|err| {
                    MqttSubscriberError::new(format!(
                        "failed to read mqtt tls key `{}`: {err}",
                        key.display()
                    ))
                })?;
                Some((cert_bytes, key_bytes))
            }
            _ => None,
        };

        if let Some(ca) = ca_bytes {
            return Ok(Transport::tls(ca, client_auth, None));
        } else if client_auth.is_some() {
            return Ok(Transport::tls_with_default_config());
        }
    }

    Ok(Transport::tls_with_default_config())
}

fn read_file(path: &PathBuf) -> std::io::Result<Vec<u8>> {
    fs::read(path)
}

fn qos_from_u8(qos: u8) -> StdResult<QoS, MqttSubscriberError> {
    match qos {
        0 => Ok(QoS::AtMostOnce),
        1 => Ok(QoS::AtLeastOnce),
        2 => Ok(QoS::ExactlyOnce),
        other => Err(MqttSubscriberError::new(format!(
            "unsupported mqtt qos `{other}`"
        ))),
    }
}

#[derive(Clone, Debug)]
pub struct MqttSubscriberConfig {
    pub chronicle: String,
    pub connector: String,
    pub url: String,
    pub topic: String,
    pub qos: u8,
    pub retain_handling: RetainHandling,
    pub client_id: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub keep_alive: Option<u64>,
    pub tls: Option<MqttSubscriberTlsConfig>,
    pub trigger_extra: JsonMap<String, JsonValue>,
    pub connector_extra: JsonMap<String, JsonValue>,
    pub(crate) retry: RetrySettings,
}

impl MqttSubscriberConfig {
    fn new(chronicle_name: &str, connector: &MqttConnector, options: &MqttTriggerOptions) -> Self {
        let trigger_extra = options.extra.clone();
        let connector_extra = map_from_btree(&connector.extra);
        let retry = RetrySettings::from_extras(&trigger_extra, &connector_extra);

        let tls = connector.tls.as_ref().map(|tls| MqttSubscriberTlsConfig {
            ca: tls.ca.clone(),
            cert: tls.cert.clone(),
            key: tls.key.clone(),
        });

        Self {
            chronicle: chronicle_name.to_string(),
            connector: connector.name.clone(),
            url: connector.url.clone(),
            topic: options.topic.clone(),
            qos: options.qos,
            retain_handling: options.retain_handling,
            client_id: connector.client_id.clone(),
            username: connector.username.clone(),
            password: connector.password.clone(),
            keep_alive: connector.keep_alive,
            tls,
            trigger_extra,
            connector_extra,
            retry,
        }
    }
}

#[derive(Clone, Debug)]
pub struct MqttSubscriberTlsConfig {
    pub ca: Option<PathBuf>,
    pub cert: Option<PathBuf>,
    pub key: Option<PathBuf>,
}

struct MqttTriggerInstance<S>
where
    S: MqttSubscriber + Send,
{
    chronicle: String,
    connector: String,
    topic: String,
    qos: u8,
    retain_handling: RetainHandling,
    client_id: Option<String>,
    subscriber: S,
    retry: RetrySettings,
    dependency_health: Option<DependencyHealth>,
    gate: RouteReadinessGate,
    disconnected: bool,
}

impl<S> MqttTriggerInstance<S>
where
    S: MqttSubscriber + Send,
{
    fn mark_connected(&mut self) {
        if self.disconnected {
            tracing::info!(
                target: "chronicle::mqtt",
                event = "transport_reconnected",
                chronicle = %self.chronicle,
                connector = %self.connector,
                topic = %self.topic,
                client_id = %self.client_id.as_deref().unwrap_or("")
            );
            self.disconnected = false;
        }
    }

    fn mark_disconnected(&mut self, err: &MqttSubscriberError) {
        if !self.disconnected {
            tracing::warn!(
                target: "chronicle::mqtt",
                event = "transport_disconnected",
                chronicle = %self.chronicle,
                connector = %self.connector,
                topic = %self.topic,
                client_id = %self.client_id.as_deref().unwrap_or(""),
                error = %err
            );
            self.disconnected = true;
        }
    }

    async fn run(self, engine: Arc<ChronicleEngine>, shutdown: CancellationToken) {
        let retry = self.retry.clone();
        let context_shutdown = shutdown.clone();
        let mut context = MqttRetryContext {
            instance: self,
            engine,
            shutdown: context_shutdown,
        };

        run_retry_loop(shutdown, retry, Duration::from_millis(50), &mut context).await;
    }

    async fn handle_message(
        &mut self,
        engine: &Arc<ChronicleEngine>,
        message: MqttMessage,
        shutdown: &CancellationToken,
    ) {
        if !self.gate.wait(shutdown).await {
            return;
        }

        let counters = metrics();
        counters.inc_mqtt_trigger_inflight();

        let payload = build_trigger_payload(&message);

        tracing::info!(
            target: "chronicle::mqtt",
            event = "trigger_received",
            chronicle = %self.chronicle,
            connector = %self.connector,
            topic = %self.topic,
            client_id = %self.client_id.as_deref().unwrap_or(""),
            qos = message.qos,
            retain = message.retain
        );

        let packet_id = message.packet_id;
        let qos = message.qos;

        match engine.execute(&self.chronicle, payload) {
            Ok(_) => {
                tracing::info!(
                    target: "chronicle::mqtt",
                    event = "trigger_completed",
                    chronicle = %self.chronicle,
                    connector = %self.connector,
                    topic = %self.topic,
                    client_id = %self.client_id.as_deref().unwrap_or(""),
                    qos = qos
                );

                if qos > 0 {
                    if let Err(err) = self.subscriber.ack(packet_id).await {
                        tracing::error!(
                            target: "chronicle::mqtt",
                            event = "ack_failed",
                            chronicle = %self.chronicle,
                            connector = %self.connector,
                            topic = %self.topic,
                            client_id = %self.client_id.as_deref().unwrap_or(""),
                            packet_id = packet_id.unwrap_or_default(),
                            error = %err
                        );
                    }
                }
            }
            Err(err) => {
                tracing::error!(
                    target: "chronicle::mqtt",
                    event = "trigger_failed",
                    chronicle = %self.chronicle,
                    connector = %self.connector,
                    topic = %self.topic,
                    client_id = %self.client_id.as_deref().unwrap_or(""),
                    qos = qos,
                    error = %err
                );
            }
        }

        counters.dec_mqtt_trigger_inflight();
    }
}

struct MqttRetryContext<S>
where
    S: MqttSubscriber + Send,
{
    instance: MqttTriggerInstance<S>,
    engine: Arc<ChronicleEngine>,
    shutdown: CancellationToken,
}

#[async_trait]
impl<S> RetryContext for MqttRetryContext<S>
where
    S: MqttSubscriber + Send,
{
    type Item = MqttMessage;
    type Error = MqttSubscriberError;

    async fn poll(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        self.instance.subscriber.next_message().await
    }

    async fn handle_item(&mut self, message: Self::Item) {
        let dependency_health = self.instance.dependency_health.clone();
        let connector = self.instance.connector.clone();
        if let Some(health) = dependency_health {
            health.report_outcome(&connector, true).await;
        }
        self.instance.mark_connected();
        if self.instance.retain_handling.should_skip(message.retain) {
            if message.qos > 0 {
                if let Err(err) = self.instance.subscriber.ack(message.packet_id).await {
                    tracing::warn!(
                        target: "chronicle::mqtt",
                        event = "retain_ack_failed",
                        chronicle = %self.instance.chronicle,
                        connector = %self.instance.connector,
                        topic = %self.instance.topic,
                        client_id = %self.instance.client_id.as_deref().unwrap_or(""),
                        packet_id = message.packet_id.unwrap_or_default(),
                        error = %err
                    );
                }
            }
        } else {
            self.instance
                .handle_message(&self.engine, message, &self.shutdown)
                .await;
        }
    }

    async fn report_error(&mut self, error: &Self::Error, _delay: Duration) {
        let dependency_health = self.instance.dependency_health.clone();
        let connector = self.instance.connector.clone();
        if let Some(health) = dependency_health {
            health.report_outcome(&connector, false).await;
        }
        self.instance.mark_disconnected(error);
        tracing::error!(
            target: "chronicle::mqtt",
            event = "subscriber_receive_failed",
            chronicle = %self.instance.chronicle,
            connector = %self.instance.connector,
            topic = %self.instance.topic,
            client_id = %self.instance.client_id.as_deref().unwrap_or(""),
            error = %error
        );

        if let Err(reconnect_err) = self.instance.subscriber.reconnect().await {
            tracing::error!(
                target: "chronicle::mqtt",
                event = "subscriber_reconnect_failed",
                chronicle = %self.instance.chronicle,
                connector = %self.instance.connector,
                topic = %self.instance.topic,
                client_id = %self.instance.client_id.as_deref().unwrap_or(""),
                error = %reconnect_err
            );
        } else if let Err(resub_err) = self
            .instance
            .subscriber
            .subscribe(
                &self.instance.topic,
                self.instance.qos,
                self.instance.retain_handling,
            )
            .await
        {
            tracing::error!(
                target: "chronicle::mqtt",
                event = "subscriber_resubscribe_failed",
                chronicle = %self.instance.chronicle,
                connector = %self.instance.connector,
                topic = %self.instance.topic,
                client_id = %self.instance.client_id.as_deref().unwrap_or(""),
                error = %resub_err
            );
        } else {
            self.instance.mark_connected();
        }
    }
}

#[async_trait]
impl<S> TransportRuntime for MqttTriggerRuntime<S>
where
    S: MqttSubscriber + Send,
{
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
struct MqttTriggerOptions {
    topic: String,
    qos: u8,
    retain_handling: RetainHandling,
    extra: JsonMap<String, JsonValue>,
}

impl MqttTriggerOptions {
    fn from_trigger(chronicle: &ChronicleDefinition) -> Result<Self, MqttTriggerError> {
        let raw = chronicle.trigger.options_json();
        let mut options =
            raw.as_object()
                .cloned()
                .ok_or_else(|| MqttTriggerError::InvalidTriggerOptions {
                    chronicle: chronicle.name.clone(),
                })?;

        let topic_value =
            options
                .remove("topic")
                .ok_or_else(|| MqttTriggerError::MissingTriggerOption {
                    chronicle: chronicle.name.clone(),
                    option: "topic".to_string(),
                })?;

        let topic = topic_value
            .as_str()
            .ok_or_else(|| MqttTriggerError::InvalidTriggerOptions {
                chronicle: chronicle.name.clone(),
            })?
            .to_string();

        let qos = match options.remove("qos") {
            Some(value) => parse_qos(&chronicle.name, value)?,
            None => 0,
        };

        let retain_handling = match options.remove("retain_handling") {
            Some(value) => RetainHandling::parse(&chronicle.name, value)?,
            None => RetainHandling::Include,
        };

        Ok(Self {
            topic,
            qos,
            retain_handling,
            extra: options,
        })
    }
}

fn parse_qos(chronicle: &str, value: JsonValue) -> StdResult<u8, MqttTriggerError> {
    let number = match value {
        JsonValue::Number(num) => num.as_u64().ok_or_else(|| MqttTriggerError::InvalidQos {
            chronicle: chronicle.to_string(),
            value: num.to_string(),
        })?,
        JsonValue::String(text) => {
            text.parse::<u64>()
                .map_err(|_| MqttTriggerError::InvalidQos {
                    chronicle: chronicle.to_string(),
                    value: text,
                })?
        }
        other => {
            return Err(MqttTriggerError::InvalidQos {
                chronicle: chronicle.to_string(),
                value: other.to_string(),
            });
        }
    };

    if number > 2 {
        return Err(MqttTriggerError::InvalidQos {
            chronicle: chronicle.to_string(),
            value: number.to_string(),
        });
    }

    Ok(number as u8)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetainHandling {
    Include,
    Ignore,
}

impl RetainHandling {
    fn parse(chronicle: &str, value: JsonValue) -> Result<Self, MqttTriggerError> {
        match value {
            JsonValue::Number(num) => {
                let code = num
                    .as_i64()
                    .ok_or_else(|| MqttTriggerError::InvalidRetainHandling {
                        chronicle: chronicle.to_string(),
                        value: num.to_string(),
                    })?;
                match code {
                    0 => Ok(RetainHandling::Include),
                    1 | 2 => Ok(RetainHandling::Ignore),
                    other => Err(MqttTriggerError::InvalidRetainHandling {
                        chronicle: chronicle.to_string(),
                        value: other.to_string(),
                    }),
                }
            }
            JsonValue::String(text) => match text.to_ascii_lowercase().as_str() {
                "include" => Ok(RetainHandling::Include),
                "ignore" => Ok(RetainHandling::Ignore),
                other => Err(MqttTriggerError::InvalidRetainHandling {
                    chronicle: chronicle.to_string(),
                    value: other.to_string(),
                }),
            },
            other => Err(MqttTriggerError::InvalidRetainHandling {
                chronicle: chronicle.to_string(),
                value: other.to_string(),
            }),
        }
    }

    fn should_skip(&self, retain: bool) -> bool {
        matches!(self, RetainHandling::Ignore) && retain
    }
}

#[derive(Clone, Debug)]
pub struct MqttMessage {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
    pub retain: bool,
    pub packet_id: Option<u16>,
    pub timestamp: Option<i64>,
}

impl MqttMessage {
    pub fn new(
        topic: impl Into<String>,
        payload: Vec<u8>,
        qos: u8,
        retain: bool,
        packet_id: Option<u16>,
        timestamp: Option<i64>,
    ) -> Self {
        Self {
            topic: topic.into(),
            payload,
            qos,
            retain,
            packet_id,
            timestamp,
        }
    }
}

fn build_trigger_payload(message: &MqttMessage) -> JsonValue {
    let mut root = JsonMap::new();
    root.insert(
        "topic".to_string(),
        JsonValue::String(message.topic.clone()),
    );
    root.insert(
        "qos".to_string(),
        JsonValue::Number(JsonNumber::from(message.qos)),
    );
    root.insert("retain".to_string(), JsonValue::Bool(message.retain));

    let parsed = parse_binary(&message.payload);
    let mut payload_map = JsonMap::new();
    payload_map.insert(
        "base64".to_string(),
        JsonValue::String(parsed.base64.clone()),
    );
    if let Some(text) = parsed.text {
        payload_map.insert("text".to_string(), JsonValue::String(text));
    }
    if let Some(json_value) = parsed.json {
        payload_map.insert("json".to_string(), json_value);
    }
    root.insert("payload".to_string(), JsonValue::Object(payload_map));

    let mut metadata = JsonMap::new();
    if let Some(packet) = message.packet_id {
        metadata.insert(
            "packet_id".to_string(),
            JsonValue::Number(JsonNumber::from(packet)),
        );
    }
    if let Some(ts) = message.timestamp {
        metadata.insert(
            "timestamp".to_string(),
            JsonValue::Number(JsonNumber::from(ts)),
        );
    }
    if !metadata.is_empty() {
        root.insert("metadata".to_string(), JsonValue::Object(metadata));
    }

    JsonValue::Object(root)
}

pub struct MqttPublishParams<'a> {
    pub connector: &'a str,
    pub topic: &'a str,
    pub qos: u8,
    pub retain: bool,
    pub payload_encoding: &'a str,
    pub encoded_payload: &'a str,
    pub timeout_ms: Option<u64>,
}

pub async fn dispatch_mqtt_publish(
    factory: &ConnectorFactoryRegistry,
    params: MqttPublishParams<'_>,
) -> Result<()> {
    #[cfg(not(feature = "mqtt"))]
    {
        let _ = (factory, params);
        return Err(crate::err!("mqtt support is disabled in this build"));
    }

    #[cfg(feature = "mqtt")]
    {
        let MqttPublishParams {
            connector,
            topic,
            qos,
            retain,
            payload_encoding,
            encoded_payload,
            timeout_ms,
        } = params;

        let publisher = factory
            .mqtt_publisher(connector)
            .await
            .map_err(|err| crate::err!("failed to acquire mqtt publisher `{connector}`: {err}"))?;

        let qos = match qos {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            2 => QoS::ExactlyOnce,
            other => {
                return Err(crate::err!(
                    "mqtt publish requires qos 0, 1, or 2 (got {other})"
                ))
            }
        };

        let payload = decode_payload(encoded_payload, payload_encoding)?;
        publish_with_timeout("mqtt", timeout_ms, |timeout| {
            let publisher = Arc::clone(&publisher);
            let payload = payload.clone();
            async move {
                publisher
                    .publish(topic, payload, qos, retain, timeout)
                    .await
            }
        })
        .await
        .map_err(|err| crate::err!("mqtt publish failed: {err}"))?;

        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum MqttTriggerError {
    #[error("chronicle `{chronicle}` mqtt trigger requires connector `{connector}`")]
    MissingConnector {
        chronicle: String,
        connector: String,
    },
    #[error("chronicle `{chronicle}` mqtt trigger options must be an object")]
    InvalidTriggerOptions { chronicle: String },
    #[error("chronicle `{chronicle}` mqtt trigger missing option `{option}`")]
    MissingTriggerOption { chronicle: String, option: String },
    #[error("chronicle `{chronicle}` mqtt trigger has invalid qos `{value}`")]
    InvalidQos { chronicle: String, value: String },
    #[error("chronicle `{chronicle}` mqtt trigger has invalid retain_handling `{value}`")]
    InvalidRetainHandling { chronicle: String, value: String },
    #[error("failed to build mqtt subscriber for chronicle `{chronicle}`: {reason}")]
    SubscriberBuild { chronicle: String, reason: String },
    #[error("chronicle `{chronicle}` failed to subscribe to `{topic}`: {reason}")]
    Subscribe {
        chronicle: String,
        topic: String,
        reason: String,
    },
}

impl MqttTriggerError {
    pub fn missing_connector(chronicle: &str, connector: &str) -> Self {
        MqttTriggerError::MissingConnector {
            chronicle: chronicle.to_string(),
            connector: connector.to_string(),
        }
    }
}
