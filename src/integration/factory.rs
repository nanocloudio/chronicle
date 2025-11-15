use crate::config::integration::{ConnectorTimeouts, SmtpTlsMode};
use crate::error::Error as ChronicleError;
#[cfg(feature = "grpc")]
use crate::integration::registry::GrpcConnector;
#[cfg(feature = "http-out")]
use crate::integration::registry::HttpClientConnector;
#[cfg(feature = "db-mariadb")]
use crate::integration::registry::MariadbConnector;
#[cfg(feature = "mqtt")]
use crate::integration::registry::MqttConnector;
#[cfg(feature = "db-postgres")]
use crate::integration::registry::PostgresConnector;
#[cfg(feature = "rabbitmq")]
use crate::integration::registry::RabbitmqConnector;
#[cfg(feature = "smtp")]
use crate::integration::registry::SmtpConnector;
use crate::integration::registry::{ConnectorRegistry, KafkaConnector};
#[cfg(feature = "kafka")]
use crate::transport::kafka_context::{
    KafkaClientContext, KafkaClientRole, KafkaConnectivityState,
};
#[cfg(feature = "grpc")]
use axum::http::Uri;
#[cfg(feature = "kafka")]
use rdkafka::util::Timeout;
#[cfg(feature = "kafka")]
use rdkafka::{
    config::ClientConfig,
    producer::{FutureProducer, Producer},
};
#[cfg(feature = "http-out")]
use reqwest::{Certificate, Client, ClientBuilder, Identity};
use serde_json::{Map as JsonMap, Value as JsonValue};
#[cfg(feature = "db-mariadb")]
use sqlx::mysql::{MySqlPool, MySqlPoolOptions};
#[cfg(feature = "db-postgres")]
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
#[cfg(any(
    feature = "http-out",
    feature = "rabbitmq",
    feature = "mqtt",
    feature = "db-mariadb",
    feature = "smtp"
))]
use std::time::Duration;
use thiserror::Error;
#[cfg(feature = "kafka")]
use tokio::task;
#[cfg(feature = "kafka")]
use tokio::time::{sleep, timeout};
#[cfg(feature = "kafka")]
use tokio_util::sync::CancellationToken;
#[cfg(feature = "grpc")]
use tonic::transport::{
    Certificate as GrpcCertificate, Channel, ClientTlsConfig, Endpoint, Identity as GrpcIdentity,
};

#[cfg(feature = "rabbitmq")]
use lapin::{
    options::{BasicPublishOptions, ConfirmSelectOptions},
    publisher_confirm::Confirmation,
    BasicProperties as AmqpBasicProperties, Channel as AmqpChannel, Connection as AmqpConnection,
    ConnectionProperties as AmqpConnectionProperties,
};
#[cfg(feature = "mqtt")]
use rumqttc::{AsyncClient, MqttOptions, QoS, Transport};
#[cfg(feature = "rabbitmq")]
use tokio::time::timeout as amqp_timeout;
#[cfg(feature = "mqtt")]
use tokio::time::timeout as mqtt_timeout;
#[cfg(feature = "rabbitmq")]
use tokio_executor_trait::Tokio as TokioExecutor;
#[cfg(feature = "mqtt")]
use url::Url;
#[cfg(feature = "mqtt")]
use uuid::Uuid;

/// Runtime factory that materialises connector instances (HTTP clients, Kafka producers,
/// MariaDB pools) on demand while caching them for reuse.
struct ConnectorCache<H> {
    inner: Mutex<HashMap<String, Arc<H>>>,
}

impl<H> ConnectorCache<H> {
    fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    fn get_or_insert_with<F>(&self, name: &str, builder: F) -> Result<Arc<H>, ConnectorFactoryError>
    where
        F: FnOnce() -> Result<H, ConnectorFactoryError>,
    {
        let mut guard = self.inner.lock().expect("connector cache lock poisoned");
        if let Some(handle) = guard.get(name) {
            return Ok(handle.clone());
        }
        let handle = Arc::new(builder()?);
        guard.insert(name.to_string(), handle.clone());
        Ok(handle)
    }
}

impl<H> fmt::Debug for ConnectorCache<H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorCache").finish()
    }
}

#[derive(Debug)]
pub struct ConnectorFactoryRegistry {
    registry: Arc<ConnectorRegistry>,
    #[cfg(feature = "http-out")]
    http_clients: ConnectorCache<HttpClientHandle>,
    #[cfg(not(feature = "http-out"))]
    #[allow(dead_code)]
    http_clients: (),
    #[cfg(feature = "grpc")]
    grpc_clients: ConnectorCache<GrpcClientHandle>,
    #[cfg(not(feature = "grpc"))]
    #[allow(dead_code)]
    grpc_clients: (),
    kafka_producers: ConnectorCache<KafkaProducerHandle>,
    #[cfg(feature = "db-mariadb")]
    mariadb_pools: ConnectorCache<MariaDbPoolHandle>,
    #[cfg(feature = "db-postgres")]
    postgres_pools: ConnectorCache<PostgresPoolHandle>,
    #[cfg(feature = "rabbitmq")]
    rabbitmq_connections: ConnectorCache<RabbitmqHandle>,
    #[cfg(feature = "rabbitmq")]
    rabbitmq_publishers: Mutex<HashMap<String, Arc<RabbitmqPublisher>>>,
    #[cfg(feature = "mqtt")]
    mqtt_clients: ConnectorCache<MqttHandle>,
    #[cfg(feature = "mqtt")]
    mqtt_publishers: Mutex<HashMap<String, Arc<MqttPublisher>>>,
    #[cfg(feature = "smtp")]
    smtp_clients: ConnectorCache<SmtpHandle>,
    #[cfg(not(feature = "smtp"))]
    #[allow(dead_code)]
    smtp_clients: (),
}

impl ConnectorFactoryRegistry {
    pub fn new(registry: Arc<ConnectorRegistry>) -> Self {
        let instance = Self {
            registry: Arc::clone(&registry),
            #[cfg(feature = "http-out")]
            http_clients: ConnectorCache::new(),
            #[cfg(not(feature = "http-out"))]
            http_clients: (),
            #[cfg(feature = "grpc")]
            grpc_clients: ConnectorCache::new(),
            #[cfg(not(feature = "grpc"))]
            grpc_clients: (),
            kafka_producers: ConnectorCache::new(),
            #[cfg(feature = "db-mariadb")]
            mariadb_pools: ConnectorCache::new(),
            #[cfg(feature = "db-postgres")]
            postgres_pools: ConnectorCache::new(),
            #[cfg(feature = "rabbitmq")]
            rabbitmq_connections: ConnectorCache::new(),
            #[cfg(feature = "rabbitmq")]
            rabbitmq_publishers: Mutex::new(HashMap::new()),
            #[cfg(feature = "mqtt")]
            mqtt_clients: ConnectorCache::new(),
            #[cfg(feature = "mqtt")]
            mqtt_publishers: Mutex::new(HashMap::new()),
            #[cfg(feature = "smtp")]
            smtp_clients: ConnectorCache::new(),
            #[cfg(not(feature = "smtp"))]
            smtp_clients: (),
        };
        registry.for_each_handle(|handle| {
            tracing::debug!(
                connector = handle.name(),
                timeout = ?handle.timeouts(),
                "connector handle registered"
            );
        });
        instance
    }

    #[cfg(feature = "http-out")]
    pub fn http_client(&self, name: &str) -> Result<Arc<HttpClientHandle>, ConnectorFactoryError> {
        let connector =
            self.require_connector(name, "http_client", ConnectorRegistry::http_client)?;
        self.http_clients
            .get_or_insert_with(name, move || build_http_client_handle(&connector))
    }

    #[cfg(not(feature = "http-out"))]
    pub fn http_client(&self, name: &str) -> Result<Arc<HttpClientHandle>, ConnectorFactoryError> {
        let name_owned = name.to_string();
        Err(ConnectorFactoryError::feature_unavailable(
            name_owned,
            "http_client",
            "http-out",
        ))
    }

    #[cfg(feature = "grpc")]
    pub fn grpc_client(&self, name: &str) -> Result<Arc<GrpcClientHandle>, ConnectorFactoryError> {
        let connector = self.require_connector(name, "grpc", ConnectorRegistry::grpc)?;
        self.grpc_clients
            .get_or_insert_with(name, move || build_grpc_client_handle(&connector))
    }

    #[cfg(not(feature = "grpc"))]
    pub fn grpc_client(&self, name: &str) -> Result<Arc<GrpcClientHandle>, ConnectorFactoryError> {
        Err(ConnectorFactoryError::feature_unavailable(
            name.to_string(),
            "grpc",
            "grpc",
        ))
    }

    pub fn kafka_producer(
        &self,
        name: &str,
    ) -> Result<Arc<KafkaProducerHandle>, ConnectorFactoryError> {
        let connector = self.require_connector(name, "kafka", ConnectorRegistry::kafka)?;
        self.kafka_producers
            .get_or_insert_with(name, move || build_kafka_handle(&connector))
    }

    #[cfg(feature = "db-mariadb")]
    pub fn mariadb_pool(
        &self,
        name: &str,
    ) -> Result<Arc<MariaDbPoolHandle>, ConnectorFactoryError> {
        let connector = self.require_connector(name, "mariadb", ConnectorRegistry::mariadb)?;
        self.mariadb_pools
            .get_or_insert_with(name, move || build_mariadb_handle(&connector))
    }

    #[cfg(not(feature = "db-mariadb"))]
    pub fn mariadb_pool(
        &self,
        name: &str,
    ) -> Result<Arc<MariaDbPoolHandle>, ConnectorFactoryError> {
        Err(ConnectorFactoryError::feature_unavailable(
            name,
            "mariadb",
            "db-mariadb",
        ))
    }

    #[cfg(feature = "db-postgres")]
    pub fn postgres_pool(
        &self,
        name: &str,
    ) -> Result<Arc<PostgresPoolHandle>, ConnectorFactoryError> {
        let connector = self.require_connector(name, "postgres", ConnectorRegistry::postgres)?;
        self.postgres_pools
            .get_or_insert_with(name, move || build_postgres_handle(&connector))
    }

    #[cfg(not(feature = "db-postgres"))]
    pub fn postgres_pool(
        &self,
        name: &str,
    ) -> Result<Arc<PostgresPoolHandle>, ConnectorFactoryError> {
        Err(ConnectorFactoryError::feature_unavailable(
            name,
            "postgres",
            "db-postgres",
        ))
    }

    #[cfg(feature = "rabbitmq")]
    pub fn rabbitmq(&self, name: &str) -> Result<Arc<RabbitmqHandle>, ConnectorFactoryError> {
        let connector = self.require_connector(name, "rabbitmq", ConnectorRegistry::rabbitmq)?;
        self.rabbitmq_connections
            .get_or_insert_with(name, move || Ok(build_rabbitmq_handle(&connector)))
    }

    #[cfg(not(feature = "rabbitmq"))]
    pub fn rabbitmq(&self, name: &str) -> Result<Arc<RabbitmqHandle>, ConnectorFactoryError> {
        Err(ConnectorFactoryError::feature_unavailable(
            name, "rabbitmq", "rabbitmq",
        ))
    }

    #[cfg(feature = "rabbitmq")]
    pub async fn rabbitmq_publisher(
        &self,
        name: &str,
    ) -> Result<Arc<RabbitmqPublisher>, ConnectorFactoryError> {
        if let Some(handle) = self
            .rabbitmq_publishers
            .lock()
            .expect("rabbitmq publisher cache")
            .get(name)
            .cloned()
        {
            return Ok(handle);
        }

        let handle = self.rabbitmq(name)?;

        let publisher = RabbitmqPublisher::connect(handle.as_ref()).await?;
        let publisher = Arc::new(publisher);

        let mut cache = self
            .rabbitmq_publishers
            .lock()
            .expect("rabbitmq publisher cache");
        cache.insert(name.to_string(), publisher.clone());

        Ok(publisher)
    }

    #[cfg(not(feature = "rabbitmq"))]
    pub async fn rabbitmq_publisher(
        &self,
        name: &str,
    ) -> Result<Arc<RabbitmqPublisher>, ConnectorFactoryError> {
        Err(ConnectorFactoryError::feature_unavailable(
            name, "rabbitmq", "rabbitmq",
        ))
    }

    #[cfg(feature = "mqtt")]
    pub async fn mqtt_publisher(
        &self,
        name: &str,
    ) -> Result<Arc<MqttPublisher>, ConnectorFactoryError> {
        {
            let cache = self.mqtt_publishers.lock().expect("mqtt publisher cache");
            if let Some(publisher) = cache.get(name) {
                return Ok(publisher.clone());
            }
        }

        let handle = self.mqtt_client(name)?;
        let publisher = MqttPublisher::connect(handle.as_ref()).await?;
        let publisher = Arc::new(publisher);

        let mut cache = self.mqtt_publishers.lock().expect("mqtt publisher cache");
        cache.insert(name.to_string(), publisher.clone());

        Ok(publisher)
    }

    #[cfg(not(feature = "mqtt"))]
    pub async fn mqtt_publisher(
        &self,
        name: &str,
    ) -> Result<Arc<MqttPublisher>, ConnectorFactoryError> {
        Err(ConnectorFactoryError::feature_unavailable(
            name, "mqtt", "mqtt",
        ))
    }

    #[cfg(feature = "mqtt")]
    pub fn mqtt_client(&self, name: &str) -> Result<Arc<MqttHandle>, ConnectorFactoryError> {
        let connector = self.require_connector(name, "mqtt", ConnectorRegistry::mqtt)?;
        self.mqtt_clients
            .get_or_insert_with(name, move || Ok(build_mqtt_handle(&connector)))
    }

    #[cfg(not(feature = "mqtt"))]
    pub fn mqtt_client(&self, name: &str) -> Result<Arc<MqttHandle>, ConnectorFactoryError> {
        Err(ConnectorFactoryError::feature_unavailable(
            name, "mqtt", "mqtt",
        ))
    }

    #[cfg(feature = "smtp")]
    pub fn smtp_mailer(&self, name: &str) -> Result<Arc<SmtpHandle>, ConnectorFactoryError> {
        let connector = self.require_connector(name, "smtp", ConnectorRegistry::smtp)?;
        self.smtp_clients
            .get_or_insert_with(name, move || build_smtp_handle(&connector))
    }

    #[cfg(not(feature = "smtp"))]
    pub fn smtp_mailer(&self, name: &str) -> Result<Arc<SmtpHandle>, ConnectorFactoryError> {
        Err(ConnectorFactoryError::feature_unavailable(
            name.to_string(),
            "smtp",
            "smtp",
        ))
    }

    pub fn require_connector<T, F>(
        &self,
        name: &str,
        expected: &'static str,
        getter: F,
    ) -> Result<T, ConnectorFactoryError>
    where
        T: Clone,
        F: for<'a> FnOnce(&'a ConnectorRegistry, &'a str) -> Option<&'a T>,
    {
        getter(&self.registry, name).cloned().ok_or_else(|| {
            ConnectorFactoryError::MissingConnector {
                name: name.to_string(),
                expected,
            }
        })
    }

    pub fn base_dir(&self) -> &Path {
        self.registry.base_dir()
    }
}

#[cfg(feature = "http-out")]
#[derive(Debug, Clone)]
pub struct HttpClientHandle {
    name: String,
    base_url: String,
    client: Client,
    default_headers: Vec<(String, String)>,
    extra: JsonMap<String, JsonValue>,
}

#[cfg(feature = "http-out")]
impl HttpClientHandle {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    pub fn default_headers(&self) -> &[(String, String)] {
        &self.default_headers
    }

    pub fn extra(&self) -> &JsonMap<String, JsonValue> {
        &self.extra
    }
}

#[cfg(not(feature = "http-out"))]
#[derive(Debug, Clone)]
pub struct HttpClientHandle;

#[cfg(feature = "grpc")]
#[derive(Debug, Clone)]
pub struct GrpcClientHandle {
    name: String,
    endpoint: String,
    channel: Channel,
    descriptor: Option<PathBuf>,
    default_metadata: Vec<(String, String)>,
    extra: JsonMap<String, JsonValue>,
}

#[cfg(feature = "grpc")]
impl GrpcClientHandle {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn channel(&self) -> Channel {
        self.channel.clone()
    }

    pub fn descriptor(&self) -> Option<&PathBuf> {
        self.descriptor.as_ref()
    }

    pub fn default_metadata(&self) -> &[(String, String)] {
        &self.default_metadata
    }

    pub fn extra(&self) -> &JsonMap<String, JsonValue> {
        &self.extra
    }
}

#[cfg(not(feature = "grpc"))]
#[derive(Debug, Clone)]
pub struct GrpcClientHandle;

#[derive(Clone)]
pub struct KafkaProducerHandle {
    name: String,
    brokers: Vec<String>,
    create_topics_if_missing: bool,
    #[cfg(feature = "kafka")]
    producer: FutureProducer<KafkaClientContext>,
    timeouts: ConnectorTimeouts,
    extra: JsonMap<String, JsonValue>,
    #[cfg(feature = "kafka")]
    monitor: CancellationToken,
}

impl KafkaProducerHandle {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn brokers(&self) -> &[String] {
        &self.brokers
    }

    pub fn create_topics_if_missing(&self) -> bool {
        self.create_topics_if_missing
    }

    #[cfg(feature = "kafka")]
    pub fn producer(&self) -> &FutureProducer<KafkaClientContext> {
        &self.producer
    }

    pub fn timeouts(&self) -> &ConnectorTimeouts {
        &self.timeouts
    }

    pub fn extra(&self) -> &JsonMap<String, JsonValue> {
        &self.extra
    }
}

#[cfg(feature = "kafka")]
impl Drop for KafkaProducerHandle {
    fn drop(&mut self) {
        self.monitor.cancel();
    }
}

impl std::fmt::Debug for KafkaProducerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaProducerHandle")
            .field("name", &self.name)
            .field("brokers", &self.brokers)
            .field("create_topics_if_missing", &self.create_topics_if_missing)
            .field("timeouts", &self.timeouts)
            .field("extra", &self.extra)
            .finish()
    }
}

#[cfg(feature = "db-mariadb")]
#[derive(Debug, Clone)]
pub struct MariaDbPoolHandle {
    name: String,
    url: String,
    pool: MySqlPool,
    schema: Option<String>,
    extra: JsonMap<String, JsonValue>,
}

#[cfg(not(feature = "db-mariadb"))]
#[derive(Debug, Clone)]
pub struct MariaDbPoolHandle;

#[cfg(feature = "db-mariadb")]
impl MariaDbPoolHandle {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn pool(&self) -> &MySqlPool {
        &self.pool
    }

    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    pub fn extra(&self) -> &JsonMap<String, JsonValue> {
        &self.extra
    }
}

#[cfg(not(feature = "db-mariadb"))]
impl MariaDbPoolHandle {
    pub fn name(&self) -> &str {
        "mariadb"
    }
}

#[cfg(feature = "db-postgres")]
#[derive(Debug, Clone)]
pub struct PostgresPoolHandle {
    name: String,
    url: String,
    pool: PgPool,
    schema: Option<String>,
    extra: JsonMap<String, JsonValue>,
}

#[cfg(not(feature = "db-postgres"))]
#[derive(Debug, Clone)]
pub struct PostgresPoolHandle;

#[cfg(feature = "db-postgres")]
impl PostgresPoolHandle {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    pub fn extra(&self) -> &JsonMap<String, JsonValue> {
        &self.extra
    }
}

#[derive(Debug, Clone)]
pub struct RabbitmqHandle {
    name: String,
    url: String,
    prefetch: Option<u32>,
    tls: Option<RabbitmqTlsConfig>,
    extra: JsonMap<String, JsonValue>,
}

impl RabbitmqHandle {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn prefetch(&self) -> Option<u32> {
        self.prefetch
    }

    pub fn tls(&self) -> Option<&RabbitmqTlsConfig> {
        self.tls.as_ref()
    }

    pub fn extra(&self) -> &JsonMap<String, JsonValue> {
        &self.extra
    }
}

#[cfg(feature = "rabbitmq")]
#[derive(Debug)]
pub struct RabbitmqPublisher {
    name: String,
    _connection: AmqpConnection,
    channel: AmqpChannel,
}

#[cfg(feature = "rabbitmq")]
impl RabbitmqPublisher {
    async fn connect(handle: &RabbitmqHandle) -> Result<Self, ConnectorFactoryError> {
        let properties =
            AmqpConnectionProperties::default().with_executor(TokioExecutor::current());

        let connection = AmqpConnection::connect(handle.url(), properties)
            .await
            .map_err(|err| {
                ConnectorFactoryError::build_failure(
                    handle.name(),
                    "rabbitmq",
                    contextual_error(
                        format!("failed to connect to {}", handle.url()),
                        ChronicleError::msg(err.to_string()),
                    ),
                )
            })?;

        let channel = connection.create_channel().await.map_err(|err| {
            ConnectorFactoryError::build_failure(
                handle.name(),
                "rabbitmq",
                contextual_error(
                    "failed to open channel",
                    ChronicleError::msg(err.to_string()),
                ),
            )
        })?;

        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .map_err(|err| {
                ConnectorFactoryError::build_failure(
                    handle.name(),
                    "rabbitmq",
                    contextual_error(
                        "failed to enable publisher confirms",
                        ChronicleError::msg(err.to_string()),
                    ),
                )
            })?;

        Ok(Self {
            name: handle.name().to_string(),
            _connection: connection,
            channel,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn publish(
        &self,
        exchange: &str,
        routing_key: &str,
        payload: &[u8],
        properties: AmqpBasicProperties,
        options: BasicPublishOptions,
        confirm: bool,
        timeout: Option<Duration>,
    ) -> Result<(), ConnectorFactoryError> {
        let confirm_future = self
            .channel
            .basic_publish(exchange, routing_key, options, payload, properties)
            .await
            .map_err(|err| {
                ConnectorFactoryError::build_failure(
                    &self.name,
                    "rabbitmq",
                    contextual_error(
                        format!(
                            "basic_publish failed (exchange: {exchange}, routing_key: {routing_key})"
                        ),
                        ChronicleError::msg(err.to_string()),
                    ),
                )
            })?;

        let confirmation = if confirm {
            if let Some(duration) = timeout {
                amqp_timeout(duration, confirm_future)
                    .await
                    .map_err(|_| {
                        ConnectorFactoryError::build_failure(
                            &self.name,
                            "rabbitmq",
                            crate::err!("publisher confirm timed out"),
                        )
                    })?
                    .map_err(|err| {
                        ConnectorFactoryError::build_failure(
                            &self.name,
                            "rabbitmq",
                            contextual_error(
                                "publisher confirm failed",
                                ChronicleError::msg(err.to_string()),
                            ),
                        )
                    })?
            } else {
                confirm_future.await.map_err(|err| {
                    ConnectorFactoryError::build_failure(
                        &self.name,
                        "rabbitmq",
                        contextual_error(
                            "publisher confirm failed",
                            ChronicleError::msg(err.to_string()),
                        ),
                    )
                })?
            }
        } else {
            confirm_future.await.map_err(|err| {
                ConnectorFactoryError::build_failure(
                    &self.name,
                    "rabbitmq",
                    contextual_error(
                        "publisher confirm failed",
                        ChronicleError::msg(err.to_string()),
                    ),
                )
            })?
        };

        match confirmation {
            Confirmation::Ack(_) | Confirmation::NotRequested => Ok(()),
            Confirmation::Nack(_) => Err(ConnectorFactoryError::build_failure(
                &self.name,
                "rabbitmq",
                crate::err!("publisher confirm returned nack"),
            )),
        }
    }
}

#[cfg(not(feature = "rabbitmq"))]
#[derive(Debug)]
pub struct RabbitmqPublisher;

#[cfg(not(feature = "mqtt"))]
#[derive(Debug)]
pub struct MqttPublisher;

#[cfg(feature = "mqtt")]
#[derive(Debug)]
pub struct MqttPublisher {
    name: String,
    client: AsyncClient,
    _driver: tokio::task::JoinHandle<()>,
}

#[cfg(feature = "mqtt")]
impl MqttPublisher {
    async fn connect(handle: &MqttHandle) -> Result<Self, ConnectorFactoryError> {
        let options = build_mqtt_options_for_handle(handle)?;
        let (client, mut eventloop) = AsyncClient::new(options, 10);
        let name = handle.name().to_string();
        let driver_name = name.clone();

        let driver = tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(_) => {}
                    Err(err) => {
                        tracing::error!(
                            target: "chronicle::mqtt",
                            connector = %driver_name,
                            error = %err,
                            "mqtt publisher event loop error"
                        );
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });

        Ok(Self {
            name,
            client,
            _driver: driver,
        })
    }

    pub async fn publish(
        &self,
        topic: &str,
        payload: Vec<u8>,
        qos: QoS,
        retain: bool,
        timeout: Option<Duration>,
    ) -> Result<(), ConnectorFactoryError> {
        if let Some(duration) = timeout {
            mqtt_timeout(
                duration,
                self.client.publish(topic.to_string(), qos, retain, payload),
            )
            .await
            .map_err(|_| {
                ConnectorFactoryError::build_failure(
                    &self.name,
                    "mqtt",
                    crate::err!("publish timed out"),
                )
            })?
            .map_err(|err| {
                ConnectorFactoryError::build_failure(
                    &self.name,
                    "mqtt",
                    contextual_error("publish failed", ChronicleError::msg(err.to_string())),
                )
            })?;
        } else {
            self.client
                .publish(topic.to_string(), qos, retain, payload)
                .await
                .map_err(|err| {
                    ConnectorFactoryError::build_failure(
                        &self.name,
                        "mqtt",
                        contextual_error("publish failed", ChronicleError::msg(err.to_string())),
                    )
                })?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct RabbitmqTlsConfig {
    ca: Option<PathBuf>,
    cert: Option<PathBuf>,
    key: Option<PathBuf>,
    extra: JsonMap<String, JsonValue>,
}

impl RabbitmqTlsConfig {
    pub fn ca(&self) -> Option<&PathBuf> {
        self.ca.as_ref()
    }

    pub fn cert(&self) -> Option<&PathBuf> {
        self.cert.as_ref()
    }

    pub fn key(&self) -> Option<&PathBuf> {
        self.key.as_ref()
    }

    pub fn extra(&self) -> &JsonMap<String, JsonValue> {
        &self.extra
    }
}

#[derive(Debug, Clone)]
pub struct MqttHandle {
    name: String,
    url: String,
    client_id: Option<String>,
    username: Option<String>,
    password: Option<String>,
    keep_alive: Option<u64>,
    tls: Option<MqttTlsConfig>,
    extra: JsonMap<String, JsonValue>,
}

impl MqttHandle {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn client_id(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    pub fn username(&self) -> Option<&str> {
        self.username.as_deref()
    }

    pub fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }

    pub fn keep_alive(&self) -> Option<u64> {
        self.keep_alive
    }

    pub fn tls(&self) -> Option<&MqttTlsConfig> {
        self.tls.as_ref()
    }

    pub fn extra(&self) -> &JsonMap<String, JsonValue> {
        &self.extra
    }
}

#[derive(Debug, Clone)]
pub struct MqttTlsConfig {
    ca: Option<PathBuf>,
    cert: Option<PathBuf>,
    key: Option<PathBuf>,
    extra: JsonMap<String, JsonValue>,
}

impl MqttTlsConfig {
    pub fn ca(&self) -> Option<&PathBuf> {
        self.ca.as_ref()
    }

    pub fn cert(&self) -> Option<&PathBuf> {
        self.cert.as_ref()
    }

    pub fn key(&self) -> Option<&PathBuf> {
        self.key.as_ref()
    }

    pub fn extra(&self) -> &JsonMap<String, JsonValue> {
        &self.extra
    }
}

#[cfg(feature = "smtp")]
#[derive(Debug, Clone)]
pub struct SmtpHandle {
    name: String,
    host: String,
    port: u16,
    auth: Option<SmtpAuthSettings>,
    tls: Option<SmtpTlsSettings>,
    default_from: Option<String>,
    timeout_secs: Option<u64>,
    extra: JsonMap<String, JsonValue>,
}

#[cfg(feature = "smtp")]
impl SmtpHandle {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn auth(&self) -> Option<&SmtpAuthSettings> {
        self.auth.as_ref()
    }

    pub fn tls(&self) -> Option<&SmtpTlsSettings> {
        self.tls.as_ref()
    }

    pub fn default_from(&self) -> Option<&str> {
        self.default_from.as_deref()
    }

    pub fn timeout_secs(&self) -> Option<u64> {
        self.timeout_secs
    }

    pub fn extra(&self) -> &JsonMap<String, JsonValue> {
        &self.extra
    }
}

#[cfg(not(feature = "smtp"))]
#[derive(Debug, Clone)]
pub struct SmtpHandle;

#[derive(Debug, Clone)]
pub struct SmtpAuthSettings {
    username: String,
    password: String,
    mechanism: Option<String>,
    extra: JsonMap<String, JsonValue>,
}

impl SmtpAuthSettings {
    pub fn username(&self) -> &str {
        &self.username
    }

    pub fn password(&self) -> &str {
        &self.password
    }

    pub fn mechanism(&self) -> Option<&str> {
        self.mechanism.as_deref()
    }

    pub fn extra(&self) -> &JsonMap<String, JsonValue> {
        &self.extra
    }
}

#[derive(Debug, Clone)]
pub struct SmtpTlsSettings {
    mode: SmtpTlsMode,
    ca: Option<PathBuf>,
    domain: Option<String>,
    cert: Option<PathBuf>,
    key: Option<PathBuf>,
    extra: JsonMap<String, JsonValue>,
}

impl SmtpTlsSettings {
    pub fn mode(&self) -> &SmtpTlsMode {
        &self.mode
    }

    pub fn ca(&self) -> Option<&PathBuf> {
        self.ca.as_ref()
    }

    pub fn domain(&self) -> Option<&str> {
        self.domain.as_deref()
    }

    pub fn cert(&self) -> Option<&PathBuf> {
        self.cert.as_ref()
    }

    pub fn key(&self) -> Option<&PathBuf> {
        self.key.as_ref()
    }

    pub fn extra(&self) -> &JsonMap<String, JsonValue> {
        &self.extra
    }
}

#[derive(Debug, Error)]
pub enum ConnectorFactoryError {
    #[error("connector `{name}` not found with expected type `{expected}`")]
    MissingConnector {
        name: String,
        expected: &'static str,
    },
    #[error("failed to build http client `{name}`")]
    HttpClient {
        name: String,
        #[source]
        source: Arc<ChronicleError>,
    },
    #[error(
        "http client connector `{name}` requested but binary built without `http-out` feature"
    )]
    HttpClientUnavailable { name: String },
    #[error("failed to build mariadb pool `{name}`")]
    MariaDb {
        name: String,
        #[source]
        source: Arc<ChronicleError>,
    },
    #[error("failed to build postgres pool `{name}`")]
    Postgres {
        name: String,
        #[source]
        source: Arc<ChronicleError>,
    },
    #[error("mariadb connector `{name}` requested but binary built without `db-mariadb` feature")]
    MariaDbUnavailable { name: String },
    #[error(
        "postgres connector `{name}` requested but binary built without `db-postgres` feature"
    )]
    PostgresUnavailable { name: String },
    #[error("failed to build gRPC client `{name}`")]
    Grpc {
        name: String,
        #[source]
        source: Arc<ChronicleError>,
    },
    #[error("failed to build kafka producer `{name}`")]
    KafkaProducer {
        name: String,
        #[source]
        source: Arc<ChronicleError>,
    },
    #[error("failed to build smtp client `{name}`")]
    Smtp {
        name: String,
        #[source]
        source: Arc<ChronicleError>,
    },
    #[error("grpc connector `{name}` requested but binary built without `grpc` feature")]
    GrpcUnavailable { name: String },
    #[error("kafka connector `{name}` requested but binary built without `kafka` feature")]
    KafkaUnavailable { name: String },
    #[error("rabbitmq connector `{name}` requested but binary built without `rabbitmq` feature")]
    RabbitmqUnavailable { name: String },
    #[error("failed to build rabbitmq client `{name}`")]
    Rabbitmq {
        name: String,
        #[source]
        source: Arc<ChronicleError>,
    },
    #[error("mqtt connector `{name}` requested but binary built without `mqtt` feature")]
    MqttUnavailable { name: String },
    #[error("failed to build mqtt client `{name}`")]
    Mqtt {
        name: String,
        #[source]
        source: Arc<ChronicleError>,
    },
    #[error("smtp connector `{name}` requested but binary built without `smtp` feature")]
    SmtpUnavailable { name: String },
}

impl ConnectorFactoryError {
    #[allow(dead_code)]
    fn build_failure(name: impl Into<String>, kind: &'static str, source: ChronicleError) -> Self {
        let name = name.into();
        let arc = Arc::new(source);
        match kind {
            "http_client" => Self::HttpClient {
                name,
                source: Arc::clone(&arc),
            },
            "grpc" => Self::Grpc {
                name,
                source: Arc::clone(&arc),
            },
            "kafka" => Self::KafkaProducer {
                name,
                source: Arc::clone(&arc),
            },
            "smtp" => Self::Smtp {
                name,
                source: Arc::clone(&arc),
            },
            "mariadb" => Self::MariaDb {
                name,
                source: Arc::clone(&arc),
            },
            "postgres" => Self::Postgres {
                name,
                source: Arc::clone(&arc),
            },
            "rabbitmq" => Self::Rabbitmq {
                name,
                source: Arc::clone(&arc),
            },
            "mqtt" => Self::Mqtt {
                name,
                source: Arc::clone(&arc),
            },
            _ => Self::HttpClient { name, source: arc },
        }
    }

    #[cfg(any(
        not(feature = "kafka"),
        not(feature = "rabbitmq"),
        not(feature = "mqtt"),
        not(feature = "db-mariadb"),
        not(feature = "db-postgres"),
        not(feature = "grpc"),
        not(feature = "smtp"),
        not(feature = "http-out")
    ))]
    fn feature_unavailable(
        name: impl Into<String>,
        kind: &'static str,
        feature: &'static str,
    ) -> Self {
        let name = name.into();
        match (kind, feature) {
            ("mariadb", "db-mariadb") => Self::MariaDbUnavailable { name },
            ("postgres", "db-postgres") => Self::PostgresUnavailable { name },
            ("rabbitmq", "rabbitmq") => Self::RabbitmqUnavailable { name },
            ("mqtt", "mqtt") => Self::MqttUnavailable { name },
            ("kafka", "kafka") => Self::KafkaUnavailable { name },
            ("grpc", "grpc") => Self::GrpcUnavailable { name },
            ("smtp", "smtp") => Self::SmtpUnavailable { name },
            ("http_client", "http-out") => Self::HttpClientUnavailable { name },
            _ => Self::MissingConnector {
                name,
                expected: kind,
            },
        }
    }
}

#[allow(dead_code)]
fn contextual_error(context: impl Into<String>, source: ChronicleError) -> ChronicleError {
    ChronicleError::with_context(context.into(), source)
}

#[cfg(feature = "http-out")]
fn build_http_client_handle(
    connector: &HttpClientConnector,
) -> Result<HttpClientHandle, ConnectorFactoryError> {
    let mut builder = ClientBuilder::new();

    if let Some(tls) = &connector.tls {
        if let Some(ca_path) = &tls.ca {
            if ca_path.exists() {
                let bytes = fs::read(ca_path).map_err(|err| {
                    ConnectorFactoryError::build_failure(
                        connector.name.clone(),
                        "http_client",
                        contextual_error(
                            format!("failed to read CA file `{}`", ca_path.display()),
                            ChronicleError::new(err),
                        ),
                    )
                })?;
                let cert = Certificate::from_pem(&bytes).map_err(|err| {
                    ConnectorFactoryError::build_failure(
                        connector.name.clone(),
                        "http_client",
                        contextual_error(
                            format!("failed to parse CA file `{}`", ca_path.display()),
                            ChronicleError::new(err),
                        ),
                    )
                })?;
                builder = builder.add_root_certificate(cert);
            }
        }

        match (&tls.cert, &tls.key) {
            (Some(_), None) | (None, Some(_)) => {
                return Err(ConnectorFactoryError::build_failure(
                    connector.name.clone(),
                    "http_client",
                    crate::err!("tls.cert and tls.key must be provided together for HTTP clients"),
                ));
            }
            (Some(cert_path), Some(key_path)) => {
                let identity_pem = join_pem_files(cert_path, key_path).map_err(|err| {
                    ConnectorFactoryError::build_failure(connector.name.clone(), "http_client", err)
                })?;
                let identity = Identity::from_pem(&identity_pem).map_err(|err| {
                    ConnectorFactoryError::build_failure(
                        connector.name.clone(),
                        "http_client",
                        contextual_error(
                            format!(
                                "failed to parse client identity from `{}` + `{}`",
                                cert_path.display(),
                                key_path.display()
                            ),
                            ChronicleError::new(err),
                        ),
                    )
                })?;
                builder = builder.identity(identity);
            }
            _ => {}
        }
    }

    if let Some(pool) = &connector.pool {
        if let Some(max_idle) = pool.max_idle_per_host {
            builder = builder.pool_max_idle_per_host(max_idle);
        }
        if let Some(idle_secs) = pool.idle_timeout_secs {
            builder = builder.pool_idle_timeout(Duration::from_secs(idle_secs));
        }
    }

    if let Some(connect_timeout) = connector.timeouts.connect {
        builder = builder.connect_timeout(connect_timeout);
    }
    if let Some(request_timeout) = connector.timeouts.request {
        builder = builder.timeout(request_timeout);
    }

    let client = builder.build().map_err(|err| {
        ConnectorFactoryError::build_failure(
            connector.name.clone(),
            "http_client",
            contextual_error("failed to build HTTP client", ChronicleError::new(err)),
        )
    })?;

    Ok(HttpClientHandle {
        name: connector.name.clone(),
        base_url: connector.base_url.clone(),
        client,
        default_headers: connector.default_headers.clone(),
        extra: map_from_btree(&connector.extra),
    })
}

#[cfg(feature = "grpc")]
fn build_grpc_client_handle(
    connector: &GrpcConnector,
) -> Result<GrpcClientHandle, ConnectorFactoryError> {
    let mut endpoint = Endpoint::from_shared(connector.endpoint.clone()).map_err(|err| {
        ConnectorFactoryError::build_failure(
            connector.name.clone(),
            "grpc",
            contextual_error(
                "invalid gRPC endpoint URL",
                ChronicleError::msg(err.to_string()),
            ),
        )
    })?;

    if let Some(connect_timeout) = connector.timeouts.connect {
        endpoint = endpoint.connect_timeout(connect_timeout);
    }

    if let Some(request_timeout) = connector.timeouts.request {
        endpoint = endpoint.timeout(request_timeout);
    }

    let uri: Uri = connector
        .endpoint
        .parse()
        .map_err(|err: axum::http::uri::InvalidUri| {
            ConnectorFactoryError::build_failure(
                connector.name.clone(),
                "grpc",
                contextual_error(
                    "failed to parse gRPC endpoint URI",
                    ChronicleError::msg(err.to_string()),
                ),
            )
        })?;

    let uses_tls = matches!(uri.scheme_str(), Some("https"));
    let tls_options = connector.tls.as_ref();

    if uses_tls || tls_options.is_some() {
        let mut tls_config = ClientTlsConfig::new();

        let domain = tls_options
            .and_then(|tls| tls.domain.clone())
            .or_else(|| uri.host().map(|host| host.to_string()))
            .ok_or_else(|| {
                ConnectorFactoryError::build_failure(
                    connector.name.clone(),
                    "grpc",
                    crate::err!("failed to determine TLS domain name for gRPC endpoint"),
                )
            })?;

        tls_config = tls_config.domain_name(domain);

        if let Some(tls) = tls_options {
            if let Some(ca_path) = &tls.ca {
                let bytes = fs::read(ca_path).map_err(|err| {
                    ConnectorFactoryError::build_failure(
                        connector.name.clone(),
                        "grpc",
                        contextual_error(
                            format!("failed to read CA file `{}`", ca_path.display()),
                            ChronicleError::new(err),
                        ),
                    )
                })?;

                let certificate = GrpcCertificate::from_pem(bytes);
                tls_config = tls_config.ca_certificate(certificate);
            }

            match (&tls.cert, &tls.key) {
                (Some(cert_path), Some(key_path)) => {
                    let cert_bytes = fs::read(cert_path).map_err(|err| {
                        ConnectorFactoryError::build_failure(
                            connector.name.clone(),
                            "grpc",
                            contextual_error(
                                format!(
                                    "failed to read client certificate `{}`",
                                    cert_path.display()
                                ),
                                ChronicleError::new(err),
                            ),
                        )
                    })?;
                    let key_bytes = fs::read(key_path).map_err(|err| {
                        ConnectorFactoryError::build_failure(
                            connector.name.clone(),
                            "grpc",
                            contextual_error(
                                format!("failed to read client key `{}`", key_path.display()),
                                ChronicleError::new(err),
                            ),
                        )
                    })?;

                    let identity = GrpcIdentity::from_pem(cert_bytes, key_bytes);
                    tls_config = tls_config.identity(identity);
                }
                (Some(_), None) | (None, Some(_)) => {
                    return Err(ConnectorFactoryError::build_failure(
                        connector.name.clone(),
                        "grpc",
                        crate::err!(
                            "tls.cert and tls.key must be provided together for gRPC clients"
                        ),
                    ));
                }
                _ => {}
            }
        }

        endpoint = endpoint.tls_config(tls_config).map_err(|err| {
            ConnectorFactoryError::build_failure(
                connector.name.clone(),
                "grpc",
                contextual_error(
                    "failed to apply TLS configuration",
                    ChronicleError::new(err),
                ),
            )
        })?;
    }

    let channel = endpoint.connect_lazy();

    Ok(GrpcClientHandle {
        name: connector.name.clone(),
        endpoint: connector.endpoint.clone(),
        channel,
        descriptor: connector.descriptor.clone(),
        default_metadata: connector
            .metadata
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        extra: map_from_btree(&connector.extra),
    })
}

#[cfg(feature = "kafka")]
fn build_kafka_handle(
    connector: &KafkaConnector,
) -> Result<KafkaProducerHandle, ConnectorFactoryError> {
    let brokers = connector.brokers.join(",");
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", brokers.as_str());

    let mut extra = map_from_btree(&connector.extra);
    if connector.force_idempotent {
        if !extra.contains_key("enable.idempotence") {
            config.set("enable.idempotence", "true");
            extra.insert("enable.idempotence".to_string(), JsonValue::Bool(true));
        }
        if !extra.contains_key("acks") {
            config.set("acks", "all");
            extra.insert("acks".to_string(), JsonValue::String("all".to_string()));
        }
    }
    if connector.create_topics_if_missing && !extra.contains_key("allow.auto.create.topics") {
        config.set("allow.auto.create.topics", "true");
    }
    apply_kafka_security(&connector.name, &mut config, &extra)?;
    for (key, value) in &extra {
        if let Some(text) = json_to_config_value(value) {
            config.set(key, text);
        }
    }

    let state = Arc::new(KafkaConnectivityState::new());
    let context = KafkaClientContext::with_state(
        connector.name.clone(),
        KafkaClientRole::Producer,
        state.clone(),
        false,
    );
    let producer: FutureProducer<KafkaClientContext> =
        config.create_with_context(context).map_err(|err| {
            ConnectorFactoryError::build_failure(
                connector.name.clone(),
                "kafka",
                contextual_error("failed to build Kafka producer", ChronicleError::new(err)),
            )
        })?;
    let monitor = CancellationToken::new();
    #[cfg(feature = "kafka")]
    {
        spawn_kafka_connectivity_monitor(
            producer.clone(),
            connector.name.clone(),
            monitor.clone(),
            state.clone(),
        );
    }

    Ok(KafkaProducerHandle {
        name: connector.name.clone(),
        brokers: connector.brokers.clone(),
        create_topics_if_missing: connector.create_topics_if_missing,
        producer,
        timeouts: connector.timeouts,
        extra,
        monitor,
    })
}

#[cfg(not(feature = "kafka"))]
fn build_kafka_handle(
    connector: &KafkaConnector,
) -> Result<KafkaProducerHandle, ConnectorFactoryError> {
    Err(ConnectorFactoryError::feature_unavailable(
        connector.name.clone(),
        "kafka",
        "kafka",
    ))
}

#[cfg(feature = "kafka")]
fn spawn_kafka_connectivity_monitor(
    producer: FutureProducer<KafkaClientContext>,
    connector_name: String,
    shutdown: CancellationToken,
    state: Arc<KafkaConnectivityState>,
) {
    task::spawn(async move {
        let check_interval = Duration::from_secs(10);

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = sleep(check_interval) => {}
            }

            let fetch_outcome = timeout(
                Duration::from_secs(5),
                task::spawn_blocking({
                    let producer = producer.clone();
                    move || {
                        producer
                            .client()
                            .fetch_metadata(None, Timeout::After(Duration::from_secs(2)))
                    }
                }),
            )
            .await;

            match fetch_outcome {
                Ok(Ok(Ok(_))) => {
                    if state.mark_connected() {
                        tracing::info!(
                            target = "chronicle::kafka",
                            event = "transport_reconnected",
                            connector = %connector_name
                        );
                    }
                }
                Ok(Ok(Err(err))) => log_disconnected(&state, &connector_name, &err.to_string()),
                Ok(Err(join_err)) => log_disconnected(
                    &state,
                    &connector_name,
                    &format!("monitor task failed: {join_err}"),
                ),
                Err(_) => log_disconnected(&state, &connector_name, "metadata fetch timed out"),
            }
        }
    });
}

#[cfg(feature = "kafka")]
fn log_disconnected(state: &KafkaConnectivityState, connector: &str, message: &str) {
    if state.mark_disconnected() {
        tracing::warn!(
            target = "chronicle::kafka",
            event = "transport_disconnected",
            connector = %connector,
            error = %message
        );
    } else {
        tracing::debug!(
            target = "chronicle::kafka",
            event = "transport_disconnected",
            connector = %connector,
            error = %message
        );
    }
}

#[cfg(feature = "kafka")]
fn apply_kafka_security(
    connector_name: &str,
    config: &mut ClientConfig,
    extra: &JsonMap<String, JsonValue>,
) -> Result<(), ConnectorFactoryError> {
    let settings = kafka_security_settings(connector_name, extra)?;

    config.set("security.protocol", settings.protocol.as_str());

    if let Some(ca) = settings.ssl_ca_location.as_deref() {
        config.set("ssl.ca.location", ca);
    }
    if let Some(cert) = settings.ssl_certificate_location.as_deref() {
        config.set("ssl.certificate.location", cert);
    }
    if let Some(key) = settings.ssl_key_location.as_deref() {
        config.set("ssl.key.location", key);
    }
    if let Some(username) = settings.sasl_username.as_deref() {
        config.set("sasl.username", username);
    }
    if let Some(password) = settings.sasl_password.as_deref() {
        config.set("sasl.password", password);
    }
    if let Some(mechanisms) = settings.sasl_mechanisms.as_deref() {
        config.set("sasl.mechanisms", mechanisms);
    }

    Ok(())
}

#[cfg(feature = "kafka")]
#[derive(Debug, Clone)]
struct KafkaSecuritySettings {
    protocol: String,
    ssl_ca_location: Option<String>,
    ssl_certificate_location: Option<String>,
    ssl_key_location: Option<String>,
    sasl_username: Option<String>,
    sasl_password: Option<String>,
    sasl_mechanisms: Option<String>,
}

#[cfg(feature = "kafka")]
fn kafka_security_settings(
    connector_name: &str,
    extra: &JsonMap<String, JsonValue>,
) -> Result<KafkaSecuritySettings, ConnectorFactoryError> {
    let tls_ca = nested_string(extra, &["security", "tls", "ca"]);
    let tls_cert = nested_string(extra, &["security", "tls", "cert"]);
    let tls_key = nested_string(extra, &["security", "tls", "key"]);
    let sasl_username = nested_string(extra, &["security", "sasl", "username"]);
    let sasl_password = nested_string(extra, &["security", "sasl", "password"]);
    let sasl_mechanism = nested_string(extra, &["security", "sasl", "mechanism"]);
    let protocol_override = nested_string(extra, &["security", "protocol"]);

    if (tls_cert.is_some() && tls_key.is_none()) || (tls_key.is_some() && tls_cert.is_none()) {
        return Err(ConnectorFactoryError::build_failure(
            connector_name.to_string(),
            "kafka",
            crate::err!("security.tls.cert and security.tls.key must be provided together"),
        ));
    }

    let has_tls = tls_ca.is_some() || tls_cert.is_some() || tls_key.is_some();
    let has_sasl = sasl_username.is_some() || sasl_password.is_some() || sasl_mechanism.is_some();

    let protocol = protocol_override.unwrap_or_else(|| {
        if has_tls && has_sasl {
            "SASL_SSL".to_string()
        } else if has_tls {
            "SSL".to_string()
        } else if has_sasl {
            "SASL_PLAINTEXT".to_string()
        } else {
            "PLAINTEXT".to_string()
        }
    });

    Ok(KafkaSecuritySettings {
        protocol,
        ssl_ca_location: tls_ca,
        ssl_certificate_location: tls_cert,
        ssl_key_location: tls_key,
        sasl_username,
        sasl_password,
        sasl_mechanisms: sasl_mechanism,
    })
}

#[cfg(feature = "kafka")]
fn nested_string(map: &JsonMap<String, JsonValue>, path: &[&str]) -> Option<String> {
    let mut cursor = map.get(*path.first()?)?;

    for segment in path.iter().skip(1) {
        cursor = match cursor {
            JsonValue::Object(obj) => obj.get(*segment)?,
            _ => return None,
        };
    }

    cursor.as_str().map(|s| s.to_string())
}

#[cfg(feature = "db-mariadb")]
fn build_mariadb_handle(
    connector: &MariadbConnector,
) -> Result<MariaDbPoolHandle, ConnectorFactoryError> {
    let mut options = MySqlPoolOptions::new();
    if let Some(pool_cfg) = &connector.pool {
        if let Some(max) = pool_cfg.max_connections {
            options = options.max_connections(max);
        } else {
            options = options.max_connections(5);
        }
        if let Some(idle) = pool_cfg.idle_timeout_secs {
            options = options.idle_timeout(Duration::from_secs(idle));
        }
    } else {
        options = options.max_connections(5);
    }

    let pool = options.connect_lazy(&connector.url).map_err(|err| {
        ConnectorFactoryError::build_failure(
            connector.name.clone(),
            "mariadb",
            contextual_error("failed to build MariaDB pool", ChronicleError::new(err)),
        )
    })?;

    Ok(MariaDbPoolHandle {
        name: connector.name.clone(),
        url: connector.url.clone(),
        pool,
        schema: connector.schema.clone(),
        extra: map_from_btree(&connector.extra),
    })
}

#[cfg(feature = "db-postgres")]
fn build_postgres_handle(
    connector: &PostgresConnector,
) -> Result<PostgresPoolHandle, ConnectorFactoryError> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect_lazy(&connector.url)
        .map_err(|err| {
            ConnectorFactoryError::build_failure(
                connector.name.clone(),
                "postgres",
                contextual_error("failed to build Postgres pool", ChronicleError::new(err)),
            )
        })?;

    Ok(PostgresPoolHandle {
        name: connector.name.clone(),
        url: connector.url.clone(),
        pool,
        schema: connector.schema.clone(),
        extra: map_from_btree(&connector.extra),
    })
}

#[cfg(feature = "rabbitmq")]
fn build_rabbitmq_handle(connector: &RabbitmqConnector) -> RabbitmqHandle {
    let tls = connector.tls.as_ref().map(|tls| RabbitmqTlsConfig {
        ca: tls.ca.clone(),
        cert: tls.cert.clone(),
        key: tls.key.clone(),
        extra: map_from_btree(&tls.extra),
    });

    RabbitmqHandle {
        name: connector.name.clone(),
        url: connector.url.clone(),
        prefetch: connector.prefetch,
        tls,
        extra: map_from_btree(&connector.extra),
    }
}

#[cfg(feature = "mqtt")]
fn build_mqtt_handle(connector: &MqttConnector) -> MqttHandle {
    let tls = connector.tls.as_ref().map(|tls| MqttTlsConfig {
        ca: tls.ca.clone(),
        cert: tls.cert.clone(),
        key: tls.key.clone(),
        extra: map_from_btree(&tls.extra),
    });

    MqttHandle {
        name: connector.name.clone(),
        url: connector.url.clone(),
        client_id: connector.client_id.clone(),
        username: connector.username.clone(),
        password: connector.password.clone(),
        keep_alive: connector.keep_alive,
        tls,
        extra: map_from_btree(&connector.extra),
    }
}

#[cfg(feature = "mqtt")]
fn build_mqtt_options_for_handle(
    handle: &MqttHandle,
) -> Result<MqttOptions, ConnectorFactoryError> {
    let url = handle.url();
    let parsed = Url::parse(url).map_err(|err| {
        ConnectorFactoryError::build_failure(
            handle.name(),
            "mqtt",
            contextual_error(
                format!("invalid mqtt url `{url}`"),
                ChronicleError::new(err),
            ),
        )
    })?;

    let host = parsed.host_str().ok_or_else(|| {
        ConnectorFactoryError::build_failure(
            handle.name(),
            "mqtt",
            crate::err!("mqtt url must specify host"),
        )
    })?;

    let scheme = parsed.scheme().to_ascii_lowercase();
    let default_port = match scheme.as_str() {
        "mqtt" | "tcp" => 1883,
        "mqtts" | "ssl" => 8883,
        other => {
            return Err(ConnectorFactoryError::build_failure(
                handle.name(),
                "mqtt",
                crate::err!(format!("unsupported mqtt url scheme `{other}`")),
            ))
        }
    };

    let port = parsed.port().unwrap_or(default_port);

    let client_id = handle
        .client_id()
        .filter(|id| !id.trim().is_empty())
        .map(|id| id.to_string())
        .unwrap_or_else(|| format!("chronicle-{}", Uuid::new_v4()));

    let mut options = MqttOptions::new(client_id, host, port);

    if let Some(secs) = handle.keep_alive() {
        options.set_keep_alive(Duration::from_secs(secs));
    }

    if let Some(user) = handle.username() {
        let pass = handle.password().unwrap_or("");
        options.set_credentials(user, pass);
    }

    if matches!(scheme.as_str(), "mqtts" | "ssl") || handle.tls().is_some() {
        let transport = build_mqtt_transport(handle)?;
        options.set_transport(transport);
    }

    Ok(options)
}

#[cfg(feature = "mqtt")]
fn build_mqtt_transport(handle: &MqttHandle) -> Result<Transport, ConnectorFactoryError> {
    if let Some(tls) = handle.tls() {
        let ca_bytes = match tls.ca() {
            Some(path) => Some(fs::read(path).map_err(|err| {
                ConnectorFactoryError::build_failure(
                    handle.name(),
                    "mqtt",
                    contextual_error(
                        format!("failed to read mqtt tls ca `{}`", path.display()),
                        ChronicleError::new(err),
                    ),
                )
            })?),
            None => None,
        };

        let client_auth = match (tls.cert(), tls.key()) {
            (Some(cert), Some(key)) => {
                let cert_bytes = fs::read(cert).map_err(|err| {
                    ConnectorFactoryError::build_failure(
                        handle.name(),
                        "mqtt",
                        contextual_error(
                            format!("failed to read mqtt tls cert `{}`", cert.display()),
                            ChronicleError::new(err),
                        ),
                    )
                })?;
                let key_bytes = fs::read(key).map_err(|err| {
                    ConnectorFactoryError::build_failure(
                        handle.name(),
                        "mqtt",
                        contextual_error(
                            format!("failed to read mqtt tls key `{}`", key.display()),
                            ChronicleError::new(err),
                        ),
                    )
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

#[cfg(feature = "smtp")]
fn build_smtp_handle(connector: &SmtpConnector) -> Result<SmtpHandle, ConnectorFactoryError> {
    if let Some(tls) = connector.tls.as_ref() {
        if let Some(ca_path) = tls.ca.as_ref() {
            if !ca_path.exists() {
                return Err(ConnectorFactoryError::build_failure(
                    connector.name.clone(),
                    "smtp",
                    crate::err!(format!("CA file `{}` not found", ca_path.display())),
                ));
            }
        }

        if let Some(cert_path) = tls.cert.as_ref() {
            if !cert_path.exists() {
                return Err(ConnectorFactoryError::build_failure(
                    connector.name.clone(),
                    "smtp",
                    crate::err!(format!(
                        "certificate file `{}` not found",
                        cert_path.display()
                    )),
                ));
            }
        }

        if let Some(key_path) = tls.key.as_ref() {
            if !key_path.exists() {
                return Err(ConnectorFactoryError::build_failure(
                    connector.name.clone(),
                    "smtp",
                    crate::err!(format!("key file `{}` not found", key_path.display())),
                ));
            }
        }
    }

    let auth = connector.auth.as_ref().map(|auth| SmtpAuthSettings {
        username: auth.username.clone(),
        password: auth.password.clone(),
        mechanism: auth.mechanism.clone(),
        extra: map_from_btree(&auth.extra),
    });

    let tls = connector.tls.as_ref().map(|tls| SmtpTlsSettings {
        mode: tls.mode.clone(),
        ca: tls.ca.clone(),
        domain: tls.domain.clone(),
        cert: tls.cert.clone(),
        key: tls.key.clone(),
        extra: map_from_btree(&tls.extra),
    });

    Ok(SmtpHandle {
        name: connector.name.clone(),
        host: connector.host.clone(),
        port: connector.port,
        auth,
        tls,
        default_from: connector.default_from.clone(),
        timeout_secs: connector.timeout_secs,
        extra: map_from_btree(&connector.extra),
    })
}

#[allow(dead_code)]
fn map_from_btree(
    map: &std::collections::BTreeMap<String, JsonValue>,
) -> JsonMap<String, JsonValue> {
    map.iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

#[cfg(feature = "kafka")]
fn json_to_config_value(value: &JsonValue) -> Option<String> {
    match value {
        JsonValue::Null => None,
        JsonValue::Bool(flag) => Some(flag.to_string()),
        JsonValue::Number(num) => Some(num.to_string()),
        JsonValue::String(text) => Some(text.clone()),
        JsonValue::Array(_) | JsonValue::Object(_) => None,
    }
}

#[allow(dead_code)]
fn join_pem_files(
    cert_path: &std::path::Path,
    key_path: &std::path::Path,
) -> Result<Vec<u8>, ChronicleError> {
    let mut buffer = fs::read(cert_path).map_err(|err| {
        contextual_error(
            format!("failed to read client cert `{}`", cert_path.display()),
            ChronicleError::new(err),
        )
    })?;
    if !buffer.ends_with(b"\n") {
        buffer.push(b'\n');
    }
    let key = fs::read(key_path).map_err(|err| {
        contextual_error(
            format!("failed to read client key `{}`", key_path.display()),
            ChronicleError::new(err),
        )
    })?;
    buffer.extend_from_slice(&key);
    Ok(buffer)
}

// tests moved to tests/unit/integration_factory_tests.rs
