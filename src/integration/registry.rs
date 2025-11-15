use crate::config::integration::{
    ConnectorConfig, ConnectorKind, ConnectorTimeouts, DbPoolOptions, GrpcConnectorOptions,
    GrpcTlsOptions, HttpClientConnectorOptions, HttpClientPoolOptions, HttpClientTlsOptions,
    HttpServerConnectorOptions, HttpServerHealthOptions, HttpServerTlsOptions, IntegrationConfig,
    KafkaConnectorOptions, MariadbConnectorOptions, MongodbConnectorOptions, MqttConnectorOptions,
    PhaseKind, PostgresConnectorOptions, RabbitmqConnectorOptions, RedisConnectorOptions,
    RedisPoolOptions, RetryBudget, ScalarValue, SmtpAuthOptions, SmtpConnectorOptions, SmtpTlsMode,
    SmtpTlsOptions,
};
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use thiserror::Error;

pub trait ConnectorHandle {
    fn name(&self) -> &str;
    fn timeouts(&self) -> &ConnectorTimeouts;
    fn retry_budget(&self) -> Option<&RetryBudget>;
    fn extra(&self) -> &BTreeMap<String, JsonValue>;
}

#[derive(Debug, Default)]
pub struct ConnectorRegistry {
    kafka: HashMap<String, KafkaConnector>,
    postgres: HashMap<String, PostgresConnector>,
    mariadb: HashMap<String, MariadbConnector>,
    http_servers: HashMap<String, HttpServerConnector>,
    http_clients: HashMap<String, HttpClientConnector>,
    grpc: HashMap<String, GrpcConnector>,
    rabbitmq: HashMap<String, RabbitmqConnector>,
    mqtt: HashMap<String, MqttConnector>,
    mongodb: HashMap<String, MongodbConnector>,
    redis: HashMap<String, RedisConnector>,
    smtp: HashMap<String, SmtpConnector>,
    unknown: HashMap<String, ConnectorConfig>,
    base_dir: PathBuf,
}

impl ConnectorRegistry {
    pub fn build(
        config: &IntegrationConfig,
        config_dir: impl AsRef<Path>,
    ) -> Result<Self, ConnectorRegistryError> {
        let base = config_dir.as_ref();
        let kafka_idempotent = collect_idempotent_kafka_connectors(config);

        let mut registry = ConnectorRegistry {
            base_dir: base.to_path_buf(),
            ..ConnectorRegistry::default()
        };

        for connector in &config.connectors {
            match connector.kind {
                ConnectorKind::Kafka => {
                    let options = decode_options::<KafkaConnectorOptions>(connector)?;
                    let handle = KafkaConnector {
                        name: connector.name.clone(),
                        brokers: options.brokers,
                        create_topics_if_missing: options.create_topics_if_missing,
                        force_idempotent: kafka_idempotent.contains(&connector.name),
                        timeouts: connector.timeouts,
                        retry_budget: connector.retry_budget.clone(),
                        extra: options.extra,
                    };
                    registry.kafka.insert(connector.name.clone(), handle);
                }
                ConnectorKind::Mariadb => {
                    let options = decode_options::<MariadbConnectorOptions>(connector)?;
                    let handle = MariadbConnector {
                        name: connector.name.clone(),
                        url: options.url,
                        schema: options.schema,
                        pool: options.pool.map(|pool| DbPoolConfig {
                            max_connections: pool.max_connections,
                            idle_timeout_secs: pool.idle_timeout_secs,
                            extra: pool.extra,
                        }),
                        timeouts: connector.timeouts,
                        retry_budget: connector.retry_budget.clone(),
                        extra: options.extra,
                    };
                    registry.mariadb.insert(connector.name.clone(), handle);
                }
                ConnectorKind::HttpServer => {
                    let options = decode_options::<HttpServerConnectorOptions>(connector)?;
                    let tls = options
                        .tls
                        .as_ref()
                        .map(|tls| resolve_http_server_tls(tls, base));
                    let health = options.health.as_ref().map(resolve_http_server_health);

                    let handle = HttpServerConnector {
                        name: connector.name.clone(),
                        host: options.host,
                        port: options.port,
                        tls,
                        health,
                        timeouts: connector.timeouts,
                        retry_budget: connector.retry_budget.clone(),
                        extra: options.extra,
                    };

                    registry.http_servers.insert(connector.name.clone(), handle);
                }
                ConnectorKind::HttpClient => {
                    let options = decode_options::<HttpClientConnectorOptions>(connector)?;
                    let tls = options
                        .tls
                        .as_ref()
                        .map(|tls| resolve_http_client_tls(tls, base));
                    let default_headers = options
                        .default_headers
                        .unwrap_or_default()
                        .into_iter()
                        .filter_map(|(name, value)| {
                            let trimmed = name.trim();
                            if trimmed.is_empty() {
                                None
                            } else {
                                Some((trimmed.to_string(), value))
                            }
                        })
                        .collect();

                    let handle = HttpClientConnector {
                        name: connector.name.clone(),
                        base_url: options.base_url,
                        tls,
                        pool: options.pool.map(HttpClientPoolConfig::from_options),
                        default_headers,
                        timeouts: connector.timeouts,
                        retry_budget: connector.retry_budget.clone(),
                        extra: options.extra,
                    };

                    registry.http_clients.insert(connector.name.clone(), handle);
                }
                ConnectorKind::Grpc => {
                    let options = decode_options::<GrpcConnectorOptions>(connector)?;
                    let tls = options.tls.as_ref().map(|tls| resolve_grpc_tls(tls, base));
                    let descriptor = resolve_optional_path(base, options.descriptor.clone());

                    let handle = GrpcConnector {
                        name: connector.name.clone(),
                        endpoint: options.endpoint,
                        tls,
                        descriptor,
                        metadata: options.metadata,
                        timeouts: connector.timeouts,
                        retry_budget: connector.retry_budget.clone(),
                        extra: options.extra,
                    };

                    registry.grpc.insert(connector.name.clone(), handle);
                }
                ConnectorKind::Postgres => {
                    let options = decode_options::<PostgresConnectorOptions>(connector)?;
                    let handle = PostgresConnector {
                        name: connector.name.clone(),
                        url: options.url,
                        schema: options.schema,
                        timeouts: connector.timeouts,
                        retry_budget: connector.retry_budget.clone(),
                        extra: options.extra,
                    };
                    registry.postgres.insert(connector.name.clone(), handle);
                }
                ConnectorKind::Rabbitmq => {
                    let options = decode_options::<RabbitmqConnectorOptions>(connector)?;
                    let handle = RabbitmqConnector {
                        name: connector.name.clone(),
                        url: options.url,
                        prefetch: options.prefetch,
                        tls: options.tls.map(|tls| RabbitmqTlsFiles {
                            ca: tls.ca.map(|p| base.join(p)),
                            cert: tls.cert.map(|p| base.join(p)),
                            key: tls.key.map(|p| base.join(p)),
                            extra: tls.extra,
                        }),
                        timeouts: connector.timeouts,
                        retry_budget: connector.retry_budget.clone(),
                        extra: options.extra,
                    };
                    registry.rabbitmq.insert(connector.name.clone(), handle);
                }
                ConnectorKind::Mqtt => {
                    let options = decode_options::<MqttConnectorOptions>(connector)?;
                    let handle = MqttConnector {
                        name: connector.name.clone(),
                        url: options.url,
                        client_id: options.client_id,
                        username: options.username,
                        password: options.password,
                        keep_alive: options.keep_alive,
                        tls: options.tls.map(|tls| MqttTlsFiles {
                            ca: tls.ca.map(|p| base.join(p)),
                            cert: tls.cert.map(|p| base.join(p)),
                            key: tls.key.map(|p| base.join(p)),
                            extra: tls.extra,
                        }),
                        timeouts: connector.timeouts,
                        retry_budget: connector.retry_budget.clone(),
                        extra: options.extra,
                    };
                    registry.mqtt.insert(connector.name.clone(), handle);
                }
                ConnectorKind::Mongodb => {
                    let options = decode_options::<MongodbConnectorOptions>(connector)?;
                    let handle = MongodbConnector {
                        name: connector.name.clone(),
                        uri: options.uri,
                        database: options.database,
                        tls: options.tls.map(|tls| MongodbTlsFiles {
                            ca: resolve_optional_path(base, tls.ca),
                            cert: resolve_optional_path(base, tls.cert),
                            key: resolve_optional_path(base, tls.key),
                            extra: tls.extra,
                        }),
                        options: options.options,
                        timeouts: connector.timeouts,
                        retry_budget: connector.retry_budget.clone(),
                        extra: options.extra,
                    };
                    registry.mongodb.insert(connector.name.clone(), handle);
                }
                ConnectorKind::Redis => {
                    let options = decode_options::<RedisConnectorOptions>(connector)?;
                    let handle = RedisConnector {
                        name: connector.name.clone(),
                        url: options.url,
                        cluster: options.cluster,
                        tls: options.tls.map(|tls| RedisTlsFiles {
                            ca: resolve_optional_path(base, tls.ca),
                            cert: resolve_optional_path(base, tls.cert),
                            key: resolve_optional_path(base, tls.key),
                            extra: tls.extra,
                        }),
                        pool: options.pool.map(RedisPoolConfig::from),
                        timeouts: connector.timeouts,
                        retry_budget: connector.retry_budget.clone(),
                        extra: options.extra,
                    };
                    registry.redis.insert(connector.name.clone(), handle);
                }
                ConnectorKind::Smtp => {
                    let options = decode_options::<SmtpConnectorOptions>(connector)?;
                    let auth = options.auth.map(resolve_smtp_auth);
                    let tls = options.tls.as_ref().map(|tls| resolve_smtp_tls(tls, base));

                    let handle = SmtpConnector {
                        name: connector.name.clone(),
                        host: options.host,
                        port: options.port.unwrap_or(587),
                        auth,
                        tls,
                        default_from: options.default_from,
                        timeout_secs: options.timeout_secs,
                        timeouts: connector.timeouts,
                        retry_budget: connector.retry_budget.clone(),
                        extra: options.extra,
                    };

                    registry.smtp.insert(connector.name.clone(), handle);
                }
                ConnectorKind::Unknown(_) => {
                    registry
                        .unknown
                        .insert(connector.name.clone(), connector.clone());
                }
            }
        }

        Ok(registry)
    }

    pub fn kafka(&self, name: &str) -> Option<&KafkaConnector> {
        self.kafka.get(name)
    }

    pub fn postgres(&self, name: &str) -> Option<&PostgresConnector> {
        self.postgres.get(name)
    }

    pub fn mariadb(&self, name: &str) -> Option<&MariadbConnector> {
        self.mariadb.get(name)
    }

    pub fn http_server(&self, name: &str) -> Option<&HttpServerConnector> {
        self.http_servers.get(name)
    }

    pub fn http_client(&self, name: &str) -> Option<&HttpClientConnector> {
        self.http_clients.get(name)
    }

    pub fn grpc(&self, name: &str) -> Option<&GrpcConnector> {
        self.grpc.get(name)
    }

    pub fn rabbitmq(&self, name: &str) -> Option<&RabbitmqConnector> {
        self.rabbitmq.get(name)
    }

    pub fn mqtt(&self, name: &str) -> Option<&MqttConnector> {
        self.mqtt.get(name)
    }

    pub fn mongodb(&self, name: &str) -> Option<&MongodbConnector> {
        self.mongodb.get(name)
    }

    pub fn redis(&self, name: &str) -> Option<&RedisConnector> {
        self.redis.get(name)
    }

    pub fn smtp(&self, name: &str) -> Option<&SmtpConnector> {
        self.smtp.get(name)
    }

    pub fn has_kafka_connectors(&self) -> bool {
        !self.kafka.is_empty()
    }

    pub fn has_postgres_connectors(&self) -> bool {
        !self.postgres.is_empty()
    }

    pub fn has_rabbitmq_connectors(&self) -> bool {
        !self.rabbitmq.is_empty()
    }

    pub fn has_mqtt_connectors(&self) -> bool {
        !self.mqtt.is_empty()
    }

    pub fn has_mongodb_connectors(&self) -> bool {
        !self.mongodb.is_empty()
    }

    pub fn has_redis_connectors(&self) -> bool {
        !self.redis.is_empty()
    }

    pub fn has_smtp_connectors(&self) -> bool {
        !self.smtp.is_empty()
    }

    pub fn unknown(&self) -> impl Iterator<Item = (&String, &ConnectorConfig)> {
        self.unknown.iter()
    }

    pub fn base_dir(&self) -> &Path {
        self.base_dir.as_path()
    }

    pub fn for_each_handle<F>(&self, mut visitor: F)
    where
        F: FnMut(&dyn ConnectorHandle),
    {
        for handle in self.kafka.values() {
            visitor(handle);
        }
        for handle in self.postgres.values() {
            visitor(handle);
        }
        for handle in self.mariadb.values() {
            visitor(handle);
        }
        for handle in self.http_servers.values() {
            visitor(handle);
        }
        for handle in self.http_clients.values() {
            visitor(handle);
        }
        for handle in self.grpc.values() {
            visitor(handle);
        }
        for handle in self.rabbitmq.values() {
            visitor(handle);
        }
        for handle in self.mqtt.values() {
            visitor(handle);
        }
        for handle in self.mongodb.values() {
            visitor(handle);
        }
        for handle in self.redis.values() {
            visitor(handle);
        }
        for handle in self.smtp.values() {
            visitor(handle);
        }
    }
}

#[derive(Debug, Clone)]
pub struct KafkaConnector {
    pub name: String,
    pub brokers: Vec<String>,
    pub create_topics_if_missing: bool,
    pub force_idempotent: bool,
    pub timeouts: ConnectorTimeouts,
    pub retry_budget: Option<RetryBudget>,
    pub extra: std::collections::BTreeMap<String, JsonValue>,
}

impl ConnectorHandle for KafkaConnector {
    fn name(&self) -> &str {
        &self.name
    }

    fn timeouts(&self) -> &ConnectorTimeouts {
        &self.timeouts
    }

    fn retry_budget(&self) -> Option<&RetryBudget> {
        self.retry_budget.as_ref()
    }

    fn extra(&self) -> &BTreeMap<String, JsonValue> {
        &self.extra
    }
}

#[derive(Debug, Clone)]
pub struct PostgresConnector {
    pub name: String,
    pub url: String,
    pub schema: Option<String>,
    pub timeouts: ConnectorTimeouts,
    pub retry_budget: Option<RetryBudget>,
    pub extra: std::collections::BTreeMap<String, JsonValue>,
}

impl ConnectorHandle for PostgresConnector {
    fn name(&self) -> &str {
        &self.name
    }

    fn timeouts(&self) -> &ConnectorTimeouts {
        &self.timeouts
    }

    fn retry_budget(&self) -> Option<&RetryBudget> {
        self.retry_budget.as_ref()
    }

    fn extra(&self) -> &BTreeMap<String, JsonValue> {
        &self.extra
    }
}

#[derive(Debug, Clone)]
pub struct MariadbConnector {
    pub name: String,
    pub url: String,
    pub schema: Option<String>,
    pub pool: Option<DbPoolConfig>,
    pub timeouts: ConnectorTimeouts,
    pub retry_budget: Option<RetryBudget>,
    pub extra: std::collections::BTreeMap<String, JsonValue>,
}

impl ConnectorHandle for MariadbConnector {
    fn name(&self) -> &str {
        &self.name
    }

    fn timeouts(&self) -> &ConnectorTimeouts {
        &self.timeouts
    }

    fn retry_budget(&self) -> Option<&RetryBudget> {
        self.retry_budget.as_ref()
    }

    fn extra(&self) -> &BTreeMap<String, JsonValue> {
        &self.extra
    }
}

#[derive(Debug, Clone)]
pub struct HttpServerConnector {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub tls: Option<HttpServerTlsFiles>,
    pub health: Option<HttpServerHealthConfig>,
    pub timeouts: ConnectorTimeouts,
    pub retry_budget: Option<RetryBudget>,
    pub extra: std::collections::BTreeMap<String, JsonValue>,
}

impl ConnectorHandle for HttpServerConnector {
    fn name(&self) -> &str {
        &self.name
    }

    fn timeouts(&self) -> &ConnectorTimeouts {
        &self.timeouts
    }

    fn retry_budget(&self) -> Option<&RetryBudget> {
        self.retry_budget.as_ref()
    }

    fn extra(&self) -> &BTreeMap<String, JsonValue> {
        &self.extra
    }
}

#[derive(Debug, Clone)]
pub struct HttpServerTlsFiles {
    pub key: Option<PathBuf>,
    pub cert: Option<PathBuf>,
    pub ca: Option<PathBuf>,
    pub extra: std::collections::BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone)]
pub struct HttpServerHealthConfig {
    pub method: Option<String>,
    pub path: Option<String>,
    pub extra: std::collections::BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone)]
pub struct HttpClientConnector {
    pub name: String,
    pub base_url: String,
    pub tls: Option<HttpClientTlsConfig>,
    pub pool: Option<HttpClientPoolConfig>,
    pub default_headers: Vec<(String, String)>,
    pub timeouts: ConnectorTimeouts,
    pub retry_budget: Option<RetryBudget>,
    pub extra: std::collections::BTreeMap<String, JsonValue>,
}

impl ConnectorHandle for HttpClientConnector {
    fn name(&self) -> &str {
        &self.name
    }

    fn timeouts(&self) -> &ConnectorTimeouts {
        &self.timeouts
    }

    fn retry_budget(&self) -> Option<&RetryBudget> {
        self.retry_budget.as_ref()
    }

    fn extra(&self) -> &BTreeMap<String, JsonValue> {
        &self.extra
    }
}

#[derive(Debug, Clone)]
pub struct GrpcConnector {
    pub name: String,
    pub endpoint: String,
    pub tls: Option<GrpcTlsFiles>,
    pub descriptor: Option<PathBuf>,
    pub metadata: std::collections::BTreeMap<String, String>,
    pub timeouts: ConnectorTimeouts,
    pub retry_budget: Option<RetryBudget>,
    pub extra: std::collections::BTreeMap<String, JsonValue>,
}

impl ConnectorHandle for GrpcConnector {
    fn name(&self) -> &str {
        &self.name
    }

    fn timeouts(&self) -> &ConnectorTimeouts {
        &self.timeouts
    }

    fn retry_budget(&self) -> Option<&RetryBudget> {
        self.retry_budget.as_ref()
    }

    fn extra(&self) -> &BTreeMap<String, JsonValue> {
        &self.extra
    }
}

#[derive(Debug, Clone)]
pub struct GrpcTlsFiles {
    pub domain: Option<String>,
    pub ca: Option<PathBuf>,
    pub cert: Option<PathBuf>,
    pub key: Option<PathBuf>,
    pub extra: std::collections::BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone)]
pub struct RabbitmqConnector {
    pub name: String,
    pub url: String,
    pub prefetch: Option<u32>,
    pub tls: Option<RabbitmqTlsFiles>,
    pub timeouts: ConnectorTimeouts,
    pub retry_budget: Option<RetryBudget>,
    pub extra: std::collections::BTreeMap<String, JsonValue>,
}

impl ConnectorHandle for RabbitmqConnector {
    fn name(&self) -> &str {
        &self.name
    }

    fn timeouts(&self) -> &ConnectorTimeouts {
        &self.timeouts
    }

    fn retry_budget(&self) -> Option<&RetryBudget> {
        self.retry_budget.as_ref()
    }

    fn extra(&self) -> &BTreeMap<String, JsonValue> {
        &self.extra
    }
}

#[derive(Debug, Clone)]
pub struct RabbitmqTlsFiles {
    pub ca: Option<PathBuf>,
    pub cert: Option<PathBuf>,
    pub key: Option<PathBuf>,
    pub extra: std::collections::BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone)]
pub struct MqttConnector {
    pub name: String,
    pub url: String,
    pub client_id: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub keep_alive: Option<u64>,
    pub tls: Option<MqttTlsFiles>,
    pub timeouts: ConnectorTimeouts,
    pub retry_budget: Option<RetryBudget>,
    pub extra: std::collections::BTreeMap<String, JsonValue>,
}

impl ConnectorHandle for MqttConnector {
    fn name(&self) -> &str {
        &self.name
    }

    fn timeouts(&self) -> &ConnectorTimeouts {
        &self.timeouts
    }

    fn retry_budget(&self) -> Option<&RetryBudget> {
        self.retry_budget.as_ref()
    }

    fn extra(&self) -> &BTreeMap<String, JsonValue> {
        &self.extra
    }
}

#[derive(Debug, Clone)]
pub struct MqttTlsFiles {
    pub ca: Option<PathBuf>,
    pub cert: Option<PathBuf>,
    pub key: Option<PathBuf>,
    pub extra: std::collections::BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone)]
pub struct MongodbConnector {
    pub name: String,
    pub uri: String,
    pub database: String,
    pub tls: Option<MongodbTlsFiles>,
    pub options: Option<JsonValue>,
    pub timeouts: ConnectorTimeouts,
    pub retry_budget: Option<RetryBudget>,
    pub extra: std::collections::BTreeMap<String, JsonValue>,
}

impl ConnectorHandle for MongodbConnector {
    fn name(&self) -> &str {
        &self.name
    }

    fn timeouts(&self) -> &ConnectorTimeouts {
        &self.timeouts
    }

    fn retry_budget(&self) -> Option<&RetryBudget> {
        self.retry_budget.as_ref()
    }

    fn extra(&self) -> &BTreeMap<String, JsonValue> {
        &self.extra
    }
}

#[derive(Debug, Clone)]
pub struct MongodbTlsFiles {
    pub ca: Option<PathBuf>,
    pub cert: Option<PathBuf>,
    pub key: Option<PathBuf>,
    pub extra: std::collections::BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone)]
pub struct RedisConnector {
    pub name: String,
    pub url: String,
    pub cluster: Option<bool>,
    pub tls: Option<RedisTlsFiles>,
    pub pool: Option<RedisPoolConfig>,
    pub timeouts: ConnectorTimeouts,
    pub retry_budget: Option<RetryBudget>,
    pub extra: std::collections::BTreeMap<String, JsonValue>,
}

impl ConnectorHandle for RedisConnector {
    fn name(&self) -> &str {
        &self.name
    }

    fn timeouts(&self) -> &ConnectorTimeouts {
        &self.timeouts
    }

    fn retry_budget(&self) -> Option<&RetryBudget> {
        self.retry_budget.as_ref()
    }

    fn extra(&self) -> &BTreeMap<String, JsonValue> {
        &self.extra
    }
}

#[derive(Debug, Clone)]
pub struct RedisTlsFiles {
    pub ca: Option<PathBuf>,
    pub cert: Option<PathBuf>,
    pub key: Option<PathBuf>,
    pub extra: std::collections::BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone)]
pub struct RedisPoolConfig {
    pub max_connections: Option<u32>,
    pub min_idle: Option<u32>,
    pub idle_timeout_secs: Option<u64>,
    pub extra: std::collections::BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone)]
pub struct SmtpConnector {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub auth: Option<SmtpAuthConfig>,
    pub tls: Option<SmtpTlsConfig>,
    pub default_from: Option<String>,
    pub timeout_secs: Option<u64>,
    pub timeouts: ConnectorTimeouts,
    pub retry_budget: Option<RetryBudget>,
    pub extra: std::collections::BTreeMap<String, JsonValue>,
}

impl ConnectorHandle for SmtpConnector {
    fn name(&self) -> &str {
        &self.name
    }

    fn timeouts(&self) -> &ConnectorTimeouts {
        &self.timeouts
    }

    fn retry_budget(&self) -> Option<&RetryBudget> {
        self.retry_budget.as_ref()
    }

    fn extra(&self) -> &BTreeMap<String, JsonValue> {
        &self.extra
    }
}

#[derive(Debug, Clone)]
pub struct SmtpAuthConfig {
    pub username: String,
    pub password: String,
    pub mechanism: Option<String>,
    pub extra: std::collections::BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone)]
pub struct SmtpTlsConfig {
    pub mode: SmtpTlsMode,
    pub ca: Option<PathBuf>,
    pub domain: Option<String>,
    pub cert: Option<PathBuf>,
    pub key: Option<PathBuf>,
    pub extra: std::collections::BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone)]
pub struct HttpClientTlsConfig {
    pub ca: Option<PathBuf>,
    pub cert: Option<PathBuf>,
    pub key: Option<PathBuf>,
    pub extra: std::collections::BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone)]
pub struct HttpClientPoolConfig {
    pub max_idle_per_host: Option<usize>,
    pub idle_timeout_secs: Option<u64>,
    pub extra: std::collections::BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone)]
pub struct DbPoolConfig {
    pub max_connections: Option<u32>,
    pub idle_timeout_secs: Option<u64>,
    pub extra: std::collections::BTreeMap<String, JsonValue>,
}

fn decode_options<T>(connector: &ConnectorConfig) -> Result<T, ConnectorRegistryError>
where
    T: serde::de::DeserializeOwned,
{
    connector
        .option_deser()
        .map_err(|source| ConnectorRegistryError::InvalidConnectorOptions {
            name: connector.name.clone(),
            source,
        })
}

fn collect_idempotent_kafka_connectors(config: &IntegrationConfig) -> BTreeSet<String> {
    let mut names = BTreeSet::new();

    for chronicle in &config.chronicles {
        let idempotent = chronicle
            .policy
            .delivery
            .as_ref()
            .and_then(|policy| policy.idempotent)
            .unwrap_or(false);

        if !idempotent {
            continue;
        }

        for phase in &chronicle.phases {
            if phase.kind != PhaseKind::KafkaProducer {
                continue;
            }

            if let Some(connector) = phase.option("connector").and_then(ScalarValue::as_str) {
                let trimmed = connector.trim();
                if !trimmed.is_empty() {
                    names.insert(trimmed.to_string());
                }
            }
        }
    }

    names
}

fn resolve_http_server_tls(options: &HttpServerTlsOptions, base: &Path) -> HttpServerTlsFiles {
    HttpServerTlsFiles {
        key: resolve_optional_path(base, options.key.clone()),
        cert: resolve_optional_path(base, options.cert.clone()),
        ca: resolve_optional_path(base, options.ca.clone()),
        extra: options.extra.clone(),
    }
}

fn resolve_http_server_health(options: &HttpServerHealthOptions) -> HttpServerHealthConfig {
    HttpServerHealthConfig {
        method: options.method.clone(),
        path: options.path.clone(),
        extra: options.extra.clone(),
    }
}

fn resolve_http_client_tls(options: &HttpClientTlsOptions, base: &Path) -> HttpClientTlsConfig {
    HttpClientTlsConfig {
        ca: resolve_optional_path(base, options.ca.clone()),
        cert: resolve_optional_path(base, options.cert.clone()),
        key: resolve_optional_path(base, options.key.clone()),
        extra: options.extra.clone(),
    }
}

fn resolve_smtp_auth(options: SmtpAuthOptions) -> SmtpAuthConfig {
    SmtpAuthConfig {
        username: options.username,
        password: options.password,
        mechanism: options.mechanism,
        extra: options.extra,
    }
}

fn resolve_smtp_tls(options: &SmtpTlsOptions, base: &Path) -> SmtpTlsConfig {
    SmtpTlsConfig {
        mode: options.mode.clone().unwrap_or(SmtpTlsMode::Starttls),
        ca: resolve_optional_path(base, options.ca.clone()),
        domain: options.domain.clone(),
        cert: resolve_optional_path(base, options.cert.clone()),
        key: resolve_optional_path(base, options.key.clone()),
        extra: options.extra.clone(),
    }
}

fn resolve_grpc_tls(options: &GrpcTlsOptions, base: &Path) -> GrpcTlsFiles {
    GrpcTlsFiles {
        domain: options.domain.clone(),
        ca: resolve_optional_path(base, options.ca.clone()),
        cert: resolve_optional_path(base, options.cert.clone()),
        key: resolve_optional_path(base, options.key.clone()),
        extra: options.extra.clone(),
    }
}

impl HttpClientPoolConfig {
    fn from_options(options: HttpClientPoolOptions) -> Self {
        Self {
            max_idle_per_host: options.max_idle_per_host,
            idle_timeout_secs: options.idle_timeout_secs,
            extra: options.extra,
        }
    }
}

impl From<DbPoolOptions> for DbPoolConfig {
    fn from(options: DbPoolOptions) -> Self {
        Self {
            max_connections: options.max_connections,
            idle_timeout_secs: options.idle_timeout_secs,
            extra: options.extra,
        }
    }
}

impl From<RedisPoolOptions> for RedisPoolConfig {
    fn from(options: RedisPoolOptions) -> Self {
        Self {
            max_connections: options.max_connections,
            min_idle: options.min_idle,
            idle_timeout_secs: options.idle_timeout_secs,
            extra: options.extra,
        }
    }
}

fn resolve_optional_path(base: &Path, value: Option<String>) -> Option<PathBuf> {
    value.map(|raw| {
        let candidate = PathBuf::from(raw);
        if candidate.is_absolute() {
            candidate
        } else {
            base.join(candidate)
        }
    })
}

#[derive(Debug, Error)]
pub enum ConnectorRegistryError {
    #[error("failed to decode options for connector `{name}`: {source}")]
    InvalidConnectorOptions {
        name: String,
        #[source]
        source: serde_json::Error,
    },
}

// tests moved to tests/unit/integration_registry_tests.rs
