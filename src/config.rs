pub mod integration;

use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct ChronicleConfig {
    #[serde(default)]
    pub kafka: KafkaConfig,
    #[serde(default)]
    pub database: Option<DatabaseConfig>,
    #[serde(default)]
    pub backpressure: BackpressureConfig,
    #[serde(default)]
    pub integration_config_path: Option<String>,
    #[serde(default)]
    pub connector_flags: ConnectorFlags,
}

pub use integration::IntegrationConfig;

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaConfig {
    pub brokers: Vec<String>,
    pub group_id: String,
    pub topics: Vec<String>,
    #[serde(default)]
    pub poll_interval_secs: Option<u64>,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            brokers: vec!["localhost:9092".to_string()],
            group_id: "chronicle-service".to_string(),
            topics: vec!["chronicle.phases".to_string()],
            poll_interval_secs: Some(5),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseConfig {
    pub url: String,
    #[serde(default)]
    pub max_connections: Option<u32>,
    #[serde(default)]
    pub acquire_timeout_secs: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct BackpressureConfig {
    #[serde(default)]
    pub http_max_concurrency: Option<usize>,
    #[serde(default)]
    pub kafka_max_inflight: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConnectorFlags {
    #[serde(default = "default_true")]
    pub rabbitmq: bool,
    #[serde(default = "default_true")]
    pub mqtt: bool,
    #[serde(default = "default_true")]
    pub mongodb: bool,
    #[serde(default = "default_true")]
    pub redis: bool,
}

impl Default for ConnectorFlags {
    fn default() -> Self {
        Self {
            rabbitmq: true,
            mqtt: true,
            mongodb: true,
            redis: true,
        }
    }
}

const fn default_true() -> bool {
    true
}

impl ChronicleConfig {
    pub fn load() -> Result<Self, ConfigError> {
        Config::builder()
            .add_source(File::with_name("config/local").required(false))
            .add_source(Environment::with_prefix("CHRONICLE").separator("__"))
            .build()?
            .try_deserialize()
    }
}
