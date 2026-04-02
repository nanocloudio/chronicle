pub mod integration;

use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
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

#[derive(Debug, Clone, Deserialize, Serialize)]
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DatabaseConfig {
    pub url: String,
    #[serde(default)]
    pub max_connections: Option<u32>,
    #[serde(default)]
    pub acquire_timeout_secs: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Default, Serialize)]
pub struct BackpressureConfig {
    #[serde(default)]
    pub http_max_concurrency: Option<usize>,
    #[serde(default)]
    pub kafka_max_inflight: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
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

#[derive(Debug, Error)]
pub enum ChronicleConfigError {
    #[error("failed to read config/local.toml: {0}")]
    Read(#[from] std::io::Error),
    #[error("invalid config/local.toml: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("failed to serialize config defaults: {0}")]
    SerializeDefaults(serde_json::Error),
    #[error("failed to deserialize merged config: {0}")]
    Deserialize(serde_json::Error),
}

impl ChronicleConfig {
    pub fn load() -> Result<Self, ChronicleConfigError> {
        let file = read_local_config(Path::new("config/local.toml"))?;
        let env = std::env::vars().collect::<BTreeMap<_, _>>();
        Self::from_sources(file.as_deref(), &env)
    }

    fn from_sources(
        file: Option<&str>,
        env: &BTreeMap<String, String>,
    ) -> Result<Self, ChronicleConfigError> {
        let mut merged = serde_json::to_value(Self::default())
            .map_err(ChronicleConfigError::SerializeDefaults)?;

        if let Some(file) = file {
            let parsed: toml::Value = toml::from_str(file)?;
            merge_json(&mut merged, toml_to_json(parsed));
        }

        for (key, value) in env {
            if let Some(path) = env_key_path(key) {
                insert_env_value(&mut merged, &path, parse_env_value(value));
            }
        }

        serde_json::from_value(merged).map_err(ChronicleConfigError::Deserialize)
    }
}

fn read_local_config(path: &Path) -> Result<Option<String>, ChronicleConfigError> {
    match fs::read_to_string(path) {
        Ok(contents) => Ok(Some(contents)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(ChronicleConfigError::Read(err)),
    }
}

fn env_key_path(key: &str) -> Option<Vec<String>> {
    let suffix = key.strip_prefix("CHRONICLE__")?;
    let path = suffix
        .split("__")
        .filter(|segment| !segment.is_empty())
        .map(|segment| segment.to_ascii_lowercase())
        .collect::<Vec<_>>();

    if path.is_empty() {
        None
    } else {
        Some(path)
    }
}

fn parse_env_value(raw: &str) -> JsonValue {
    let document = format!("value = {raw}");
    if let Ok(parsed) = toml::from_str::<toml::Table>(&document) {
        if parsed.contains_key("value") {
            return toml_to_json(toml::Value::Table(parsed))
                .get("value")
                .cloned()
                .unwrap_or_else(|| JsonValue::String(raw.to_string()));
        }
    }

    JsonValue::String(raw.to_string())
}

fn toml_to_json(value: toml::Value) -> JsonValue {
    match value {
        toml::Value::String(value) => JsonValue::String(value),
        toml::Value::Integer(value) => JsonValue::Number(JsonNumber::from(value)),
        toml::Value::Float(value) => JsonNumber::from_f64(value)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        toml::Value::Boolean(value) => JsonValue::Bool(value),
        toml::Value::Datetime(value) => JsonValue::String(value.to_string()),
        toml::Value::Array(values) => {
            JsonValue::Array(values.into_iter().map(toml_to_json).collect::<Vec<_>>())
        }
        toml::Value::Table(values) => JsonValue::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, toml_to_json(value)))
                .collect::<JsonMap<_, _>>(),
        ),
    }
}

fn merge_json(target: &mut JsonValue, incoming: JsonValue) {
    match (target, incoming) {
        (JsonValue::Object(target), JsonValue::Object(incoming)) => {
            for (key, value) in incoming {
                match target.get_mut(&key) {
                    Some(existing) => merge_json(existing, value),
                    None => {
                        target.insert(key, value);
                    }
                }
            }
        }
        (target, incoming) => *target = incoming,
    }
}

fn insert_env_value(root: &mut JsonValue, path: &[String], value: JsonValue) {
    if path.is_empty() {
        *root = value;
        return;
    }

    let mut current = root;
    for segment in &path[..path.len() - 1] {
        let object = current
            .as_object_mut()
            .expect("config merge root should remain an object");
        current = object
            .entry(segment.clone())
            .or_insert_with(|| JsonValue::Object(JsonMap::new()));
    }

    let object = current
        .as_object_mut()
        .expect("config merge leaf parent should remain an object");
    object.insert(path[path.len() - 1].clone(), value);
}

#[cfg(test)]
mod tests {
    use super::{ChronicleConfig, ChronicleConfigError};
    use std::collections::BTreeMap;

    #[test]
    fn load_uses_defaults_without_sources() -> Result<(), ChronicleConfigError> {
        let config = ChronicleConfig::from_sources(None, &BTreeMap::new())?;
        assert_eq!(config.kafka.group_id, "chronicle-service");
        assert_eq!(config.kafka.brokers, vec!["localhost:9092".to_string()]);
        assert!(config.connector_flags.redis);
        Ok(())
    }

    #[test]
    fn file_overrides_defaults() -> Result<(), ChronicleConfigError> {
        let file = r#"
            integration_config_path = "tests/fixtures/chronicle-integration.yaml"
            [connector_flags]
            redis = false
        "#;

        let config = ChronicleConfig::from_sources(Some(file), &BTreeMap::new())?;
        assert_eq!(
            config.integration_config_path.as_deref(),
            Some("tests/fixtures/chronicle-integration.yaml")
        );
        assert!(!config.connector_flags.redis);
        assert!(config.connector_flags.mqtt);
        Ok(())
    }

    #[test]
    fn env_overrides_file_values() -> Result<(), ChronicleConfigError> {
        let file = r#"
            [connector_flags]
            redis = false
            [kafka]
            brokers = ["kafka-a:9092"]
        "#;
        let env = BTreeMap::from([
            ("CHRONICLE__CONNECTOR_FLAGS__REDIS".to_string(), "true".to_string()),
            (
                "CHRONICLE__KAFKA__BROKERS".to_string(),
                "[\"kafka-b:9092\", \"kafka-c:9092\"]".to_string(),
            ),
        ]);

        let config = ChronicleConfig::from_sources(Some(file), &env)?;
        assert!(config.connector_flags.redis);
        assert_eq!(
            config.kafka.brokers,
            vec!["kafka-b:9092".to_string(), "kafka-c:9092".to_string()]
        );
        Ok(())
    }
}
