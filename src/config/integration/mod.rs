mod app;
mod chronicles;
mod connectors;
mod management;

use serde::de::Error as _;
use serde::Deserialize;
use serde_yaml::{self, Value as YamlValue};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use thiserror::Error;

pub use app::{
    known_feature_flags, AppConfig, AppLimits, HalfOpenPolicy, HttpLimits, JitterMode, KafkaLimits,
    MinReadyRoutes, OverflowPolicy, RetryBudget, RouteLimits,
};
pub use chronicles::{
    BackoffPolicy, ChronicleDefinition, ChroniclePhase, ChroniclePolicy, ChronicleTrigger,
    DeliveryPolicy, PhaseKind,
};
pub use connectors::{
    CircuitBreakerConfig, ConnectorConfig, ConnectorKind, ConnectorTimeouts, DbPoolOptions,
    GrpcConnectorOptions, GrpcTlsOptions, HttpClientConnectorOptions, HttpClientPoolOptions,
    HttpClientTlsOptions, HttpServerConnectorOptions, HttpServerHealthOptions,
    HttpServerTlsOptions, KafkaConnectorOptions, KafkaSaslOptions, KafkaTlsOptions,
    KafkaTopicOptions, MariadbConnectorOptions, MariadbTlsOptions, MongodbConnectorOptions,
    MongodbTlsOptions, MqttConnectorOptions, MqttTlsOptions, OptionMap, PostgresConnectorOptions,
    PostgresTlsOptions, RabbitmqConnectorOptions, RabbitmqTlsOptions, RedisConnectorOptions,
    RedisPoolOptions, RedisTlsOptions, ScalarValue, SmtpAuthOptions, SmtpConnectorOptions,
    SmtpTlsMode, SmtpTlsOptions,
};
pub use management::{ManagementConfig, ManagementEndpointConfig};

#[derive(Debug, Clone)]
pub struct IntegrationConfig {
    pub api_version: ApiVersion,
    pub app: AppConfig,
    pub connectors: Vec<ConnectorConfig>,
    pub chronicles: Vec<ChronicleDefinition>,
    pub management: Option<ManagementConfig>,
}

const TOP_LEVEL_FIELDS: &str = "api_version, app, connectors, chronicles, management";

impl IntegrationConfig {
    pub fn from_reader(mut reader: impl Read) -> Result<Self, IntegrationConfigError> {
        let mut contents = String::new();
        reader.read_to_string(&mut contents)?;
        Self::from_yaml_str(&contents)
    }

    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, IntegrationConfigError> {
        let file = File::open(path)?;
        Self::from_reader(file)
    }

    fn from_yaml_str(contents: &str) -> Result<Self, IntegrationConfigError> {
        let mut documents = serde_yaml::Deserializer::from_str(contents);
        let mut parsed = None;
        let mut extra_errors = Vec::new();

        for (index, document) in documents.by_ref().enumerate() {
            if index == 0 {
                parsed = Some(RawIntegrationFile::deserialize(document)?);
            } else {
                let _: YamlValue = YamlValue::deserialize(document)?;
                extra_errors
                    .push("error[root]: multiple YAML documents are not supported".to_string());
                break;
            }
        }

        let Some(raw) = parsed else {
            let err = serde_yaml::Error::custom(
                "integration config must contain exactly one YAML document",
            );
            return Err(IntegrationConfigError::Parse(err));
        };

        Self::from_raw(raw, extra_errors).map_err(IntegrationConfigError::Invalid)
    }

    fn from_raw(
        raw: RawIntegrationFile,
        mut errors: Vec<String>,
    ) -> Result<Self, IntegrationValidationError> {
        let RawIntegrationFile {
            api_version: raw_api_version,
            app: raw_app,
            connectors: raw_connectors,
            chronicles: raw_chronicles,
            management: raw_management,
            extra_fields,
        } = raw;

        if !extra_fields.is_empty() {
            for key in extra_fields.keys() {
                errors.push(format!(
                    "error[root]: unknown top-level key \"{key}\" (expected one of {TOP_LEVEL_FIELDS})"
                ));
            }
        }

        let api_version = parse_api_version(raw_api_version, &mut errors);
        let app = app::parse_app_config(raw_app, &mut errors);
        let connectors = connectors::parse_connectors(raw_connectors, &mut errors);
        let chronicles = chronicles::parse_chronicles(raw_chronicles, &mut errors);
        let management =
            raw_management.and_then(|section| management::resolve_management(section, &mut errors));

        chronicles::validate_references(&connectors, &chronicles, &mut errors);
        connectors::validate_connector_tls_requirements(&connectors, &mut errors);
        chronicles::validate_trigger_requirements(
            &api_version,
            &connectors,
            &chronicles,
            &mut errors,
        );
        chronicles::validate_phase_requirements(&chronicles, &mut errors);
        chronicles::validate_policy_requirements(&chronicles, &connectors, &app, &mut errors);
        validate_feature_gates(&app, &connectors, &chronicles, &mut errors);

        if errors.is_empty() {
            Ok(Self {
                api_version,
                app,
                connectors,
                chronicles,
                management,
            })
        } else {
            let schema_version = schema_version_label(&api_version);
            Err(IntegrationValidationError::new(errors, schema_version))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ApiVersion {
    #[default]
    V1,
    Unsupported(String),
}

fn parse_api_version(raw: Option<String>, errors: &mut Vec<String>) -> ApiVersion {
    match raw {
        None => {
            errors
                .push("error[root]: api_version is required (supported versions: v1)".to_string());
            ApiVersion::V1
        }
        Some(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                errors.push("api_version must be a non-empty string".to_string());
                ApiVersion::V1
            } else if trimmed.eq_ignore_ascii_case("v1") {
                ApiVersion::V1
            } else {
                errors.push(format!(
                    "api_version `{trimmed}` is not supported (supported versions: v1)"
                ));
                ApiVersion::Unsupported(trimmed.to_string())
            }
        }
    }
}

fn schema_version_label(version: &ApiVersion) -> String {
    match version {
        ApiVersion::V1 => "v1".to_string(),
        ApiVersion::Unsupported(other) => other.clone(),
    }
}

#[derive(Debug, Deserialize)]
struct RawIntegrationFile {
    #[serde(default)]
    api_version: Option<String>,
    #[serde(default)]
    app: Option<app::RawAppSection>,
    #[serde(default)]
    connectors: Vec<connectors::RawConnector>,
    #[serde(default)]
    chronicles: Vec<chronicles::RawChronicle>,
    #[serde(default)]
    management: Option<management::RawManagementSection>,
    #[serde(default)]
    #[serde(flatten)]
    extra_fields: BTreeMap<String, YamlValue>,
}

#[derive(Debug, Error)]
pub enum IntegrationConfigError {
    #[error("failed to read integration config: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse integration config: {0}")]
    Parse(#[from] serde_yaml::Error),
    #[error(transparent)]
    Invalid(IntegrationValidationError),
}

#[derive(Debug, Error)]
#[error("integration config validation failed:\nschema_version: \"{schema_version}\"\n{rendered}")]
pub struct IntegrationValidationError {
    schema_version: String,
    rendered: String,
}

impl IntegrationValidationError {
    pub fn new(messages: Vec<String>, schema_version: impl Into<String>) -> Self {
        let rendered = messages
            .iter()
            .map(|msg| format!("- {msg}"))
            .collect::<Vec<_>>()
            .join("\n");
        Self {
            schema_version: schema_version.into(),
            rendered,
        }
    }
}

fn validate_feature_gates(
    app: &AppConfig,
    connectors: &[ConnectorConfig],
    chronicles: &[ChronicleDefinition],
    errors: &mut Vec<String>,
) {
    let has_flag = |flag: &str| app.feature_flags.iter().any(|enabled| enabled == flag);

    for connector in connectors {
        let required_flag = match connector.kind {
            ConnectorKind::Rabbitmq => Some("rabbitmq"),
            ConnectorKind::Mqtt => Some("mqtt"),
            ConnectorKind::Mongodb => Some("mongodb"),
            ConnectorKind::Redis => Some("redis"),
            _ => None,
        };

        if let Some(flag) = required_flag {
            if !has_flag(flag) {
                errors.push(format!(
                    "connector `{}` of type `{}` requires feature flag `{}` in app.feature_flags",
                    connector.name,
                    connector.kind.as_str(),
                    flag
                ));
            }
        }
    }

    for chronicle in chronicles {
        for (index, phase) in chronicle.phases.iter().enumerate() {
            let required_flag = match phase.kind {
                PhaseKind::Parallel => Some("parallel_phase"),
                PhaseKind::Serialize => Some("serialize_phase"),
                _ => None,
            };

            if let Some(flag) = required_flag {
                if !has_flag(flag) {
                    errors.push(format!(
                        "chronicle `{}` phase `{}` (index {}) requires feature flag `{}` in app.feature_flags",
                        chronicle.name,
                        phase.name,
                        index,
                        flag
                    ));
                }
            }
        }
    }
}

// tests moved to tests/unit/config_integration_tests.rs
