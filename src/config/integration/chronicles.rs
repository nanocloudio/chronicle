use super::app::{
    ensure_positive_duration, parse_half_open_policy, parse_retry_budget, parse_route_limits,
    AppConfig, HalfOpenPolicy, RetryBudget, RouteLimits,
};
use super::connectors::{
    expand_option_map, flatten_options, flatten_options_optional, ConnectorConfig, ConnectorKind,
    OptionMap, ScalarValue,
};
use crate::retry::merge_retry_budgets;
use humantime::parse_duration;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use serde_yaml::Value as YamlValue;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct ChronicleDefinition {
    pub name: String,
    pub trigger: ChronicleTrigger,
    pub phases: Vec<ChroniclePhase>,
    pub policy: ChroniclePolicy,
}

#[derive(Debug, Clone)]
pub struct ChronicleTrigger {
    pub connector: String,
    pub options: OptionMap,
    pub kafka: Option<OptionMap>,
}

impl ChronicleTrigger {
    pub fn option(&self, key: &str) -> Option<&ScalarValue> {
        self.options.get(key)
    }

    pub fn options_json(&self) -> JsonValue {
        expand_option_map(&self.options)
    }

    pub fn kafka_json(&self) -> Option<JsonValue> {
        self.kafka.as_ref().map(expand_option_map)
    }

    pub fn option_deser<T>(&self) -> Result<T, serde_json::Error>
    where
        T: DeserializeOwned,
    {
        serde_json::from_value(self.options_json())
    }

    pub fn kafka_deser<T>(&self) -> Result<Option<T>, serde_json::Error>
    where
        T: DeserializeOwned,
    {
        match &self.kafka {
            Some(options) => serde_json::from_value(expand_option_map(options)).map(Some),
            None => Ok(None),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChroniclePhase {
    pub name: String,
    pub kind: PhaseKind,
    pub options: OptionMap,
}

impl ChroniclePhase {
    pub fn option(&self, key: &str) -> Option<&ScalarValue> {
        self.options.get(key)
    }

    pub fn options_json(&self) -> JsonValue {
        expand_option_map(&self.options)
    }

    pub fn option_deser<T>(&self) -> Result<T, serde_json::Error>
    where
        T: DeserializeOwned,
    {
        serde_json::from_value(self.options_json())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PhaseKind {
    Transform,
    KafkaProducer,
    RabbitmqPublish,
    MqttPublish,
    Parallel,
    Mongodb,
    Redis,
    HttpClient,
    GrpcClient,
    PostgresIdempotentInsert,
    MariadbIdempotentInsert,
    Serialize,
    SmtpSend,
    Unknown(String),
}

impl PhaseKind {
    pub(crate) fn from_raw(raw: &str) -> Self {
        match raw {
            "transform" => PhaseKind::Transform,
            "kafka_producer" => PhaseKind::KafkaProducer,
            "rabbitmq" => PhaseKind::RabbitmqPublish,
            "mqtt" => PhaseKind::MqttPublish,
            "parallel" => PhaseKind::Parallel,
            "mongodb" => PhaseKind::Mongodb,
            "redis" => PhaseKind::Redis,
            "http_client" => PhaseKind::HttpClient,
            "grpc_client" => PhaseKind::GrpcClient,
            "postgres_idempotent_insert" => PhaseKind::PostgresIdempotentInsert,
            "mariadb_idempotent_insert" => PhaseKind::MariadbIdempotentInsert,
            "serialize" => PhaseKind::Serialize,
            "smtp_send" => PhaseKind::SmtpSend,
            other => PhaseKind::Unknown(other.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            PhaseKind::Transform => "transform",
            PhaseKind::KafkaProducer => "kafka_producer",
            PhaseKind::RabbitmqPublish => "rabbitmq",
            PhaseKind::MqttPublish => "mqtt",
            PhaseKind::Parallel => "parallel",
            PhaseKind::Mongodb => "mongodb",
            PhaseKind::Redis => "redis",
            PhaseKind::HttpClient => "http_client",
            PhaseKind::GrpcClient => "grpc_client",
            PhaseKind::PostgresIdempotentInsert => "postgres_idempotent_insert",
            PhaseKind::MariadbIdempotentInsert => "mariadb_idempotent_insert",
            PhaseKind::Serialize => "serialize",
            PhaseKind::SmtpSend => "smtp_send",
            PhaseKind::Unknown(other) => other.as_str(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ChroniclePolicy {
    pub requires: Vec<String>,
    pub allow_partial_delivery: bool,
    pub readiness_priority: Option<i32>,
    pub limits: Option<RouteLimits>,
    pub retry_budget: Option<RetryBudget>,
    pub half_open_counts_as_ready: Option<HalfOpenPolicy>,
    pub delivery: Option<DeliveryPolicy>,
    pub fallback: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct DeliveryPolicy {
    pub retries: Option<u32>,
    pub backoff: Option<BackoffPolicy>,
    pub idempotent: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackoffPolicy {
    pub min: Duration,
    pub max: Duration,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawChronicle {
    pub(crate) name: String,
    pub(crate) trigger: RawTrigger,
    #[serde(default)]
    pub(crate) phases: Vec<RawPhase>,
    #[serde(default)]
    pub(crate) policy: Option<RawChroniclePolicy>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawTrigger {
    pub(crate) connector: String,
    #[serde(default)]
    pub(crate) options: BTreeMap<String, YamlValue>,
    #[serde(default)]
    pub(crate) kafka: BTreeMap<String, YamlValue>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawPhase {
    pub(crate) name: String,
    #[serde(rename = "type")]
    pub(crate) kind: String,
    #[serde(default)]
    pub(crate) options: BTreeMap<String, YamlValue>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawChroniclePolicy {
    #[serde(default)]
    pub(crate) requires: Vec<String>,
    #[serde(default)]
    pub(crate) allow_partial_delivery: Option<bool>,
    #[serde(default)]
    pub(crate) readiness_priority: Option<i32>,
    #[serde(default)]
    pub(crate) limits: Option<super::app::RawRouteLimits>,
    #[serde(default)]
    pub(crate) retry_budget: Option<super::app::RawRetryBudget>,
    #[serde(default)]
    pub(crate) half_open_counts_as_ready: Option<String>,
    #[serde(default)]
    pub(crate) delivery: Option<RawDeliveryPolicy>,
    #[serde(default)]
    pub(crate) fallback: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawDeliveryPolicy {
    #[serde(default)]
    pub(crate) retries: Option<u32>,
    #[serde(default)]
    pub(crate) backoff: Option<String>,
    #[serde(default)]
    pub(crate) idempotent: Option<bool>,
}

pub(crate) fn parse_chronicles(
    raw_chronicles: Vec<RawChronicle>,
    errors: &mut Vec<String>,
) -> Vec<ChronicleDefinition> {
    let mut chronicles = Vec::with_capacity(raw_chronicles.len());

    for chronicle in raw_chronicles {
        let RawChronicle {
            name: chronicle_name,
            trigger: raw_trigger,
            phases: raw_phases,
            policy: raw_policy,
        } = chronicle;

        let trigger_options = flatten_options(raw_trigger.options, errors);
        let trigger_kafka = flatten_options_optional(raw_trigger.kafka, errors);
        let phases = raw_phases
            .into_iter()
            .map(|phase| {
                let options = flatten_options(phase.options, errors);
                ChroniclePhase {
                    name: phase.name,
                    kind: PhaseKind::from_raw(&phase.kind),
                    options,
                }
            })
            .collect::<Vec<_>>();

        let policy = parse_chronicle_policy(raw_policy, errors, &chronicle_name);

        chronicles.push(ChronicleDefinition {
            name: chronicle_name,
            trigger: ChronicleTrigger {
                connector: raw_trigger.connector,
                options: trigger_options,
                kafka: trigger_kafka,
            },
            phases,
            policy,
        });
    }

    chronicles
}

pub(crate) fn validate_references(
    connectors: &[ConnectorConfig],
    chronicles: &[ChronicleDefinition],
    errors: &mut Vec<String>,
) {
    let connector_index = connectors
        .iter()
        .map(|connector| (connector.name.clone(), connector.kind.clone()))
        .collect::<HashMap<_, _>>();

    for chronicle in chronicles {
        match connector_index.get(&chronicle.trigger.connector) {
            Some(kind) => {
                if matches!(kind, ConnectorKind::HttpClient) {
                    errors.push(format!(
                        "chronicle `{}` trigger references HTTP connector `{}` configured as a client; triggers require `role = server`",
                        chronicle.name, chronicle.trigger.connector
                    ));
                }

                if chronicle.trigger.kafka.is_some() && !matches!(kind, ConnectorKind::Kafka) {
                    errors.push(format!(
                        "chronicle `{}` trigger expects Kafka connector `{}` but found `{}`",
                        chronicle.name,
                        chronicle.trigger.connector,
                        kind.as_str()
                    ));
                } else if chronicle.trigger.kafka.is_none() {
                    let declared_type = chronicle
                        .trigger
                        .option("type")
                        .and_then(ScalarValue::as_str)
                        .map(|value| value.to_ascii_lowercase());
                    let has_queue = chronicle.trigger.option("queue").is_some();
                    let has_topic = chronicle.trigger.option("topic").is_some();

                    let expects_rabbitmq = declared_type.as_deref() == Some("rabbitmq")
                        || (declared_type.is_none() && has_queue);
                    let expects_mqtt = declared_type.as_deref() == Some("mqtt")
                        || (declared_type.is_none() && has_topic);

                    if expects_rabbitmq
                        && !matches!(kind, ConnectorKind::Rabbitmq | ConnectorKind::Unknown(_))
                    {
                        errors.push(format!(
                            "chronicle `{}` trigger expects RabbitMQ connector `{}` but found `{}`",
                            chronicle.name,
                            chronicle.trigger.connector,
                            kind.as_str()
                        ));
                    } else if expects_mqtt
                        && !matches!(kind, ConnectorKind::Mqtt | ConnectorKind::Unknown(_))
                    {
                        errors.push(format!(
                            "chronicle `{}` trigger expects MQTT connector `{}` but found `{}`",
                            chronicle.name,
                            chronicle.trigger.connector,
                            kind.as_str()
                        ));
                    }
                }
            }
            None => errors.push(format!(
                "chronicle `{}` references unknown trigger connector `{}`",
                chronicle.name, chronicle.trigger.connector
            )),
        }

        for (index, phase) in chronicle.phases.iter().enumerate() {
            validate_phase_connector_binding(chronicle, index, phase, &connector_index, errors);
        }
    }
}

pub(crate) fn validate_trigger_requirements(
    api_version: &crate::config::integration::ApiVersion,
    connectors: &[ConnectorConfig],
    chronicles: &[ChronicleDefinition],
    errors: &mut Vec<String>,
) {
    let mut connector_index = HashMap::new();
    for connector in connectors {
        connector_index.insert(connector.name.clone(), connector.kind.clone());
    }

    for chronicle in chronicles {
        let Some(connector_kind) = connector_index.get(&chronicle.trigger.connector) else {
            continue;
        };

        let trigger = &chronicle.trigger;

        match connector_kind {
            ConnectorKind::Kafka => validate_kafka_trigger_options(chronicle, errors),
            ConnectorKind::Rabbitmq => validate_rabbitmq_trigger_options(chronicle, errors),
            ConnectorKind::Mqtt => validate_mqtt_trigger_options(chronicle, errors),
            ConnectorKind::Mongodb => validate_mongodb_trigger_options(chronicle, errors),
            ConnectorKind::Redis => validate_redis_trigger_options(chronicle, errors),
            ConnectorKind::HttpServer | ConnectorKind::HttpClient => {
                if trigger.option("codec").is_some() {
                    errors.push(format!(
                        "chronicle `{}` HTTP trigger does not support `codec` overrides",
                        chronicle.name
                    ));
                }
            }
            ConnectorKind::Grpc
            | ConnectorKind::Postgres
            | ConnectorKind::Mariadb
            | ConnectorKind::Smtp
            | ConnectorKind::Unknown(_) => {}
        }

        validate_trigger_backoff_fields(
            chronicle,
            api_version,
            connector_kind,
            "primary",
            &chronicle.trigger.options,
            errors,
        );

        if let Some(kafka) = &chronicle.trigger.kafka {
            validate_trigger_backoff_fields(
                chronicle,
                api_version,
                connector_kind,
                "kafka",
                kafka,
                errors,
            );
        }
    }
}

pub(crate) fn validate_phase_requirements(
    chronicles: &[ChronicleDefinition],
    errors: &mut Vec<String>,
) {
    for chronicle in chronicles {
        for phase in &chronicle.phases {
            match phase.kind {
                PhaseKind::KafkaProducer => {
                    if phase.option("topic").is_none() {
                        errors.push(format!(
                            "chronicle `{}` kafka_producer phase `{}` requires option `topic`",
                            chronicle.name, phase.name
                        ));
                    }
                }
                PhaseKind::RabbitmqPublish => {}
                PhaseKind::MqttPublish => {
                    if phase.option("topic").is_none() {
                        errors.push(format!(
                            "chronicle `{}` mqtt phase `{}` requires option `topic`",
                            chronicle.name, phase.name
                        ));
                    }
                }
                PhaseKind::Mongodb => {
                    for required in ["database", "collection"] {
                        if phase.option(required).is_none() {
                            errors.push(format!(
                                "chronicle `{}` mongodb phase `{}` requires option `{}`",
                                chronicle.name, phase.name, required
                            ));
                        }
                    }
                }
                PhaseKind::Redis => {
                    if phase.option("command").is_none() {
                        errors.push(format!(
                            "chronicle `{}` redis phase `{}` requires option `command`",
                            chronicle.name, phase.name
                        ));
                    }
                }
                PhaseKind::PostgresIdempotentInsert | PhaseKind::MariadbIdempotentInsert => {}
                PhaseKind::SmtpSend => {
                    if phase.option("to").is_none() {
                        errors.push(format!(
                            "chronicle `{}` smtp phase `{}` requires option `to`",
                            chronicle.name, phase.name
                        ));
                    }
                }
                PhaseKind::Transform
                | PhaseKind::Parallel
                | PhaseKind::Serialize
                | PhaseKind::HttpClient
                | PhaseKind::GrpcClient
                | PhaseKind::Unknown(_) => {}
            }
        }
    }
}

pub(crate) fn validate_policy_requirements(
    chronicles: &[ChronicleDefinition],
    connectors: &[ConnectorConfig],
    app: &AppConfig,
    errors: &mut Vec<String>,
) {
    let connector_index = connectors
        .iter()
        .map(|connector| (connector.name.clone(), connector.kind.clone()))
        .collect::<HashMap<_, _>>();
    let connector_config_index = connectors
        .iter()
        .map(|connector| (connector.name.clone(), connector))
        .collect::<HashMap<_, _>>();

    for chronicle in chronicles {
        if let Some(fallback) = &chronicle.policy.fallback {
            for (connector, phase) in fallback {
                if !chronicle
                    .phases
                    .iter()
                    .any(|candidate| candidate.name == *phase)
                {
                    errors.push(format!(
                        "chronicle `{}` policy references unknown fallback phase `{}`",
                        chronicle.name, phase
                    ));
                }

                if let Some(kind) = connector_index.get(connector) {
                    if !matches!(
                        kind,
                        ConnectorKind::Rabbitmq
                            | ConnectorKind::Mqtt
                            | ConnectorKind::Kafka
                            | ConnectorKind::Unknown(_)
                    ) {
                        errors.push(format!(
                            "chronicle `{}` policy fallback connector `{connector}` must be a queue/broker connector, found `{}`",
                            chronicle.name,
                            kind.as_str()
                        ));
                    }
                } else {
                    errors.push(format!(
                        "chronicle `{}` policy references unknown fallback connector `{connector}`",
                        chronicle.name
                    ));
                }
            }
        }

        if let Some(delivery) = chronicle.policy.delivery.as_ref() {
            if let Some(retries) = delivery.retries {
                let mut scopes = Vec::new();
                scopes.push(app.retry_budget.as_ref());
                for name in chronicle_connector_names(chronicle) {
                    if let Some(connector) = connector_config_index.get(&name) {
                        scopes.push(connector.retry_budget.as_ref());
                    }
                }
                scopes.push(chronicle.policy.retry_budget.as_ref());
                if let Some(budget) = merge_retry_budgets(scopes) {
                    if let Some(max_attempts) = budget.max_attempts {
                        let requested_attempts = retries.saturating_add(1);
                        if requested_attempts > max_attempts {
                            errors.push(format!(
                                "chronicle `{}` policy.delivery.retries ({}) exceeds retry_budget.max_attempts ({})",
                                chronicle.name, retries, max_attempts
                            ));
                        }
                    }
                }
            }
        }

        validate_policy_route_limit_overrides(chronicle, app, errors);
    }
}

fn validate_policy_route_limit_overrides(
    chronicle: &ChronicleDefinition,
    app: &AppConfig,
    errors: &mut Vec<String>,
) {
    let Some(policy_limits) = chronicle.policy.limits.as_ref() else {
        return;
    };
    let app_route_limits = app.limits.routes.as_ref();

    if let (Some(policy_max), Some(app_max)) = (
        policy_limits.max_inflight,
        app_route_limits.and_then(|limits| limits.max_inflight),
    ) {
        if policy_max > app_max {
            errors.push(format!(
                "chronicle `{}` policy.limits.max_inflight ({}) exceeds app.limits.routes.max_inflight ({})",
                chronicle.name, policy_max, app_max
            ));
        }
    }

    if let (Some(policy_depth), Some(app_depth)) = (
        policy_limits.max_queue_depth,
        app_route_limits.and_then(|limits| limits.max_queue_depth),
    ) {
        if policy_depth > app_depth {
            errors.push(format!(
                "chronicle `{}` policy.limits.max_queue_depth ({}) exceeds app.limits.routes.max_queue_depth ({})",
                chronicle.name, policy_depth, app_depth
            ));
        }
    }
}

fn validate_phase_connector_binding(
    chronicle: &ChronicleDefinition,
    phase_index: usize,
    phase: &ChroniclePhase,
    connectors: &HashMap<String, ConnectorKind>,
    errors: &mut Vec<String>,
) {
    let connector_name = phase
        .option("connector")
        .and_then(ScalarValue::as_str)
        .map(str::to_string);

    let expected_kind = match phase.kind {
        PhaseKind::KafkaProducer => Some(ConnectorKind::Kafka),
        PhaseKind::HttpClient => Some(ConnectorKind::HttpClient),
        PhaseKind::GrpcClient => Some(ConnectorKind::Grpc),
        PhaseKind::RabbitmqPublish => Some(ConnectorKind::Rabbitmq),
        PhaseKind::MqttPublish => Some(ConnectorKind::Mqtt),
        PhaseKind::Parallel => None,
        PhaseKind::Mongodb => Some(ConnectorKind::Mongodb),
        PhaseKind::Redis => Some(ConnectorKind::Redis),
        PhaseKind::PostgresIdempotentInsert => Some(ConnectorKind::Postgres),
        PhaseKind::MariadbIdempotentInsert => Some(ConnectorKind::Mariadb),
        PhaseKind::SmtpSend => Some(ConnectorKind::Smtp),
        PhaseKind::Transform | PhaseKind::Serialize | PhaseKind::Unknown(_) => None,
    };

    let Some(name) = connector_name else {
        if expected_kind.is_some() {
            errors.push(format!(
                "chronicle `{}` phase `{}` (index {}) requires a `connector` option",
                chronicle.name, phase.name, phase_index
            ));
        }
        return;
    };

    let Some(actual_kind) = connectors.get(&name) else {
        errors.push(format!(
            "chronicle `{}` phase `{}` references unknown connector `{}`",
            chronicle.name, phase.name, name
        ));
        return;
    };

    let Some(expected_kind) = expected_kind else {
        return;
    };

    if !matches_connector_kind(actual_kind, &expected_kind) {
        errors.push(format!(
            "chronicle `{}` phase `{}` expects connector `{}` to be `{}` but found `{}`",
            chronicle.name,
            phase.name,
            name,
            expected_kind.as_str(),
            actual_kind.as_str()
        ));
    }
}

fn parse_chronicle_policy(
    raw: Option<RawChroniclePolicy>,
    errors: &mut Vec<String>,
    chronicle_name: &str,
) -> ChroniclePolicy {
    let raw = raw.unwrap_or_default();

    let requires = raw
        .requires
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect();

    let limits_context = format!("chronicle `{}` policy.limits", chronicle_name);
    let limits = parse_route_limits(raw.limits, errors, &limits_context);

    let retry_budget = parse_retry_budget(
        raw.retry_budget,
        errors,
        &format!("chronicle `{}` policy.retry_budget", chronicle_name),
    );

    let half_open_counts_as_ready = raw
        .half_open_counts_as_ready
        .as_deref()
        .map(str::trim)
        .and_then(|trimmed| {
            let field_label = format!(
                "chronicle `{}` policy.half_open_counts_as_ready",
                chronicle_name
            );
            if trimmed.is_empty() {
                errors.push(format!(
                    "{field_label} must be one of `never`, `endpoint`, or `route` (got ``)"
                ));
                None
            } else if let Some(policy_value) = parse_half_open_policy(trimmed) {
                Some(policy_value)
            } else {
                errors.push(format!(
                    "{field_label} must be one of `never`, `endpoint`, or `route` (got `{trimmed}`)"
                ));
                None
            }
        });

    let fallback = raw.fallback.and_then(|fallback_map| {
        let mut parsed = BTreeMap::new();
        for (connector, target) in fallback_map {
            let key = connector.trim();
            let value = target.trim();
            if key.is_empty() || value.is_empty() {
                errors.push(format!(
                    "chronicle `{}` policy.fallback entries must provide non-empty connector and target phase names",
                    chronicle_name
                ));
                continue;
            }
            parsed.insert(key.to_string(), value.to_string());
        }

        if parsed.is_empty() {
            None
        } else {
            Some(parsed)
        }
    });

    ChroniclePolicy {
        requires,
        allow_partial_delivery: raw.allow_partial_delivery.unwrap_or(false),
        readiness_priority: raw.readiness_priority,
        limits,
        retry_budget,
        half_open_counts_as_ready,
        delivery: parse_delivery_policy(raw.delivery, errors, chronicle_name),
        fallback,
    }
}

fn chronicle_connector_names(chronicle: &ChronicleDefinition) -> Vec<String> {
    let mut connectors = BTreeSet::new();
    for phase in &chronicle.phases {
        if let Some(name) = phase.option("connector").and_then(ScalarValue::as_str) {
            let trimmed = name.trim();
            if !trimmed.is_empty() {
                connectors.insert(trimmed.to_string());
            }
        }
    }
    for name in &chronicle.policy.requires {
        let trimmed = name.trim();
        if !trimmed.is_empty() {
            connectors.insert(trimmed.to_string());
        }
    }
    connectors.into_iter().collect()
}

fn parse_delivery_policy(
    raw: Option<RawDeliveryPolicy>,
    errors: &mut Vec<String>,
    chronicle_name: &str,
) -> Option<DeliveryPolicy> {
    let raw_policy = raw?;

    let mut policy = DeliveryPolicy {
        retries: raw_policy.retries,
        idempotent: raw_policy.idempotent,
        ..DeliveryPolicy::default()
    };

    let field_label = format!("chronicle `{}` policy.delivery.backoff", chronicle_name);
    policy.backoff = parse_backoff_policy(raw_policy.backoff, &field_label, errors);

    if policy.retries.is_none() && policy.backoff.is_none() && policy.idempotent.is_none() {
        None
    } else {
        Some(policy)
    }
}

fn parse_backoff_policy(
    raw: Option<String>,
    field_label: &str,
    errors: &mut Vec<String>,
) -> Option<BackoffPolicy> {
    let raw_value = raw?;

    let trimmed = raw_value.trim();
    if trimmed.is_empty() {
        errors.push(format!(
            "{field_label} must be a non-empty duration or range"
        ));
        return None;
    }

    let parts: Vec<&str> = trimmed.split("..").collect();
    match parts.len() {
        1 => parse_backoff_segment(trimmed, field_label, errors).map(|duration| BackoffPolicy {
            min: duration,
            max: duration,
        }),
        2 => {
            let min = parse_backoff_segment(parts[0], field_label, errors);
            let max = parse_backoff_segment(parts[1], field_label, errors);
            match (min, max) {
                (Some(min_duration), Some(max_duration)) => {
                    if min_duration > max_duration {
                        errors.push(format!(
                            "{field_label} requires the minimum duration to be less than or equal to the maximum"
                        ));
                        None
                    } else {
                        Some(BackoffPolicy {
                            min: min_duration,
                            max: max_duration,
                        })
                    }
                }
                _ => None,
            }
        }
        _ => {
            errors.push(format!(
                "{field_label} must be a duration or range separated by `..`"
            ));
            None
        }
    }
}

fn parse_backoff_segment(
    raw: &str,
    field_label: &str,
    errors: &mut Vec<String>,
) -> Option<Duration> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        errors.push(format!(
            "{field_label} segments must be non-empty duration strings"
        ));
        return None;
    }

    match parse_duration(trimmed) {
        Ok(duration) => ensure_positive_duration(duration, field_label, errors),
        Err(_) => {
            errors.push(format!(
                "{field_label} segments must be valid durations (got `{trimmed}`)"
            ));
            None
        }
    }
}

fn validate_kafka_trigger_options(chronicle: &ChronicleDefinition, errors: &mut Vec<String>) {
    let Some(kafka) = chronicle.trigger.kafka.as_ref() else {
        errors.push(format!(
            "chronicle `{}` Kafka trigger requires a `kafka` options section",
            chronicle.name
        ));
        return;
    };
    let required = ["topic", "group_id"];
    for field in required {
        if !kafka.contains_key(field) {
            errors.push(format!(
                "chronicle `{}` Kafka trigger requires option `{}`",
                chronicle.name, field
            ));
        }
    }
}

fn validate_rabbitmq_trigger_options(chronicle: &ChronicleDefinition, errors: &mut Vec<String>) {
    if chronicle.trigger.option("queue").is_none() {
        errors.push(format!(
            "chronicle `{}` rabbitmq trigger requires option `queue`",
            chronicle.name
        ));
    }
}

fn validate_mqtt_trigger_options(chronicle: &ChronicleDefinition, errors: &mut Vec<String>) {
    if chronicle.trigger.option("topic").is_none() {
        errors.push(format!(
            "chronicle `{}` mqtt trigger requires option `topic`",
            chronicle.name
        ));
    }
}

fn validate_mongodb_trigger_options(chronicle: &ChronicleDefinition, errors: &mut Vec<String>) {
    for field in ["database", "collection"] {
        if chronicle.trigger.option(field).is_none() {
            errors.push(format!(
                "chronicle `{}` mongodb trigger requires option `{}`",
                chronicle.name, field
            ));
        }
    }
}

fn validate_redis_trigger_options(chronicle: &ChronicleDefinition, errors: &mut Vec<String>) {
    let mode = chronicle
        .trigger
        .option("mode")
        .and_then(ScalarValue::as_str)
        .map(|value| value.to_ascii_lowercase());

    match mode.as_deref() {
        Some("pubsub") => {
            let has_channels = chronicle
                .trigger
                .options
                .keys()
                .any(|key| key == "channels" || key.starts_with("channels."));
            if !has_channels {
                errors.push(format!(
                    "chronicle `{}` redis trigger requires option `channels` when `mode = pubsub`",
                    chronicle.name
                ));
            }
        }
        _ => {
            if chronicle.trigger.option("stream").is_none() {
                errors.push(format!(
                    "chronicle `{}` redis trigger requires option `stream`",
                    chronicle.name
                ));
            }
        }
    }
}

fn validate_trigger_backoff_fields(
    chronicle: &ChronicleDefinition,
    api_version: &crate::config::integration::ApiVersion,
    connector_kind: &ConnectorKind,
    trigger_label: &str,
    options: &OptionMap,
    errors: &mut Vec<String>,
) {
    let trigger = &chronicle.trigger;
    let is_v1 = matches!(api_version, crate::config::integration::ApiVersion::V1);
    let supports_backoff = matches!(
        connector_kind,
        ConnectorKind::Kafka
            | ConnectorKind::Rabbitmq
            | ConnectorKind::Mqtt
            | ConnectorKind::Redis
            | ConnectorKind::Mongodb
    );

    if !supports_backoff {
        for key in ["retry_initial", "retry_max", "retry_multiplier"] {
            if options.contains_key(key) {
                errors.push(format!(
                    "chronicle `{}` {trigger_label} trigger does not support option `{key}`",
                    chronicle.name
                ));
            }
        }
        return;
    }

    if is_v1 {
        if trigger.option("retry_initial_ms").is_some() {
            errors.push(format!(
                "chronicle `{}` {trigger_label} trigger uses deprecated option `retry_initial_ms` (use `retry_initial`)",
                chronicle.name
            ));
        }

        if trigger.option("retry_max_ms").is_some() {
            errors.push(format!(
                "chronicle `{}` {trigger_label} trigger uses deprecated option `retry_max_ms` (use `retry_max`)",
                chronicle.name
            ));
        }
    }

    let retry_initial = trigger.option("retry_initial").and_then(|value| {
        parse_duration_option(chronicle, trigger_label, "retry_initial", value, errors)
    });

    let retry_max = trigger.option("retry_max").and_then(|value| {
        parse_duration_option(chronicle, trigger_label, "retry_max", value, errors)
    });

    if let (Some(initial), Some(maximum)) = (retry_initial, retry_max) {
        if maximum < initial {
            errors.push(format!(
                "chronicle `{}` {trigger_label} trigger option `retry_max` must be greater than or equal to `retry_initial`",
                chronicle.name
            ));
        }
    }

    if let Some(value) = trigger.option("retry_multiplier") {
        if let Some(multiplier) =
            parse_float_option(chronicle, trigger_label, "retry_multiplier", value, errors)
        {
            if !(1.1..=10.0).contains(&multiplier) {
                errors.push(format!(
                    "chronicle `{}` {trigger_label} trigger option `retry_multiplier` must be between 1.1 and 10.0",
                    chronicle.name
                ));
            }
        }
    }

    if let Some(limit) = trigger.option("retry_limit") {
        parse_integer_option(chronicle, trigger_label, "retry_limit", limit, errors);
    }
}

fn parse_duration_option(
    chronicle: &ChronicleDefinition,
    trigger_label: &str,
    field: &str,
    value: &ScalarValue,
    errors: &mut Vec<String>,
) -> Option<Duration> {
    match value {
        ScalarValue::String(text) => match parse_duration(text) {
            Ok(duration) => {
                let label = format!(
                    "chronicle `{}` {trigger_label} trigger option `{field}`",
                    chronicle.name
                );
                ensure_positive_duration(duration, &label, errors)
            }
            Err(_) => {
                errors.push(format!(
                    "chronicle `{}` {trigger_label} trigger option `{field}` must be a valid duration (e.g., `200ms`, `5s`)",
                    chronicle.name
                ));
                None
            }
        },
        _ => {
            errors.push(format!(
                "chronicle `{}` {trigger_label} trigger option `{field}` must be provided as a duration string",
                chronicle.name
            ));
            None
        }
    }
}

fn parse_float_option(
    chronicle: &ChronicleDefinition,
    trigger_label: &str,
    field: &str,
    value: &ScalarValue,
    errors: &mut Vec<String>,
) -> Option<f64> {
    match value {
        ScalarValue::Number(num) => num
            .as_f64()
            .or_else(|| num.as_i64().map(|v| v as f64))
            .or_else(|| num.as_u64().map(|v| v as f64))
            .or_else(|| {
                errors.push(format!(
                    "chronicle `{}` {trigger_label} trigger option `{field}` must be a number",
                    chronicle.name
                ));
                None
            }),
        ScalarValue::String(text) => match text.parse::<f64>() {
            Ok(val) => Some(val),
            Err(_) => {
                errors.push(format!(
                    "chronicle `{}` {trigger_label} trigger option `{field}` must be a number",
                    chronicle.name
                ));
                None
            }
        },
        _ => {
            errors.push(format!(
                "chronicle `{}` {trigger_label} trigger option `{field}` must be a number",
                chronicle.name
            ));
            None
        }
    }
}

fn parse_integer_option(
    chronicle: &ChronicleDefinition,
    trigger_label: &str,
    field: &str,
    value: &ScalarValue,
    errors: &mut Vec<String>,
) -> Option<i64> {
    match value {
        ScalarValue::Number(num) => num
            .as_i64()
            .or_else(|| num.as_u64().and_then(|v| i64::try_from(v).ok()))
            .or_else(|| {
                errors.push(format!(
                    "chronicle `{}` {trigger_label} trigger option `{field}` must be an integer",
                    chronicle.name
                ));
                None
            }),
        ScalarValue::String(text) => match text.trim().parse::<i64>() {
            Ok(val) => Some(val),
            Err(_) => {
                errors.push(format!(
                    "chronicle `{}` {trigger_label} trigger option `{field}` must be an integer",
                    chronicle.name
                ));
                None
            }
        },
        _ => {
            errors.push(format!(
                "chronicle `{}` {trigger_label} trigger option `{field}` must be an integer",
                chronicle.name
            ));
            None
        }
    }
}

fn matches_connector_kind(actual: &ConnectorKind, expected: &ConnectorKind) -> bool {
    matches!(
        (actual, expected),
        (ConnectorKind::Unknown(_), _)
            | (ConnectorKind::Kafka, ConnectorKind::Kafka)
            | (ConnectorKind::Rabbitmq, ConnectorKind::Rabbitmq)
            | (ConnectorKind::Mqtt, ConnectorKind::Mqtt)
            | (ConnectorKind::Redis, ConnectorKind::Redis)
            | (ConnectorKind::Mongodb, ConnectorKind::Mongodb)
            | (ConnectorKind::HttpClient, ConnectorKind::HttpClient)
            | (ConnectorKind::Grpc, ConnectorKind::Grpc)
            | (ConnectorKind::Postgres, ConnectorKind::Postgres)
            | (ConnectorKind::Mariadb, ConnectorKind::Mariadb)
            | (ConnectorKind::Smtp, ConnectorKind::Smtp)
    )
}
