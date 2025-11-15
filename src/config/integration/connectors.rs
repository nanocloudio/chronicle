use super::app::{
    ensure_positive_duration, parse_duration_value, parse_retry_budget, value_to_string,
    RetryBudget,
};
use humantime::parse_duration;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use serde_yaml::Value as YamlValue;
use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::time::Duration;

/// Flat key/value bag used for connector, trigger, and phase options.
pub type OptionMap = BTreeMap<String, ScalarValue>;

#[derive(Debug, Clone)]
pub struct ConnectorConfig {
    pub name: String,
    pub kind: ConnectorKind,
    pub options: OptionMap,
    pub timeouts: ConnectorTimeouts,
    pub requires: Vec<String>,
    pub warmup: bool,
    pub warmup_timeout: Option<Duration>,
    pub circuit_breaker: Option<CircuitBreakerConfig>,
    pub retry_budget: Option<RetryBudget>,
}

impl ConnectorConfig {
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
        serde_json::from_value(expand_option_map(&self.options))
    }

    pub fn timeout(&self, key: &str) -> Option<Duration> {
        self.timeouts.get(key)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ConnectorTimeouts {
    pub connect: Option<Duration>,
    pub request: Option<Duration>,
    pub read: Option<Duration>,
    pub write: Option<Duration>,
    pub query: Option<Duration>,
}

impl ConnectorTimeouts {
    fn from_option_map(connector: &str, raw: Option<OptionMap>, errors: &mut Vec<String>) -> Self {
        let mut timeouts = ConnectorTimeouts::default();
        if let Some(map) = raw {
            for (key, value) in map {
                match key.as_str() {
                    "connect" => timeouts.connect = parse_timeout_value(connector, "connect", &value, errors),
                    "request" => timeouts.request = parse_timeout_value(connector, "request", &value, errors),
                    "read" => timeouts.read = parse_timeout_value(connector, "read", &value, errors),
                    "write" => timeouts.write = parse_timeout_value(connector, "write", &value, errors),
                    "query" => timeouts.query = parse_timeout_value(connector, "query", &value, errors),
                    other => errors.push(format!(
                        "connector `{connector}` timeouts contains unsupported key `{other}` (allowed keys: connect, request, read, write, query)"
                    )),
                }
            }
        }
        timeouts
    }

    pub fn get(&self, key: &str) -> Option<Duration> {
        match key {
            "connect" => self.connect,
            "request" => self.request,
            "read" => self.read,
            "write" => self.write,
            "query" => self.query,
            _ => None,
        }
    }

    pub fn connect(&self) -> Option<Duration> {
        self.connect
    }

    pub fn request(&self) -> Option<Duration> {
        self.request
    }

    pub fn query(&self) -> Option<Duration> {
        self.query
    }
}

fn parse_timeout_value(
    connector: &str,
    field: &str,
    value: &ScalarValue,
    errors: &mut Vec<String>,
) -> Option<Duration> {
    let raw = value.to_lossy_string();
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        errors.push(format!(
            "connector `{connector}` timeouts.{field} must be a non-empty duration string"
        ));
        return None;
    }

    match parse_duration(trimmed) {
        Ok(duration) => Some(duration),
        Err(_) => {
            errors.push(format!(
                "connector `{connector}` timeouts.{field} must be a valid duration (got `{trimmed}`)"
            ));
            None
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ConnectorKind {
    Kafka,
    Postgres,
    Mariadb,
    HttpServer,
    HttpClient,
    Grpc,
    Rabbitmq,
    Mqtt,
    Mongodb,
    Redis,
    Smtp,
    Unknown(String),
}

impl ConnectorKind {
    pub(crate) fn from_raw(
        name: &str,
        raw: &str,
        options: &OptionMap,
        errors: &mut Vec<String>,
    ) -> Self {
        match raw {
            "kafka" => ConnectorKind::Kafka,
            "postgres" => ConnectorKind::Postgres,
            "mariadb" => ConnectorKind::Mariadb,
            "http_server" => ConnectorKind::HttpServer,
            "http_client" => ConnectorKind::HttpClient,
            "grpc" => ConnectorKind::Grpc,
            "http" => {
                let role = options
                    .get("role")
                    .and_then(ScalarValue::as_str)
                    .map(str::to_lowercase);

                match role.as_deref() {
                    Some("server") => ConnectorKind::HttpServer,
                    Some("client") => ConnectorKind::HttpClient,
                    Some(other) => {
                        errors.push(format!(
                            "connector `{name}` has unsupported http role `{other}` (expected `server` or `client`)"
                        ));
                        ConnectorKind::Unknown(raw.to_string())
                    }
                    None => {
                        errors.push(format!(
                            "connector `{name}` uses type `http` but is missing required option `role`"
                        ));
                        ConnectorKind::Unknown(raw.to_string())
                    }
                }
            }
            "rabbitmq" => ConnectorKind::Rabbitmq,
            "mqtt" => ConnectorKind::Mqtt,
            "mongodb" => ConnectorKind::Mongodb,
            "redis" => ConnectorKind::Redis,
            "smtp" => ConnectorKind::Smtp,
            other => ConnectorKind::Unknown(other.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            ConnectorKind::Kafka => "kafka",
            ConnectorKind::Postgres => "postgres",
            ConnectorKind::Mariadb => "mariadb",
            ConnectorKind::HttpServer => "http_server",
            ConnectorKind::HttpClient => "http_client",
            ConnectorKind::Grpc => "grpc",
            ConnectorKind::Rabbitmq => "rabbitmq",
            ConnectorKind::Mqtt => "mqtt",
            ConnectorKind::Mongodb => "mongodb",
            ConnectorKind::Redis => "redis",
            ConnectorKind::Smtp => "smtp",
            ConnectorKind::Unknown(other) => other.as_str(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CircuitBreakerConfig {
    pub failure_rate: f64,
    pub window: Duration,
    pub open_base: Duration,
    pub open_max: Duration,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    Null,
    Bool(bool),
    Number(JsonNumber),
    String(String),
}

impl ScalarValue {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            ScalarValue::String(inner) => Some(inner),
            _ => None,
        }
    }

    pub fn to_json(&self) -> JsonValue {
        match self {
            ScalarValue::Null => JsonValue::Null,
            ScalarValue::Bool(inner) => JsonValue::Bool(*inner),
            ScalarValue::Number(inner) => JsonValue::Number(inner.clone()),
            ScalarValue::String(inner) => JsonValue::String(inner.clone()),
        }
    }

    pub fn to_lossy_string(&self) -> String {
        match self {
            ScalarValue::Null => "null".to_string(),
            ScalarValue::Bool(inner) => inner.to_string(),
            ScalarValue::Number(inner) => inner.to_string(),
            ScalarValue::String(inner) => inner.clone(),
        }
    }
}

impl fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScalarValue::Null => write!(f, "null"),
            ScalarValue::Bool(inner) => write!(f, "{inner}"),
            ScalarValue::Number(inner) => write!(f, "{inner}"),
            ScalarValue::String(inner) => write!(f, "{inner}"),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawConnector {
    pub(crate) name: String,
    #[serde(rename = "type")]
    pub(crate) kind: String,
    #[serde(default)]
    pub(crate) warmup: bool,
    #[serde(default)]
    pub(crate) warmup_timeout: Option<String>,
    #[serde(default)]
    pub(crate) options: BTreeMap<String, YamlValue>,
    #[serde(default)]
    pub(crate) timeouts: BTreeMap<String, YamlValue>,
    #[serde(default)]
    pub(crate) requires: Vec<String>,
    #[serde(default)]
    pub(crate) cb: Option<RawCircuitBreakerConfig>,
    #[serde(default)]
    pub(crate) retry_budget: Option<super::app::RawRetryBudget>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawCircuitBreakerConfig {
    #[serde(default)]
    pub(crate) failure_rate: Option<f64>,
    #[serde(default)]
    pub(crate) window: Option<String>,
    #[serde(default)]
    pub(crate) open_base: Option<String>,
    #[serde(default)]
    pub(crate) open_max: Option<String>,
}

pub(crate) fn parse_connectors(
    raw_connectors: Vec<RawConnector>,
    errors: &mut Vec<String>,
) -> Vec<ConnectorConfig> {
    let mut seen_connectors = HashSet::new();
    let mut connectors = Vec::with_capacity(raw_connectors.len());

    for connector in raw_connectors {
        let RawConnector {
            name,
            kind,
            warmup,
            warmup_timeout,
            options,
            timeouts,
            requires,
            cb,
            retry_budget,
        } = connector;

        if !seen_connectors.insert(name.clone()) {
            errors.push(format!("duplicate connector definition for `{}`", name));
            continue;
        }

        let mut options = flatten_options(options, errors);
        let kind = ConnectorKind::from_raw(&name, &kind, &options, errors);

        if matches!(kind, ConnectorKind::HttpServer | ConnectorKind::HttpClient) {
            options.remove("role");
        }

        let warmup_label = format!("connector `{name}` warmup_timeout");
        let warmup_timeout = parse_duration_value(&warmup_label, warmup_timeout, errors)
            .and_then(|duration| ensure_positive_duration(duration, &warmup_label, errors));

        let circuit_breaker = parse_circuit_breaker(&name, cb, errors);
        let retry_budget = parse_retry_budget(
            retry_budget,
            errors,
            &format!("connector `{}` retry_budget", name),
        );
        let raw_timeouts = flatten_options_optional(timeouts, errors);
        let timeouts = ConnectorTimeouts::from_option_map(&name, raw_timeouts, errors);
        let requires = requires
            .into_iter()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .collect();

        connectors.push(ConnectorConfig {
            name,
            kind,
            options,
            timeouts,
            requires,
            warmup,
            warmup_timeout,
            circuit_breaker,
            retry_budget,
        });
    }

    connectors
}

pub(crate) fn flatten_options(
    values: BTreeMap<String, YamlValue>,
    errors: &mut Vec<String>,
) -> OptionMap {
    let mut target = OptionMap::new();

    for (key, value) in values {
        flatten_value(&mut target, &key, value, errors);
    }

    target
}

pub(crate) fn flatten_options_optional(
    values: BTreeMap<String, YamlValue>,
    errors: &mut Vec<String>,
) -> Option<OptionMap> {
    if values.is_empty() {
        None
    } else {
        let flattened = flatten_options(values, errors);
        if flattened.is_empty() {
            None
        } else {
            Some(flattened)
        }
    }
}

pub(crate) fn expand_option_map(options: &OptionMap) -> JsonValue {
    let mut root = JsonValue::Object(JsonMap::new());

    for (key, value) in options {
        insert_nested_json(&mut root, key, value.to_json());
    }

    root
}

fn insert_nested_json(target: &mut JsonValue, raw_key: &str, value: JsonValue) {
    let segments = parse_key_segments(raw_key);

    if segments.is_empty() {
        return;
    }

    let mut cursor = target;

    for (idx, segment) in segments.iter().enumerate() {
        let is_last = idx == segments.len() - 1;
        let next_segment = segments.get(idx + 1);

        match segment {
            KeySegment::Field(name) => {
                let object = ensure_object(cursor);
                if is_last {
                    object.insert((*name).to_string(), value);
                    return;
                }

                cursor = object
                    .entry((*name).to_string())
                    .or_insert_with(|| match next_segment {
                        Some(KeySegment::Index(_)) => JsonValue::Array(Vec::new()),
                        _ => JsonValue::Object(JsonMap::new()),
                    });
            }
            KeySegment::Index(index) => {
                let array = ensure_array(cursor);
                if array.len() <= *index {
                    array.resize(index + 1, JsonValue::Null);
                }

                if is_last {
                    array[*index] = value;
                    return;
                }

                let next_is_array = matches!(next_segment, Some(KeySegment::Index(_)));
                if array[*index].is_null()
                    || (!array[*index].is_array() && !array[*index].is_object())
                {
                    array[*index] = if next_is_array {
                        JsonValue::Array(Vec::new())
                    } else {
                        JsonValue::Object(JsonMap::new())
                    };
                }

                cursor = array.get_mut(*index).expect("index within bounds");
            }
        }
    }
}

fn flatten_value(target: &mut OptionMap, prefix: &str, value: YamlValue, errors: &mut Vec<String>) {
    let value = strip_tag(value);

    match value {
        YamlValue::Mapping(map) => {
            for (raw_key, raw_value) in map {
                let key_fragment = value_to_string(&raw_key);
                let combined = if prefix.is_empty() {
                    key_fragment
                } else {
                    format!("{prefix}.{key_fragment}")
                };

                flatten_value(target, &combined, raw_value, errors);
            }
        }
        YamlValue::Sequence(sequence) => {
            for (index, item) in sequence.into_iter().enumerate() {
                let combined = if prefix.is_empty() {
                    index.to_string()
                } else {
                    format!("{prefix}.{index}")
                };

                flatten_value(target, &combined, item, errors);
            }
        }
        YamlValue::Tagged(tagged) => {
            flatten_value(target, prefix, tagged.value, errors);
        }
        scalar => {
            let key = prefix.trim();

            if key.is_empty() {
                errors.push("ignoring empty configuration key".to_string());
                return;
            }

            match yaml_to_scalar(scalar, errors) {
                Some(scalar) => {
                    if target.insert(key.to_string(), scalar).is_some() {
                        errors.push(format!("duplicate option key `{}`", key));
                    }
                }
                None => errors.push(format!(
                    "unable to process value for configuration key `{}`",
                    key
                )),
            }
        }
    }
}

fn yaml_to_scalar(value: YamlValue, errors: &mut Vec<String>) -> Option<ScalarValue> {
    let value = strip_tag(value);

    match value {
        YamlValue::Null => Some(ScalarValue::Null),
        YamlValue::Bool(inner) => Some(ScalarValue::Bool(inner)),
        YamlValue::Number(number) => {
            if let Some(int) = number.as_i64() {
                Some(ScalarValue::Number(int.into()))
            } else if let Some(uint) = number.as_u64() {
                Some(ScalarValue::Number(uint.into()))
            } else if let Some(float) = number.as_f64() {
                match JsonNumber::from_f64(float) {
                    Some(value) => Some(ScalarValue::Number(value)),
                    None => {
                        errors.push(format!(
                            "unable to convert floating point value `{}` into JSON",
                            float
                        ));
                        None
                    }
                }
            } else {
                errors.push("encountered unsupported numeric value in configuration".to_string());
                None
            }
        }
        YamlValue::String(inner) => Some(ScalarValue::String(inner)),
        YamlValue::Sequence(_) | YamlValue::Mapping(_) => {
            errors.push("expected scalar but nested structure found".to_string());
            None
        }
        YamlValue::Tagged(tagged) => yaml_to_scalar(tagged.value, errors),
    }
}

fn strip_tag(value: YamlValue) -> YamlValue {
    let mut current = value;
    while let YamlValue::Tagged(tagged) = current {
        current = tagged.value;
    }

    current
}

fn ensure_object(value: &mut JsonValue) -> &mut JsonMap<String, JsonValue> {
    if !value.is_object() {
        *value = JsonValue::Object(JsonMap::new());
    }

    match value {
        JsonValue::Object(map) => map,
        _ => unreachable!("value ensured to be object"),
    }
}

fn ensure_array(value: &mut JsonValue) -> &mut Vec<JsonValue> {
    if !value.is_array() {
        *value = JsonValue::Array(Vec::new());
    }

    match value {
        JsonValue::Array(array) => array,
        _ => unreachable!("value ensured to be array"),
    }
}

fn parse_key_segments(key: &str) -> Vec<KeySegment<'_>> {
    key.split('.')
        .map(str::trim)
        .filter(|segment| !segment.is_empty())
        .map(|segment| match segment.parse::<usize>() {
            Ok(value) => KeySegment::Index(value),
            Err(_) => KeySegment::Field(segment),
        })
        .collect()
}

#[derive(Debug)]
enum KeySegment<'a> {
    Field(&'a str),
    Index(usize),
}

fn parse_circuit_breaker(
    connector: &str,
    raw: Option<RawCircuitBreakerConfig>,
    errors: &mut Vec<String>,
) -> Option<CircuitBreakerConfig> {
    let raw = raw?;

    let mut valid = true;
    let failure_rate = match raw.failure_rate {
        Some(value) if (0.0..=1.0).contains(&value) && value > 0.0 => value,
        Some(value) => {
            errors.push(format!(
                "connector `{connector}` cb.failure_rate must be greater than 0 and at most 1.0 (got {value})"
            ));
            valid = false;
            value
        }
        None => {
            errors.push(format!(
                "connector `{connector}` cb.failure_rate is required when `cb` is configured"
            ));
            valid = false;
            0.0
        }
    };

    let window_label = format!("connector `{connector}` cb.window");
    let window = parse_duration_value(&window_label, raw.window, errors)
        .and_then(|duration| ensure_positive_duration(duration, &window_label, errors));

    let base_label = format!("connector `{connector}` cb.open_base");
    let open_base = parse_duration_value(&base_label, raw.open_base, errors)
        .and_then(|duration| ensure_positive_duration(duration, &base_label, errors));

    let max_label = format!("connector `{connector}` cb.open_max");
    let open_max = parse_duration_value(&max_label, raw.open_max, errors)
        .and_then(|duration| ensure_positive_duration(duration, &max_label, errors));

    if window.is_none() || open_base.is_none() || open_max.is_none() {
        valid = false;
    }

    if let (Some(base), Some(max)) = (open_base, open_max) {
        if max < base {
            errors.push(format!(
                "connector `{connector}` cb.open_max must be greater than or equal to cb.open_base"
            ));
            valid = false;
        }
    }

    if !valid {
        return None;
    }

    Some(CircuitBreakerConfig {
        failure_rate,
        window: window.expect("window validated"),
        open_base: open_base.expect("open_base validated"),
        open_max: open_max.expect("open_max validated"),
    })
}

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaConnectorOptions {
    #[serde(deserialize_with = "deserialize_broker_list")]
    pub brokers: Vec<String>,
    #[serde(default)]
    pub create_topics_if_missing: bool,
    #[serde(flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RabbitmqConnectorOptions {
    pub url: String,
    #[serde(default)]
    pub prefetch: Option<u32>,
    #[serde(default)]
    pub tls: Option<RabbitmqTlsOptions>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RabbitmqTlsOptions {
    #[serde(default)]
    pub ca: Option<String>,
    #[serde(default)]
    pub cert: Option<String>,
    #[serde(default)]
    pub key: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MqttConnectorOptions {
    pub url: String,
    #[serde(default)]
    pub client_id: Option<String>,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub keep_alive: Option<u64>,
    #[serde(default)]
    pub tls: Option<MqttTlsOptions>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MqttTlsOptions {
    #[serde(default)]
    pub ca: Option<String>,
    #[serde(default)]
    pub cert: Option<String>,
    #[serde(default)]
    pub key: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MongodbConnectorOptions {
    #[serde(alias = "url")]
    pub uri: String,
    pub database: String,
    #[serde(default)]
    pub tls: Option<MongodbTlsOptions>,
    #[serde(default)]
    pub options: Option<JsonValue>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MongodbTlsOptions {
    #[serde(default)]
    pub ca: Option<String>,
    #[serde(default)]
    pub cert: Option<String>,
    #[serde(default)]
    pub key: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaTlsOptions {
    #[serde(default)]
    pub ca: Option<String>,
    #[serde(default)]
    pub cert: Option<String>,
    #[serde(default)]
    pub key: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaSaslOptions {
    pub mechanism: String,
    pub username: String,
    pub password: String,
    #[serde(flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaTopicOptions {
    #[serde(default)]
    pub partitions: Option<i32>,
    #[serde(default)]
    pub replication_factor: Option<i16>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MariadbTlsOptions {
    #[serde(default)]
    pub mode: Option<String>,
    #[serde(default)]
    pub ca: Option<String>,
    #[serde(default)]
    pub cert: Option<String>,
    #[serde(default)]
    pub key: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PostgresTlsOptions {
    #[serde(default)]
    pub mode: Option<String>,
    #[serde(default)]
    pub ca: Option<String>,
    #[serde(default)]
    pub cert: Option<String>,
    #[serde(default)]
    pub key: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RedisConnectorOptions {
    pub url: String,
    #[serde(default)]
    pub cluster: Option<bool>,
    #[serde(default)]
    pub tls: Option<RedisTlsOptions>,
    #[serde(default)]
    pub pool: Option<RedisPoolOptions>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SmtpConnectorOptions {
    pub host: String,
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default)]
    pub auth: Option<SmtpAuthOptions>,
    #[serde(default)]
    pub tls: Option<SmtpTlsOptions>,
    #[serde(default, rename = "from")]
    pub default_from: Option<String>,
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SmtpAuthOptions {
    pub username: String,
    pub password: String,
    #[serde(default)]
    pub mechanism: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SmtpTlsOptions {
    #[serde(default)]
    pub mode: Option<SmtpTlsMode>,
    #[serde(default)]
    pub ca: Option<String>,
    #[serde(default)]
    pub domain: Option<String>,
    #[serde(default)]
    pub cert: Option<String>,
    #[serde(default)]
    pub key: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SmtpTlsMode {
    Starttls,
    Tls,
    None,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RedisTlsOptions {
    #[serde(default)]
    pub ca: Option<String>,
    #[serde(default)]
    pub cert: Option<String>,
    #[serde(default)]
    pub key: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct RedisPoolOptions {
    #[serde(default)]
    pub max_connections: Option<u32>,
    #[serde(default)]
    pub min_idle: Option<u32>,
    #[serde(default)]
    pub idle_timeout_secs: Option<u64>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PostgresConnectorOptions {
    pub url: String,
    #[serde(default)]
    pub schema: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MariadbConnectorOptions {
    pub url: String,
    #[serde(default)]
    pub schema: Option<String>,
    #[serde(default)]
    pub pool: Option<DbPoolOptions>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HttpServerConnectorOptions {
    #[serde(default = "default_http_host")]
    pub host: String,
    pub port: u16,
    #[serde(default)]
    pub tls: Option<HttpServerTlsOptions>,
    #[serde(default)]
    pub health: Option<HttpServerHealthOptions>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HttpServerTlsOptions {
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default)]
    pub cert: Option<String>,
    #[serde(default)]
    pub ca: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HttpServerHealthOptions {
    #[serde(default)]
    pub method: Option<String>,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HttpClientConnectorOptions {
    pub base_url: String,
    #[serde(default)]
    pub tls: Option<HttpClientTlsOptions>,
    #[serde(default)]
    pub pool: Option<HttpClientPoolOptions>,
    #[serde(default)]
    pub default_headers: Option<BTreeMap<String, String>>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HttpClientTlsOptions {
    #[serde(default)]
    pub ca: Option<String>,
    #[serde(default)]
    pub cert: Option<String>,
    #[serde(default)]
    pub key: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct HttpClientPoolOptions {
    #[serde(default)]
    pub max_idle_per_host: Option<usize>,
    #[serde(default)]
    pub idle_timeout_secs: Option<u64>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GrpcConnectorOptions {
    pub endpoint: String,
    #[serde(default)]
    pub tls: Option<GrpcTlsOptions>,
    #[serde(default)]
    pub descriptor: Option<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GrpcTlsOptions {
    #[serde(default)]
    pub domain: Option<String>,
    #[serde(default)]
    pub ca: Option<String>,
    #[serde(default)]
    pub cert: Option<String>,
    #[serde(default)]
    pub key: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct DbPoolOptions {
    #[serde(default)]
    pub max_connections: Option<u32>,
    #[serde(default)]
    pub idle_timeout_secs: Option<u64>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

pub(crate) fn default_http_host() -> String {
    "0.0.0.0".to_string()
}

fn deserialize_broker_list<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct BrokerVisitor;

    impl<'de> serde::de::Visitor<'de> for BrokerVisitor {
        type Value = Vec<String>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("string or sequence of strings describing Kafka brokers")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(value
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect())
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut brokers = Vec::new();
            while let Some(item) = seq.next_element::<String>()? {
                brokers.push(item);
            }
            Ok(brokers)
        }
    }

    deserializer.deserialize_any(BrokerVisitor)
}

pub(crate) fn validate_connector_tls_requirements(
    connectors: &[ConnectorConfig],
    errors: &mut Vec<String>,
) {
    for connector in connectors {
        let options = connector.options_json();

        match connector.kind {
            ConnectorKind::HttpServer => validate_http_server_tls(connector, &options, errors),
            ConnectorKind::HttpClient => validate_http_client_tls(connector, &options, errors),
            ConnectorKind::Kafka => validate_kafka_tls(connector, &options, errors),
            ConnectorKind::Mariadb => validate_mariadb_tls(connector, &options, errors),
            ConnectorKind::Postgres => validate_postgres_tls(connector, &options, errors),
            ConnectorKind::Grpc => validate_grpc_tls(connector, &options, errors),
            ConnectorKind::Rabbitmq => validate_rabbitmq_tls(connector, &options, errors),
            ConnectorKind::Mqtt => validate_mqtt_tls(connector, &options, errors),
            ConnectorKind::Redis => validate_redis_tls(connector, &options, errors),
            ConnectorKind::Smtp => validate_smtp_tls(connector, &options, errors),
            _ => validate_generic_tls_block(connector, &options, errors),
        }
    }
}

fn validate_http_server_tls(
    connector: &ConnectorConfig,
    options: &JsonValue,
    errors: &mut Vec<String>,
) {
    let tls = option_object(options, "tls");
    let Some(tls) = tls else {
        return;
    };

    let (has_cert, has_key) = tls_cert_key_status(tls, connector, errors);

    if !(has_cert && has_key) {
        errors.push(format!(
            "connector `{}` must provide both `tls.cert` and `tls.key` to enable HTTPS listeners",
            connector.name
        ));
    }
}

fn validate_http_client_tls(
    connector: &ConnectorConfig,
    options: &JsonValue,
    errors: &mut Vec<String>,
) {
    let base_url = option_str(options, "base_url");
    let tls = option_object(options, "tls");
    let uses_https = base_url.is_some_and(|url| url.starts_with("https://"));

    if uses_https && tls.is_none() {
        errors.push(format!(
            "connector `{}` uses https:// but is missing required `tls` section",
            connector.name
        ));
        return;
    }

    if let Some(tls) = tls {
        let (has_cert, has_key) = tls_cert_key_status(tls, connector, errors);

        if (has_cert || has_key) && !uses_https {
            errors.push(format!(
                "connector `{}` configures TLS client certificates but `base_url` does not start with https://",
                connector.name
            ));
        }

        if uses_https && !map_has_non_empty_string(tls, "ca") {
            errors.push(format!(
                "connector `{}` must provide `tls.ca` when using https:// URLs",
                connector.name
            ));
        }
    }
}

fn validate_kafka_tls(connector: &ConnectorConfig, options: &JsonValue, errors: &mut Vec<String>) {
    let tls = option_object(options, "tls");
    let sasl = option_object(options, "sasl");
    let brokers = option_str(options, "brokers");

    if let Some(brokers) = brokers {
        let uses_tls = brokers
            .split(',')
            .any(|broker| broker.trim().starts_with("ssl://"));
        if uses_tls && tls.is_none() {
            errors.push(format!(
                "connector `{}` uses ssl:// brokers but is missing the `tls` section required to trust peers",
                connector.name
            ));
        }
    }

    if sasl.is_some() && tls.is_none() {
        errors.push(format!(
            "connector `{}` configures SASL but is missing the `tls` section required to secure credentials",
            connector.name
        ));
    }
}

fn validate_mariadb_tls(
    connector: &ConnectorConfig,
    options: &JsonValue,
    errors: &mut Vec<String>,
) {
    validate_relational_tls(connector, options, errors);
}

fn validate_postgres_tls(
    connector: &ConnectorConfig,
    options: &JsonValue,
    errors: &mut Vec<String>,
) {
    validate_relational_tls(connector, options, errors);
}

fn validate_relational_tls(
    connector: &ConnectorConfig,
    options: &JsonValue,
    errors: &mut Vec<String>,
) {
    let Some(tls) = option_object(options, "tls") else {
        return;
    };

    let mode = tls
        .get("mode")
        .and_then(JsonValue::as_str)
        .map(|value| value.trim().to_ascii_lowercase());
    let tls_disabled = matches!(mode.as_deref(), Some("disable"));

    let (has_cert, has_key) = tls_cert_key_status(tls, connector, errors);
    let has_ca = map_has_non_empty_string(tls, "ca");

    if tls_disabled && (has_cert || has_key || has_ca) {
        errors.push(format!(
            "connector `{}` sets `tls.mode` to `disable` but also configures TLS credentials",
            connector.name
        ));
    }
}

fn validate_grpc_tls(connector: &ConnectorConfig, options: &JsonValue, errors: &mut Vec<String>) {
    let endpoint = option_str(options, "endpoint");
    let uses_tls = endpoint
        .map(|value| value.starts_with("https://"))
        .unwrap_or(false);
    let tls = option_object(options, "tls");

    if uses_tls && tls.is_none() {
        errors.push(format!(
            "connector `{}` uses an https endpoint but is missing the `tls` section required to trust peers",
            connector.name
        ));
        return;
    }

    if let Some(tls) = tls {
        let (has_cert, has_key) = tls_cert_key_status(tls, connector, errors);

        if (has_cert || has_key) && !uses_tls {
            errors.push(format!(
                "connector `{}` configures TLS client certificates but `endpoint` does not start with https://",
                connector.name
            ));
        }

        if uses_tls && !map_has_non_empty_string(tls, "ca") {
            errors.push(format!(
                "connector `{}` must provide `tls.ca` when using https-based gRPC endpoints",
                connector.name
            ));
        }
    }
}

fn validate_rabbitmq_tls(
    connector: &ConnectorConfig,
    options: &JsonValue,
    errors: &mut Vec<String>,
) {
    let url = option_str(options, "url");
    let uses_tls = url.is_some_and(|value| value.starts_with("amqps://"));
    let tls = option_object(options, "tls");

    if uses_tls && tls.is_none() {
        errors.push(format!(
            "connector `{}` uses amqps:// but is missing the `tls` section required to trust peers",
            connector.name
        ));
        return;
    }

    if let Some(tls) = tls {
        let (has_cert, has_key) = tls_cert_key_status(tls, connector, errors);

        if (has_cert || has_key) && !uses_tls {
            errors.push(format!(
                "connector `{}` configures TLS client certificates but `url` does not start with amqps://",
                connector.name
            ));
        }

        if uses_tls && !map_has_non_empty_string(tls, "ca") {
            errors.push(format!(
                "connector `{}` must provide `tls.ca` when using amqps:// URLs",
                connector.name
            ));
        }
    }
}

fn validate_mqtt_tls(connector: &ConnectorConfig, options: &JsonValue, errors: &mut Vec<String>) {
    let url = option_str(options, "url");
    let uses_tls = url.is_some_and(|value| value.starts_with("mqtts://"));
    let tls = option_object(options, "tls");

    if uses_tls && tls.is_none() {
        errors.push(format!(
            "connector `{}` uses mqtts:// but is missing the `tls` section required to trust peers",
            connector.name
        ));
        return;
    }

    if let Some(tls) = tls {
        let (has_cert, has_key) = tls_cert_key_status(tls, connector, errors);

        if (has_cert || has_key) && !uses_tls {
            errors.push(format!(
                "connector `{}` configures TLS client certificates but `url` does not start with mqtts://",
                connector.name
            ));
        }

        if uses_tls && !map_has_non_empty_string(tls, "ca") {
            errors.push(format!(
                "connector `{}` must provide `tls.ca` when using mqtts:// URLs",
                connector.name
            ));
        }
    }
}

fn validate_redis_tls(connector: &ConnectorConfig, options: &JsonValue, errors: &mut Vec<String>) {
    let url = option_str(options, "url");
    let uses_tls = url.is_some_and(|value| value.starts_with("rediss://"));
    let tls = option_object(options, "tls");

    if uses_tls && tls.is_none() {
        errors.push(format!(
            "connector `{}` uses rediss:// but is missing the `tls` section required to trust peers",
            connector.name
        ));
        return;
    }

    if let Some(tls) = tls {
        let (has_cert, has_key) = tls_cert_key_status(tls, connector, errors);

        if (has_cert || has_key) && !uses_tls {
            errors.push(format!(
                "connector `{}` configures TLS client certificates but `url` does not start with rediss://",
                connector.name
            ));
        }

        if uses_tls && !map_has_non_empty_string(tls, "ca") {
            errors.push(format!(
                "connector `{}` must provide `tls.ca` when using rediss:// URLs",
                connector.name
            ));
        }
    }
}

fn validate_generic_tls_block(
    connector: &ConnectorConfig,
    options: &JsonValue,
    errors: &mut Vec<String>,
) {
    if let Some(tls) = option_object(options, "tls") {
        tls_cert_key_status(tls, connector, errors);
    }
}

fn validate_smtp_tls(connector: &ConnectorConfig, options: &JsonValue, errors: &mut Vec<String>) {
    let Some(tls) = option_object(options, "tls") else {
        return;
    };

    let mode = tls
        .get("mode")
        .and_then(JsonValue::as_str)
        .map(|value| value.trim().to_ascii_lowercase());

    if let Some(ref mode) = mode {
        match mode.as_str() {
            "starttls" | "tls" | "none" => {}
            other => errors.push(format!(
                "connector `{}` has invalid tls.mode `{other}` (expected `starttls`, `tls`, or `none`)",
                connector.name
            )),
        }
    }

    let (has_cert, has_key) = tls_cert_key_status(tls, connector, errors);

    if matches!(mode.as_deref(), Some("none")) && (has_cert || has_key) {
        errors.push(format!(
            "connector `{}` sets tls.mode to `none` but also configures client certificates",
            connector.name
        ));
    }

    if (has_cert || has_key) && !map_has_non_empty_string(tls, "domain") {
        errors.push(format!(
            "connector `{}` configures client certificates but must provide `tls.domain`",
            connector.name
        ));
    }
}

fn tls_cert_key_status(
    tls: &JsonMap<String, JsonValue>,
    connector: &ConnectorConfig,
    errors: &mut Vec<String>,
) -> (bool, bool) {
    let cert_value = map_non_empty_str(tls, "cert");
    let key_value = map_non_empty_str(tls, "key");
    let has_cert = cert_value.is_some();
    let has_key = key_value.is_some();

    if has_cert ^ has_key {
        errors.push(format!(
            "connector `{}` must provide both `tls.cert` and `tls.key` when configuring client certificates",
            connector.name
        ));
    }

    if let Some(key_value) = key_value {
        if looks_like_inline_private_key(key_value) {
            errors.push(format!(
                "connector `{}` tls.key must reference a filesystem path; inline PEM blobs are not supported",
                connector.name
            ));
        }
    }

    (has_cert, has_key)
}

fn option_object<'a>(value: &'a JsonValue, key: &str) -> Option<&'a JsonMap<String, JsonValue>> {
    value
        .as_object()
        .and_then(|map| map.get(key))
        .and_then(JsonValue::as_object)
}

fn option_str<'a>(value: &'a JsonValue, key: &str) -> Option<&'a str> {
    value
        .as_object()
        .and_then(|map| map.get(key))
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

fn map_has_non_empty_string(map: &JsonMap<String, JsonValue>, key: &str) -> bool {
    map_non_empty_str(map, key).is_some()
}

fn map_non_empty_str<'a>(map: &'a JsonMap<String, JsonValue>, key: &str) -> Option<&'a str> {
    map.get(key)
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn looks_like_inline_private_key(value: &str) -> bool {
    let trimmed = value.trim_start();
    trimmed.starts_with("-----BEGIN") && trimmed.contains("PRIVATE KEY-----")
}
