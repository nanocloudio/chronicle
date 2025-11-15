use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use humantime::parse_duration;
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::collections::BTreeMap;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct RetrySettings {
    initial: Duration,
    max: Duration,
    multiplier: f64,
}

impl RetrySettings {
    pub fn from_extras(
        trigger_extra: &JsonMap<String, JsonValue>,
        connector_extra: &JsonMap<String, JsonValue>,
    ) -> Self {
        let initial = lookup_retry_duration(
            trigger_extra,
            connector_extra,
            "retry_initial",
            "retry_initial_ms",
        )
        .unwrap_or_else(|| Duration::from_millis(200));

        let mut max =
            lookup_retry_duration(trigger_extra, connector_extra, "retry_max", "retry_max_ms")
                .unwrap_or_else(|| Duration::from_secs(5));
        if max < initial {
            max = initial;
        }
        let multiplier = lookup_multiplier(trigger_extra, connector_extra, "retry_multiplier")
            .unwrap_or(2.0)
            .clamp(1.1, 10.0);

        Self {
            initial,
            max,
            multiplier,
        }
    }
}

impl RetrySettings {
    pub fn initial(&self) -> Duration {
        self.initial
    }

    pub fn max(&self) -> Duration {
        self.max
    }

    pub fn multiplier(&self) -> f64 {
        self.multiplier
    }
}

pub struct RetryBackoff {
    policy: RetrySettings,
    current: Duration,
}

impl RetryBackoff {
    pub fn new(policy: RetrySettings) -> Self {
        let current = policy.initial;
        Self { policy, current }
    }

    pub fn on_success(&mut self) {
        self.current = self.policy.initial;
    }

    pub fn on_failure(&mut self) -> Duration {
        let delay = self.current.max(Duration::from_millis(50));
        let next = (delay.as_millis() as f64 * self.policy.multiplier)
            .round()
            .max(self.policy.initial.as_millis() as f64);
        let capped = next.min(self.policy.max.as_millis() as f64);
        let next_duration = Duration::from_millis(capped as u64);
        self.current = std::cmp::min(next_duration, self.policy.max);
        delay
    }
}

#[derive(Debug)]
pub struct ParsedBinary {
    pub base64: String,
    pub text: Option<String>,
    pub json: Option<JsonValue>,
}

pub fn parse_binary(bytes: &[u8]) -> ParsedBinary {
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

pub fn map_from_btree(map: &BTreeMap<String, JsonValue>) -> JsonMap<String, JsonValue> {
    map.iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn lookup_retry_duration(
    trigger_extra: &JsonMap<String, JsonValue>,
    connector_extra: &JsonMap<String, JsonValue>,
    key: &str,
    legacy_key: &str,
) -> Option<Duration> {
    lookup_duration_value(trigger_extra, connector_extra, key)
        .or_else(|| lookup_legacy_duration(trigger_extra, connector_extra, legacy_key))
}

fn lookup_multiplier(
    trigger_extra: &JsonMap<String, JsonValue>,
    connector_extra: &JsonMap<String, JsonValue>,
    key: &str,
) -> Option<f64> {
    lookup_number(trigger_extra, connector_extra, key).map(|value| value.max(1.0))
}

fn lookup_number(
    trigger_extra: &JsonMap<String, JsonValue>,
    connector_extra: &JsonMap<String, JsonValue>,
    key: &str,
) -> Option<f64> {
    trigger_extra
        .get(key)
        .or_else(|| connector_extra.get(key))
        .and_then(value_to_f64)
}

fn lookup_duration_value(
    trigger_extra: &JsonMap<String, JsonValue>,
    connector_extra: &JsonMap<String, JsonValue>,
    key: &str,
) -> Option<Duration> {
    trigger_extra
        .get(key)
        .or_else(|| connector_extra.get(key))
        .and_then(duration_from_json)
}

fn lookup_legacy_duration(
    trigger_extra: &JsonMap<String, JsonValue>,
    connector_extra: &JsonMap<String, JsonValue>,
    key: &str,
) -> Option<Duration> {
    lookup_number(trigger_extra, connector_extra, key)
        .map(|value| Duration::from_millis(value.max(0.0) as u64))
}

fn duration_from_json(value: &JsonValue) -> Option<Duration> {
    match value {
        JsonValue::String(text) => parse_duration(text).ok(),
        JsonValue::Number(num) => num
            .as_f64()
            .map(|ms| Duration::from_millis(ms.max(0.0) as u64)),
        _ => None,
    }
}

fn value_to_f64(value: &JsonValue) -> Option<f64> {
    match value {
        JsonValue::Number(num) => num.as_f64(),
        JsonValue::String(text) => text.trim().parse::<f64>().ok(),
        _ => None,
    }
}
