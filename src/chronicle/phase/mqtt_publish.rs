use crate::chronicle::context::ExecutionContext;
use crate::chronicle::phase::utils::{
    required_option, required_string_option, resolve_optional_template, resolve_string,
    resolve_template,
};
use crate::chronicle::phase::{PhaseExecutionError, PhaseHandler};
use crate::config::integration::ChroniclePhase;
use crate::integration::factory::ConnectorFactoryRegistry;
use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use chrono::Utc;
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use std::sync::atomic::{AtomicU16, Ordering};

#[derive(Debug, Default)]
pub struct MqttPublishPhaseExecutor {
    packet_seq: AtomicU16,
}

#[async_trait]
impl PhaseHandler for MqttPublishPhaseExecutor {
    async fn execute(
        &self,
        phase: &ChroniclePhase,
        context: &mut ExecutionContext,
        connectors: &ConnectorFactoryRegistry,
    ) -> Result<JsonValue, PhaseExecutionError> {
        let options = phase.options_json();

        let connector_name = required_string_option(phase, &options, "connector")?;

        let topic_template = required_option(phase, &options, "topic")?;

        let handle = connectors.mqtt_client(connector_name).map_err(|err| {
            PhaseExecutionError::invalid_configuration(
                phase.name.clone(),
                format!("failed to acquire mqtt connector `{connector_name}`: {err}"),
            )
        })?;

        let topic = resolve_string(context, topic_template)?;

        let qos = resolve_qos(options.get("qos"), context, phase)?;
        let retain = resolve_retain(options.get("retain"), context, phase)?;
        let encoding = resolve_encoding(options.get("payload_encoding"), context, phase)?;

        let payload =
            resolve_optional_template(context, options.get("payload"))?.unwrap_or_else(|| {
                context
                    .last_slot_value()
                    .cloned()
                    .unwrap_or(JsonValue::Null)
            });

        let packet_id = if qos == 0 {
            None
        } else {
            Some(next_packet_id(&self.packet_seq))
        };

        let mut output = JsonMap::new();
        output.insert(
            "connector".to_string(),
            JsonValue::String(connector_name.to_string()),
        );
        output.insert("topic".to_string(), JsonValue::String(topic));
        let encoded_payload = encode_payload(&payload, encoding, &phase.name)?;

        output.insert("payload".to_string(), payload);
        output.insert(
            "payload_encoding".to_string(),
            JsonValue::String(encoding.as_str().to_string()),
        );
        output.insert(
            "payload_encoded".to_string(),
            JsonValue::String(encoded_payload),
        );
        output.insert("qos".to_string(), JsonValue::Number(JsonNumber::from(qos)));
        output.insert("retain".to_string(), JsonValue::Bool(retain));
        if let Some(id) = packet_id {
            output.insert(
                "packet_id".to_string(),
                JsonValue::Number(JsonNumber::from(id)),
            );
        } else {
            output.insert("packet_id".to_string(), JsonValue::Null);
        }
        output.insert(
            "published_at".to_string(),
            JsonValue::String(Utc::now().to_rfc3339()),
        );
        output.insert(
            "url".to_string(),
            JsonValue::String(handle.url().to_string()),
        );

        Ok(JsonValue::Object(output))
    }
}

#[derive(Debug, Clone, Copy)]
enum PayloadEncoding {
    Json,
    Base64,
}

impl PayloadEncoding {
    fn as_str(&self) -> &'static str {
        match self {
            PayloadEncoding::Json => "json",
            PayloadEncoding::Base64 => "base64",
        }
    }
}

fn resolve_encoding(
    template: Option<&JsonValue>,
    context: &ExecutionContext,
    phase: &ChroniclePhase,
) -> Result<PayloadEncoding, PhaseExecutionError> {
    if let Some(value) = template {
        let text = resolve_string(context, value)?;
        let normalised = text.trim().to_ascii_lowercase();

        if normalised.is_empty() || normalised == "json" {
            Ok(PayloadEncoding::Json)
        } else if normalised == "base64" {
            Ok(PayloadEncoding::Base64)
        } else {
            Err(PhaseExecutionError::invalid_input(
                &phase.name,
                format!("payload_encoding must be `json` or `base64`, found `{text}`"),
            ))
        }
    } else {
        Ok(PayloadEncoding::Json)
    }
}

fn encode_payload(
    payload: &JsonValue,
    encoding: PayloadEncoding,
    phase: &str,
) -> Result<String, PhaseExecutionError> {
    match encoding {
        PayloadEncoding::Json => serde_json::to_string(payload).map_err(|err| {
            PhaseExecutionError::invalid_input(
                phase.to_string(),
                format!("failed to encode payload as json: {err}"),
            )
        }),
        PayloadEncoding::Base64 => {
            let bytes = payload_bytes(payload, phase)?;
            Ok(BASE64.encode(bytes))
        }
    }
}

fn payload_bytes(payload: &JsonValue, phase: &str) -> Result<Vec<u8>, PhaseExecutionError> {
    match payload {
        JsonValue::Null => Ok(Vec::new()),
        JsonValue::String(text) => match BASE64.decode(text) {
            Ok(decoded) => Ok(decoded),
            Err(_) => Ok(text.as_bytes().to_vec()),
        },
        JsonValue::Array(items) => {
            let mut bytes = Vec::with_capacity(items.len());
            for (index, item) in items.iter().enumerate() {
                let value = match item {
                    JsonValue::Number(num) => {
                        if let Some(u) = num.as_u64() {
                            if u <= u8::MAX as u64 {
                                u as u8
                            } else {
                                return Err(PhaseExecutionError::invalid_input(
                                    phase.to_string(),
                                    format!("payload byte at index {index} exceeds 255"),
                                ));
                            }
                        } else if let Some(i) = num.as_i64() {
                            if (0..=255).contains(&i) {
                                i as u8
                            } else {
                                return Err(PhaseExecutionError::invalid_input(
                                    phase.to_string(),
                                    format!("payload byte at index {index} exceeds range 0-255"),
                                ));
                            }
                        } else {
                            return Err(PhaseExecutionError::invalid_input(
                                phase.to_string(),
                                format!("payload byte at index {index} must be numeric"),
                            ));
                        }
                    }
                    other => {
                        return Err(PhaseExecutionError::invalid_input(
                            phase.to_string(),
                            format!("payload array expects numeric entries, found {other:?}"),
                        ));
                    }
                };
                bytes.push(value);
            }
            Ok(bytes)
        }
        other => serde_json::to_vec(other).map_err(|err| {
            PhaseExecutionError::invalid_input(
                phase.to_string(),
                format!("failed to serialise payload for base64 encoding: {err}"),
            )
        }),
    }
}

fn resolve_qos(
    template: Option<&JsonValue>,
    context: &ExecutionContext,
    phase: &ChroniclePhase,
) -> Result<u8, PhaseExecutionError> {
    if let Some(value) = template {
        let resolved = resolve_template(context, value)?;

        match resolved {
            JsonValue::Number(num) => {
                let qos = num.as_u64().ok_or_else(|| {
                    PhaseExecutionError::invalid_input(
                        &phase.name,
                        "qos must be an integer between 0 and 2".to_string(),
                    )
                })?;
                validate_qos(qos as i64, &phase.name)
            }
            JsonValue::String(text) => {
                let parsed = text.parse::<i64>().map_err(|_| {
                    PhaseExecutionError::invalid_input(
                        &phase.name,
                        format!("qos value `{text}` is not a valid integer"),
                    )
                })?;
                validate_qos(parsed, &phase.name)
            }
            JsonValue::Null => Ok(0),
            other => {
                if other.is_boolean() {
                    Err(PhaseExecutionError::invalid_input(
                        &phase.name,
                        format!("qos must be numeric, found {other:?}"),
                    ))
                } else {
                    Err(PhaseExecutionError::invalid_input(
                        &phase.name,
                        format!("qos expected number or string, found {other:?}"),
                    ))
                }
            }
        }
    } else {
        Ok(0)
    }
}

fn validate_qos(value: i64, phase: &str) -> Result<u8, PhaseExecutionError> {
    if (0..=2).contains(&value) {
        Ok(value as u8)
    } else {
        Err(PhaseExecutionError::invalid_input(
            phase.to_string(),
            "qos must be between 0 and 2 (inclusive)".to_string(),
        ))
    }
}

fn resolve_retain(
    template: Option<&JsonValue>,
    context: &ExecutionContext,
    phase: &ChroniclePhase,
) -> Result<bool, PhaseExecutionError> {
    if let Some(value) = template {
        let resolved = resolve_template(context, value)?;

        match resolved {
            JsonValue::Bool(flag) => Ok(flag),
            JsonValue::Number(num) => Ok(num != JsonNumber::from(0)),
            JsonValue::String(text) => match text.to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" | "on" => Ok(true),
                "false" | "0" | "no" | "off" => Ok(false),
                other => Err(PhaseExecutionError::invalid_input(
                    &phase.name,
                    format!("retain value `{other}` is not a boolean"),
                )),
            },
            JsonValue::Null => Ok(false),
            other => Err(PhaseExecutionError::invalid_input(
                &phase.name,
                format!("retain expected boolean-compatible value, found {other:?}"),
            )),
        }
    } else {
        Ok(false)
    }
}

fn next_packet_id(seq: &AtomicU16) -> u64 {
    let previous = seq
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
            let next = if current == u16::MAX {
                1
            } else {
                current.saturating_add(1)
            };
            Some(next)
        })
        .unwrap_or(1);

    if previous == 0 {
        1
    } else {
        previous as u64
    }
}
