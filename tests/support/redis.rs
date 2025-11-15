#![allow(dead_code)]

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use chronicle::transport::redis::RedisDelivery;
use chrono::Utc;
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use std::str;

/// Mirror of the production payload builder for Redis deliveries, scoped to tests.
pub fn build_trigger_payload(delivery: &RedisDelivery) -> Result<JsonValue, anyhow::Error> {
    let now = Utc::now().timestamp_millis();
    match delivery {
        RedisDelivery::PubSub { channel, payload } => {
            let mut root = JsonMap::new();
            root.insert("channel".to_string(), JsonValue::String(channel.clone()));
            root.insert(
                "payload".to_string(),
                JsonValue::Object(binary_to_json_map(payload)),
            );
            root.insert("attributes".to_string(), JsonValue::Object(JsonMap::new()));
            root.insert(
                "metadata".to_string(),
                JsonValue::Object(metadata_with_timestamp(now)),
            );

            Ok(JsonValue::Object(root))
        }
        RedisDelivery::Stream { stream, id, fields } => {
            let mut root = JsonMap::new();
            root.insert("stream".to_string(), JsonValue::String(stream.clone()));
            root.insert("id".to_string(), JsonValue::String(id.clone()));

            let mut attributes = JsonMap::new();
            let mut payload_value: Option<JsonValue> = None;
            let mut metadata = metadata_with_timestamp(now);

            for (field, value) in fields {
                let value_json = JsonValue::Object(binary_to_json_map(value));
                if field == "payload" {
                    payload_value = Some(value_json.clone());
                    continue;
                }

                if field == "timestamp" {
                    if let Ok(text) = str::from_utf8(value) {
                        if let Ok(parsed_ts) = text.parse::<i64>() {
                            metadata.insert(
                                "timestamp".to_string(),
                                JsonValue::Number(JsonNumber::from(parsed_ts)),
                            );
                        }
                    }
                }

                attributes.insert(field.clone(), value_json);
            }

            root.insert(
                "payload".to_string(),
                payload_value.unwrap_or_else(|| JsonValue::Object(binary_to_json_map(&[]))),
            );

            root.insert("attributes".to_string(), JsonValue::Object(attributes));
            root.insert("metadata".to_string(), JsonValue::Object(metadata));

            Ok(JsonValue::Object(root))
        }
    }
}

fn binary_to_json_map(bytes: &[u8]) -> JsonMap<String, JsonValue> {
    let mut map = JsonMap::new();
    if let Ok(text) = std::str::from_utf8(bytes) {
        map.insert("text".to_string(), JsonValue::String(text.to_string()));
        if let Ok(json) = serde_json::from_str::<JsonValue>(text) {
            map.insert("json".to_string(), json);
        }
    } else if !bytes.is_empty() {
        map.insert(
            "base64".to_string(),
            JsonValue::String(BASE64_STANDARD.encode(bytes)),
        );
    }
    map
}

fn metadata_with_timestamp(timestamp: i64) -> JsonMap<String, JsonValue> {
    let mut metadata = JsonMap::new();
    metadata.insert(
        "timestamp".to_string(),
        JsonValue::Number(JsonNumber::from(timestamp)),
    );
    metadata
}
