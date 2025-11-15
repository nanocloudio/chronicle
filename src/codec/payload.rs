#![forbid(unsafe_code)]

use base64::engine::general_purpose::STANDARD as BASE64_ENGINE;
use base64::Engine;
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};

/// Represents an encoded payload plus metadata such as codec/content-type.
#[derive(Clone, Debug, PartialEq)]
pub struct EncodedPayload {
    pub codec: String,
    pub content_type: Option<String>,
    pub data: Vec<u8>,
    pub metadata: JsonMap<String, JsonValue>,
}

impl EncodedPayload {
    pub fn new(codec: impl Into<String>, content_type: Option<String>, data: Vec<u8>) -> Self {
        Self {
            codec: codec.into(),
            content_type,
            data,
            metadata: JsonMap::new(),
        }
    }

    pub fn with_metadata(
        codec: impl Into<String>,
        content_type: Option<String>,
        data: Vec<u8>,
        metadata: JsonMap<String, JsonValue>,
    ) -> Self {
        Self {
            codec: codec.into(),
            content_type,
            data,
            metadata,
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn into_data(self) -> Vec<u8> {
        self.data
    }

    pub fn content_type(&self) -> Option<&str> {
        self.content_type.as_deref()
    }

    pub fn codec(&self) -> &str {
        &self.codec
    }

    /// Serialises the payload into the JSON shape historically used by serialize phases.
    pub fn to_json(&self) -> JsonValue {
        let mut object = self.metadata.clone();
        object.insert("codec".to_string(), JsonValue::String(self.codec.clone()));
        if let Some(ct) = &self.content_type {
            object.insert("content_type".to_string(), JsonValue::String(ct.clone()));
        }
        object.insert(
            "binary".to_string(),
            JsonValue::String(BASE64_ENGINE.encode(&self.data)),
        );
        object.insert(
            "length".to_string(),
            JsonValue::Number(JsonNumber::from(self.data.len() as u64)),
        );
        JsonValue::Object(object)
    }

    /// Attempts to interpret a JSON value as an encoded payload.
    pub fn from_json(value: &JsonValue) -> Option<Self> {
        let map = value.as_object()?;
        let codec = map.get("codec")?.as_str()?.to_string();
        let binary = map.get("binary")?.as_str()?.to_string();
        let content_type = map
            .get("content_type")
            .and_then(JsonValue::as_str)
            .map(|s| s.to_string());
        let data = BASE64_ENGINE.decode(binary).ok()?;

        let mut metadata = map.clone();
        metadata.remove("codec");
        metadata.remove("binary");
        metadata.remove("length");
        metadata.remove("content_type");

        Some(Self {
            codec,
            content_type,
            data,
            metadata,
        })
    }

    /// Encodes arbitrary JSON into an `application/json` payload.
    pub fn from_json_value(value: &JsonValue) -> Result<Self, serde_json::Error> {
        let data = serde_json::to_vec(value)?;
        Ok(Self::new(
            "json",
            Some("application/json".to_string()),
            data,
        ))
    }
}
