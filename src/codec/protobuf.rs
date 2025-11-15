#![forbid(unsafe_code)]

use crate::chronicle::engine::ChronicleEngineError;
use crate::codec::payload::EncodedPayload;
use prost_reflect::{prost::Message, DescriptorPool, DynamicMessage, MessageDescriptor};
use serde_json::{Deserializer as JsonDeserializer, Map as JsonMap, Value as JsonValue};
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct ProtobufCodec {
    descriptor: Arc<DescriptorSource>,
    message: String,
}

impl ProtobufCodec {
    pub fn new(descriptor: Arc<DescriptorSource>, message: String) -> Self {
        Self {
            descriptor,
            message,
        }
    }

    pub fn encode(
        &self,
        chronicle: &str,
        phase: &str,
        input: JsonValue,
    ) -> Result<EncodedPayload, ChronicleEngineError> {
        let descriptor = self.descriptor.load(&self.message).map_err(|reason| {
            ChronicleEngineError::SchemaResolution {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                reason,
            }
        })?;

        let json_string =
            serde_json::to_string(&input).map_err(|err| ChronicleEngineError::Serialization {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                reason: format!("failed to serialise input as JSON: {err}"),
            })?;

        let mut deserializer = JsonDeserializer::from_str(&json_string);
        let message = DynamicMessage::deserialize(descriptor.message.clone(), &mut deserializer)
            .map_err(|err| ChronicleEngineError::Serialization {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                reason: format!("failed to decode protobuf JSON: {err}"),
            })?;
        deserializer
            .end()
            .map_err(|err| ChronicleEngineError::Serialization {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                reason: format!("unexpected trailing JSON when decoding protobuf: {err}"),
            })?;

        let bytes = message.encode_to_vec();

        let mut metadata = JsonMap::new();
        metadata.insert(
            "wire_format".to_string(),
            JsonValue::String("protobuf-binary".to_string()),
        );
        metadata.insert(
            "message".to_string(),
            JsonValue::String(descriptor.message.full_name().to_string()),
        );
        metadata.insert(
            "descriptor_path".to_string(),
            JsonValue::String(descriptor.path.to_string_lossy().into_owned()),
        );

        Ok(EncodedPayload::with_metadata(
            "protobuf",
            Some("application/x-protobuf".to_string()),
            bytes,
            metadata,
        ))
    }
}

pub struct DescriptorSource {
    path: PathBuf,
    cached: Mutex<Option<Arc<DescriptorPool>>>,
}

impl DescriptorSource {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            cached: Mutex::new(None),
        }
    }

    pub fn load(&self, message: &str) -> Result<DescriptorData, String> {
        let pool = {
            let mut guard = self.cached.lock().expect("descriptor cache lock");
            if let Some(pool) = guard.as_ref() {
                pool.clone()
            } else {
                let bytes = fs::read(&self.path).map_err(|err| {
                    format!(
                        "failed to read descriptor file `{}`: {err}",
                        self.path.display()
                    )
                })?;

                let pool = DescriptorPool::decode(bytes.as_slice()).map_err(|err| {
                    format!(
                        "failed to decode descriptor file `{}`: {err}",
                        self.path.display()
                    )
                })?;

                let pool = Arc::new(pool);
                *guard = Some(pool.clone());
                pool
            }
        };

        let message_descriptor = pool.get_message_by_name(message).ok_or_else(|| {
            format!(
                "message `{message}` not present in descriptor `{}`",
                self.path.display()
            )
        })?;

        Ok(DescriptorData {
            path: self.path.clone(),
            message: message_descriptor,
        })
    }
}

#[derive(Clone)]
pub struct DescriptorData {
    pub path: PathBuf,
    pub message: MessageDescriptor,
}
