#![forbid(unsafe_code)]

use crate::chronicle::engine::ChronicleEngineError;
use crate::codec::payload::EncodedPayload;
use serde_json::{Map as JsonMap, Value as JsonValue};

#[derive(Clone, Copy, Default)]
pub struct CborCodec;

impl CborCodec {
    pub fn encode(
        &self,
        chronicle: &str,
        phase: &str,
        input: JsonValue,
    ) -> Result<EncodedPayload, ChronicleEngineError> {
        let bytes =
            serde_cbor::to_vec(&input).map_err(|err| ChronicleEngineError::Serialization {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                reason: format!("failed to encode CBOR payload: {err}"),
            })?;

        let mut metadata = JsonMap::new();
        metadata.insert(
            "wire_format".to_string(),
            JsonValue::String("cbor-binary".to_string()),
        );

        Ok(EncodedPayload::with_metadata(
            "cbor",
            Some("application/cbor".to_string()),
            bytes,
            metadata,
        ))
    }
}
