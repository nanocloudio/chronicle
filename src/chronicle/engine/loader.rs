//! Schema loading and file resolution.
//!
//! This module contains pure functions for loading and validating schemas,
//! resolving file paths, and parsing schema registry configurations.
//! All functions are synchronous and have no Tokio dependencies.

use super::ChronicleEngineError;
use crate::codec::{
    avro::{
        AvroCodec, FileSchemaSource, RegistryCredentials, RegistrySchemaConfig,
        RegistrySchemaSource, SchemaRegistryVersion, SchemaSource,
    },
    cbor::CborCodec,
    protobuf::{DescriptorSource, ProtobufCodec},
};
use crate::config::integration::ChroniclePhase;
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::path::{Path, PathBuf};
use std::sync::Arc;

// ============================================================================
// Serialize Phase Plan
// ============================================================================

#[derive(Clone)]
pub(crate) struct SerializePhasePlan {
    pub(crate) name: String,
    pub(crate) codec: SerializeCodec,
    pub(crate) input_template: Option<JsonValue>,
}

impl SerializePhasePlan {
    pub(crate) fn encode(
        &self,
        chronicle: &str,
        input: JsonValue,
    ) -> Result<JsonValue, ChronicleEngineError> {
        let payload = match &self.codec {
            SerializeCodec::Avro(codec) => codec.encode(chronicle, &self.name, input)?,
            SerializeCodec::Protobuf(codec) => codec.encode(chronicle, &self.name, input)?,
            SerializeCodec::Cbor(codec) => codec.encode(chronicle, &self.name, input)?,
        };
        Ok(payload.to_json())
    }
}

#[derive(Clone)]
pub(crate) enum SerializeCodec {
    Avro(AvroCodec),
    Protobuf(ProtobufCodec),
    Cbor(CborCodec),
}

// ============================================================================
// Schema Loading Functions
// ============================================================================

pub(crate) fn build_serialize_plan(
    chronicle: &str,
    phase: &ChroniclePhase,
    base_dir: &Path,
    raw_options: &JsonValue,
) -> Result<SerializePhasePlan, ChronicleEngineError> {
    let mut options =
        clone_object(raw_options).ok_or_else(|| ChronicleEngineError::InvalidPhaseOptions {
            chronicle: chronicle.to_string(),
            phase: phase.name.clone(),
            reason: "serialize options must be an object".to_string(),
        })?;

    let codec_raw = take_string(chronicle, &phase.name, &mut options, "codec")?;
    let codec_normalised = codec_raw.to_lowercase();

    let input_template = options.remove("input");
    let schema_value = options.remove("schema");
    let registry_value = options.remove("registry");
    let protobuf_value = options.remove("protobuf");

    let codec = match codec_normalised.as_str() {
        "avro" => {
            let schema = resolve_schema_source(
                chronicle,
                &phase.name,
                base_dir,
                schema_value,
                registry_value,
            )?;
            SerializeCodec::Avro(AvroCodec::new(schema))
        }
        "protobuf" => {
            if let Some(value) = registry_value {
                return Err(ChronicleEngineError::InvalidPhaseOptions {
                    chronicle: chronicle.to_string(),
                    phase: phase.name.clone(),
                    reason: format!(
                        "codec `protobuf` does not support registry configuration: {value:?}"
                    ),
                });
            }

            let descriptor_path = schema_value
                .map(|value| resolve_file_path(chronicle, &phase.name, base_dir, value))
                .transpose()?
                .ok_or_else(|| ChronicleEngineError::InvalidPhaseOptions {
                    chronicle: chronicle.to_string(),
                    phase: phase.name.clone(),
                    reason: "codec `protobuf` requires a schema path pointing to a descriptor set"
                        .to_string(),
                })?;

            let message = extract_protobuf_message(chronicle, &phase.name, protobuf_value)?;

            SerializeCodec::Protobuf(ProtobufCodec::new(
                Arc::new(DescriptorSource::new(descriptor_path)),
                message,
            ))
        }
        "cbor" => SerializeCodec::Cbor(CborCodec),
        other => {
            return Err(ChronicleEngineError::InvalidPhaseOptions {
                chronicle: chronicle.to_string(),
                phase: phase.name.clone(),
                reason: format!("unsupported serialize codec `{other}`"),
            });
        }
    };

    Ok(SerializePhasePlan {
        name: phase.name.clone(),
        codec,
        input_template,
    })
}

fn resolve_schema_source(
    chronicle: &str,
    phase: &str,
    base_dir: &Path,
    schema_value: Option<JsonValue>,
    registry_value: Option<JsonValue>,
) -> Result<SchemaSource, ChronicleEngineError> {
    let file_schema = if let Some(value) = schema_value {
        Some(parse_schema_file(chronicle, phase, base_dir, value)?)
    } else {
        None
    };

    if let Some(value) = registry_value {
        let registry =
            parse_schema_registry(chronicle, phase, value, file_schema.clone(), base_dir)?;
        return Ok(SchemaSource::Registry(registry));
    }

    if let Some(file) = file_schema {
        return Ok(SchemaSource::File(file));
    }

    Err(ChronicleEngineError::InvalidPhaseOptions {
        chronicle: chronicle.to_string(),
        phase: phase.to_string(),
        reason: "serialize phase requires either a schema file or schema registry configuration"
            .to_string(),
    })
}

pub(crate) fn parse_schema_file(
    chronicle: &str,
    phase: &str,
    base_dir: &Path,
    value: JsonValue,
) -> Result<Arc<FileSchemaSource>, ChronicleEngineError> {
    let resolved = resolve_file_path(chronicle, phase, base_dir, value)?;
    Ok(Arc::new(FileSchemaSource::new(resolved)))
}

pub(crate) fn resolve_file_path(
    chronicle: &str,
    phase: &str,
    base_dir: &Path,
    value: JsonValue,
) -> Result<PathBuf, ChronicleEngineError> {
    let (path_value, type_value) = match value {
        JsonValue::String(path) => (Some(path), None),
        JsonValue::Object(mut map) => {
            let type_value = map
                .remove("type")
                .and_then(|value| value.as_str().map(str::to_string));
            let path_value = map
                .remove("path")
                .and_then(|value| value.as_str().map(str::to_string));
            (path_value, type_value)
        }
        other => {
            return Err(ChronicleEngineError::InvalidPhaseOptions {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                reason: format!("schema configuration must be a string or object, found {other:?}"),
            });
        }
    };

    if let Some(kind) = type_value {
        if kind.to_lowercase() != "file" {
            return Err(ChronicleEngineError::InvalidPhaseOptions {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                reason: format!("unsupported schema type `{kind}`; expected `file`"),
            });
        }
    }

    let path_str = path_value.ok_or_else(|| ChronicleEngineError::InvalidPhaseOptions {
        chronicle: chronicle.to_string(),
        phase: phase.to_string(),
        reason: "schema configuration requires `path`".to_string(),
    })?;

    let path = PathBuf::from(&path_str);
    Ok(if path.is_absolute() {
        path
    } else {
        base_dir.join(path_str)
    })
}

fn parse_schema_registry(
    chronicle: &str,
    phase: &str,
    value: JsonValue,
    fallback: Option<Arc<FileSchemaSource>>,
    base_dir: &Path,
) -> Result<Arc<RegistrySchemaSource>, ChronicleEngineError> {
    let mut map =
        value
            .as_object()
            .cloned()
            .ok_or_else(|| ChronicleEngineError::InvalidPhaseOptions {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                reason: "registry configuration must be an object".to_string(),
            })?;

    let endpoint = map
        .remove("url")
        .and_then(|value| value.as_str().map(str::to_string))
        .ok_or_else(|| ChronicleEngineError::InvalidPhaseOptions {
            chronicle: chronicle.to_string(),
            phase: phase.to_string(),
            reason: "registry configuration requires `url`".to_string(),
        })?;

    let subject = map
        .remove("subject")
        .and_then(|value| value.as_str().map(str::to_string))
        .ok_or_else(|| ChronicleEngineError::InvalidPhaseOptions {
            chronicle: chronicle.to_string(),
            phase: phase.to_string(),
            reason: "registry configuration requires `subject`".to_string(),
        })?;

    let version = map
        .remove("version")
        .map(|value| parse_schema_registry_version(chronicle, phase, value))
        .transpose()?
        .unwrap_or(SchemaRegistryVersion::Latest);

    let credentials = match map.remove("credentials") {
        Some(value) => parse_registry_credentials(chronicle, phase, value)?,
        None => None,
    };

    if let Some(schema_value) = map.remove("schema") {
        let file_override = parse_schema_file(chronicle, phase, base_dir, schema_value)?;
        let effective_fallback = Some(file_override).or(fallback);
        return RegistrySchemaSource::new(
            chronicle,
            phase,
            RegistrySchemaConfig {
                endpoint,
                subject,
                version,
                credentials,
                fallback: effective_fallback,
            },
        )
        .map(Arc::new);
    }

    RegistrySchemaSource::new(
        chronicle,
        phase,
        RegistrySchemaConfig {
            endpoint,
            subject,
            version,
            credentials,
            fallback,
        },
    )
    .map(Arc::new)
}

fn extract_protobuf_message(
    chronicle: &str,
    phase: &str,
    value: Option<JsonValue>,
) -> Result<String, ChronicleEngineError> {
    let value = value.ok_or_else(|| ChronicleEngineError::InvalidPhaseOptions {
        chronicle: chronicle.to_string(),
        phase: phase.to_string(),
        reason: "codec `protobuf` requires a `protobuf` configuration block".to_string(),
    })?;

    let map = match value {
        JsonValue::Object(map) => map,
        other => {
            return Err(ChronicleEngineError::InvalidPhaseOptions {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                reason: format!("`protobuf` configuration must be an object, found {other:?}"),
            });
        }
    };

    map.get("message")
        .and_then(JsonValue::as_str)
        .map(|s| s.to_string())
        .ok_or_else(|| ChronicleEngineError::InvalidPhaseOptions {
            chronicle: chronicle.to_string(),
            phase: phase.to_string(),
            reason: "`protobuf.message` option is required".to_string(),
        })
}

fn parse_schema_registry_version(
    chronicle: &str,
    phase: &str,
    value: JsonValue,
) -> Result<SchemaRegistryVersion, ChronicleEngineError> {
    match value {
        JsonValue::String(text) => {
            if text.eq_ignore_ascii_case("latest") {
                Ok(SchemaRegistryVersion::Latest)
            } else {
                let parsed = text.parse::<u32>().map_err(|err| {
                    ChronicleEngineError::InvalidPhaseOptions {
                        chronicle: chronicle.to_string(),
                        phase: phase.to_string(),
                        reason: format!("invalid registry version `{text}`: {err}"),
                    }
                })?;
                Ok(SchemaRegistryVersion::Version(parsed))
            }
        }
        JsonValue::Number(num) => {
            if let Some(value) = num.as_u64() {
                let clamped = u32::try_from(value).map_err(|_| {
                    ChronicleEngineError::InvalidPhaseOptions {
                        chronicle: chronicle.to_string(),
                        phase: phase.to_string(),
                        reason: format!("registry version `{value}` exceeds u32 range"),
                    }
                })?;
                Ok(SchemaRegistryVersion::Version(clamped))
            } else {
                Err(ChronicleEngineError::InvalidPhaseOptions {
                    chronicle: chronicle.to_string(),
                    phase: phase.to_string(),
                    reason: format!("registry version must be positive integer, found {num}"),
                })
            }
        }
        other => Err(ChronicleEngineError::InvalidPhaseOptions {
            chronicle: chronicle.to_string(),
            phase: phase.to_string(),
            reason: format!("registry version must be string or number, found {other:?}"),
        }),
    }
}

fn parse_registry_credentials(
    chronicle: &str,
    phase: &str,
    value: JsonValue,
) -> Result<Option<RegistryCredentials>, ChronicleEngineError> {
    let map = match value {
        JsonValue::Null => return Ok(None),
        JsonValue::Object(map) => map,
        other => {
            return Err(ChronicleEngineError::InvalidPhaseOptions {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                reason: format!("registry credentials must be an object, found {other:?}"),
            });
        }
    };

    let username = map
        .get("username")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| ChronicleEngineError::InvalidPhaseOptions {
            chronicle: chronicle.to_string(),
            phase: phase.to_string(),
            reason: "registry credentials require `username`".to_string(),
        })?
        .to_string();

    let password = map
        .get("password")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| ChronicleEngineError::InvalidPhaseOptions {
            chronicle: chronicle.to_string(),
            phase: phase.to_string(),
            reason: "registry credentials require `password`".to_string(),
        })?
        .to_string();

    Ok(Some(RegistryCredentials { username, password }))
}

// ============================================================================
// Helper Functions (shared with planner)
// ============================================================================

pub(crate) fn clone_object(value: &JsonValue) -> Option<JsonMap<String, JsonValue>> {
    value.as_object().cloned()
}

pub(crate) fn take_string(
    chronicle: &str,
    phase: &str,
    map: &mut JsonMap<String, JsonValue>,
    key: &str,
) -> Result<String, ChronicleEngineError> {
    match map.remove(key) {
        Some(JsonValue::String(inner)) => Ok(inner),
        Some(other) => Err(ChronicleEngineError::InvalidPhaseOptions {
            chronicle: chronicle.to_string(),
            phase: phase.to_string(),
            reason: format!("expected `{key}` to be a string, got {other:?}"),
        }),
        None => Err(ChronicleEngineError::MissingOption {
            chronicle: chronicle.to_string(),
            phase: phase.to_string(),
            option: key.to_string(),
        }),
    }
}
