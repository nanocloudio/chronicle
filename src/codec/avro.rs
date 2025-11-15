#![forbid(unsafe_code)]

use crate::chronicle::engine::ChronicleEngineError;
use crate::codec::payload::EncodedPayload;
use apache_avro::{to_value, Schema, Writer};
use reqwest::Client;
use serde::Deserialize;
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::runtime::{Handle, Runtime};
use tokio::task::block_in_place;
use tracing::warn;

#[derive(Clone)]
pub struct AvroCodec {
    schema: SchemaSource,
}

impl AvroCodec {
    pub fn new(schema: SchemaSource) -> Self {
        Self { schema }
    }

    pub fn schema(&self) -> &SchemaSource {
        &self.schema
    }

    pub fn encode(
        &self,
        chronicle: &str,
        phase: &str,
        input: JsonValue,
    ) -> Result<EncodedPayload, ChronicleEngineError> {
        let schema_info = self.schema.load(chronicle, phase)?;

        let avro_value = to_value(&input).map_err(|err| ChronicleEngineError::Serialization {
            chronicle: chronicle.to_string(),
            phase: phase.to_string(),
            reason: format!("failed to convert input to Avro value: {err}"),
        })?;

        let resolved_value = avro_value.resolve(&schema_info.schema).map_err(|err| {
            ChronicleEngineError::Serialization {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                reason: format!("failed to resolve value against schema: {err}"),
            }
        })?;

        let mut writer = Writer::new(&schema_info.schema, Vec::new());
        writer
            .append(resolved_value)
            .map_err(|err| ChronicleEngineError::Serialization {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                reason: format!("failed to append Avro record: {err}"),
            })?;
        writer
            .flush()
            .map_err(|err| ChronicleEngineError::Serialization {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                reason: format!("failed to flush Avro writer: {err}"),
            })?;

        let mut bytes = writer
            .into_inner()
            .map_err(|err| ChronicleEngineError::Serialization {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                reason: format!("failed to retrieve Avro payload: {err}"),
            })?;

        let mut wire_format = "avro-binary".to_string();

        if let Some(schema_id) = schema_info.schema_id {
            let mut confluent = Vec::with_capacity(bytes.len() + 5);
            confluent.push(0);
            confluent.extend_from_slice(&(schema_id as u32).to_be_bytes());
            confluent.extend_from_slice(&bytes);
            bytes = confluent;
            wire_format = "confluent-wire".to_string();
        }

        let mut metadata = JsonMap::new();
        metadata.insert("wire_format".to_string(), JsonValue::String(wire_format));
        if let Some(schema_id) = schema_info.schema_id {
            metadata.insert(
                "schema_id".to_string(),
                JsonValue::Number(JsonNumber::from(schema_id)),
            );
        }
        if let Some(version) = schema_info.version {
            metadata.insert(
                "schema_version".to_string(),
                JsonValue::Number(JsonNumber::from(version)),
            );
        }
        match &schema_info.origin {
            SchemaOrigin::File(path) => {
                metadata.insert(
                    "schema_path".to_string(),
                    JsonValue::String(path.to_string_lossy().into_owned()),
                );
            }
            SchemaOrigin::Registry { subject } => {
                metadata.insert(
                    "schema_subject".to_string(),
                    JsonValue::String(subject.clone()),
                );
            }
        }

        Ok(EncodedPayload::with_metadata(
            "avro",
            Some("application/avro".to_string()),
            bytes,
            metadata,
        ))
    }
}

#[derive(Clone)]
pub enum SchemaSource {
    File(Arc<FileSchemaSource>),
    Registry(Arc<RegistrySchemaSource>),
}

impl SchemaSource {
    pub fn load(&self, chronicle: &str, phase: &str) -> Result<SchemaInfo, ChronicleEngineError> {
        match self {
            SchemaSource::File(source) => source.load(chronicle, phase),
            SchemaSource::Registry(source) => source.load(chronicle, phase),
        }
    }
}

pub struct FileSchemaSource {
    path: PathBuf,
    cached: Mutex<Option<Schema>>,
}

impl FileSchemaSource {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            cached: Mutex::new(None),
        }
    }

    pub fn load(&self, chronicle: &str, phase: &str) -> Result<SchemaInfo, ChronicleEngineError> {
        {
            let cached = self.cached.lock().expect("schema cache lock");
            if let Some(schema) = cached.as_ref() {
                return Ok(SchemaInfo {
                    schema: schema.clone(),
                    schema_id: None,
                    version: None,
                    origin: SchemaOrigin::File(self.path.clone()),
                });
            }
        }

        let schema_text = fs::read_to_string(&self.path).map_err(|err| {
            ChronicleEngineError::SchemaResolution {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                reason: format!(
                    "failed to read schema file `{}`: {err}",
                    self.path.display()
                ),
            }
        })?;

        let schema = Schema::parse_str(&schema_text).map_err(|err| {
            ChronicleEngineError::SchemaResolution {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                reason: format!(
                    "failed to parse schema file `{}`: {err}",
                    self.path.display()
                ),
            }
        })?;

        {
            let mut cached = self.cached.lock().expect("schema cache lock");
            *cached = Some(schema.clone());
        }

        Ok(SchemaInfo {
            schema,
            schema_id: None,
            version: None,
            origin: SchemaOrigin::File(self.path.clone()),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

pub struct RegistrySchemaSource {
    endpoint: String,
    subject: String,
    version: SchemaRegistryVersion,
    credentials: Option<RegistryCredentials>,
    client: Client,
    cache: Mutex<Option<RegistryCachedSchema>>,
    fallback: Option<Arc<FileSchemaSource>>,
}

impl RegistrySchemaSource {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        chronicle: &str,
        phase: &str,
        endpoint: String,
        subject: String,
        version: SchemaRegistryVersion,
        credentials: Option<RegistryCredentials>,
        fallback: Option<Arc<FileSchemaSource>>,
    ) -> Result<Self, ChronicleEngineError> {
        let client =
            Client::builder()
                .build()
                .map_err(|err| ChronicleEngineError::InvalidPhaseOptions {
                    chronicle: chronicle.to_string(),
                    phase: phase.to_string(),
                    reason: format!("failed to build schema registry client: {err}"),
                })?;

        Ok(Self {
            endpoint,
            subject,
            version,
            credentials,
            client,
            cache: Mutex::new(None),
            fallback,
        })
    }

    pub fn load(&self, chronicle: &str, phase: &str) -> Result<SchemaInfo, ChronicleEngineError> {
        if let Some(cached) = self.cache.lock().expect("registry cache").clone() {
            return Ok(SchemaInfo {
                schema: cached.schema.clone(),
                schema_id: Some(cached.schema_id),
                version: Some(cached.version),
                origin: SchemaOrigin::Registry {
                    subject: self.subject.clone(),
                },
            });
        }

        match self.fetch_schema() {
            Ok(fetched) => {
                let info = SchemaInfo {
                    schema: fetched.schema.clone(),
                    schema_id: Some(fetched.schema_id),
                    version: Some(fetched.version),
                    origin: SchemaOrigin::Registry {
                        subject: self.subject.clone(),
                    },
                };

                let mut cache = self.cache.lock().expect("registry cache");
                *cache = Some(fetched);
                Ok(info)
            }
            Err(reason) => {
                if let Some(fallback) = &self.fallback {
                    warn!(
                        target: "chronicle::codec::avro",
                        event = "schema_registry_fallback",
                        chronicle = %chronicle,
                        phase = %phase,
                        subject = %self.subject,
                        reason = %reason
                    );
                    fallback.load(chronicle, phase)
                } else {
                    Err(ChronicleEngineError::SchemaResolution {
                        chronicle: chronicle.to_string(),
                        phase: phase.to_string(),
                        reason,
                    })
                }
            }
        }
    }

    fn fetch_schema(&self) -> Result<RegistryCachedSchema, String> {
        match Handle::try_current() {
            Ok(handle) => block_in_place(|| handle.block_on(self.fetch_schema_async())),
            Err(_) => {
                let runtime = Runtime::new()
                    .map_err(|err| format!("failed to create runtime for schema fetch: {err}"))?;
                runtime.block_on(self.fetch_schema_async())
            }
        }
    }

    async fn fetch_schema_async(&self) -> Result<RegistryCachedSchema, String> {
        let endpoint = self.endpoint.trim_end_matches('/');
        let path_segment = self.version.path_segment();
        let subject_encoded = urlencoding::encode(&self.subject);
        let url = format!(
            "{endpoint}/subjects/{subject}/versions/{version}",
            subject = subject_encoded,
            version = path_segment
        );

        let mut request = self.client.get(&url);
        if let Some(credentials) = &self.credentials {
            request = request.basic_auth(&credentials.username, Some(&credentials.password));
        }

        let response = request.send().await.map_err(|err| {
            let hint = "Ensure the schema registry is running and reachable, \
                        or remove the `registry` section from the serialize phase configuration.";
            if err.is_connect() {
                format!("unable to reach schema registry at `{url}`: {err}. {hint}")
            } else if err.is_timeout() {
                format!("schema registry request to `{url}` timed out: {err}. {hint}")
            } else {
                format!("failed to fetch schema from `{url}`: {err}. {hint}")
            }
        })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<unavailable>".to_string());
            return Err(format!(
                "schema registry at `{url}` responded with status {status}: {body}. \
                 Ensure the registry credentials and endpoint are correct, \
                 or remove the `registry` block from the configuration."
            ));
        }

        let registry_response: SchemaRegistryResponse = response.json().await.map_err(|err| {
            format!("failed to decode schema registry response from `{url}`: {err}")
        })?;

        let schema = Schema::parse_str(&registry_response.schema).map_err(|err| {
            format!("schema registry returned invalid Avro schema for `{url}`: {err}")
        })?;

        Ok(RegistryCachedSchema {
            schema,
            schema_id: registry_response.id,
            version: registry_response.version,
        })
    }
}

#[derive(Clone)]
pub enum SchemaOrigin {
    File(PathBuf),
    Registry { subject: String },
}

#[derive(Clone)]
pub enum SchemaRegistryVersion {
    Latest,
    Version(u32),
}

impl SchemaRegistryVersion {
    pub fn path_segment(&self) -> String {
        match self {
            SchemaRegistryVersion::Latest => "latest".to_string(),
            SchemaRegistryVersion::Version(v) => v.to_string(),
        }
    }
}

#[derive(Clone)]
pub struct RegistryCredentials {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Deserialize)]
struct SchemaRegistryResponse {
    id: i32,
    schema: String,
    version: i32,
}

#[derive(Clone)]
struct RegistryCachedSchema {
    schema: Schema,
    schema_id: i32,
    version: i32,
}

pub struct SchemaInfo {
    pub schema: Schema,
    pub schema_id: Option<i32>,
    pub version: Option<i32>,
    pub origin: SchemaOrigin,
}
