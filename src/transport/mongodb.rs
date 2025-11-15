#![forbid(unsafe_code)]

use crate::chronicle::engine::ChronicleEngine;
use crate::chronicle::retry_runner::{run_retry_loop, RetryContext};
use crate::chronicle::trigger_common::{map_from_btree, RetrySettings};
use crate::config::integration::{ChronicleDefinition, IntegrationConfig, ScalarValue};
use crate::error::Result;
use crate::integration::registry::{ConnectorRegistry, MongodbConnector};
use crate::readiness::DependencyHealth;
use crate::transport::{TaskTransportRuntime, TransportKind, TransportRuntime};
use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD as BASE64_ENGINE;
use base64::Engine;
use futures_util::StreamExt;
use mongodb::bson::Document;
use mongodb::change_stream::event::{
    ChangeNamespace, ChangeStreamEvent, OperationType, ResumeToken,
};
use mongodb::change_stream::ChangeStream;
use mongodb::options::{ChangeStreamOptions, ClientOptions, Tls, TlsOptions};
use mongodb::Client;
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use std::marker::PhantomData;
use std::result::Result as StdResult;
use std::{env, fs, future::Future, io::ErrorKind, path::PathBuf, sync::Arc, time::Duration};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

const CHANGE_STREAM_IDLE_DELAY_MS: u64 = 200;

pub struct MongodbTriggerRuntime<S>
where
    S: MongodbChangeStream + Send,
{
    inner: TaskTransportRuntime,
    listener_count: usize,
    _marker: PhantomData<S>,
}

impl<S> MongodbTriggerRuntime<S>
where
    S: MongodbChangeStream + Send,
{
    pub async fn build_with<F, Fut>(
        config: Arc<IntegrationConfig>,
        registry: Arc<ConnectorRegistry>,
        engine: Arc<ChronicleEngine>,
        dependency_health: Option<DependencyHealth>,
        mut factory: F,
    ) -> Result<Self, MongodbTriggerError>
    where
        F: FnMut(MongodbChangeStreamConfig) -> Fut,
        Fut: Future<Output = StdResult<S, MongodbChangeStreamError>>,
    {
        let mut watchers = Vec::new();

        for chronicle in &config.chronicles {
            let trigger_type = chronicle
                .trigger
                .option("type")
                .and_then(ScalarValue::as_str)
                .map(|value| value.to_ascii_lowercase());

            let connector_handle = registry.mongodb(&chronicle.trigger.connector);

            let is_mongodb = match trigger_type.as_deref() {
                Some("mongodb") => true,
                Some(_) => false,
                None => connector_handle.is_some(),
            };

            if !is_mongodb {
                continue;
            }

            let connector =
                connector_handle
                    .cloned()
                    .ok_or_else(|| MongodbTriggerError::MissingConnector {
                        chronicle: chronicle.name.clone(),
                        connector: chronicle.trigger.connector.clone(),
                    })?;

            let options = MongodbTriggerOptions::from_trigger(chronicle)?;

            let mut resume_store = ResumeStore::new(&chronicle.name, &connector.name);

            let resume_token =
                resume_store
                    .load()
                    .map_err(|reason| MongodbTriggerError::InvalidStoredResume {
                        chronicle: chronicle.name.clone(),
                        path: resume_store.path_string(),
                        reason,
                    })?;

            let config =
                MongodbChangeStreamConfig::new(connector.clone(), &options, resume_token.clone());

            let retry_policy =
                RetrySettings::from_extras(&options.extra, &map_from_btree(&connector.extra));

            let stream = factory(config.clone()).await.map_err(|err| {
                MongodbTriggerError::ChangeStreamBuild {
                    chronicle: chronicle.name.clone(),
                    reason: err.to_string(),
                }
            })?;

            watchers.push(MongodbTriggerInstance {
                chronicle: chronicle.name.clone(),
                connector: connector.name.clone(),
                target: options
                    .collection
                    .clone()
                    .unwrap_or_else(|| connector.database.clone()),
                stream,
                resume_store,
                retry: retry_policy,
                dependency_health: dependency_health.clone(),
                disconnected: false,
            });
        }

        let listener_count = watchers.len();
        Ok(Self {
            inner: TaskTransportRuntime::new(TransportKind::MongodbIn, "mongodb", {
                let engine_shared = Arc::clone(&engine);
                move |shutdown| {
                    watchers
                        .into_iter()
                        .map(|instance| {
                            let engine = Arc::clone(&engine_shared);
                            let shutdown = shutdown.clone();
                            tokio::spawn(async move { instance.run(engine, shutdown).await })
                        })
                        .collect()
                }
            }),
            listener_count,
            _marker: PhantomData,
        })
    }

    pub fn listener_count(&self) -> usize {
        self.listener_count
    }
}

#[async_trait]
impl<S> TransportRuntime for MongodbTriggerRuntime<S>
where
    S: MongodbChangeStream + Send,
{
    fn kind(&self) -> TransportKind {
        self.inner.kind()
    }

    fn name(&self) -> &'static str {
        self.inner.name()
    }

    fn health(&self) -> crate::transport::TransportHealth {
        self.inner.health()
    }

    async fn prepare(&mut self) -> Result<()> {
        self.inner.prepare().await
    }

    async fn start(&mut self, shutdown: CancellationToken) -> Result<()> {
        self.inner.start(shutdown).await
    }

    fn run(&mut self) -> crate::transport::TransportRun {
        self.inner.run()
    }

    async fn shutdown(&mut self) -> Result<()> {
        self.inner.shutdown().await
    }
}

struct MongodbTriggerInstance<S>
where
    S: MongodbChangeStream + Send,
{
    chronicle: String,
    connector: String,
    target: String,
    stream: S,
    resume_store: ResumeStore,
    retry: RetrySettings,
    dependency_health: Option<DependencyHealth>,
    disconnected: bool,
}

impl<S> MongodbTriggerInstance<S>
where
    S: MongodbChangeStream + Send,
{
    async fn run(self, engine: Arc<ChronicleEngine>, shutdown: CancellationToken) {
        let retry = self.retry.clone();
        let mut context = MongodbRetryContext {
            instance: self,
            engine,
        };

        run_retry_loop(
            shutdown,
            retry,
            Duration::from_millis(CHANGE_STREAM_IDLE_DELAY_MS),
            &mut context,
        )
        .await;
    }

    fn mark_connected(&mut self) {
        if self.disconnected {
            tracing::info!(
                target: "chronicle::mongodb",
                event = "transport_reconnected",
                chronicle = %self.chronicle,
                connector = %self.connector,
                target = %self.target
            );
            self.disconnected = false;
        }
    }

    fn mark_disconnected(&mut self, err: &MongodbChangeStreamError) {
        if !self.disconnected {
            tracing::warn!(
                target: "chronicle::mongodb",
                event = "transport_disconnected",
                chronicle = %self.chronicle,
                connector = %self.connector,
                target = %self.target,
                error = %err
            );
            self.disconnected = true;
        }
    }

    async fn handle_event(&mut self, engine: &Arc<ChronicleEngine>, event: MongodbChangeEvent) {
        self.mark_connected();
        let payload = build_change_payload(&event.event);

        tracing::info!(
            target: "chronicle::mongodb",
            event = "trigger_received",
            chronicle = %self.chronicle,
            connector = %self.connector,
            target = %self.target,
            operation = ?event.event.operation_type
        );

        match engine.execute(&self.chronicle, payload) {
            Ok(_) => {
                tracing::info!(
                    target: "chronicle::mongodb",
                    event = "trigger_completed",
                    chronicle = %self.chronicle,
                    connector = %self.connector,
                    target = %self.target,
                    operation = ?event.event.operation_type
                );

                if let Err(reason) = self.resume_store.persist(&event.event.id) {
                    tracing::error!(
                        target: "chronicle::mongodb",
                        event = "resume_token_persist_failed",
                        chronicle = %self.chronicle,
                        connector = %self.connector,
                        target = %self.target,
                        error = %reason
                    );
                }
            }
            Err(err) => {
                tracing::error!(
                    target: "chronicle::mongodb",
                    event = "trigger_failed",
                    chronicle = %self.chronicle,
                    connector = %self.connector,
                    target = %self.target,
                    error = %err
                );
            }
        }
    }
}

struct MongodbRetryContext<S>
where
    S: MongodbChangeStream + Send,
{
    instance: MongodbTriggerInstance<S>,
    engine: Arc<ChronicleEngine>,
}

#[async_trait]
impl<S> RetryContext for MongodbRetryContext<S>
where
    S: MongodbChangeStream + Send,
{
    type Item = MongodbChangeEvent;
    type Error = MongodbChangeStreamError;

    async fn poll(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        self.instance.stream.next_event().await
    }

    async fn handle_item(&mut self, item: Self::Item) {
        let dependency_health = self.instance.dependency_health.clone();
        let connector = self.instance.connector.clone();
        if let Some(health) = dependency_health {
            health.report_outcome(&connector, true).await;
        }
        self.instance.mark_connected();
        self.instance.handle_event(&self.engine, item).await;
    }

    async fn report_error(&mut self, error: &Self::Error, _delay: Duration) {
        let dependency_health = self.instance.dependency_health.clone();
        let connector = self.instance.connector.clone();
        if let Some(health) = dependency_health {
            health.report_outcome(&connector, false).await;
        }
        self.instance.mark_disconnected(error);
        tracing::error!(
            target: "chronicle::mongodb",
            event = "change_stream_error",
            chronicle = %self.instance.chronicle,
            connector = %self.instance.connector,
            target = %self.instance.target,
            error = %error
        );

        if let Err(reconnect_err) = self.instance.stream.reconnect().await {
            tracing::error!(
                target: "chronicle::mongodb",
                event = "change_stream_reconnect_failed",
                chronicle = %self.instance.chronicle,
                connector = %self.instance.connector,
                target = %self.instance.target,
                error = %reconnect_err
            );
        } else {
            self.instance.mark_connected();
        }
    }
}

#[async_trait]
pub trait MongodbChangeStream: Send + 'static {
    async fn next_event(
        &mut self,
    ) -> StdResult<Option<MongodbChangeEvent>, MongodbChangeStreamError>;
    async fn reconnect(&mut self) -> StdResult<(), MongodbChangeStreamError>;
}

pub struct MongodbChangeStreamDriver {
    config: MongodbChangeStreamConfig,
    client: Client,
    stream: Option<ChangeStream<ChangeStreamEvent<Document>>>,
    resume_from: Option<ResumeToken>,
}

impl MongodbChangeStreamDriver {
    pub async fn connect(
        config: MongodbChangeStreamConfig,
    ) -> StdResult<Self, MongodbChangeStreamError> {
        let client = build_client_for_connector(&config.connector).await?;

        Ok(Self {
            stream: None,
            resume_from: config.resume_token.clone(),
            config,
            client,
        })
    }

    async fn ensure_stream(&mut self) -> StdResult<(), MongodbChangeStreamError> {
        if self.stream.is_some() {
            return Ok(());
        }

        let database = self.client.database(&self.config.connector.database);
        let pipeline = self.config.pipeline.clone();

        let mut options = ChangeStreamOptions::default();
        if let Some(token) = self.resume_from.clone() {
            options.resume_after = Some(token);
        }

        let stream = if let Some(collection) = &self.config.collection {
            let collection_handle = database.collection::<Document>(collection);
            let collection_name = collection_handle.name().to_string();
            collection_handle
                .watch(pipeline.clone(), Some(options.clone()))
                .await
                .map_err(|err| {
                    MongodbChangeStreamError::new(format!(
                        "failed to open change stream on collection `{collection_name}`: {err}"
                    ))
                })?
        } else {
            database
                .watch(pipeline.clone(), Some(options.clone()))
                .await
                .map_err(|err| {
                    MongodbChangeStreamError::new(format!(
                        "failed to open database change stream: {err}"
                    ))
                })?
        };

        self.stream = Some(stream);
        Ok(())
    }
}

#[async_trait]
impl MongodbChangeStream for MongodbChangeStreamDriver {
    async fn next_event(
        &mut self,
    ) -> StdResult<Option<MongodbChangeEvent>, MongodbChangeStreamError> {
        self.ensure_stream().await?;

        if let Some(stream) = self.stream.as_mut() {
            match stream.next().await {
                Some(Ok(event)) => {
                    self.resume_from = Some(event.id.clone());
                    Ok(Some(MongodbChangeEvent { event }))
                }
                Some(Err(err)) => {
                    self.stream = None;
                    Err(MongodbChangeStreamError::new(err.to_string()))
                }
                None => {
                    self.stream = None;
                    Ok(None)
                }
            }
        } else {
            Ok(None)
        }
    }

    async fn reconnect(&mut self) -> StdResult<(), MongodbChangeStreamError> {
        self.stream = None;
        self.ensure_stream().await
    }
}

#[derive(Clone)]
pub struct MongodbChangeStreamConfig {
    connector: MongodbConnector,
    collection: Option<String>,
    pipeline: Vec<Document>,
    resume_token: Option<ResumeToken>,
}

impl MongodbChangeStreamConfig {
    fn new(
        connector: MongodbConnector,
        options: &MongodbTriggerOptions,
        resume_token: Option<ResumeToken>,
    ) -> Self {
        Self {
            connector,
            collection: options.collection.clone(),
            pipeline: options.pipeline.clone(),
            resume_token,
        }
    }
}

#[derive(Clone)]
struct MongodbTriggerOptions {
    collection: Option<String>,
    pipeline: Vec<Document>,
    extra: JsonMap<String, JsonValue>,
}

impl MongodbTriggerOptions {
    fn from_trigger(chronicle: &ChronicleDefinition) -> Result<Self, MongodbTriggerError> {
        let raw = chronicle.trigger.options_json();
        let mut options =
            raw.as_object()
                .cloned()
                .ok_or_else(|| MongodbTriggerError::InvalidTriggerOptions {
                    chronicle: chronicle.name.clone(),
                })?;

        let collection = options
            .remove("collection")
            .and_then(|value| value.as_str().map(|v| v.to_string()));

        let pipeline = parse_pipeline(chronicle, options.remove("pipeline"))?;

        Ok(Self {
            collection,
            pipeline,
            extra: options,
        })
    }
}

pub struct MongodbChangeEvent {
    pub event: ChangeStreamEvent<Document>,
}

async fn build_client_for_connector(
    connector: &MongodbConnector,
) -> StdResult<Client, MongodbChangeStreamError> {
    let mut options = ClientOptions::parse(&connector.uri).await.map_err(|err| {
        MongodbChangeStreamError::new(format!(
            "failed to parse mongodb uri `{}`: {err}",
            connector.uri
        ))
    })?;

    if let Some(tls) = &connector.tls {
        let mut tls_options = TlsOptions::default();
        if let Some(ca) = &tls.ca {
            tls_options.ca_file_path = Some(ca.clone());
        }
        if let Some(cert) = &tls.cert {
            tls_options.cert_key_file_path = Some(cert.clone());
        }
        options.tls = Some(Tls::Enabled(tls_options));
    }

    Client::with_options(options).map_err(|err| {
        MongodbChangeStreamError::new(format!("failed to construct mongodb client: {err}"))
    })
}

fn parse_pipeline(
    chronicle: &ChronicleDefinition,
    value: Option<JsonValue>,
) -> Result<Vec<Document>, MongodbTriggerError> {
    let Some(raw) = value else {
        return Ok(Vec::new());
    };

    match raw {
        JsonValue::Array(stages) => {
            let mut docs = Vec::with_capacity(stages.len());
            for stage in stages {
                let doc = mongodb::bson::to_document(&stage).map_err(|err| {
                    MongodbTriggerError::InvalidPipelineStage {
                        chronicle: chronicle.name.clone(),
                        reason: err.to_string(),
                    }
                })?;
                docs.push(doc);
            }
            Ok(docs)
        }
        JsonValue::Null => Ok(Vec::new()),
        other => Err(MongodbTriggerError::InvalidPipelineType {
            chronicle: chronicle.name.clone(),
            value: other.to_string(),
        }),
    }
}

fn build_change_payload(event: &ChangeStreamEvent<Document>) -> JsonValue {
    let mut root = JsonMap::new();

    root.insert(
        "operationType".to_string(),
        JsonValue::String(operation_type_name(&event.operation_type)),
    );

    root.insert(
        "ns".to_string(),
        event
            .ns
            .as_ref()
            .map(namespace_to_json)
            .unwrap_or(JsonValue::Null),
    );

    root.insert(
        "documentKey".to_string(),
        event
            .document_key
            .as_ref()
            .map(document_to_json)
            .unwrap_or(JsonValue::Null),
    );

    root.insert(
        "fullDocument".to_string(),
        event
            .full_document
            .as_ref()
            .map(document_to_json)
            .unwrap_or(JsonValue::Null),
    );

    if let Some(before) = event.full_document_before_change.as_ref() {
        root.insert(
            "fullDocumentBeforeChange".to_string(),
            document_to_json(before),
        );
    }

    if let Some(update) = event.update_description.as_ref() {
        root.insert("updateDescription".to_string(), value_to_json(update));
    }

    let mut metadata = JsonMap::new();

    if let Some(cluster) = event.cluster_time {
        let mut cluster_map = JsonMap::new();
        cluster_map.insert(
            "seconds".to_string(),
            JsonValue::Number(JsonNumber::from(cluster.time as u64)),
        );
        cluster_map.insert(
            "increment".to_string(),
            JsonValue::Number(JsonNumber::from(cluster.increment as u64)),
        );
        metadata.insert("clusterTime".to_string(), JsonValue::Object(cluster_map));
    }

    if let Some(wall) = event.wall_time {
        if let Ok(formatted) = wall.try_to_rfc3339_string() {
            metadata.insert("wallTime".to_string(), JsonValue::String(formatted));
        }
    }

    if let Some(token) = encode_resume_token(&event.id) {
        metadata.insert("resumeToken".to_string(), JsonValue::String(token));
    }

    if !metadata.is_empty() {
        root.insert("metadata".to_string(), JsonValue::Object(metadata));
    }

    JsonValue::Object(root)
}

fn namespace_to_json(ns: &ChangeNamespace) -> JsonValue {
    let mut map = JsonMap::new();
    map.insert("db".to_string(), JsonValue::String(ns.db.clone()));
    match &ns.coll {
        Some(coll) => map.insert("coll".to_string(), JsonValue::String(coll.clone())),
        None => map.insert("coll".to_string(), JsonValue::Null),
    };
    JsonValue::Object(map)
}

fn document_to_json(doc: &Document) -> JsonValue {
    serde_json::to_value(doc).unwrap_or(JsonValue::Null)
}

fn value_to_json<T>(value: &T) -> JsonValue
where
    T: serde::Serialize,
{
    serde_json::to_value(value).unwrap_or(JsonValue::Null)
}

fn encode_resume_token(token: &ResumeToken) -> Option<String> {
    mongodb::bson::to_vec(token)
        .ok()
        .map(|bytes| BASE64_ENGINE.encode(bytes))
}

fn operation_type_name(op: &OperationType) -> String {
    match op {
        OperationType::Insert => "insert".to_string(),
        OperationType::Update => "update".to_string(),
        OperationType::Replace => "replace".to_string(),
        OperationType::Delete => "delete".to_string(),
        OperationType::Drop => "drop".to_string(),
        OperationType::Invalidate => "invalidate".to_string(),
        OperationType::DropDatabase => "dropDatabase".to_string(),
        OperationType::Rename => "rename".to_string(),
        OperationType::Other(value) => value.clone(),
        _ => "other".to_string(),
    }
}

#[derive(Debug, Error)]
pub enum MongodbTriggerError {
    #[error("chronicle `{chronicle}` mongodb trigger requires connector `{connector}`")]
    MissingConnector {
        chronicle: String,
        connector: String,
    },
    #[error("chronicle `{chronicle}` mongodb trigger options must be an object")]
    InvalidTriggerOptions { chronicle: String },
    #[error("chronicle `{chronicle}` mongodb trigger pipeline must be an array (got {value})")]
    InvalidPipelineType { chronicle: String, value: String },
    #[error("chronicle `{chronicle}` mongodb trigger pipeline stage invalid: {reason}")]
    InvalidPipelineStage { chronicle: String, reason: String },
    #[error("failed to build mongodb change stream for chronicle `{chronicle}`: {reason}")]
    ChangeStreamBuild { chronicle: String, reason: String },
    #[error(
        "chronicle `{chronicle}` mongodb trigger resume token at `{path}` is invalid: {reason}"
    )]
    InvalidStoredResume {
        chronicle: String,
        path: String,
        reason: String,
    },
}

#[derive(Clone, Debug)]
pub struct MongodbChangeStreamError {
    message: String,
}

impl MongodbChangeStreamError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for MongodbChangeStreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for MongodbChangeStreamError {}

struct ResumeStore {
    path: PathBuf,
    last_persisted: Option<ResumeToken>,
}

impl ResumeStore {
    fn new(chronicle: &str, connector: &str) -> Self {
        let file_name = format!(
            "{}-{}.resume",
            safe_filename(chronicle),
            safe_filename(connector)
        );
        let path = resume_store_root().join("mongodb").join(file_name);
        Self {
            path,
            last_persisted: None,
        }
    }

    fn load(&mut self) -> Result<Option<ResumeToken>, String> {
        match fs::read_to_string(&self.path) {
            Ok(contents) => {
                let trimmed = contents.trim();
                if trimmed.is_empty() {
                    self.last_persisted = None;
                    return Ok(None);
                }
                let bytes = BASE64_ENGINE
                    .decode(trimmed)
                    .map_err(|err| format!("failed to decode resume token base64: {err}"))?;
                let token: ResumeToken = mongodb::bson::from_slice(&bytes)
                    .map_err(|err| format!("failed to parse resume token: {err}"))?;
                self.last_persisted = Some(token.clone());
                Ok(Some(token))
            }
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err.to_string()),
        }
    }

    fn persist(&mut self, token: &ResumeToken) -> Result<(), String> {
        if self.last_persisted.as_ref() == Some(token) {
            return Ok(());
        }

        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|err| err.to_string())?;
        }

        let bytes =
            mongodb::bson::to_vec(token).map_err(|err| format!("failed to encode token: {err}"))?;
        let encoded = BASE64_ENGINE.encode(bytes);
        fs::write(&self.path, encoded.as_bytes()).map_err(|err| err.to_string())?;
        self.last_persisted = Some(token.clone());
        Ok(())
    }

    fn path_string(&self) -> String {
        self.path.to_string_lossy().into_owned()
    }
}

fn resume_store_root() -> PathBuf {
    if let Ok(dir) = env::var("CHRONICLE_STATE_DIR") {
        PathBuf::from(dir)
    } else {
        PathBuf::from(".chronicle").join("state")
    }
}

fn safe_filename(input: &str) -> String {
    input
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}
