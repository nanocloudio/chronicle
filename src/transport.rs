//! Transport layer: trigger runtimes and inbound message handling.
//!
//! This module contains the **trigger** (inbound) implementations that receive
//! events from external sources and initiate chronicle execution.
//!
//! # Architecture Boundaries
//!
//! Chronicle uses a clear separation between inbound and outbound data flows:
//!
//! - **Triggers** (inbound, this module): Components that receive events from
//!   external sources and initiate chronicle execution. Examples:
//!   - [`kafka::KafkaTriggerRuntime`] - Consumes from Kafka topics
//!   - [`rabbitmq::RabbitmqTriggerRuntime`] - Consumes from RabbitMQ queues
//!   - [`http_server::HttpTriggerRoute`] - Handles HTTP requests
//!   - [`mqtt::MqttTriggerRuntime`] - Subscribes to MQTT topics
//!   - [`redis::RedisTriggerRuntime`] - Reads from Redis streams
//!   - [`mongodb::MongodbTriggerRuntime`] - Tails MongoDB change streams
//!
//! - **Connectors** (outbound): Configuration wrappers and client factories for
//!   sending data to external systems. See [`integration::registry`] for connector
//!   handles like `KafkaConnector`, `PostgresConnector`, `HttpClientConnector`.
//!
//! - **Phases** (processing): Transform and route data between triggers and
//!   connectors. See [`chronicle::phase`] for phase handlers.
//!
//! # Shared Utilities
//!
//! - [`runtime::sleep_with_shutdown`] - Shutdown-aware async sleep
//! - [`kafka_util`] - Kafka-specific helpers (commit logging, backoff)
//! - [`readiness_gate`] - Route-level readiness gating
//!
//! [`integration::registry`]: crate::integration::registry
//! [`chronicle::phase`]: crate::chronicle::phase

use crate::error::Result;
use async_trait::async_trait;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub mod broker;
#[cfg(feature = "http-in")]
pub mod health;
#[cfg(feature = "http-in")]
pub mod http_server;
#[cfg(feature = "kafka")]
pub mod kafka;
#[cfg(feature = "kafka")]
pub mod kafka_context;
#[cfg(feature = "kafka")]
pub mod kafka_util;
#[cfg(feature = "db-mongodb")]
pub mod mongodb;
#[cfg(feature = "mqtt")]
pub mod mqtt;
#[cfg(feature = "rabbitmq")]
pub mod rabbitmq;
pub mod readiness_gate;
#[cfg(feature = "db-redis")]
pub mod redis;
pub mod runtime;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TransportKind {
    HttpIn,
    KafkaIn,
    RabbitmqIn,
    MongodbIn,
    RedisIn,
    MqttIn,
}

impl Display for TransportKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TransportKind::HttpIn => f.write_str("http-in"),
            TransportKind::KafkaIn => f.write_str("kafka-in"),
            TransportKind::RabbitmqIn => f.write_str("rabbitmq-in"),
            TransportKind::MongodbIn => f.write_str("mongodb-in"),
            TransportKind::RedisIn => f.write_str("redis-in"),
            TransportKind::MqttIn => f.write_str("mqtt-in"),
        }
    }
}

#[derive(Clone, Debug)]
pub enum TransportHealth {
    Idle,
    Starting,
    Running,
    Degraded { reason: String },
    Shutdown,
}

pub struct TransportRun {
    kind: TransportKind,
    name: &'static str,
    future: Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>,
}

impl TransportRun {
    pub fn new<F>(kind: TransportKind, name: &'static str, future: F) -> Self
    where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        Self {
            kind,
            name,
            future: Box::pin(future),
        }
    }

    pub fn kind(&self) -> TransportKind {
        self.kind
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub async fn wait(self) -> Result<()> {
        self.future.await
    }
}

#[async_trait]
pub trait TransportRuntime: Send {
    fn kind(&self) -> TransportKind;
    fn name(&self) -> &'static str;
    fn health(&self) -> TransportHealth;
    async fn prepare(&mut self) -> Result<()>;
    async fn start(&mut self, shutdown: CancellationToken) -> Result<()>;
    fn run(&mut self) -> TransportRun;
    async fn shutdown(&mut self) -> Result<()>;
}

pub type DynTransportRuntime = Box<dyn TransportRuntime>;

type RuntimeSpawner = Box<dyn FnOnce(CancellationToken) -> Vec<JoinHandle<()>> + Send>;

pub struct TaskTransportRuntime {
    kind: TransportKind,
    name: &'static str,
    spawner: Option<RuntimeSpawner>,
    tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
    health: Arc<Mutex<TransportHealth>>,
    shutdown: Option<CancellationToken>,
    run_registered: bool,
}

impl TaskTransportRuntime {
    pub fn new<F>(kind: TransportKind, name: &'static str, spawner: F) -> Self
    where
        F: FnOnce(CancellationToken) -> Vec<JoinHandle<()>> + Send + 'static,
    {
        Self {
            kind,
            name,
            spawner: Some(Box::new(spawner)),
            tasks: Arc::new(Mutex::new(Vec::new())),
            health: Arc::new(Mutex::new(TransportHealth::Idle)),
            shutdown: None,
            run_registered: false,
        }
    }

    fn update_health(&self, value: TransportHealth) {
        if let Ok(mut guard) = self.health.lock() {
            *guard = value;
        }
    }
}

#[async_trait]
impl TransportRuntime for TaskTransportRuntime {
    fn kind(&self) -> TransportKind {
        self.kind
    }

    fn name(&self) -> &'static str {
        self.name
    }

    fn health(&self) -> TransportHealth {
        self.health
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or(TransportHealth::Degraded {
                reason: "failed to read transport health state".to_string(),
            })
    }

    async fn prepare(&mut self) -> Result<()> {
        Ok(())
    }

    async fn start(&mut self, shutdown: CancellationToken) -> Result<()> {
        let Some(spawner) = self.spawner.take() else {
            return Err(crate::err!("transport `{}` already started", self.name));
        };

        self.update_health(TransportHealth::Starting);
        let spawned = spawner(shutdown.clone());

        if let Ok(mut guard) = self.tasks.lock() {
            guard.extend(spawned);
        }

        self.shutdown = Some(shutdown);
        self.update_health(TransportHealth::Running);
        Ok(())
    }

    fn run(&mut self) -> TransportRun {
        // Contract: run() must only be called once per transport.
        // Debug assertion catches programming errors; in release, we return an error.
        debug_assert!(
            !self.run_registered,
            "transport `{}` run() called multiple times",
            self.name
        );
        if self.run_registered {
            let name = self.name;
            return TransportRun::new(self.kind, name, async move {
                Err(crate::err!(
                    "transport `{name}` run() called multiple times"
                ))
            });
        }
        self.run_registered = true;

        let tasks = Arc::clone(&self.tasks);
        let health = Arc::clone(&self.health);
        let name = self.name;
        let kind = self.kind;

        TransportRun::new(kind, name, async move {
            loop {
                let handle = {
                    let Ok(mut guard) = tasks.lock() else {
                        // Mutex poisoned - prior panic left inconsistent state
                        return Err(crate::err!(
                            "transport `{name}` task collection lock poisoned"
                        ));
                    };
                    guard.pop()
                };

                match handle {
                    Some(handle) => match handle.await {
                        Ok(()) => {}
                        Err(join_err) => {
                            if let Ok(mut guard) = health.lock() {
                                *guard = TransportHealth::Degraded {
                                    reason: format!("task join error: {join_err}"),
                                };
                            }

                            return Err(crate::err!(
                                "transport `{name}` worker terminated unexpectedly: {join_err}"
                            ));
                        }
                    },
                    None => {
                        if let Ok(mut guard) = health.lock() {
                            *guard = TransportHealth::Shutdown;
                        }
                        return Ok(());
                    }
                }
            }
        })
    }

    async fn shutdown(&mut self) -> Result<()> {
        if let Some(token) = self.shutdown.as_ref() {
            token.cancel();
        }
        self.update_health(TransportHealth::Shutdown);
        Ok(())
    }
}
