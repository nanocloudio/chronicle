//! Legacy connector implementations.
//!
//! This module contains older connector code that predates the newer trigger/transport
//! architecture. New inbound trigger implementations should be placed in [`transport`]
//! instead.
//!
//! # Architecture Boundaries
//!
//! Chronicle uses a clear separation between inbound and outbound data flows:
//!
//! - **Triggers** (inbound): Components that receive events from external sources
//!   and initiate chronicle execution. Located in [`transport`] (e.g., `KafkaTriggerRuntime`,
//!   `RabbitmqTriggerRuntime`, `HttpTriggerRoute`).
//!
//! - **Connectors** (outbound): Configuration wrappers and client factories for
//!   sending data to external systems. Located in [`integration::registry`] (e.g.,
//!   `KafkaConnector`, `PostgresConnector`, `HttpClientConnector`).
//!
//! - **Phases** (processing): Transform and route data between triggers and connectors.
//!   Located in [`chronicle::phase`].
//!
//! # Legacy Note
//!
//! The `kafka.rs` module in this directory contains `KafkaConsumerLoop`, which is
//! technically trigger (inbound) code but remains here for backward compatibility
//! with the legacy `app.rs` startup path. New Kafka trigger implementations should
//! use `KafkaTriggerRuntime` from [`transport::kafka`] instead.
//!
//! [`transport`]: crate::transport
//! [`integration::registry`]: crate::integration::registry
//! [`chronicle::phase`]: crate::chronicle::phase

pub mod database;
#[cfg(feature = "kafka")]
pub mod kafka;
pub mod management;
