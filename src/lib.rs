//! Chronicle - Event-driven integration engine.
//!
//! Chronicle provides a declarative YAML-based configuration system for building
//! event-driven data pipelines. It supports multiple connectors (HTTP, Kafka, Redis,
//! PostgreSQL, etc.) and composable processing phases.
//!
//! # Public API
//!
//! The stable public API consists of:
//!
//! - [`config`] - Configuration loading and validation
//! - [`chronicle::engine`] - Chronicle execution engine
//! - [`error`] - Error types and result aliases
//! - [`app::ChronicleApp`] - Application entry point
//!
//! # Internal Modules
//!
//! The following modules are exposed for integration testing but are not part of
//! the stable public API. They may change without notice:
//!
//! - `codec` - Message encoding/decoding
//! - `transport` - Protocol transport implementations
//! - `integration` - Connector registry and factory
//! - `readiness` - Service readiness management
//! - `metrics`, `telemetry`, `logging` - Observability infrastructure

#![allow(
    clippy::result_large_err,
    reason = "Error types contain contextual information"
)]

extern crate self as chronicle_core;

// === Public API ===

/// Application entry point and runtime orchestration.
pub mod app;

/// Configuration loading and validation.
pub mod config;

/// Chronicle execution engine and core abstractions.
pub mod chronicle;

/// Error types and result aliases.
pub mod error;

// === Internal modules (exposed for testing) ===

#[doc(hidden)]
pub mod app_state;

#[doc(hidden)]
pub mod backpressure;

#[doc(hidden)]
pub mod codec;

#[doc(hidden)]
pub mod connectors;

#[doc(hidden)]
pub mod domain;

#[doc(hidden)]
pub mod integration;

#[doc(hidden)]
pub mod logging;

#[doc(hidden)]
pub mod metrics;

#[doc(hidden)]
pub mod readiness;

#[doc(hidden)]
pub mod retry;

#[doc(hidden)]
pub mod telemetry;

#[doc(hidden)]
pub mod transport;

/// Re-export of execution context types.
pub mod context {
    pub use crate::chronicle::context::*;
}
