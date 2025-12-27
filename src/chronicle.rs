//! Chronicle execution pipeline.
//!
//! The chronicle module implements a three-stage pipeline for processing events:
//!
//! # Pipeline Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────┐
//! │                         STARTUP (sync)                               │
//! │  ┌──────────┐    ┌──────────┐    ┌──────────────────────────────┐  │
//! │  │  Loader  │───▶│ Planner  │───▶│      ChronicleEngine         │  │
//! │  │ (schema) │    │ (plans)  │    │ (ready to accept requests)   │  │
//! │  └──────────┘    └──────────┘    └──────────────────────────────┘  │
//! └─────────────────────────────────────────────────────────────────────┘
//!                                 │
//!                                 ▼
//! ┌─────────────────────────────────────────────────────────────────────┐
//! │                         RUNTIME (async)                              │
//! │  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────┐  │
//! │  │   Trigger    │───▶│   Execute    │───▶│     Dispatcher       │  │
//! │  │ (http/kafka) │    │ (sync plan)  │    │ (async delivery)     │  │
//! │  └──────────────┘    └──────────────┘    └──────────────────────┘  │
//! └─────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Module Organization
//!
//! - [`engine`] - Synchronous loader + planner (no Tokio deps)
//! - [`dispatcher`] - Async executor for action delivery (requires Tokio)
//! - [`context`] - Execution context and template resolution
//! - [`phase`] - Phase handlers for each processing step
//! - [`retry_runner`] - Retry loop abstractions for transports
//!
//! # Design Rationale
//!
//! The loader and planner are intentionally synchronous to:
//! - Enable simple unit testing without async runtime
//! - Provide fast startup with synchronous config validation
//! - Separate concerns between planning and execution

pub mod context;
pub mod dispatcher;
pub mod engine;
pub mod phase;
pub mod retry_runner;
pub mod trigger_common;

#[cfg(feature = "mqtt")]
pub mod mqtt_triggers {
    pub use crate::transport::mqtt::*;
}

#[cfg(feature = "rabbitmq")]
pub mod rabbitmq_triggers {
    pub use crate::transport::rabbitmq::*;
}
