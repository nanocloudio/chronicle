#![allow(clippy::result_large_err)]

extern crate self as chronicle_core;

pub mod app;
pub mod app_state;
pub mod backpressure;
pub mod chronicle;
pub mod codec;
pub mod config;
pub mod connectors;
pub mod domain;
pub mod error;
pub mod integration;
pub mod logging;
pub mod metrics;
pub mod readiness;
pub mod retry;
pub mod telemetry;

pub mod transport;

pub mod context {
    pub use crate::chronicle::context::*;
}
