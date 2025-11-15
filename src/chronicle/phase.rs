mod executor;
#[cfg(feature = "http-out")]
pub mod http_client;
pub mod kafka_producer;
#[cfg(feature = "db-mariadb")]
pub mod mariadb_insert;
pub mod model;
#[cfg(feature = "mqtt")]
pub mod mqtt_publish;
#[cfg(feature = "db-postgres")]
pub mod postgres_insert;
pub mod transform;
pub mod utils;

use crate::chronicle::context::{ContextError, ExecutionContext};
use crate::config::integration::{ChronicleDefinition, ChroniclePhase, PhaseKind};
use crate::error::ChronicleError;
use crate::integration::factory::ConnectorFactoryRegistry;
use async_trait::async_trait;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

pub use executor::PhaseExecutor;

/// Registry for the handlers capable of executing each phase kind within a chronicle.
#[derive(Default)]
pub struct PhaseRegistry {
    handlers: HashMap<PhaseKind, Arc<dyn PhaseHandler>>,
}

impl PhaseRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_handler<H>(&mut self, kind: PhaseKind, handler: H)
    where
        H: PhaseHandler + 'static,
    {
        self.handlers.insert(kind, Arc::new(handler));
    }

    pub async fn execute(
        &self,
        chronicle: &ChronicleDefinition,
        connectors: &ConnectorFactoryRegistry,
        context: &mut ExecutionContext,
    ) -> Result<(), PhaseRegistryError> {
        for (index, phase) in chronicle.phases.iter().enumerate() {
            let handler = self
                .handlers
                .get(&phase.kind)
                .ok_or_else(|| PhaseRegistryError::MissingHandler {
                    chronicle: chronicle.name.clone(),
                    phase: phase.name.clone(),
                    kind: phase.kind.clone(),
                })?
                .clone();

            let output = handler
                .execute(phase, context, connectors)
                .await
                .map_err(|err| PhaseRegistryError::PhaseFailed {
                    chronicle: chronicle.name.clone(),
                    phase: phase.name.clone(),
                    source: err,
                })?;

            context.insert_slot(index + 1, output);
        }

        Ok(())
    }
}

#[async_trait]
pub trait PhaseHandler: Send + Sync {
    async fn execute(
        &self,
        phase: &ChroniclePhase,
        context: &mut ExecutionContext,
        connectors: &ConnectorFactoryRegistry,
    ) -> Result<JsonValue, PhaseExecutionError>;
}

#[derive(Debug, Error)]
pub enum PhaseRegistryError {
    #[error("chronicle `{chronicle}` phase `{phase}` has no handler for kind `{kind:?}`")]
    MissingHandler {
        chronicle: String,
        phase: String,
        kind: PhaseKind,
    },
    #[error("chronicle `{chronicle}` phase `{phase}` failed: {source}")]
    PhaseFailed {
        chronicle: String,
        phase: String,
        #[source]
        source: PhaseExecutionError,
    },
}

#[derive(Debug, Error)]
pub enum PhaseExecutionError {
    #[error("phase `{phase}` missing required option `{option}`")]
    MissingOption { phase: String, option: String },
    #[error("phase `{phase}` configuration error: {reason}")]
    InvalidConfiguration { phase: String, reason: String },
    #[error("phase `{phase}` input error: {reason}")]
    InvalidInput { phase: String, reason: String },
    #[error("phase `{phase}` transport error: {source}")]
    Transport {
        phase: String,
        #[source]
        source: ChronicleError,
    },
    #[error(transparent)]
    Context(#[from] ContextError),
}

impl PhaseExecutionError {
    pub fn missing_option(phase: impl Into<String>, option: impl Into<String>) -> Self {
        PhaseExecutionError::MissingOption {
            phase: phase.into(),
            option: option.into(),
        }
    }

    pub fn invalid_configuration(phase: impl Into<String>, reason: impl Into<String>) -> Self {
        PhaseExecutionError::InvalidConfiguration {
            phase: phase.into(),
            reason: reason.into(),
        }
    }

    pub fn invalid_input(phase: impl Into<String>, reason: impl Into<String>) -> Self {
        PhaseExecutionError::InvalidInput {
            phase: phase.into(),
            reason: reason.into(),
        }
    }

    pub fn transport(phase: impl Into<String>, source: impl Into<ChronicleError>) -> Self {
        PhaseExecutionError::Transport {
            phase: phase.into(),
            source: source.into(),
        }
    }
}
