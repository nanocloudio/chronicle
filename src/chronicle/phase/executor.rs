use crate::chronicle::phase::model::{PhaseDefinition, PhaseInstance, PhaseMessage, PhaseStatus};
use crate::connectors::database::Database;
use crate::error::{Context, Result};
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Clone)]
pub struct PhaseExecutor {
    inner: Arc<PhaseExecutorInner>,
}

struct PhaseExecutorInner {
    db: Option<Database>,
    definitions: Mutex<Vec<PhaseDefinition>>,
}

impl PhaseExecutor {
    pub fn new(db: Option<Database>) -> Self {
        let inner = PhaseExecutorInner {
            db,
            definitions: Mutex::new(Vec::new()),
        };

        Self {
            inner: Arc::new(inner),
        }
    }

    pub async fn register_definition(&self, definition: PhaseDefinition) -> Result<PhaseInstance> {
        tracing::info!(
            name = %definition.name,
            step_count = definition.steps.len(),
            "registering phase definition"
        );

        {
            let mut guard = self.inner.definitions.lock().await;
            guard.push(definition);
        }

        // Placeholder: in a future iteration we will persist the phase and schedule execution.
        let instance = PhaseInstance {
            id: Uuid::new_v4(),
            status: PhaseStatus::Pending,
        };

        Ok(instance)
    }

    pub async fn handle_message(&self, message: PhaseMessage) -> Result<()> {
        match message {
            PhaseMessage::Heartbeat { source } => {
                tracing::trace!("phase executor heartbeat from {source}");
                Ok(())
            }
            PhaseMessage::Trigger { phase_id, payload } => {
                tracing::info!(%phase_id, ?payload, "received phase trigger");
                self.rehydrate_and_execute(phase_id, payload)
                    .await
                    .context("failed to execute phase")?;
                Ok(())
            }
        }
    }

    async fn rehydrate_and_execute(
        &self,
        phase_id: Uuid,
        payload: serde_json::Value,
    ) -> Result<()> {
        tracing::debug!(%phase_id, "rehydrating phase instance");

        // Placeholder for future database interaction.
        let _ = (&self.inner.db, payload);

        tracing::info!(%phase_id, "phase completed (stub)");
        Ok(())
    }
}
