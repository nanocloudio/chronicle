use crate::chronicle::context::ExecutionContext;
use crate::chronicle::phase::utils::resolve_template;
use crate::chronicle::phase::{PhaseExecutionError, PhaseHandler};
use crate::config::integration::ChroniclePhase;
use crate::integration::factory::ConnectorFactoryRegistry;
use async_trait::async_trait;
use serde_json::Value as JsonValue;

#[derive(Debug, Default)]
pub struct TransformPhaseExecutor;

#[async_trait]
impl PhaseHandler for TransformPhaseExecutor {
    async fn execute(
        &self,
        phase: &ChroniclePhase,
        context: &mut ExecutionContext,
        _connectors: &ConnectorFactoryRegistry,
    ) -> Result<JsonValue, PhaseExecutionError> {
        let template = phase.options_json();
        let resolved = resolve_template(context, &template)?;
        Ok(resolved)
    }
}
