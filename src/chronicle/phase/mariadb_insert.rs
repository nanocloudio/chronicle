use crate::chronicle::context::ExecutionContext;
use crate::chronicle::phase::utils::{
    required_option, required_string_option, resolve_optional_template, resolve_template,
};
use crate::chronicle::phase::{PhaseExecutionError, PhaseHandler};
use crate::config::integration::ChroniclePhase;
use crate::integration::factory::ConnectorFactoryRegistry;
use async_trait::async_trait;
use serde_json::{json, Map as JsonMap, Value as JsonValue};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Default, Clone)]
pub struct MariaDbInsertPhaseExecutor {
    seen: Arc<Mutex<HashSet<String>>>,
}

#[async_trait]
impl PhaseHandler for MariaDbInsertPhaseExecutor {
    async fn execute(
        &self,
        phase: &ChroniclePhase,
        context: &mut ExecutionContext,
        connectors: &ConnectorFactoryRegistry,
    ) -> Result<JsonValue, PhaseExecutionError> {
        let options = phase.options_json();

        let connector_name = required_string_option(phase, &options, "connector")?;
        let key_template = required_option(phase, &options, "key")?;
        let values_template = options.get("value");

        let handle = connectors.mariadb_pool(connector_name).map_err(|err| {
            PhaseExecutionError::invalid_configuration(
                phase.name.clone(),
                format!("failed to acquire mariadb connector `{connector_name}`: {err}"),
            )
        })?;

        let key_value = resolve_template(context, key_template)?;
        let key_fingerprint = serde_json::to_string(&key_value).map_err(|err| {
            PhaseExecutionError::invalid_input(
                phase.name.clone(),
                format!("failed to serialise key template to JSON: {err}"),
            )
        })?;

        let value = resolve_optional_template(context, values_template)?.unwrap_or(JsonValue::Null);

        let mut seen = self.seen.lock().await;
        let inserted = seen.insert(format!("{connector_name}:{key_fingerprint}"));

        let mut output = JsonMap::new();
        output.insert(
            "connector".to_string(),
            JsonValue::String(connector_name.to_string()),
        );
        output.insert("key".to_string(), key_value);
        output.insert("value".to_string(), value);
        output.insert(
            "schema".to_string(),
            handle
                .schema()
                .map(|s| JsonValue::String(s.to_string()))
                .unwrap_or(JsonValue::Null),
        );
        output.insert(
            "url".to_string(),
            JsonValue::String(handle.url().to_string()),
        );
        output.insert("inserted".to_string(), JsonValue::Bool(inserted));
        output.insert("count".to_string(), json!(if inserted { 1 } else { 0 }));

        Ok(JsonValue::Object(output))
    }
}
