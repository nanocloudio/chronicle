use crate::chronicle::context::ExecutionContext;
use crate::chronicle::phase::utils::{
    required_option, required_string_option, resolve_string, resolve_template,
};
use crate::chronicle::phase::{PhaseExecutionError, PhaseHandler};
use crate::config::integration::ChroniclePhase;
use crate::integration::factory::ConnectorFactoryRegistry;
use async_trait::async_trait;
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};

#[derive(Debug, Default)]
pub struct PostgresInsertPhaseExecutor;

#[async_trait]
impl PhaseHandler for PostgresInsertPhaseExecutor {
    async fn execute(
        &self,
        phase: &ChroniclePhase,
        context: &mut ExecutionContext,
        connectors: &ConnectorFactoryRegistry,
    ) -> Result<JsonValue, PhaseExecutionError> {
        let options = phase.options_json();

        let connector_name = required_string_option(phase, &options, "connector")?;

        let sql_template = required_option(phase, &options, "sql")?;

        let handle = connectors.postgres_pool(connector_name).map_err(|err| {
            PhaseExecutionError::invalid_configuration(
                phase.name.clone(),
                format!("failed to acquire postgres connector `{connector_name}`: {err}"),
            )
        })?;

        let sql = resolve_string(context, sql_template)?;

        let parameters = resolve_parameters(phase, options.get("values"), context)?;

        let mut output = JsonMap::new();
        output.insert(
            "connector".to_string(),
            JsonValue::String(connector_name.to_string()),
        );
        output.insert("sql".to_string(), JsonValue::String(sql));
        output.insert("parameters".to_string(), parameters);
        output.insert(
            "url".to_string(),
            JsonValue::String(handle.url().to_string()),
        );
        output.insert(
            "schema".to_string(),
            handle
                .schema()
                .map(|s| JsonValue::String(s.to_string()))
                .unwrap_or(JsonValue::Null),
        );
        output.insert("count".to_string(), JsonValue::Number(JsonNumber::from(1)));

        Ok(JsonValue::Object(output))
    }
}

fn resolve_parameters(
    phase: &ChroniclePhase,
    template: Option<&JsonValue>,
    context: &ExecutionContext,
) -> Result<JsonValue, PhaseExecutionError> {
    match template {
        None => Ok(JsonValue::Object(JsonMap::new())),
        Some(JsonValue::Object(map)) => {
            let mut resolved = JsonMap::with_capacity(map.len());
            for (key, value) in map {
                let value = resolve_template(context, value)?;
                resolved.insert(key.clone(), value);
            }
            Ok(JsonValue::Object(resolved))
        }
        Some(other) => Err(PhaseExecutionError::invalid_input(
            phase.name.clone(),
            format!("values must be an object, found {other:?}"),
        )),
    }
}
