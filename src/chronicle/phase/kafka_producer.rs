use crate::chronicle::context::ExecutionContext;
use crate::chronicle::phase::utils::{
    required_option, required_string_option, resolve_optional_string, resolve_optional_template,
    resolve_string, resolve_template,
};
use crate::chronicle::phase::{PhaseExecutionError, PhaseHandler};
use crate::config::integration::ChroniclePhase;
use crate::integration::factory::ConnectorFactoryRegistry;
use async_trait::async_trait;
use serde_json::{json, Map as JsonMap, Value as JsonValue};

#[derive(Debug, Default)]
pub struct KafkaProducerPhaseExecutor;

#[async_trait]
impl PhaseHandler for KafkaProducerPhaseExecutor {
    async fn execute(
        &self,
        phase: &ChroniclePhase,
        context: &mut ExecutionContext,
        connectors: &ConnectorFactoryRegistry,
    ) -> Result<JsonValue, PhaseExecutionError> {
        let options = phase.options_json();

        let connector_name = required_string_option(phase, &options, "connector")?;

        let topic_template = required_option(phase, &options, "topic")?;

        let producer = connectors.kafka_producer(connector_name).map_err(|err| {
            PhaseExecutionError::invalid_configuration(
                phase.name.clone(),
                format!("failed to acquire kafka producer `{connector_name}`: {err}"),
            )
        })?;

        let topic = resolve_string(context, topic_template)?;

        let acks = resolve_optional_string(context, options.get("acks"))?;

        let key =
            resolve_optional_template(context, options.get("key"))?.unwrap_or(JsonValue::Null);

        let headers = if let Some(template) = options.get("header") {
            resolve_header_map(phase, template, context)?
        } else {
            JsonMap::new()
        };

        let payload =
            resolve_optional_template(context, options.get("payload"))?.unwrap_or_else(|| {
                context
                    .last_slot_value()
                    .cloned()
                    .unwrap_or(JsonValue::Null)
            });

        let mut output = JsonMap::new();
        output.insert(
            "connector".to_string(),
            JsonValue::String(connector_name.to_string()),
        );
        output.insert("topic".to_string(), JsonValue::String(topic));
        output.insert("payload".to_string(), payload);
        output.insert("key".to_string(), key);
        output.insert(
            "brokers".to_string(),
            JsonValue::Array(
                producer
                    .brokers()
                    .iter()
                    .map(|broker| JsonValue::String(broker.clone()))
                    .collect(),
            ),
        );
        output.insert(
            "create_topics_if_missing".to_string(),
            JsonValue::Bool(producer.create_topics_if_missing()),
        );

        if !headers.is_empty() {
            output.insert("header".to_string(), JsonValue::Object(headers));
        }

        if let Some(acks) = acks {
            output.insert("acks".to_string(), JsonValue::String(acks));
        }

        output.insert("count".to_string(), json!(1));

        Ok(JsonValue::Object(output))
    }
}

fn resolve_header_map(
    phase: &ChroniclePhase,
    template: &JsonValue,
    context: &ExecutionContext,
) -> Result<JsonMap<String, JsonValue>, PhaseExecutionError> {
    let mut resolved = JsonMap::new();

    match template {
        JsonValue::Object(map) => {
            for (key, value) in map {
                let resolved_value = resolve_template(context, value)?;
                resolved.insert(key.clone(), resolved_value);
            }
        }
        other => {
            return Err(PhaseExecutionError::invalid_input(
                phase.name.clone(),
                format!("expected header object, found {other:?}"),
            ));
        }
    }

    Ok(resolved)
}

// tests moved to tests/unit/chronicle_kafka_producer_phase_tests.rs
