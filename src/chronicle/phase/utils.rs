use crate::chronicle::context::ExecutionContext;
use crate::chronicle::phase::PhaseExecutionError;
use crate::config::integration::ChroniclePhase;
use serde_json::Value as JsonValue;

/// Returns an option from the phase's JSON options, failing if missing.
pub fn required_option<'a>(
    phase: &ChroniclePhase,
    options: &'a JsonValue,
    key: &str,
) -> Result<&'a JsonValue, PhaseExecutionError> {
    options
        .get(key)
        .ok_or_else(|| PhaseExecutionError::missing_option(&phase.name, key))
}

/// Returns a string option, failing if the value is absent or not a string.
pub fn required_string_option<'a>(
    phase: &ChroniclePhase,
    options: &'a JsonValue,
    key: &str,
) -> Result<&'a str, PhaseExecutionError> {
    let value = required_option(phase, options, key)?;
    value.as_str().ok_or_else(|| {
        PhaseExecutionError::invalid_input(&phase.name, format!("option `{key}` must be a string"))
    })
}

/// Resolves a template while applying canonical jq normalization.
pub fn resolve_template(
    context: &ExecutionContext,
    template: &JsonValue,
) -> Result<JsonValue, PhaseExecutionError> {
    context
        .resolve_template_canonical(template)
        .map_err(PhaseExecutionError::from)
}

/// Resolves a template to string while applying canonical jq normalization.
pub fn resolve_string(
    context: &ExecutionContext,
    template: &JsonValue,
) -> Result<String, PhaseExecutionError> {
    context
        .resolve_to_string_canonical(template)
        .map_err(PhaseExecutionError::from)
}

/// Resolves an optional template to a JSON value.
pub fn resolve_optional_template(
    context: &ExecutionContext,
    template: Option<&JsonValue>,
) -> Result<Option<JsonValue>, PhaseExecutionError> {
    match template {
        Some(value) => Ok(Some(resolve_template(context, value)?)),
        None => Ok(None),
    }
}

/// Resolves an optional template to a string value.
pub fn resolve_optional_string(
    context: &ExecutionContext,
    template: Option<&JsonValue>,
) -> Result<Option<String>, PhaseExecutionError> {
    match template {
        Some(value) => Ok(Some(resolve_string(context, value)?)),
        None => Ok(None),
    }
}
