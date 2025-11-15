use crate::chronicle::context::ExecutionContext;
use crate::chronicle::phase::utils::{
    required_string_option, resolve_optional_string, resolve_optional_template, resolve_string,
};
use crate::chronicle::phase::{PhaseExecutionError, PhaseHandler};
use crate::codec::http::{
    build_connector_request, decode_body, flatten_response_headers, ConnectorRequest,
};
use crate::config::integration::ChroniclePhase;
use crate::integration::factory::ConnectorFactoryRegistry;
use crate::metrics::metrics;
use async_trait::async_trait;
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::time::Instant;

#[derive(Debug, Default)]
pub struct HttpClientPhaseExecutor;

#[async_trait]
impl PhaseHandler for HttpClientPhaseExecutor {
    async fn execute(
        &self,
        phase: &ChroniclePhase,
        context: &mut ExecutionContext,
        connectors: &ConnectorFactoryRegistry,
    ) -> Result<JsonValue, PhaseExecutionError> {
        let options = phase.options_json();

        let connector_name = required_string_option(phase, &options, "connector")?;

        let handle = connectors.http_client(connector_name).map_err(|err| {
            PhaseExecutionError::invalid_configuration(
                phase.name.clone(),
                format!("failed to acquire http client connector `{connector_name}`: {err}"),
            )
        })?;

        let method_template = options
            .get("method")
            .cloned()
            .unwrap_or_else(|| JsonValue::String("GET".to_string()));
        let path_template = options
            .get("path")
            .cloned()
            .unwrap_or_else(|| JsonValue::String("/".to_string()));
        let body_template = options.get("body").cloned();
        let headers_template = options.get("header").cloned();
        let content_type_template = options.get("contentType").cloned();

        let method = context
            .resolve_to_string_canonical(&method_template)
            .map_err(PhaseExecutionError::from)?;
        let path = context
            .resolve_to_string_canonical(&path_template)
            .map_err(PhaseExecutionError::from)?;

        let content_type = resolve_optional_string(context, content_type_template.as_ref())?;

        let resolved_headers = resolve_headers(headers_template.as_ref(), context)?;

        let body = resolve_optional_template(context, body_template.as_ref())?;

        let request = build_connector_request(
            handle.client(),
            ConnectorRequest {
                method: &method,
                base_url: handle.base_url(),
                path: &path,
                default_headers: handle.default_headers(),
                headers: &resolved_headers,
                body: body.as_ref(),
                content_type: content_type.as_deref(),
            },
        )
        .map_err(|err| {
            PhaseExecutionError::invalid_configuration(phase.name.clone(), err.to_string())
        })?;

        let started = Instant::now();
        let response = request.send().await.map_err(|err| {
            metrics().record_http_request(&phase.name, 0, started.elapsed());
            PhaseExecutionError::transport(
                phase.name.clone(),
                crate::err!("http client `{connector_name}` request failed: {err}"),
            )
        })?;

        let status = response.status().as_u16();
        let response = response.error_for_status().map_err(|err| {
            metrics().record_http_request(&phase.name, status, started.elapsed());
            PhaseExecutionError::transport(
                phase.name.clone(),
                crate::err!("http client `{connector_name}` request failed: {err}"),
            )
        })?;

        let headers_json = flatten_response_headers(response.headers());

        let bytes = response.bytes().await.map_err(|err| {
            metrics().record_http_request(&phase.name, status, started.elapsed());
            PhaseExecutionError::transport(
                phase.name.clone(),
                crate::err!("http client `{connector_name}` failed to read response: {err}"),
            )
        })?;

        let duration_ms = started.elapsed().as_millis() as u64;
        metrics().record_http_request(&phase.name, status, started.elapsed());

        let body = decode_body(&bytes);

        let mut output = JsonMap::new();
        output.insert("status".to_string(), JsonValue::Number(status.into()));
        output.insert(
            "durationMs".to_string(),
            JsonValue::Number((duration_ms).into()),
        );
        if !headers_json.is_empty() {
            output.insert("header".to_string(), JsonValue::Object(headers_json));
        }
        if let Some(body_value) = body {
            output.insert("body".to_string(), body_value);
        } else {
            output.insert("body".to_string(), JsonValue::Null);
        }

        Ok(JsonValue::Object(output))
    }
}

fn resolve_headers(
    template: Option<&JsonValue>,
    context: &ExecutionContext,
) -> Result<Vec<(String, String)>, PhaseExecutionError> {
    let mut headers = Vec::new();
    if let Some(JsonValue::Object(map)) = template {
        for (key, value) in map {
            let resolved = resolve_string(context, value)?;
            headers.push((key.clone(), resolved));
        }
    }
    Ok(headers)
}
