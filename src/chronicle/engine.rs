use crate::chronicle::context::{ContextError, ExecutionContext, ExecutionObservability};
use crate::codec::{
    avro::{
        AvroCodec, FileSchemaSource, RegistryCredentials, RegistrySchemaSource,
        SchemaRegistryVersion, SchemaSource,
    },
    cbor::CborCodec,
    protobuf::{DescriptorSource, ProtobufCodec},
};
use crate::config::integration::{
    AppConfig, ChronicleDefinition, ChroniclePhase, ChroniclePolicy, DeliveryPolicy,
    HalfOpenPolicy, IntegrationConfig, OptionMap, OverflowPolicy, PhaseKind, RetryBudget,
    RouteLimits, ScalarValue, SmtpTlsMode,
};
use crate::integration::registry::{
    ConnectorRegistry, GrpcConnector, HttpClientConnector, HttpServerConnector, KafkaConnector,
    MariadbConnector, MongodbConnector, MqttConnector, PostgresConnector, RabbitmqConnector,
    RedisConnector, SmtpConnector,
};
use crate::metrics::metrics;
use crate::retry::merge_retry_budgets;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use chrono::Utc;
use humantime::parse_duration;
use serde_json::{json, Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use std::collections::HashMap;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Duration;
use thiserror::Error;

#[derive(Clone)]
pub struct ChronicleEngine {
    _config: Arc<IntegrationConfig>,
    _registry: Arc<ConnectorRegistry>,
    plans: HashMap<String, ChroniclePlan>,
}

impl ChronicleEngine {
    pub fn new(
        config: Arc<IntegrationConfig>,
        registry: Arc<ConnectorRegistry>,
    ) -> Result<Self, ChronicleEngineError> {
        let mut plans = HashMap::new();

        for chronicle in &config.chronicles {
            let plan = ChroniclePlan::build(chronicle, &config.app, &registry)?;
            plans.insert(chronicle.name.clone(), plan);
        }

        Ok(Self {
            _config: config,
            _registry: registry,
            plans,
        })
    }

    pub fn execute(
        &self,
        chronicle_name: &str,
        trigger_payload: JsonValue,
    ) -> Result<ChronicleExecution, ChronicleEngineError> {
        let plan = self
            .plans
            .get(chronicle_name)
            .ok_or_else(|| ChronicleEngineError::UnknownChronicle {
                name: chronicle_name.to_string(),
            })?
            .clone();

        let _inflight_guard = plan.concurrency().acquire(&plan.name)?;

        let mut context = ExecutionContext::new();
        context.insert_slot(0, trigger_payload);

        let observability = context.observability();

        let trace_field = observability.trace_id.as_deref().unwrap_or("");
        let record_field = observability.record_id.as_deref().unwrap_or("");

        let chronicle_span = tracing::info_span!(
            target: "chronicle::engine",
            "chronicle",
            chronicle = %plan.name,
            trace_id = trace_field,
            record_id = record_field
        );
        let _chronicle_guard = chronicle_span.enter();

        tracing::info!(
            target: "chronicle::engine",
            event = "chronicle_started",
            chronicle = %plan.name,
            trigger = plan.trigger_kind(),
            phases = plan.phases.len(),
            trace_id = trace_field,
            record_id = record_field
        );

        let mut actions = Vec::new();

        for (phase_index, phase) in plan.phases.iter().enumerate() {
            let slot_index = phase_index + 1;
            let phase_name = phase.name().to_string();
            let phase_kind = phase.kind_name().to_string();
            let phase_span = tracing::info_span!(
                target: "chronicle::engine",
                "phase",
                chronicle = %plan.name,
                phase = %phase_name,
                kind = phase_kind.as_str(),
                index = phase_index as i32,
                trace_id = trace_field,
                record_id = record_field
            );
            let _phase_guard = phase_span.enter();

            tracing::info!(
                target: "chronicle::engine",
                event = "phase_started",
                chronicle = %plan.name,
                phase = %phase_name,
                kind = phase_kind.as_str(),
                index = phase_index as i32,
                trace_id = trace_field,
                record_id = record_field
            );

            let phase_actions = self.execute_phase_plan(
                &plan,
                phase,
                phase_index,
                slot_index,
                &mut context,
                &observability,
            )?;
            actions.extend(phase_actions);

            tracing::info!(
                target: "chronicle::engine",
                event = "phase_completed",
                chronicle = %plan.name,
                phase = %phase_name,
                kind = phase_kind.as_str(),
                index = phase_index as i32
            );
        }

        let fallback_actions = if plan.fallbacks.is_empty() {
            None
        } else {
            let mut map = BTreeMap::new();
            let mut fallback_index = plan.phases.len();
            for (fallback_name, fallback_plan) in &plan.fallbacks {
                let mut fallback_context = context.clone();
                let slot_index = fallback_index + 1;
                let actions = self.execute_phase_plan(
                    &plan,
                    fallback_plan,
                    fallback_index,
                    slot_index,
                    &mut fallback_context,
                    &observability,
                )?;
                map.insert(fallback_name.clone(), actions);
                fallback_index += 1;
            }
            Some(map)
        };

        let response = build_http_response(&context, plan.phases.len());
        let context_snapshot = context.into_vec();

        tracing::info!(
            target: "chronicle::engine",
            event = "chronicle_completed",
            chronicle = %plan.name,
            phases = plan.phases.len(),
            response = response
                .as_ref()
                .map(|resp| resp.status)
                .unwrap_or(202),
            trace_id = trace_field,
            record_id = record_field
        );

        Ok(ChronicleExecution {
            name: plan.name,
            actions,
            context: context_snapshot,
            response,
            trace_id: observability.trace_id,
            record_id: observability.record_id,
            delivery: plan.policy.delivery.clone(),
            allow_partial_delivery: plan.policy.allow_partial_delivery,
            fallback: plan.policy.fallback.clone(),
            fallback_actions,
            retry_budget: plan.policy.retry_budget.clone(),
        })
    }

    #[allow(clippy::only_used_in_recursion)]
    fn execute_phase_plan(
        &self,
        plan: &ChroniclePlan,
        phase: &PhasePlan,
        _phase_index: usize,
        slot_index: usize,
        context: &mut ExecutionContext,
        observability: &ExecutionObservability,
    ) -> Result<Vec<ChronicleAction>, ChronicleEngineError> {
        let mut actions = Vec::new();
        let phase_name = phase.name().to_string();
        let trace_field = observability.trace_id.as_deref().unwrap_or("");
        let record_field = observability.record_id.as_deref().unwrap_or("");

        match phase {
            PhasePlan::Transform { name, template } => {
                let resolved = context
                    .resolve_template(template)
                    .map_err(|err| map_context_error(err, &plan.name, Some(name)))?;
                tracing::debug!(
                    target: "chronicle::engine",
                    event = "transform_resolved",
                    chronicle = %plan.name,
                    phase = %phase_name,
                    result = ?resolved
                );
                context.insert_slot(slot_index, resolved);
            }
            PhasePlan::KafkaProducer {
                name,
                connector,
                topic,
                payload_template,
                acks,
                extra,
            } => {
                let (connector_name, handle) = connector.clone().expect_kafka(&plan.name, name)?;

                let payload = if let Some(template) = payload_template.as_ref() {
                    context
                        .resolve_template(template)
                        .map_err(|err| map_context_error(err, &plan.name, Some(name)))?
                } else {
                    context
                        .slot_value(slot_index - 1)
                        .cloned()
                        .unwrap_or(JsonValue::Null)
                };

                tracing::info!(
                    target: "chronicle::engine",
                    event = "kafka_publish_enqueued",
                    chronicle = %plan.name,
                    phase = %phase_name,
                    connector = %connector_name,
                    topic = %topic,
                    trace_id = trace_field,
                    record_id = record_field
                );

                actions.push(ChronicleAction::KafkaPublish {
                    connector: connector_name,
                    brokers: handle.brokers.clone(),
                    topic: topic.clone(),
                    create_topics_if_missing: handle.create_topics_if_missing,
                    payload,
                    acks: acks.clone(),
                    extra: extra.clone(),
                    trace_id: observability.trace_id.clone(),
                    record_id: observability.record_id.clone(),
                });

                context.insert_slot(slot_index, json!({ "count": 1 }));
            }
            PhasePlan::HttpClient {
                name,
                connector,
                method_template,
                path_template,
                headers_template,
                body_template,
                content_type,
                timeout_ms,
                extra,
            } => {
                let (connector_name, handle) =
                    connector.clone().expect_http_client(&plan.name, name)?;

                let method = context
                    .resolve_to_string(method_template)
                    .map_err(|err| map_context_error(err, &plan.name, Some(name)))?;
                let path = context
                    .resolve_to_string(path_template)
                    .map_err(|err| map_context_error(err, &plan.name, Some(name)))?;

                let headers = if let Some(template) = headers_template.as_ref() {
                    if let Some(map) = template.as_object() {
                        let mut resolved = BTreeMap::new();
                        for (key, value) in map {
                            let resolved_value = context
                                .resolve_to_string(value)
                                .map_err(|err| map_context_error(err, &plan.name, Some(name)))?;
                            resolved.insert(key.clone(), resolved_value);
                        }
                        resolved
                    } else {
                        BTreeMap::new()
                    }
                } else {
                    BTreeMap::new()
                };

                let body = if let Some(template) = body_template.as_ref() {
                    context
                        .resolve_template(template)
                        .map_err(|err| map_context_error(err, &plan.name, Some(name)))?
                } else {
                    JsonValue::Null
                };

                tracing::info!(
                    target: "chronicle::engine",
                    event = "http_request_enqueued",
                    chronicle = %plan.name,
                    phase = %phase_name,
                    connector = %connector_name,
                    method = %method,
                    path = %path,
                    trace_id = trace_field,
                    record_id = record_field
                );

                actions.push(ChronicleAction::HttpRequest {
                    connector: connector_name,
                    base_url: handle.base_url.clone(),
                    method,
                    path,
                    headers,
                    body,
                    content_type: content_type.clone(),
                    timeout_ms: *timeout_ms,
                    extra: extra.clone(),
                    trace_id: observability.trace_id.clone(),
                    record_id: observability.record_id.clone(),
                });

                context.insert_slot(slot_index, JsonValue::Null);
            }
            PhasePlan::GrpcRequest {
                name,
                connector,
                service,
                method,
                descriptor,
                request_template,
                metadata_template,
                timeout_ms,
                extra,
            } => {
                let (connector_name, config) = connector.clone().expect_grpc(&plan.name, name)?;

                let request_payload = if let Some(template) = request_template.as_ref() {
                    context
                        .resolve_template(template)
                        .map_err(|err| map_context_error(err, &plan.name, Some(name)))?
                } else {
                    context
                        .slot_value(slot_index - 1)
                        .cloned()
                        .unwrap_or(JsonValue::Null)
                };

                let mut metadata_map: BTreeMap<String, String> = config
                    .metadata
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect();

                if let Some(template) = metadata_template.as_ref() {
                    let map = template.as_object().ok_or_else(|| {
                        ChronicleEngineError::InvalidPhaseOptions {
                            chronicle: plan.name.clone(),
                            phase: name.clone(),
                            reason: "grpc client option `metadata` must be an object".to_string(),
                        }
                    })?;

                    for (key, value) in map {
                        let resolved_value = context
                            .resolve_to_string(value)
                            .map_err(|err| map_context_error(err, &plan.name, Some(name)))?;
                        metadata_map.insert(key.clone(), resolved_value);
                    }
                }

                let metadata_pairs = metadata_map.into_iter().collect::<Vec<_>>();

                tracing::info!(
                    target: "chronicle::engine",
                    event = "grpc_request_enqueued",
                    chronicle = %plan.name,
                    phase = %phase_name,
                    connector = %connector_name,
                    service = %service,
                    method = %method,
                    trace_id = trace_field,
                    record_id = record_field
                );

                actions.push(ChronicleAction::GrpcRequest {
                    connector: connector_name,
                    endpoint: config.endpoint.clone(),
                    service: service.clone(),
                    method: method.clone(),
                    descriptor_path: descriptor.clone(),
                    request: request_payload,
                    metadata: metadata_pairs,
                    timeout_ms: *timeout_ms,
                    extra: extra.clone(),
                    trace_id: observability.trace_id.clone(),
                    record_id: observability.record_id.clone(),
                });

                context.insert_slot(slot_index, JsonValue::Null);
            }
            PhasePlan::RabbitmqPublish {
                name,
                connector,
                exchange_template,
                routing_key_template,
                body_template,
                headers_template,
                properties_template,
                mandatory,
                confirm,
                timeout_ms,
                extra,
            } => {
                let (connector_name, handle) =
                    connector.clone().expect_rabbitmq(&plan.name, name)?;

                let collector = metrics();

                let build_result =
                    (|| -> Result<(ChronicleAction, JsonValue), ChronicleEngineError> {
                        let exchange =
                            if let Some(template) = exchange_template.as_ref() {
                                Some(context.resolve_to_string(template).map_err(|err| {
                                    map_context_error(err, &plan.name, Some(name))
                                })?)
                            } else {
                                None
                            };

                        let routing_key =
                            if let Some(template) = routing_key_template.as_ref() {
                                Some(context.resolve_to_string(template).map_err(|err| {
                                    map_context_error(err, &plan.name, Some(name))
                                })?)
                            } else {
                                None
                            };

                        let payload = if let Some(template) = body_template.as_ref() {
                            context
                                .resolve_template(template)
                                .map_err(|err| map_context_error(err, &plan.name, Some(name)))?
                        } else {
                            context
                                .slot_value(slot_index - 1)
                                .cloned()
                                .unwrap_or(JsonValue::Null)
                        };

                        let headers_value = resolve_template_to_object(
                            &plan.name,
                            name,
                            headers_template.as_ref(),
                            context,
                            "headers",
                        )?;

                        let properties_value = resolve_template_to_object(
                            &plan.name,
                            name,
                            properties_template.as_ref(),
                            context,
                            "properties",
                        )?;

                        tracing::info!(
                            target: "chronicle::engine",
                            event = "rabbitmq_publish_enqueued",
                            chronicle = %plan.name,
                            phase = %phase_name,
                            connector = %connector_name,
                            exchange = ?exchange,
                            routing_key = ?routing_key,
                            trace_id = trace_field,
                            record_id = record_field
                        );

                        let action = ChronicleAction::RabbitmqPublish {
                            connector: connector_name.clone(),
                            url: handle.url.clone(),
                            exchange: exchange.clone(),
                            routing_key: routing_key.clone(),
                            payload: payload.clone(),
                            headers: headers_value.clone(),
                            properties: properties_value.clone(),
                            mandatory: *mandatory,
                            confirm: *confirm,
                            timeout_ms: *timeout_ms,
                            extra: extra.clone(),
                            trace_id: observability.trace_id.clone(),
                            record_id: observability.record_id.clone(),
                        };

                        let mut context_record = JsonMap::new();
                        context_record.insert(
                            "connector".to_string(),
                            JsonValue::String(connector_name.clone()),
                        );
                        context_record.insert(
                            "exchange".to_string(),
                            exchange
                                .clone()
                                .map(JsonValue::String)
                                .unwrap_or(JsonValue::Null),
                        );
                        context_record.insert(
                            "routing_key".to_string(),
                            routing_key
                                .clone()
                                .map(JsonValue::String)
                                .unwrap_or(JsonValue::Null),
                        );
                        if let Some(flag) = *mandatory {
                            context_record.insert("mandatory".to_string(), JsonValue::Bool(flag));
                        }
                        if let Some(flag) = *confirm {
                            context_record.insert("confirm".to_string(), JsonValue::Bool(flag));
                        }
                        if let Some(timeout) = *timeout_ms {
                            context_record.insert(
                                "timeout_ms".to_string(),
                                JsonValue::Number(JsonNumber::from(timeout)),
                            );
                        }
                        context_record
                            .insert("status".to_string(), JsonValue::String("pending".into()));
                        context_record.insert("headers".to_string(), headers_value);
                        context_record.insert("properties".to_string(), properties_value);
                        context_record.insert(
                            "count".to_string(),
                            JsonValue::Number(JsonNumber::from(1_u64)),
                        );

                        Ok((action, JsonValue::Object(context_record)))
                    })();

                match build_result {
                    Ok((action, context_value)) => {
                        actions.push(action);
                        context.insert_slot(slot_index, context_value);
                        collector.inc_rabbitmq_publish_success();
                    }
                    Err(err) => {
                        collector.inc_rabbitmq_publish_failure();
                        return Err(err);
                    }
                }
            }
            PhasePlan::MqttPublish {
                name,
                connector,
                topic_template,
                payload_template,
                qos_template,
                retain_template,
                encoding_template,
                extra,
                packet_seq,
                timeout_ms,
            } => {
                let (connector_name, handle) = connector.clone().expect_mqtt(&plan.name, name)?;

                let collector = metrics();

                let build_result =
                    (|| -> Result<(ChronicleAction, JsonValue), ChronicleEngineError> {
                        let timeout_ms = *timeout_ms;

                        let topic = context
                            .resolve_to_string(topic_template)
                            .map_err(|err| map_context_error(err, &plan.name, Some(name)))?;

                        let payload = if let Some(template) = payload_template.as_ref() {
                            context
                                .resolve_template(template)
                                .map_err(|err| map_context_error(err, &plan.name, Some(name)))?
                        } else {
                            context
                                .slot_value(slot_index - 1)
                                .cloned()
                                .unwrap_or(JsonValue::Null)
                        };

                        let encoding = resolve_mqtt_encoding(
                            &plan.name,
                            name,
                            encoding_template.as_ref(),
                            context,
                        )?;

                        let qos =
                            resolve_mqtt_qos(&plan.name, name, qos_template.as_ref(), context)?;

                        let retain = resolve_mqtt_retain(
                            &plan.name,
                            name,
                            retain_template.as_ref(),
                            context,
                        )?;

                        let encoded_payload =
                            encode_mqtt_payload(&plan.name, name, &payload, encoding)?;

                        let packet_id = if qos == 0 {
                            None
                        } else {
                            Some(next_packet_id(packet_seq))
                        };

                        let published_at = Utc::now().to_rfc3339();

                        tracing::info!(
                            target: "chronicle::engine",
                            event = "mqtt_publish_enqueued",
                            chronicle = %plan.name,
                            phase = %phase_name,
                            connector = %connector_name,
                            topic = %topic,
                            qos = qos,
                            retain = retain,
                            trace_id = trace_field,
                            record_id = record_field
                        );

                        let action = ChronicleAction::MqttPublish {
                            connector: connector_name.clone(),
                            url: handle.url.clone(),
                            topic: topic.clone(),
                            qos,
                            retain,
                            payload: payload.clone(),
                            payload_encoding: encoding.as_str().to_string(),
                            encoded_payload: encoded_payload.clone(),
                            packet_id,
                            published_at: published_at.clone(),
                            extra: extra.clone(),
                            trace_id: observability.trace_id.clone(),
                            record_id: observability.record_id.clone(),
                            timeout_ms,
                        };

                        let mut context_record = JsonMap::new();
                        context_record.insert(
                            "connector".to_string(),
                            JsonValue::String(connector_name.clone()),
                        );
                        context_record
                            .insert("topic".to_string(), JsonValue::String(topic.clone()));
                        context_record.insert("payload".to_string(), payload);
                        context_record.insert(
                            "payload_encoding".to_string(),
                            JsonValue::String(encoding.as_str().to_string()),
                        );
                        context_record.insert(
                            "payload_encoded".to_string(),
                            JsonValue::String(encoded_payload),
                        );
                        context_record
                            .insert("qos".to_string(), JsonValue::Number(JsonNumber::from(qos)));
                        context_record.insert("retain".to_string(), JsonValue::Bool(retain));
                        context_record.insert(
                            "packet_id".to_string(),
                            packet_id
                                .map(|id| JsonValue::Number(JsonNumber::from(id)))
                                .unwrap_or(JsonValue::Null),
                        );
                        context_record
                            .insert("published_at".to_string(), JsonValue::String(published_at));
                        if let Some(ms) = timeout_ms {
                            context_record.insert(
                                "timeout_ms".to_string(),
                                JsonValue::Number(JsonNumber::from(ms)),
                            );
                        }
                        context_record
                            .insert("status".to_string(), JsonValue::String("pending".into()));

                        Ok((action, JsonValue::Object(context_record)))
                    })();

                match build_result {
                    Ok((action, context_value)) => {
                        actions.push(action);
                        context.insert_slot(slot_index, context_value);
                        collector.inc_mqtt_publish_success();
                    }
                    Err(err) => {
                        collector.inc_mqtt_publish_failure();
                        return Err(err);
                    }
                }
            }
            PhasePlan::Parallel(parallel_plan) => {
                let mut children_map = JsonMap::new();
                let mut encountered_optional_failure = false;

                for child in &parallel_plan.children {
                    let mut child_context = context.clone();
                    match self.execute_phase_plan(
                        plan,
                        &child.plan,
                        _phase_index,
                        slot_index,
                        &mut child_context,
                        observability,
                    ) {
                        Ok(child_actions) => {
                            actions.extend(child_actions);
                            let output = child_context
                                .slot_value(slot_index)
                                .cloned()
                                .unwrap_or(JsonValue::Null);

                            let mut entry = JsonMap::new();
                            entry.insert(
                                "status".to_string(),
                                JsonValue::String("pending".to_string()),
                            );
                            entry.insert("output".to_string(), output);
                            entry.insert("optional".to_string(), JsonValue::Bool(child.optional));
                            entry.insert(
                                "fallback".to_string(),
                                child
                                    .fallback
                                    .as_ref()
                                    .map(|f| JsonValue::String(f.clone()))
                                    .unwrap_or(JsonValue::Null),
                            );
                            entry.insert(
                                "timeout_ms".to_string(),
                                child
                                    .timeout_ms
                                    .map(|ms| JsonValue::Number(JsonNumber::from(ms)))
                                    .unwrap_or(JsonValue::Null),
                            );
                            entry.insert("error".to_string(), JsonValue::Null);
                            children_map.insert(child.name.clone(), JsonValue::Object(entry));
                        }
                        Err(err) => {
                            let mut entry = JsonMap::new();
                            entry.insert("output".to_string(), JsonValue::Null);
                            entry.insert("optional".to_string(), JsonValue::Bool(child.optional));
                            entry.insert(
                                "fallback".to_string(),
                                child
                                    .fallback
                                    .as_ref()
                                    .map(|f| JsonValue::String(f.clone()))
                                    .unwrap_or(JsonValue::Null),
                            );
                            entry.insert(
                                "timeout_ms".to_string(),
                                child
                                    .timeout_ms
                                    .map(|ms| JsonValue::Number(JsonNumber::from(ms)))
                                    .unwrap_or(JsonValue::Null),
                            );
                            entry.insert("error".to_string(), JsonValue::String(err.to_string()));

                            if child.optional {
                                entry.insert(
                                    "status".to_string(),
                                    JsonValue::String("skipped".to_string()),
                                );
                                encountered_optional_failure = true;
                                children_map.insert(child.name.clone(), JsonValue::Object(entry));
                            } else {
                                entry.insert(
                                    "status".to_string(),
                                    JsonValue::String("failed".to_string()),
                                );
                                children_map.insert(child.name.clone(), JsonValue::Object(entry));
                                return Err(err);
                            }
                        }
                    }
                }

                let aggregation_mode = match parallel_plan.aggregation {
                    ParallelAggregation::All => "all",
                    ParallelAggregation::Any => "any",
                    ParallelAggregation::Quorum { .. } => "quorum",
                };

                let aggregation_state = if encountered_optional_failure {
                    "partial"
                } else {
                    "pending"
                };

                let mut record = JsonMap::new();
                record.insert(
                    "aggregation".to_string(),
                    JsonValue::String(aggregation_mode.to_string()),
                );

                if let ParallelAggregation::Quorum { threshold } = parallel_plan.aggregation {
                    record.insert(
                        "quorum".to_string(),
                        JsonValue::Number(JsonNumber::from(threshold as u64)),
                    );
                }

                if let Some(timeout) = parallel_plan.timeout_ms {
                    record.insert(
                        "timeout_ms".to_string(),
                        JsonValue::Number(JsonNumber::from(timeout)),
                    );
                }

                record.insert(
                    "aggregation_state".to_string(),
                    JsonValue::String(aggregation_state.to_string()),
                );
                record.insert("children".to_string(), JsonValue::Object(children_map));

                context.insert_slot(slot_index, JsonValue::Object(record));
            }
            PhasePlan::Mongodb(mongo_plan) => {
                let (connector_name, config) = mongo_plan
                    .connector
                    .clone()
                    .expect_mongodb(&plan.name, &mongo_plan.name)?;

                let operation = match mongo_plan.operation {
                    MongoOperation::InsertOne => "insert_one",
                    MongoOperation::ReplaceOne => "replace_one",
                    MongoOperation::UpdateOne => "update_one",
                    MongoOperation::DeleteOne => "delete_one",
                    MongoOperation::Aggregate => "aggregate",
                }
                .to_string();

                let document = if let Some(template) = mongo_plan.document_template.as_ref() {
                    Some(context.resolve_template(template).map_err(|err| {
                        map_context_error(err, &plan.name, Some(&mongo_plan.name))
                    })?)
                } else {
                    None
                };

                let filter = if let Some(template) = mongo_plan.filter_template.as_ref() {
                    Some(context.resolve_template(template).map_err(|err| {
                        map_context_error(err, &plan.name, Some(&mongo_plan.name))
                    })?)
                } else {
                    None
                };

                let update = if let Some(template) = mongo_plan.update_template.as_ref() {
                    Some(context.resolve_template(template).map_err(|err| {
                        map_context_error(err, &plan.name, Some(&mongo_plan.name))
                    })?)
                } else {
                    None
                };

                let pipeline = if let Some(template) = mongo_plan.pipeline_template.as_ref() {
                    Some(context.resolve_template(template).map_err(|err| {
                        map_context_error(err, &plan.name, Some(&mongo_plan.name))
                    })?)
                } else {
                    None
                };

                tracing::info!(
                    target: "chronicle::engine",
                    event = "mongodb_operation_enqueued",
                    chronicle = %plan.name,
                    phase = %phase_name,
                    connector = %connector_name,
                    collection = %mongo_plan.collection,
                    operation = %operation,
                    trace_id = trace_field,
                    record_id = record_field
                );

                actions.push(ChronicleAction::MongodbCommand {
                    connector: connector_name.clone(),
                    uri: config.uri.clone(),
                    database: config.database.clone(),
                    options: config.options.clone(),
                    collection: mongo_plan.collection.clone(),
                    operation: operation.clone(),
                    document: document.clone(),
                    filter: filter.clone(),
                    update: update.clone(),
                    pipeline: pipeline.clone(),
                    timeout_ms: mongo_plan.timeout_ms,
                    trace_id: observability.trace_id.clone(),
                    record_id: observability.record_id.clone(),
                    extra: mongo_plan.extra.clone(),
                });

                let mut context_record = JsonMap::new();
                context_record.insert("connector".to_string(), JsonValue::String(connector_name));
                context_record.insert(
                    "collection".to_string(),
                    JsonValue::String(mongo_plan.collection.clone()),
                );
                context_record.insert("operation".to_string(), JsonValue::String(operation));
                if let Some(timeout) = mongo_plan.timeout_ms {
                    context_record.insert(
                        "timeout_ms".to_string(),
                        JsonValue::Number(JsonNumber::from(timeout)),
                    );
                }
                context_record.insert("status".to_string(), JsonValue::String("pending".into()));
                context_record.insert("inserted_id".to_string(), JsonValue::Null);
                context_record.insert("matched_count".to_string(), JsonValue::Null);
                context_record.insert("modified_count".to_string(), JsonValue::Null);
                context_record.insert("deleted_count".to_string(), JsonValue::Null);
                context_record.insert("results".to_string(), JsonValue::Null);

                if let Some(doc) = document {
                    context_record.insert("document".to_string(), doc);
                }
                if let Some(flt) = filter {
                    context_record.insert("filter".to_string(), flt);
                }
                if let Some(upd) = update {
                    context_record.insert("update".to_string(), upd);
                }
                if let Some(pipe) = pipeline {
                    context_record.insert("pipeline".to_string(), pipe);
                }

                context.insert_slot(slot_index, JsonValue::Object(context_record));
            }
            PhasePlan::Redis(redis_plan) => {
                let (connector_name, config) = redis_plan
                    .connector
                    .clone()
                    .expect_redis(&plan.name, &redis_plan.name)?;

                let resolved_arguments = context
                    .resolve_template(&redis_plan.arguments)
                    .map_err(|err| map_context_error(err, &plan.name, Some(&redis_plan.name)))?;

                tracing::info!(
                    target: "chronicle::engine",
                    event = "redis_command_enqueued",
                    chronicle = %plan.name,
                    phase = %phase_name,
                    connector = %connector_name,
                    command = %redis_plan.command,
                    trace_id = trace_field,
                    record_id = record_field
                );

                actions.push(ChronicleAction::RedisCommand {
                    connector: connector_name.clone(),
                    url: config.url.clone(),
                    cluster: config.cluster,
                    command: redis_plan.command.clone(),
                    arguments: resolved_arguments.clone(),
                    timeout_ms: redis_plan.timeout_ms,
                    trace_id: observability.trace_id.clone(),
                    record_id: observability.record_id.clone(),
                    extra: JsonValue::Null,
                });

                let mut context_record = JsonMap::new();
                context_record.insert("connector".to_string(), JsonValue::String(connector_name));
                context_record.insert(
                    "command".to_string(),
                    JsonValue::String(redis_plan.command.clone()),
                );
                if let Some(timeout) = redis_plan.timeout_ms {
                    context_record.insert(
                        "timeout_ms".to_string(),
                        JsonValue::Number(JsonNumber::from(timeout)),
                    );
                }
                context_record.insert("status".to_string(), JsonValue::String("pending".into()));
                context_record.insert("result".to_string(), JsonValue::Null);
                context_record.insert("arguments".to_string(), resolved_arguments);

                context.insert_slot(slot_index, JsonValue::Object(context_record));
            }
            PhasePlan::SmtpSend {
                name,
                connector,
                from_template,
                to_template,
                cc_template,
                bcc_template,
                reply_to_template,
                subject_template,
                body_template,
                headers_template,
                attachments_template,
                extra,
            } => {
                let (connector_name, config) = connector.clone().expect_smtp(&plan.name, name)?;

                let mut resolved_from = resolve_optional_string_field(
                    &plan.name,
                    name,
                    from_template.as_ref(),
                    context,
                )?;
                if resolved_from.is_none() {
                    resolved_from = config.default_from.clone();
                }

                let to = resolve_email_recipients(
                    &plan.name,
                    name,
                    "to",
                    to_template.as_ref(),
                    context,
                )?;
                let cc = resolve_email_recipients(
                    &plan.name,
                    name,
                    "cc",
                    cc_template.as_ref(),
                    context,
                )?;
                let bcc = resolve_email_recipients(
                    &plan.name,
                    name,
                    "bcc",
                    bcc_template.as_ref(),
                    context,
                )?;

                if to.is_empty() && cc.is_empty() && bcc.is_empty() {
                    return Err(ChronicleEngineError::InvalidPhaseOptions {
                        chronicle: plan.name.clone(),
                        phase: name.clone(),
                        reason:
                            "smtp phase requires at least one recipient across `to`, `cc`, or `bcc`"
                                .to_string(),
                    });
                }

                let reply_to = resolve_optional_string_field(
                    &plan.name,
                    name,
                    reply_to_template.as_ref(),
                    context,
                )?;

                let subject = resolve_optional_string_field(
                    &plan.name,
                    name,
                    subject_template.as_ref(),
                    context,
                )?;

                let body_value = if let Some(template) = body_template.as_ref() {
                    context
                        .resolve_template(template)
                        .map_err(|err| map_context_error(err, &plan.name, Some(name)))?
                } else {
                    JsonValue::Null
                };

                let headers_value = resolve_template_to_object(
                    &plan.name,
                    name,
                    headers_template.as_ref(),
                    context,
                    "headers",
                )?;

                let attachments_value = if let Some(template) = attachments_template.as_ref() {
                    context
                        .resolve_template(template)
                        .map_err(|err| map_context_error(err, &plan.name, Some(name)))?
                } else {
                    JsonValue::Null
                };

                let mut message = JsonMap::new();

                if let Some(from) = resolved_from.clone() {
                    message.insert("from".to_string(), JsonValue::String(from));
                }

                if !to.is_empty() {
                    message.insert(
                        "to".to_string(),
                        JsonValue::Array(
                            to.iter()
                                .map(|addr| JsonValue::String(addr.clone()))
                                .collect(),
                        ),
                    );
                }

                if !cc.is_empty() {
                    message.insert(
                        "cc".to_string(),
                        JsonValue::Array(
                            cc.iter()
                                .map(|addr| JsonValue::String(addr.clone()))
                                .collect(),
                        ),
                    );
                }

                if !bcc.is_empty() {
                    message.insert(
                        "bcc".to_string(),
                        JsonValue::Array(
                            bcc.iter()
                                .map(|addr| JsonValue::String(addr.clone()))
                                .collect(),
                        ),
                    );
                }

                if let Some(reply) = reply_to.clone() {
                    message.insert("reply_to".to_string(), JsonValue::String(reply));
                }

                if let Some(subject) = subject.clone() {
                    message.insert("subject".to_string(), JsonValue::String(subject));
                }

                if !body_value.is_null() {
                    message.insert("body".to_string(), body_value.clone());
                }

                if let JsonValue::Object(map) = &headers_value {
                    if !map.is_empty() {
                        message.insert("headers".to_string(), headers_value.clone());
                    }
                }

                if !attachments_value.is_null() {
                    message.insert("attachments".to_string(), attachments_value.clone());
                }

                let message_value = JsonValue::Object(message.clone());

                tracing::info!(
                    target: "chronicle::engine",
                    event = "smtp_email_enqueued",
                    chronicle = %plan.name,
                    phase = %phase_name,
                    connector = %connector_name,
                    host = %config.host,
                    port = config.port,
                    trace_id = trace_field,
                    record_id = record_field
                );

                actions.push(ChronicleAction::SmtpEmail {
                    connector: connector_name.clone(),
                    host: config.host.clone(),
                    port: config.port,
                    tls_mode: config
                        .tls
                        .as_ref()
                        .map(|tls| smtp_tls_mode_to_string(&tls.mode).to_string()),
                    requires_auth: config.auth.is_some(),
                    timeout_secs: config.timeout_secs,
                    message: message_value.clone(),
                    extra: extra.clone(),
                    trace_id: observability.trace_id.clone(),
                    record_id: observability.record_id.clone(),
                });

                let mut context_record = JsonMap::new();
                context_record.insert("connector".to_string(), JsonValue::String(connector_name));
                if let Some(from) = resolved_from {
                    context_record.insert("from".to_string(), JsonValue::String(from));
                }
                if let Some(subject) = subject {
                    context_record.insert("subject".to_string(), JsonValue::String(subject));
                }
                context_record.insert(
                    "recipients".to_string(),
                    json!({
                        "to": to,
                        "cc": cc,
                        "bcc": bcc,
                    }),
                );
                context_record.insert("status".to_string(), JsonValue::String("pending".into()));
                context_record.insert("message".to_string(), message_value);

                context.insert_slot(slot_index, JsonValue::Object(context_record));
            }
            PhasePlan::Serialize(plan_cfg) => {
                let input = if let Some(template) = plan_cfg.input_template.as_ref() {
                    context
                        .resolve_template(template)
                        .map_err(|err| map_context_error(err, &plan.name, Some(&plan_cfg.name)))?
                } else {
                    context
                        .slot_value(slot_index - 1)
                        .cloned()
                        .unwrap_or(JsonValue::Null)
                };
                let encoded = plan_cfg.encode(&plan.name, input)?;
                context.insert_slot(slot_index, encoded);
            }
            PhasePlan::MariadbInsert {
                name,
                connector,
                key_template,
                value_template,
                timeout_ms,
                extra,
            } => {
                let (connector_name, handle) =
                    connector.clone().expect_mariadb(&plan.name, name)?;

                let key = context
                    .resolve_template(key_template)
                    .map_err(|err| map_context_error(err, &plan.name, Some(name)))?;

                let values = if let Some(template) = value_template.as_ref() {
                    context
                        .resolve_template(template)
                        .map_err(|err| map_context_error(err, &plan.name, Some(name)))?
                } else {
                    context
                        .slot_value(slot_index - 1)
                        .cloned()
                        .unwrap_or(JsonValue::Null)
                };
                let key_snapshot = key.clone();
                let values_snapshot = values.clone();

                let context_connector = connector_name.clone();

                tracing::info!(
                    target: "chronicle::engine",
                    event = "mariadb_insert_enqueued",
                    chronicle = %plan.name,
                    phase = %phase_name,
                    connector = %connector_name,
                    trace_id = trace_field,
                    record_id = record_field
                );

                actions.push(ChronicleAction::MariadbInsert {
                    connector: connector_name,
                    url: handle.url.clone(),
                    schema: handle.schema.clone(),
                    key,
                    values,
                    timeout_ms: *timeout_ms,
                    extra: extra.clone(),
                    trace_id: observability.trace_id.clone(),
                    record_id: observability.record_id.clone(),
                });

                let mut context_record = JsonMap::new();
                context_record.insert(
                    "connector".to_string(),
                    JsonValue::String(context_connector),
                );
                context_record.insert("key".to_string(), key_snapshot);
                context_record.insert("values".to_string(), values_snapshot);
                if let Some(timeout) = *timeout_ms {
                    context_record.insert(
                        "timeout_ms".to_string(),
                        JsonValue::Number(JsonNumber::from(timeout)),
                    );
                }
                context_record.insert("status".to_string(), JsonValue::String("pending".into()));
                context_record.insert(
                    "count".to_string(),
                    JsonValue::Number(JsonNumber::from(1_u64)),
                );

                context.insert_slot(slot_index, JsonValue::Object(context_record));
            }
            PhasePlan::PostgresInsert {
                name,
                connector,
                sql_template,
                values_template,
                extra,
                timeout_ms,
            } => {
                let (connector_name, handle) =
                    connector.clone().expect_postgres(&plan.name, name)?;

                let sql = context
                    .resolve_to_string(sql_template)
                    .map_err(|err| map_context_error(err, &plan.name, Some(name)))?;

                let parameters = if let Some(template) = values_template.as_ref() {
                    context
                        .resolve_template(template)
                        .map_err(|err| map_context_error(err, &plan.name, Some(name)))?
                } else {
                    context
                        .slot_value(slot_index - 1)
                        .cloned()
                        .unwrap_or(JsonValue::Null)
                };
                let sql_snapshot = sql.clone();
                let parameters_snapshot = parameters.clone();

                tracing::info!(
                    target: "chronicle::engine",
                    event = "postgres_query_enqueued",
                    chronicle = %plan.name,
                    phase = %phase_name,
                    connector = %connector_name,
                    trace_id = trace_field,
                    record_id = record_field,
                    sql = %sql
                );

                let context_connector = connector_name.clone();

                actions.push(ChronicleAction::PostgresQuery {
                    connector: connector_name,
                    url: handle.url.clone(),
                    schema: handle.schema.clone(),
                    sql,
                    parameters,
                    extra: extra.clone(),
                    trace_id: observability.trace_id.clone(),
                    record_id: observability.record_id.clone(),
                    timeout_ms: *timeout_ms,
                });

                let mut context_record = JsonMap::new();
                context_record.insert(
                    "connector".to_string(),
                    JsonValue::String(context_connector),
                );
                context_record.insert("sql".to_string(), JsonValue::String(sql_snapshot));
                context_record.insert("parameters".to_string(), parameters_snapshot);
                if let Some(ms) = timeout_ms {
                    context_record.insert(
                        "timeout_ms".to_string(),
                        JsonValue::Number(JsonNumber::from(*ms)),
                    );
                }
                context_record.insert("status".to_string(), JsonValue::String("pending".into()));

                context.insert_slot(slot_index, JsonValue::Object(context_record));
            }
            PhasePlan::Unsupported { name, kind } => {
                return Err(ChronicleEngineError::UnsupportedPhase {
                    chronicle: plan.name.clone(),
                    phase: name.clone(),
                    kind: kind.clone(),
                });
            }
        }

        Ok(actions)
    }

    pub fn route_metadata(&self, chronicle_name: &str) -> Option<RouteMetadata> {
        self.plans
            .get(chronicle_name)
            .map(|plan| plan.route_metadata())
    }
}

fn build_http_response(context: &ExecutionContext, phase_count: usize) -> Option<HttpResponse> {
    let slot_index = if phase_count > 0 { phase_count } else { 0 };
    let slot_value = context.slot_value(slot_index)?;
    let map = slot_value.as_object()?;

    let status = map.get("status").and_then(|value| value.as_i64())?;
    let status = u16::try_from(status).unwrap_or(200);
    let content_type = map
        .get("contentType")
        .and_then(|value| value.as_str())
        .map(|s| s.to_string());
    let body = map.get("body").cloned().unwrap_or(JsonValue::Null);

    Some(HttpResponse {
        status,
        content_type,
        body,
    })
}

fn map_context_error(
    err: ContextError,
    chronicle: &str,
    phase: Option<&str>,
) -> ChronicleEngineError {
    let location = match phase {
        Some(name) => format!("chronicle `{}`, phase `{}`", chronicle, name),
        None => format!("chronicle `{}`", chronicle),
    };

    ChronicleEngineError::ContextResolution {
        location,
        expression: err.expression().to_string(),
        reason: err.to_string(),
    }
}

#[derive(Clone, Debug)]
pub struct ChronicleExecution {
    pub name: String,
    pub actions: Vec<ChronicleAction>,
    pub context: Vec<JsonValue>,
    pub response: Option<HttpResponse>,
    pub trace_id: Option<String>,
    pub record_id: Option<String>,
    pub delivery: Option<DeliveryPolicy>,
    pub allow_partial_delivery: bool,
    pub fallback: Option<BTreeMap<String, String>>,
    pub fallback_actions: Option<BTreeMap<String, Vec<ChronicleAction>>>,
    pub retry_budget: Option<RetryBudget>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DependencySource {
    Inferred,
    Explicit,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RouteRuntimeLimits {
    pub max_inflight: Option<u32>,
    pub overflow_policy: Option<OverflowPolicy>,
    pub max_queue_depth: Option<u32>,
}

impl RouteRuntimeLimits {
    fn from_route_limits(limits: &RouteLimits) -> Self {
        Self {
            max_inflight: limits.max_inflight,
            overflow_policy: limits.overflow_policy,
            max_queue_depth: limits.max_queue_depth,
        }
    }

    fn merge(
        inherited: Option<&RouteRuntimeLimits>,
        override_: Option<&RouteRuntimeLimits>,
    ) -> Self {
        let max_inflight = override_
            .and_then(|limits| limits.max_inflight)
            .or_else(|| inherited.and_then(|limits| limits.max_inflight));
        let overflow_policy = override_
            .and_then(|limits| limits.overflow_policy)
            .or_else(|| inherited.and_then(|limits| limits.overflow_policy));
        let max_queue_depth = override_
            .and_then(|limits| limits.max_queue_depth)
            .or_else(|| inherited.and_then(|limits| limits.max_queue_depth));

        Self {
            max_inflight,
            overflow_policy,
            max_queue_depth,
        }
    }
}

#[derive(Clone)]
struct RouteConcurrency {
    state: Option<Arc<RouteLimitState>>,
}

impl RouteConcurrency {
    fn new(limits: &RouteRuntimeLimits) -> Self {
        match limits.max_inflight {
            Some(0) | None => Self { state: None },
            Some(limit) => {
                let policy = limits.overflow_policy.unwrap_or(OverflowPolicy::Reject);
                let state = RouteLimitState::new(limit, policy, limits.max_queue_depth);
                Self { state: Some(state) }
            }
        }
    }

    fn acquire(&self, chronicle: &str) -> Result<RoutePermit, ChronicleEngineError> {
        match &self.state {
            Some(state) => state.acquire(chronicle),
            None => Ok(RoutePermit::unbounded()),
        }
    }
}

#[derive(Debug)]
struct RouteLimitState {
    limit: u32,
    policy: OverflowPolicy,
    max_queue_depth: Option<u32>,
    inner: Arc<RouteLimitInner>,
}

impl RouteLimitState {
    fn new(limit: u32, policy: OverflowPolicy, max_queue_depth: Option<u32>) -> Arc<Self> {
        Arc::new(Self {
            limit,
            policy,
            max_queue_depth,
            inner: Arc::new(RouteLimitInner::default()),
        })
    }

    fn acquire(self: &Arc<Self>, chronicle: &str) -> Result<RoutePermit, ChronicleEngineError> {
        let mut counts = lock_counts(&self.inner.counts);
        if counts.inflight < self.limit {
            counts.inflight += 1;
            return Ok(RoutePermit::limited(self.clone()));
        }

        match self.policy {
            OverflowPolicy::Reject => {
                metrics().record_limit_enforcement(chronicle, "reject");
                Err(ChronicleEngineError::RouteBackpressure {
                    chronicle: chronicle.to_string(),
                    policy: self.policy,
                    limit: self.limit,
                })
            }
            OverflowPolicy::Shed => {
                let counters = metrics();
                counters.record_limit_enforcement(chronicle, "shed");
                counters.record_route_shed(chronicle);
                Err(ChronicleEngineError::RouteBackpressure {
                    chronicle: chronicle.to_string(),
                    policy: self.policy,
                    limit: self.limit,
                })
            }
            OverflowPolicy::Queue => {
                let max_depth = self.max_queue_depth.unwrap_or(1024);
                if counts.waiting >= max_depth {
                    metrics().record_limit_enforcement(chronicle, "queue");
                    return Err(ChronicleEngineError::RouteQueueOverflow {
                        chronicle: chronicle.to_string(),
                        max_queue_depth: max_depth,
                    });
                }

                counts.waiting += 1;
                let collector = metrics();
                collector.set_route_queue_depth(chronicle, counts.waiting);
                loop {
                    counts = wait_counts(&self.inner.condvar, counts);
                    if counts.inflight < self.limit {
                        counts.waiting = counts.waiting.saturating_sub(1);
                        collector.set_route_queue_depth(chronicle, counts.waiting);
                        counts.inflight += 1;
                        return Ok(RoutePermit::limited(self.clone()));
                    }
                }
            }
        }
    }

    fn release(&self) {
        let mut counts = lock_counts(&self.inner.counts);
        if counts.inflight > 0 {
            counts.inflight -= 1;
        }
        self.inner.condvar.notify_one();
    }
}

#[derive(Debug, Default)]
struct RouteLimitInner {
    counts: Mutex<RouteLimitCounts>,
    condvar: Condvar,
}

#[derive(Debug, Default)]
struct RouteLimitCounts {
    inflight: u32,
    waiting: u32,
}

#[derive(Debug)]
struct RoutePermit {
    state: Option<Arc<RouteLimitState>>,
}

impl RoutePermit {
    fn unbounded() -> Self {
        Self { state: None }
    }

    fn limited(state: Arc<RouteLimitState>) -> Self {
        Self { state: Some(state) }
    }
}

impl Drop for RoutePermit {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            state.release();
        }
    }
}

fn lock_counts(mutex: &Mutex<RouteLimitCounts>) -> MutexGuard<'_, RouteLimitCounts> {
    mutex.lock().unwrap_or_else(|err| err.into_inner())
}

fn wait_counts<'a>(
    condvar: &Condvar,
    guard: MutexGuard<'a, RouteLimitCounts>,
) -> MutexGuard<'a, RouteLimitCounts> {
    condvar.wait(guard).unwrap_or_else(|err| err.into_inner())
}

#[derive(Clone, Debug, PartialEq)]
pub struct RouteMetadata {
    pub name: String,
    pub dependencies: Vec<String>,
    pub dependency_source: DependencySource,
    pub inferred_dependencies: Vec<String>,
    pub allow_partial_delivery: bool,
    pub readiness_priority: Option<i32>,
    pub retry_budget: Option<RetryBudget>,
    pub half_open_counts_as_ready: Option<HalfOpenPolicy>,
    pub delivery: Option<DeliveryPolicy>,
    pub fallback: Option<BTreeMap<String, String>>,
    pub limits: RouteRuntimeLimits,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HttpResponse {
    pub status: u16,
    pub content_type: Option<String>,
    pub body: JsonValue,
}

#[derive(Clone, Debug)]
pub enum ChronicleAction {
    KafkaPublish {
        connector: String,
        brokers: Vec<String>,
        topic: String,
        create_topics_if_missing: bool,
        payload: JsonValue,
        acks: Option<String>,
        extra: JsonValue,
        trace_id: Option<String>,
        record_id: Option<String>,
    },
    HttpRequest {
        connector: String,
        base_url: String,
        method: String,
        path: String,
        headers: BTreeMap<String, String>,
        body: JsonValue,
        content_type: Option<String>,
        timeout_ms: Option<u64>,
        extra: JsonValue,
        trace_id: Option<String>,
        record_id: Option<String>,
    },
    GrpcRequest {
        connector: String,
        endpoint: String,
        service: String,
        method: String,
        descriptor_path: PathBuf,
        request: JsonValue,
        metadata: Vec<(String, String)>,
        timeout_ms: Option<u64>,
        extra: JsonValue,
        trace_id: Option<String>,
        record_id: Option<String>,
    },
    MqttPublish {
        connector: String,
        url: String,
        topic: String,
        qos: u8,
        retain: bool,
        payload: JsonValue,
        payload_encoding: String,
        encoded_payload: String,
        packet_id: Option<u64>,
        published_at: String,
        extra: JsonValue,
        trace_id: Option<String>,
        record_id: Option<String>,
        timeout_ms: Option<u64>,
    },
    RabbitmqPublish {
        connector: String,
        url: String,
        exchange: Option<String>,
        routing_key: Option<String>,
        payload: JsonValue,
        headers: JsonValue,
        properties: JsonValue,
        mandatory: Option<bool>,
        confirm: Option<bool>,
        timeout_ms: Option<u64>,
        extra: JsonValue,
        trace_id: Option<String>,
        record_id: Option<String>,
    },
    SmtpEmail {
        connector: String,
        host: String,
        port: u16,
        tls_mode: Option<String>,
        requires_auth: bool,
        timeout_secs: Option<u64>,
        message: JsonValue,
        extra: JsonValue,
        trace_id: Option<String>,
        record_id: Option<String>,
    },
    MariadbInsert {
        connector: String,
        url: String,
        schema: Option<String>,
        key: JsonValue,
        values: JsonValue,
        timeout_ms: Option<u64>,
        extra: JsonValue,
        trace_id: Option<String>,
        record_id: Option<String>,
    },
    PostgresQuery {
        connector: String,
        url: String,
        schema: Option<String>,
        sql: String,
        parameters: JsonValue,
        extra: JsonValue,
        trace_id: Option<String>,
        record_id: Option<String>,
        timeout_ms: Option<u64>,
    },
    MongodbCommand {
        connector: String,
        uri: String,
        database: String,
        options: Option<JsonValue>,
        collection: String,
        operation: String,
        document: Option<JsonValue>,
        filter: Option<JsonValue>,
        update: Option<JsonValue>,
        pipeline: Option<JsonValue>,
        timeout_ms: Option<u64>,
        trace_id: Option<String>,
        record_id: Option<String>,
        extra: JsonValue,
    },
    RedisCommand {
        connector: String,
        url: String,
        cluster: Option<bool>,
        command: String,
        arguments: JsonValue,
        timeout_ms: Option<u64>,
        trace_id: Option<String>,
        record_id: Option<String>,
        extra: JsonValue,
    },
}

#[derive(Clone, Debug)]
struct RoutePolicyPlan {
    dependencies: RouteDependencies,
    allow_partial_delivery: bool,
    readiness_priority: Option<i32>,
    retry_budget: Option<RetryBudget>,
    half_open_counts_as_ready: Option<HalfOpenPolicy>,
    delivery: Option<DeliveryPolicy>,
    fallback: Option<BTreeMap<String, String>>,
    limits: RouteLimitsPlan,
}

impl RoutePolicyPlan {
    fn from_definition(policy: &ChroniclePolicy, app: &AppConfig, phases: &[PhasePlan]) -> Self {
        let dependencies = RouteDependencies::build(policy, phases);
        let limits = RouteLimitsPlan::from_app_and_policy(app, policy);
        let mut connector_budgets: Vec<Option<&RetryBudget>> = Vec::new();
        for phase in phases {
            phase.collect_retry_budgets(&mut connector_budgets);
        }
        let mut scopes = Vec::with_capacity(2 + connector_budgets.len());
        scopes.push(app.retry_budget.as_ref());
        scopes.extend(connector_budgets.iter().copied());
        scopes.push(policy.retry_budget.as_ref());
        let effective_retry_budget =
            merge_retry_budgets(scopes).or_else(|| app.retry_budget.clone());

        Self {
            dependencies,
            allow_partial_delivery: policy.allow_partial_delivery,
            readiness_priority: policy.readiness_priority,
            retry_budget: effective_retry_budget,
            half_open_counts_as_ready: policy.half_open_counts_as_ready,
            delivery: policy.delivery.clone(),
            fallback: policy.fallback.clone(),
            limits,
        }
    }

    fn to_metadata(&self, chronicle: &str) -> RouteMetadata {
        RouteMetadata {
            name: chronicle.to_string(),
            dependencies: self.dependencies.names.clone(),
            dependency_source: self.dependencies.dependency_source(),
            inferred_dependencies: self.dependencies.inferred.clone(),
            allow_partial_delivery: self.allow_partial_delivery,
            readiness_priority: self.readiness_priority,
            retry_budget: self.retry_budget.clone(),
            half_open_counts_as_ready: self.half_open_counts_as_ready,
            delivery: self.delivery.clone(),
            fallback: self.fallback.clone(),
            limits: self.limits.effective.clone(),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct RouteLimitsPlan {
    effective: RouteRuntimeLimits,
}

impl RouteLimitsPlan {
    fn from_app_and_policy(app: &AppConfig, policy: &ChroniclePolicy) -> Self {
        let inherited = app
            .limits
            .routes
            .as_ref()
            .map(RouteRuntimeLimits::from_route_limits);
        let override_ = policy
            .limits
            .as_ref()
            .map(RouteRuntimeLimits::from_route_limits);
        let effective = RouteRuntimeLimits::merge(inherited.as_ref(), override_.as_ref());

        Self { effective }
    }
}

#[derive(Clone, Debug, Default)]
struct RouteDependencies {
    names: Vec<String>,
    inferred: Vec<String>,
    explicit: bool,
}

impl RouteDependencies {
    fn build(policy: &ChroniclePolicy, phases: &[PhasePlan]) -> Self {
        let inferred = collect_phase_connectors(phases);

        if !policy.requires.is_empty() {
            let names = dedupe_connectors(policy.requires.iter().cloned());
            Self {
                names,
                inferred,
                explicit: true,
            }
        } else {
            Self {
                names: inferred.clone(),
                inferred,
                explicit: false,
            }
        }
    }

    fn dependency_source(&self) -> DependencySource {
        if self.explicit {
            DependencySource::Explicit
        } else {
            DependencySource::Inferred
        }
    }
}

impl PhasePlan {
    fn collect_connectors(&self, connectors: &mut BTreeSet<String>) {
        match self {
            PhasePlan::KafkaProducer { connector, .. }
            | PhasePlan::HttpClient { connector, .. }
            | PhasePlan::GrpcRequest { connector, .. }
            | PhasePlan::MariadbInsert { connector, .. }
            | PhasePlan::PostgresInsert { connector, .. }
            | PhasePlan::RabbitmqPublish { connector, .. }
            | PhasePlan::MqttPublish { connector, .. }
            | PhasePlan::SmtpSend { connector, .. }
            | PhasePlan::Mongodb(MongodbPhasePlan { connector, .. })
            | PhasePlan::Redis(RedisPhasePlan { connector, .. }) => {
                connectors.insert(connector.name().to_string());
            }
            PhasePlan::Parallel(plan) => {
                for child in &plan.children {
                    child.plan.collect_connectors(connectors);
                }
            }
            PhasePlan::Serialize(_)
            | PhasePlan::Transform { .. }
            | PhasePlan::Unsupported { .. } => {}
        }
    }

    fn collect_retry_budgets<'a>(&'a self, budgets: &mut Vec<Option<&'a RetryBudget>>) {
        match self {
            PhasePlan::KafkaProducer { connector, .. }
            | PhasePlan::HttpClient { connector, .. }
            | PhasePlan::GrpcRequest { connector, .. }
            | PhasePlan::MariadbInsert { connector, .. }
            | PhasePlan::PostgresInsert { connector, .. }
            | PhasePlan::RabbitmqPublish { connector, .. }
            | PhasePlan::MqttPublish { connector, .. }
            | PhasePlan::SmtpSend { connector, .. }
            | PhasePlan::Mongodb(MongodbPhasePlan { connector, .. })
            | PhasePlan::Redis(RedisPhasePlan { connector, .. }) => {
                budgets.push(connector.retry_budget())
            }
            PhasePlan::Parallel(plan) => {
                for child in &plan.children {
                    child.plan.collect_retry_budgets(budgets);
                }
            }
            PhasePlan::Serialize(_)
            | PhasePlan::Transform { .. }
            | PhasePlan::Unsupported { .. } => {}
        }
    }
}

fn collect_phase_connectors(phases: &[PhasePlan]) -> Vec<String> {
    let mut connectors = BTreeSet::new();

    for phase in phases {
        phase.collect_connectors(&mut connectors);
    }

    connectors.into_iter().collect()
}

fn dedupe_connectors<I>(items: I) -> Vec<String>
where
    I: IntoIterator<Item = String>,
{
    let mut connectors = BTreeSet::new();
    for item in items {
        let trimmed = item.trim();
        if !trimmed.is_empty() {
            connectors.insert(trimmed.to_string());
        }
    }
    connectors.into_iter().collect()
}

#[derive(Clone)]
struct ChroniclePlan {
    name: String,
    _trigger: TriggerPlan,
    phases: Vec<PhasePlan>,
    fallbacks: BTreeMap<String, PhasePlan>,
    policy: RoutePolicyPlan,
    concurrency: RouteConcurrency,
}

impl ChroniclePlan {
    fn build(
        chronicle: &ChronicleDefinition,
        app: &AppConfig,
        registry: &ConnectorRegistry,
    ) -> Result<Self, ChronicleEngineError> {
        let trigger = TriggerPlan::from_definition(chronicle, registry);

        let mut phases = Vec::with_capacity(chronicle.phases.len());
        for phase in &chronicle.phases {
            let plan = PhasePlan::from_definition(&chronicle.name, phase, registry)?;
            phases.push(plan);
        }

        let fallback_target_set: BTreeSet<String> = chronicle
            .policy
            .fallback
            .as_ref()
            .map(|map| map.values().cloned().collect())
            .unwrap_or_default();

        let (phases, fallbacks) = if fallback_target_set.is_empty() {
            (phases, BTreeMap::new())
        } else {
            let mut retained = Vec::with_capacity(phases.len());
            let mut fallback_plans = BTreeMap::new();
            for plan in phases.into_iter() {
                let label = plan.name().to_string();
                if fallback_target_set.contains(&label) {
                    fallback_plans.insert(label, plan);
                } else {
                    retained.push(plan);
                }
            }

            if let Some(missing) = fallback_target_set
                .iter()
                .find(|name| !fallback_plans.contains_key(*name))
            {
                return Err(ChronicleEngineError::UnknownFallbackPhase {
                    chronicle: chronicle.name.clone(),
                    phase: (*missing).clone(),
                });
            }

            (retained, fallback_plans)
        };

        let policy = RoutePolicyPlan::from_definition(&chronicle.policy, app, &phases);
        let concurrency = RouteConcurrency::new(&policy.limits.effective);

        Ok(Self {
            name: chronicle.name.clone(),
            _trigger: trigger,
            phases,
            fallbacks,
            policy,
            concurrency,
        })
    }

    fn trigger_kind(&self) -> &'static str {
        match self._trigger {
            TriggerPlan::Kafka { .. } => "kafka",
            TriggerPlan::Http { .. } => "http",
            TriggerPlan::Unknown { .. } => "unknown",
        }
    }

    fn route_metadata(&self) -> RouteMetadata {
        self.policy.to_metadata(&self.name)
    }

    fn concurrency(&self) -> &RouteConcurrency {
        &self.concurrency
    }
}

#[derive(Clone)]
enum TriggerPlan {
    Kafka {
        _connector: ConnectorHandle,
        _options: JsonValue,
    },
    Http {
        _connector: ConnectorHandle,
        _options: JsonValue,
    },
    Unknown {
        _connector: ConnectorHandle,
        _options: JsonValue,
    },
}

impl TriggerPlan {
    fn from_definition(chronicle: &ChronicleDefinition, registry: &ConnectorRegistry) -> Self {
        if chronicle.trigger.kafka.is_some() {
            let connector = registry
                .kafka(&chronicle.trigger.connector)
                .cloned()
                .map(|handle| ConnectorHandle::Kafka {
                    name: chronicle.trigger.connector.clone(),
                    config: handle,
                })
                .unwrap_or_else(|| ConnectorHandle::Missing {
                    name: chronicle.trigger.connector.clone(),
                    expected: "kafka",
                });

            Self::Kafka {
                _connector: connector,
                _options: chronicle.trigger.kafka_json().unwrap_or(JsonValue::Null),
            }
        } else {
            let connector = registry
                .http_server(&chronicle.trigger.connector)
                .cloned()
                .map(|handle| ConnectorHandle::HttpServer {
                    name: chronicle.trigger.connector.clone(),
                    _config: handle,
                })
                .unwrap_or_else(|| ConnectorHandle::Missing {
                    name: chronicle.trigger.connector.clone(),
                    expected: "http_server",
                });

            let options = chronicle.trigger.options_json();

            match connector {
                ConnectorHandle::HttpServer { .. } => Self::Http {
                    _connector: connector,
                    _options: options,
                },
                _ => Self::Unknown {
                    _connector: connector,
                    _options: options,
                },
            }
        }
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone)]
enum PhasePlan {
    Transform {
        name: String,
        template: JsonValue,
    },
    KafkaProducer {
        name: String,
        connector: ConnectorHandle,
        topic: String,
        payload_template: Option<JsonValue>,
        acks: Option<String>,
        extra: JsonValue,
    },
    HttpClient {
        name: String,
        connector: ConnectorHandle,
        method_template: JsonValue,
        path_template: JsonValue,
        headers_template: Option<JsonValue>,
        body_template: Option<JsonValue>,
        content_type: Option<String>,
        timeout_ms: Option<u64>,
        extra: JsonValue,
    },
    GrpcRequest {
        name: String,
        connector: ConnectorHandle,
        service: String,
        method: String,
        descriptor: PathBuf,
        request_template: Option<JsonValue>,
        metadata_template: Option<JsonValue>,
        timeout_ms: Option<u64>,
        extra: JsonValue,
    },
    RabbitmqPublish {
        name: String,
        connector: ConnectorHandle,
        exchange_template: Option<JsonValue>,
        routing_key_template: Option<JsonValue>,
        body_template: Option<JsonValue>,
        headers_template: Option<JsonValue>,
        properties_template: Option<JsonValue>,
        mandatory: Option<bool>,
        confirm: Option<bool>,
        timeout_ms: Option<u64>,
        extra: JsonValue,
    },
    MqttPublish {
        name: String,
        connector: ConnectorHandle,
        topic_template: JsonValue,
        payload_template: Option<JsonValue>,
        qos_template: Option<JsonValue>,
        retain_template: Option<JsonValue>,
        encoding_template: Option<JsonValue>,
        extra: JsonValue,
        packet_seq: Arc<AtomicU16>,
        timeout_ms: Option<u64>,
    },
    Parallel(ParallelPhasePlan),
    SmtpSend {
        name: String,
        connector: ConnectorHandle,
        from_template: Option<JsonValue>,
        to_template: Option<JsonValue>,
        cc_template: Option<JsonValue>,
        bcc_template: Option<JsonValue>,
        reply_to_template: Option<JsonValue>,
        subject_template: Option<JsonValue>,
        body_template: Option<JsonValue>,
        headers_template: Option<JsonValue>,
        attachments_template: Option<JsonValue>,
        extra: JsonValue,
    },
    Serialize(SerializePhasePlan),
    Mongodb(MongodbPhasePlan),
    Redis(RedisPhasePlan),
    MariadbInsert {
        name: String,
        connector: ConnectorHandle,
        key_template: JsonValue,
        value_template: Option<JsonValue>,
        timeout_ms: Option<u64>,
        extra: JsonValue,
    },
    PostgresInsert {
        name: String,
        connector: ConnectorHandle,
        sql_template: JsonValue,
        values_template: Option<JsonValue>,
        extra: JsonValue,
        timeout_ms: Option<u64>,
    },
    Unsupported {
        name: String,
        kind: String,
    },
}

impl PhasePlan {
    fn from_definition(
        chronicle_name: &str,
        phase: &ChroniclePhase,
        registry: &ConnectorRegistry,
    ) -> Result<Self, ChronicleEngineError> {
        let raw_options = phase.options_json();

        match phase.kind {
            PhaseKind::Transform => Ok(Self::Transform {
                name: phase.name.clone(),
                template: raw_options,
            }),
            PhaseKind::KafkaProducer => {
                let mut options = clone_object(&raw_options).ok_or_else(|| {
                    ChronicleEngineError::InvalidPhaseOptions {
                        chronicle: chronicle_name.to_string(),
                        phase: phase.name.clone(),
                        reason: "kafka producer options must be an object".to_string(),
                    }
                })?;

                let connector_name =
                    take_string(chronicle_name, &phase.name, &mut options, "connector")?;

                let topic = take_string(chronicle_name, &phase.name, &mut options, "topic")?;

                let acks = take_optional_string(&mut options, "acks");
                let payload_template = options.remove("payload");

                let connector = registry
                    .kafka(&connector_name)
                    .cloned()
                    .map(|config| ConnectorHandle::Kafka {
                        name: connector_name.clone(),
                        config,
                    })
                    .unwrap_or_else(|| ConnectorHandle::Missing {
                        name: connector_name.clone(),
                        expected: "kafka",
                    });

                Ok(Self::KafkaProducer {
                    name: phase.name.clone(),
                    connector,
                    topic,
                    payload_template,
                    acks,
                    extra: JsonValue::Object(options),
                })
            }
            PhaseKind::HttpClient => {
                let mut options = clone_object(&raw_options).ok_or_else(|| {
                    ChronicleEngineError::InvalidPhaseOptions {
                        chronicle: chronicle_name.to_string(),
                        phase: phase.name.clone(),
                        reason: "http client options must be an object".to_string(),
                    }
                })?;

                let connector_name =
                    take_string(chronicle_name, &phase.name, &mut options, "connector")?;

                let method_template = options
                    .remove("method")
                    .unwrap_or_else(|| JsonValue::String("GET".to_string()));
                let path_template = options
                    .remove("path")
                    .unwrap_or_else(|| JsonValue::String(String::from("/")));
                let headers_template = options.remove("header");
                let body_template = options.remove("body");
                let content_type = take_optional_string(&mut options, "contentType");
                let timeout_ms =
                    take_optional_timeout_ms(chronicle_name, &phase.name, &mut options)?;

                let connector = registry
                    .http_client(&connector_name)
                    .cloned()
                    .map(|config| ConnectorHandle::HttpClient {
                        name: connector_name.clone(),
                        config,
                    })
                    .unwrap_or_else(|| ConnectorHandle::Missing {
                        name: connector_name.clone(),
                        expected: "http_client",
                    });

                let timeout_ms = timeout_ms.or(connector.request_timeout_ms());

                Ok(Self::HttpClient {
                    name: phase.name.clone(),
                    connector,
                    method_template,
                    path_template,
                    headers_template,
                    body_template,
                    content_type,
                    timeout_ms,
                    extra: JsonValue::Object(options),
                })
            }
            PhaseKind::GrpcClient => {
                let mut options = clone_object(&raw_options).ok_or_else(|| {
                    ChronicleEngineError::InvalidPhaseOptions {
                        chronicle: chronicle_name.to_string(),
                        phase: phase.name.clone(),
                        reason: "grpc client options must be an object".to_string(),
                    }
                })?;

                let connector_name =
                    take_string(chronicle_name, &phase.name, &mut options, "connector")?;

                let rpc = take_optional_string(&mut options, "rpc");
                let service_option = take_optional_string(&mut options, "service");
                let method_option = take_optional_string(&mut options, "method");

                let (service, method) = if let Some(rpc_value) = rpc {
                    let parts: Vec<&str> = rpc_value.split('/').collect();
                    if parts.len() != 2 || parts[0].trim().is_empty() || parts[1].trim().is_empty()
                    {
                        return Err(ChronicleEngineError::InvalidPhaseOptions {
                            chronicle: chronicle_name.to_string(),
                            phase: phase.name.clone(),
                            reason: format!(
                                "grpc client phase option `rpc` must be in `<service>/<method>` form, got `{rpc_value}`"
                            ),
                        });
                    }
                    (parts[0].trim().to_string(), parts[1].trim().to_string())
                } else {
                    let service =
                        service_option.ok_or_else(|| ChronicleEngineError::MissingOption {
                            chronicle: chronicle_name.to_string(),
                            phase: phase.name.clone(),
                            option: "service".to_string(),
                        })?;
                    let method =
                        method_option.ok_or_else(|| ChronicleEngineError::MissingOption {
                            chronicle: chronicle_name.to_string(),
                            phase: phase.name.clone(),
                            option: "method".to_string(),
                        })?;
                    (service, method)
                };

                let descriptor_option = take_optional_string(&mut options, "descriptor");

                let request_template = options.remove("request");
                let metadata_template = options.remove("metadata");
                let timeout_ms =
                    take_optional_timeout_ms(chronicle_name, &phase.name, &mut options)?;

                let connector_entry = registry.grpc(&connector_name).cloned();
                let descriptor_from_connector = connector_entry
                    .as_ref()
                    .and_then(|cfg| cfg.descriptor.clone());

                let connector = connector_entry
                    .clone()
                    .map(|config| ConnectorHandle::Grpc {
                        name: connector_name.clone(),
                        config,
                    })
                    .unwrap_or_else(|| ConnectorHandle::Missing {
                        name: connector_name.clone(),
                        expected: "grpc",
                    });

                let timeout_ms = timeout_ms.or(connector.request_timeout_ms());

                let descriptor_path = if let Some(value) = descriptor_option {
                    let trimmed = value.trim();
                    if trimmed.is_empty() {
                        return Err(ChronicleEngineError::InvalidPhaseOptions {
                            chronicle: chronicle_name.to_string(),
                            phase: phase.name.clone(),
                            reason: "grpc client option `descriptor` must be a non-empty string"
                                .to_string(),
                        });
                    }
                    let candidate = PathBuf::from(trimmed);
                    if candidate.is_absolute() {
                        candidate
                    } else {
                        registry.base_dir().join(candidate)
                    }
                } else if let Some(path) = descriptor_from_connector {
                    path
                } else {
                    return Err(ChronicleEngineError::InvalidPhaseOptions {
                        chronicle: chronicle_name.to_string(),
                        phase: phase.name.clone(),
                        reason: format!(
                            "grpc client phase `{}` requires a `descriptor` option because connector `{}` does not declare one",
                            phase.name, connector_name
                        ),
                    });
                };

                Ok(Self::GrpcRequest {
                    name: phase.name.clone(),
                    connector,
                    service,
                    method,
                    descriptor: descriptor_path,
                    request_template,
                    metadata_template,
                    timeout_ms,
                    extra: JsonValue::Object(options),
                })
            }
            PhaseKind::RabbitmqPublish => {
                let mut options = clone_object(&raw_options).ok_or_else(|| {
                    ChronicleEngineError::InvalidPhaseOptions {
                        chronicle: chronicle_name.to_string(),
                        phase: phase.name.clone(),
                        reason: "rabbitmq phase options must be an object".to_string(),
                    }
                })?;

                let connector_name =
                    take_string(chronicle_name, &phase.name, &mut options, "connector")?;

                let exchange_template = options.remove("exchange");
                let routing_key_template = options.remove("routing_key");
                let body_template = options.remove("body");
                let headers_template = options.remove("headers");
                let properties_template = options.remove("properties");

                let mandatory = match options.remove("mandatory") {
                    Some(JsonValue::Bool(flag)) => Some(flag),
                    Some(other) => {
                        return Err(ChronicleEngineError::InvalidPhaseOptions {
                            chronicle: chronicle_name.to_string(),
                            phase: phase.name.clone(),
                            reason: format!(
                                "rabbitmq phase option `mandatory` expected boolean but found {other:?}"
                            ),
                        });
                    }
                    None => None,
                };

                let confirm = match options.remove("confirm") {
                    Some(JsonValue::Bool(flag)) => Some(flag),
                    Some(other) => {
                        return Err(ChronicleEngineError::InvalidPhaseOptions {
                            chronicle: chronicle_name.to_string(),
                            phase: phase.name.clone(),
                            reason: format!(
                                "rabbitmq phase option `confirm` expected boolean but found {other:?}"
                            ),
                        });
                    }
                    None => None,
                };

                let timeout_ms =
                    take_optional_timeout_ms(chronicle_name, &phase.name, &mut options)?;

                let connector = registry
                    .rabbitmq(&connector_name)
                    .cloned()
                    .map(|config| ConnectorHandle::Rabbitmq {
                        name: connector_name.clone(),
                        config,
                    })
                    .unwrap_or_else(|| ConnectorHandle::Missing {
                        name: connector_name.clone(),
                        expected: "rabbitmq",
                    });

                let timeout_ms = timeout_ms.or(connector.request_timeout_ms());

                Ok(Self::RabbitmqPublish {
                    name: phase.name.clone(),
                    connector,
                    exchange_template,
                    routing_key_template,
                    body_template,
                    headers_template,
                    properties_template,
                    mandatory,
                    confirm,
                    timeout_ms,
                    extra: JsonValue::Object(options),
                })
            }
            PhaseKind::MqttPublish => {
                let mut options = clone_object(&raw_options).ok_or_else(|| {
                    ChronicleEngineError::InvalidPhaseOptions {
                        chronicle: chronicle_name.to_string(),
                        phase: phase.name.clone(),
                        reason: "mqtt phase options must be an object".to_string(),
                    }
                })?;

                let connector_name =
                    take_string(chronicle_name, &phase.name, &mut options, "connector")?;

                let topic_template =
                    options
                        .remove("topic")
                        .ok_or_else(|| ChronicleEngineError::MissingOption {
                            chronicle: chronicle_name.to_string(),
                            phase: phase.name.clone(),
                            option: "topic".to_string(),
                        })?;

                let payload_template = options.remove("payload");
                let qos_template = options.remove("qos");
                let retain_template = options.remove("retain");
                let encoding_template = options
                    .remove("payload_encoding")
                    .or_else(|| options.remove("payloadEncoding"));
                let timeout_ms =
                    take_optional_timeout_ms(chronicle_name, &phase.name, &mut options)?;

                let connector = registry
                    .mqtt(&connector_name)
                    .cloned()
                    .map(|config| ConnectorHandle::Mqtt {
                        name: connector_name.clone(),
                        config,
                    })
                    .unwrap_or_else(|| ConnectorHandle::Missing {
                        name: connector_name.clone(),
                        expected: "mqtt",
                    });

                let timeout_ms = timeout_ms.or(connector.request_timeout_ms());

                Ok(Self::MqttPublish {
                    name: phase.name.clone(),
                    connector,
                    topic_template,
                    payload_template,
                    qos_template,
                    retain_template,
                    encoding_template,
                    extra: JsonValue::Object(options),
                    packet_seq: Arc::new(AtomicU16::new(1)),
                    timeout_ms,
                })
            }
            PhaseKind::Parallel => {
                let mut options = clone_object(&raw_options).ok_or_else(|| {
                    ChronicleEngineError::InvalidPhaseOptions {
                        chronicle: chronicle_name.to_string(),
                        phase: phase.name.clone(),
                        reason: "parallel phase options must be an object".to_string(),
                    }
                })?;

                let phases_value = options.remove("phases").ok_or_else(|| {
                    ChronicleEngineError::InvalidPhaseOptions {
                        chronicle: chronicle_name.to_string(),
                        phase: phase.name.clone(),
                        reason: "parallel phase requires `phases` array".to_string(),
                    }
                })?;

                let phases_array = phases_value.as_array().ok_or_else(|| {
                    ChronicleEngineError::InvalidPhaseOptions {
                        chronicle: chronicle_name.to_string(),
                        phase: phase.name.clone(),
                        reason: "parallel phase option `phases` must be an array".to_string(),
                    }
                })?;

                if phases_array.is_empty() {
                    return Err(ChronicleEngineError::InvalidPhaseOptions {
                        chronicle: chronicle_name.to_string(),
                        phase: phase.name.clone(),
                        reason: "parallel phase requires at least one child".to_string(),
                    });
                }

                let timeout_ms =
                    take_optional_timeout_ms(chronicle_name, &phase.name, &mut options)?;

                let aggregation = parse_parallel_aggregation(
                    chronicle_name,
                    &phase.name,
                    &mut options,
                    phases_array.len(),
                )?;

                let mut children = Vec::with_capacity(phases_array.len());
                for (index, child) in phases_array.iter().enumerate() {
                    let plan =
                        build_parallel_child(chronicle_name, &phase.name, index, child, registry)?;
                    children.push(plan);
                }

                if !options.is_empty() {
                    let unknown = options.keys().cloned().collect::<Vec<_>>().join(", ");
                    return Err(ChronicleEngineError::InvalidPhaseOptions {
                        chronicle: chronicle_name.to_string(),
                        phase: phase.name.clone(),
                        reason: format!("parallel phase has unsupported option(s): {unknown}"),
                    });
                }

                Ok(Self::Parallel(ParallelPhasePlan {
                    name: phase.name.clone(),
                    timeout_ms,
                    aggregation,
                    children,
                }))
            }
            PhaseKind::SmtpSend => {
                let mut options = clone_object(&raw_options).ok_or_else(|| {
                    ChronicleEngineError::InvalidPhaseOptions {
                        chronicle: chronicle_name.to_string(),
                        phase: phase.name.clone(),
                        reason: "smtp phase options must be an object".to_string(),
                    }
                })?;

                let connector_name =
                    take_string(chronicle_name, &phase.name, &mut options, "connector")?;

                let from_template = options.remove("from");
                let to_template = options.remove("to");
                let cc_template = options.remove("cc");
                let bcc_template = options.remove("bcc");
                let reply_to_template = options.remove("reply_to");
                let subject_template = options.remove("subject");
                let body_template = options.remove("body");
                let headers_template = options.remove("headers");
                let attachments_template = options.remove("attachments");

                let connector = registry
                    .smtp(&connector_name)
                    .cloned()
                    .map(|config| ConnectorHandle::Smtp {
                        name: connector_name.clone(),
                        config,
                    })
                    .unwrap_or_else(|| ConnectorHandle::Missing {
                        name: connector_name.clone(),
                        expected: "smtp",
                    });

                Ok(Self::SmtpSend {
                    name: phase.name.clone(),
                    connector,
                    from_template,
                    to_template,
                    cc_template,
                    bcc_template,
                    reply_to_template,
                    subject_template,
                    body_template,
                    headers_template,
                    attachments_template,
                    extra: JsonValue::Object(options),
                })
            }
            PhaseKind::Serialize => {
                let plan =
                    build_serialize_plan(chronicle_name, phase, registry.base_dir(), &raw_options)?;
                Ok(Self::Serialize(plan))
            }
            PhaseKind::Mongodb => {
                let mut options = clone_object(&raw_options).ok_or_else(|| {
                    ChronicleEngineError::InvalidPhaseOptions {
                        chronicle: chronicle_name.to_string(),
                        phase: phase.name.clone(),
                        reason: "mongodb phase options must be an object".to_string(),
                    }
                })?;

                let connector_name =
                    take_string(chronicle_name, &phase.name, &mut options, "connector")?;
                let collection =
                    take_string(chronicle_name, &phase.name, &mut options, "collection")?;
                let operation_raw =
                    take_string(chronicle_name, &phase.name, &mut options, "operation")?;

                let operation = match operation_raw.to_ascii_lowercase().as_str() {
                    "insert_one" => MongoOperation::InsertOne,
                    "replace_one" => MongoOperation::ReplaceOne,
                    "update_one" => MongoOperation::UpdateOne,
                    "delete_one" => MongoOperation::DeleteOne,
                    "aggregate" => MongoOperation::Aggregate,
                    other => {
                        return Err(ChronicleEngineError::InvalidPhaseOptions {
                            chronicle: chronicle_name.to_string(),
                            phase: phase.name.clone(),
                            reason: format!("unsupported mongodb operation `{other}`"),
                        })
                    }
                };

                let document_template = normalize_template(options.remove("document"));
                let filter_template = normalize_template(options.remove("filter"));
                let update_template = normalize_template(options.remove("update"));
                let pipeline_template = normalize_template(options.remove("pipeline"));
                let timeout_ms =
                    take_optional_timeout_ms(chronicle_name, &phase.name, &mut options)?;

                validate_mongodb_templates(
                    chronicle_name,
                    &phase.name,
                    operation,
                    &document_template,
                    &filter_template,
                    &update_template,
                    &pipeline_template,
                )?;

                let connector = registry
                    .mongodb(&connector_name)
                    .cloned()
                    .map(|config| ConnectorHandle::Mongodb {
                        name: connector_name.clone(),
                        config,
                    })
                    .unwrap_or_else(|| ConnectorHandle::Missing {
                        name: connector_name.clone(),
                        expected: "mongodb",
                    });

                let timeout_ms = timeout_ms
                    .or(connector.request_timeout_ms())
                    .or(connector.query_timeout_ms());

                Ok(Self::Mongodb(MongodbPhasePlan {
                    name: phase.name.clone(),
                    connector,
                    collection,
                    operation,
                    document_template,
                    filter_template,
                    update_template,
                    pipeline_template,
                    timeout_ms,
                    extra: JsonValue::Object(options),
                }))
            }
            PhaseKind::Redis => {
                let mut options = clone_object(&raw_options).ok_or_else(|| {
                    ChronicleEngineError::InvalidPhaseOptions {
                        chronicle: chronicle_name.to_string(),
                        phase: phase.name.clone(),
                        reason: "redis phase options must be an object".to_string(),
                    }
                })?;

                let connector_name =
                    take_string(chronicle_name, &phase.name, &mut options, "connector")?;
                let command = take_string(chronicle_name, &phase.name, &mut options, "command")?;
                let timeout_ms =
                    take_optional_timeout_ms(chronicle_name, &phase.name, &mut options)?;

                let connector = registry
                    .redis(&connector_name)
                    .cloned()
                    .map(|config| ConnectorHandle::Redis {
                        name: connector_name.clone(),
                        config,
                    })
                    .unwrap_or_else(|| ConnectorHandle::Missing {
                        name: connector_name.clone(),
                        expected: "redis",
                    });

                let timeout_ms = timeout_ms.or(connector.request_timeout_ms());

                let arguments = JsonValue::Object(options.clone());

                Ok(Self::Redis(RedisPhasePlan {
                    name: phase.name.clone(),
                    connector,
                    command,
                    arguments,
                    timeout_ms,
                }))
            }
            PhaseKind::MariadbIdempotentInsert => {
                let mut options = clone_object(&raw_options).ok_or_else(|| {
                    ChronicleEngineError::InvalidPhaseOptions {
                        chronicle: chronicle_name.to_string(),
                        phase: phase.name.clone(),
                        reason: "mariadb options must be an object".to_string(),
                    }
                })?;

                let connector_name =
                    take_string(chronicle_name, &phase.name, &mut options, "connector")?;

                let key_template =
                    options
                        .remove("key")
                        .ok_or_else(|| ChronicleEngineError::MissingOption {
                            chronicle: chronicle_name.to_string(),
                            phase: phase.name.clone(),
                            option: "key".to_string(),
                        })?;

                let value_template = options.remove("value");
                let timeout_ms =
                    take_optional_timeout_ms(chronicle_name, &phase.name, &mut options)?;

                let connector = registry
                    .mariadb(&connector_name)
                    .cloned()
                    .map(|config| ConnectorHandle::Mariadb {
                        name: connector_name.clone(),
                        config,
                    })
                    .unwrap_or_else(|| ConnectorHandle::Missing {
                        name: connector_name.clone(),
                        expected: "mariadb",
                    });

                let timeout_ms = timeout_ms
                    .or(connector.query_timeout_ms())
                    .or(connector.request_timeout_ms());

                Ok(Self::MariadbInsert {
                    name: phase.name.clone(),
                    connector,
                    key_template,
                    value_template,
                    timeout_ms,
                    extra: JsonValue::Object(options),
                })
            }
            PhaseKind::PostgresIdempotentInsert => {
                let mut options = clone_object(&raw_options).ok_or_else(|| {
                    ChronicleEngineError::InvalidPhaseOptions {
                        chronicle: chronicle_name.to_string(),
                        phase: phase.name.clone(),
                        reason: "postgres phase options must be an object".to_string(),
                    }
                })?;

                let connector_name =
                    take_string(chronicle_name, &phase.name, &mut options, "connector")?;

                let sql_template =
                    options
                        .remove("sql")
                        .ok_or_else(|| ChronicleEngineError::MissingOption {
                            chronicle: chronicle_name.to_string(),
                            phase: phase.name.clone(),
                            option: "sql".to_string(),
                        })?;

                let values_template = options.remove("values");
                let timeout_ms =
                    take_optional_timeout_ms(chronicle_name, &phase.name, &mut options)?;

                let connector = registry
                    .postgres(&connector_name)
                    .cloned()
                    .map(|config| ConnectorHandle::Postgres {
                        name: connector_name.clone(),
                        config,
                    })
                    .unwrap_or_else(|| ConnectorHandle::Missing {
                        name: connector_name.clone(),
                        expected: "postgres",
                    });

                let timeout_ms = timeout_ms
                    .or(connector.query_timeout_ms())
                    .or(connector.request_timeout_ms());

                Ok(Self::PostgresInsert {
                    name: phase.name.clone(),
                    connector,
                    sql_template,
                    values_template,
                    extra: JsonValue::Object(options),
                    timeout_ms,
                })
            }
            PhaseKind::Unknown(ref kind) => Ok(Self::Unsupported {
                name: phase.name.clone(),
                kind: kind.clone(),
            }),
        }
    }

    fn name(&self) -> &str {
        match self {
            PhasePlan::Transform { name, .. }
            | PhasePlan::KafkaProducer { name, .. }
            | PhasePlan::HttpClient { name, .. }
            | PhasePlan::GrpcRequest { name, .. }
            | PhasePlan::RabbitmqPublish { name, .. }
            | PhasePlan::MqttPublish { name, .. }
            | PhasePlan::Parallel(ParallelPhasePlan { name, .. })
            | PhasePlan::SmtpSend { name, .. }
            | PhasePlan::Serialize(SerializePhasePlan { name, .. })
            | PhasePlan::Mongodb(MongodbPhasePlan { name, .. })
            | PhasePlan::Redis(RedisPhasePlan { name, .. })
            | PhasePlan::MariadbInsert { name, .. }
            | PhasePlan::PostgresInsert { name, .. }
            | PhasePlan::Unsupported { name, .. } => name,
        }
    }

    fn kind_name(&self) -> &str {
        match self {
            PhasePlan::Transform { .. } => "transform",
            PhasePlan::KafkaProducer { .. } => "kafka_producer",
            PhasePlan::HttpClient { .. } => "http_client",
            PhasePlan::GrpcRequest { .. } => "grpc_client",
            PhasePlan::RabbitmqPublish { .. } => "rabbitmq",
            PhasePlan::MqttPublish { .. } => "mqtt",
            PhasePlan::Parallel(_) => "parallel",
            PhasePlan::SmtpSend { .. } => "smtp_send",
            PhasePlan::Serialize(_) => "serialize",
            PhasePlan::Mongodb(_) => "mongodb",
            PhasePlan::Redis(_) => "redis",
            PhasePlan::MariadbInsert { .. } => "mariadb_idempotent_insert",
            PhasePlan::PostgresInsert { .. } => "postgres_idempotent_insert",
            PhasePlan::Unsupported { kind, .. } => kind.as_str(),
        }
    }
}

#[derive(Clone)]
struct SerializePhasePlan {
    name: String,
    codec: SerializeCodec,
    input_template: Option<JsonValue>,
}

impl SerializePhasePlan {
    fn encode(&self, chronicle: &str, input: JsonValue) -> Result<JsonValue, ChronicleEngineError> {
        let payload = match &self.codec {
            SerializeCodec::Avro(codec) => codec.encode(chronicle, &self.name, input)?,
            SerializeCodec::Protobuf(codec) => codec.encode(chronicle, &self.name, input)?,
            SerializeCodec::Cbor(codec) => codec.encode(chronicle, &self.name, input)?,
        };
        Ok(payload.to_json())
    }
}

#[derive(Clone)]
enum SerializeCodec {
    Avro(AvroCodec),
    Protobuf(ProtobufCodec),
    Cbor(CborCodec),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ParallelAggregation {
    All,
    Any,
    Quorum { threshold: usize },
}

#[derive(Clone)]
struct ParallelChildPlan {
    name: String,
    plan: PhasePlan,
    timeout_ms: Option<u64>,
    optional: bool,
    fallback: Option<String>,
}

#[derive(Clone)]
struct ParallelPhasePlan {
    name: String,
    timeout_ms: Option<u64>,
    aggregation: ParallelAggregation,
    children: Vec<ParallelChildPlan>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MongoOperation {
    InsertOne,
    ReplaceOne,
    UpdateOne,
    DeleteOne,
    Aggregate,
}

#[derive(Clone)]
struct MongodbPhasePlan {
    name: String,
    connector: ConnectorHandle,
    collection: String,
    operation: MongoOperation,
    document_template: Option<JsonValue>,
    filter_template: Option<JsonValue>,
    update_template: Option<JsonValue>,
    pipeline_template: Option<JsonValue>,
    timeout_ms: Option<u64>,
    extra: JsonValue,
}

#[derive(Clone)]
struct RedisPhasePlan {
    name: String,
    connector: ConnectorHandle,
    command: String,
    arguments: JsonValue,
    timeout_ms: Option<u64>,
}

fn build_serialize_plan(
    chronicle: &str,
    phase: &ChroniclePhase,
    base_dir: &Path,
    raw_options: &JsonValue,
) -> Result<SerializePhasePlan, ChronicleEngineError> {
    let mut options =
        clone_object(raw_options).ok_or_else(|| ChronicleEngineError::InvalidPhaseOptions {
            chronicle: chronicle.to_string(),
            phase: phase.name.clone(),
            reason: "serialize options must be an object".to_string(),
        })?;

    let codec_raw = take_string(chronicle, &phase.name, &mut options, "codec")?;
    let codec_normalised = codec_raw.to_lowercase();

    let input_template = options.remove("input");
    let schema_value = options.remove("schema");
    let registry_value = options.remove("registry");
    let protobuf_value = options.remove("protobuf");

    let codec = match codec_normalised.as_str() {
        "avro" => {
            let schema = resolve_schema_source(
                chronicle,
                &phase.name,
                base_dir,
                schema_value,
                registry_value,
            )?;
            SerializeCodec::Avro(AvroCodec::new(schema))
        }
        "protobuf" => {
            if let Some(value) = registry_value {
                return Err(ChronicleEngineError::InvalidPhaseOptions {
                    chronicle: chronicle.to_string(),
                    phase: phase.name.clone(),
                    reason: format!(
                        "codec `protobuf` does not support registry configuration: {value:?}"
                    ),
                });
            }

            let descriptor_path = schema_value
                .map(|value| resolve_file_path(chronicle, &phase.name, base_dir, value))
                .transpose()?
                .ok_or_else(|| ChronicleEngineError::InvalidPhaseOptions {
                    chronicle: chronicle.to_string(),
                    phase: phase.name.clone(),
                    reason: "codec `protobuf` requires a schema path pointing to a descriptor set"
                        .to_string(),
                })?;

            let message = extract_protobuf_message(chronicle, &phase.name, protobuf_value)?;

            SerializeCodec::Protobuf(ProtobufCodec::new(
                Arc::new(DescriptorSource::new(descriptor_path)),
                message,
            ))
        }
        "cbor" => SerializeCodec::Cbor(CborCodec),
        other => {
            return Err(ChronicleEngineError::InvalidPhaseOptions {
                chronicle: chronicle.to_string(),
                phase: phase.name.clone(),
                reason: format!("unsupported serialize codec `{other}`"),
            });
        }
    };

    Ok(SerializePhasePlan {
        name: phase.name.clone(),
        codec,
        input_template,
    })
}

fn resolve_schema_source(
    chronicle: &str,
    phase: &str,
    base_dir: &Path,
    schema_value: Option<JsonValue>,
    registry_value: Option<JsonValue>,
) -> Result<SchemaSource, ChronicleEngineError> {
    let file_schema = if let Some(value) = schema_value {
        Some(parse_schema_file(chronicle, phase, base_dir, value)?)
    } else {
        None
    };

    if let Some(value) = registry_value {
        let registry =
            parse_schema_registry(chronicle, phase, value, file_schema.clone(), base_dir)?;
        return Ok(SchemaSource::Registry(registry));
    }

    if let Some(file) = file_schema {
        return Ok(SchemaSource::File(file));
    }

    Err(ChronicleEngineError::InvalidPhaseOptions {
        chronicle: chronicle.to_string(),
        phase: phase.to_string(),
        reason: "serialize phase requires either a schema file or schema registry configuration"
            .to_string(),
    })
}

fn parse_schema_file(
    chronicle: &str,
    phase: &str,
    base_dir: &Path,
    value: JsonValue,
) -> Result<Arc<FileSchemaSource>, ChronicleEngineError> {
    let resolved = resolve_file_path(chronicle, phase, base_dir, value)?;
    Ok(Arc::new(FileSchemaSource::new(resolved)))
}

fn resolve_file_path(
    chronicle: &str,
    phase: &str,
    base_dir: &Path,
    value: JsonValue,
) -> Result<PathBuf, ChronicleEngineError> {
    let (path_value, type_value) = match value {
        JsonValue::String(path) => (Some(path), None),
        JsonValue::Object(mut map) => {
            let type_value = map
                .remove("type")
                .and_then(|value| value.as_str().map(str::to_string));
            let path_value = map
                .remove("path")
                .and_then(|value| value.as_str().map(str::to_string));
            (path_value, type_value)
        }
        other => {
            return Err(ChronicleEngineError::InvalidPhaseOptions {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                reason: format!("schema configuration must be a string or object, found {other:?}"),
            });
        }
    };

    if let Some(kind) = type_value {
        if kind.to_lowercase() != "file" {
            return Err(ChronicleEngineError::InvalidPhaseOptions {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                reason: format!("unsupported schema type `{kind}`; expected `file`"),
            });
        }
    }

    let path_str = path_value.ok_or_else(|| ChronicleEngineError::InvalidPhaseOptions {
        chronicle: chronicle.to_string(),
        phase: phase.to_string(),
        reason: "schema configuration requires `path`".to_string(),
    })?;

    let path = PathBuf::from(&path_str);
    Ok(if path.is_absolute() {
        path
    } else {
        base_dir.join(path_str)
    })
}

fn parse_schema_registry(
    chronicle: &str,
    phase: &str,
    value: JsonValue,
    fallback: Option<Arc<FileSchemaSource>>,
    base_dir: &Path,
) -> Result<Arc<RegistrySchemaSource>, ChronicleEngineError> {
    let mut map =
        value
            .as_object()
            .cloned()
            .ok_or_else(|| ChronicleEngineError::InvalidPhaseOptions {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                reason: "registry configuration must be an object".to_string(),
            })?;

    let endpoint = map
        .remove("url")
        .and_then(|value| value.as_str().map(str::to_string))
        .ok_or_else(|| ChronicleEngineError::InvalidPhaseOptions {
            chronicle: chronicle.to_string(),
            phase: phase.to_string(),
            reason: "registry configuration requires `url`".to_string(),
        })?;

    let subject = map
        .remove("subject")
        .and_then(|value| value.as_str().map(str::to_string))
        .ok_or_else(|| ChronicleEngineError::InvalidPhaseOptions {
            chronicle: chronicle.to_string(),
            phase: phase.to_string(),
            reason: "registry configuration requires `subject`".to_string(),
        })?;

    let version = map
        .remove("version")
        .map(|value| parse_schema_registry_version(chronicle, phase, value))
        .transpose()?
        .unwrap_or(SchemaRegistryVersion::Latest);

    let credentials = match map.remove("credentials") {
        Some(value) => parse_registry_credentials(chronicle, phase, value)?,
        None => None,
    };

    if let Some(schema_value) = map.remove("schema") {
        // Allow nested schema override for clarity; merge with fallback if provided.
        let file_override = parse_schema_file(chronicle, phase, base_dir, schema_value)?;
        let effective_fallback = Some(file_override).or(fallback);
        return RegistrySchemaSource::new(
            chronicle,
            phase,
            endpoint,
            subject,
            version,
            credentials,
            effective_fallback,
        )
        .map(Arc::new);
    }

    RegistrySchemaSource::new(
        chronicle,
        phase,
        endpoint,
        subject,
        version,
        credentials,
        fallback,
    )
    .map(Arc::new)
}

fn extract_protobuf_message(
    chronicle: &str,
    phase: &str,
    value: Option<JsonValue>,
) -> Result<String, ChronicleEngineError> {
    let value = value.ok_or_else(|| ChronicleEngineError::InvalidPhaseOptions {
        chronicle: chronicle.to_string(),
        phase: phase.to_string(),
        reason: "codec `protobuf` requires a `protobuf` configuration block".to_string(),
    })?;

    let map = match value {
        JsonValue::Object(map) => map,
        other => {
            return Err(ChronicleEngineError::InvalidPhaseOptions {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                reason: format!("`protobuf` configuration must be an object, found {other:?}"),
            });
        }
    };

    map.get("message")
        .and_then(JsonValue::as_str)
        .map(|s| s.to_string())
        .ok_or_else(|| ChronicleEngineError::InvalidPhaseOptions {
            chronicle: chronicle.to_string(),
            phase: phase.to_string(),
            reason: "`protobuf.message` option is required".to_string(),
        })
}

fn parse_schema_registry_version(
    chronicle: &str,
    phase: &str,
    value: JsonValue,
) -> Result<SchemaRegistryVersion, ChronicleEngineError> {
    match value {
        JsonValue::String(text) => {
            if text.eq_ignore_ascii_case("latest") {
                Ok(SchemaRegistryVersion::Latest)
            } else {
                let parsed = text.parse::<u32>().map_err(|err| {
                    ChronicleEngineError::InvalidPhaseOptions {
                        chronicle: chronicle.to_string(),
                        phase: phase.to_string(),
                        reason: format!("invalid registry version `{text}`: {err}"),
                    }
                })?;
                Ok(SchemaRegistryVersion::Version(parsed))
            }
        }
        JsonValue::Number(num) => {
            if let Some(value) = num.as_u64() {
                let clamped = u32::try_from(value).map_err(|_| {
                    ChronicleEngineError::InvalidPhaseOptions {
                        chronicle: chronicle.to_string(),
                        phase: phase.to_string(),
                        reason: format!("registry version `{value}` exceeds u32 range"),
                    }
                })?;
                Ok(SchemaRegistryVersion::Version(clamped))
            } else {
                Err(ChronicleEngineError::InvalidPhaseOptions {
                    chronicle: chronicle.to_string(),
                    phase: phase.to_string(),
                    reason: format!("registry version must be positive integer, found {num}"),
                })
            }
        }
        other => Err(ChronicleEngineError::InvalidPhaseOptions {
            chronicle: chronicle.to_string(),
            phase: phase.to_string(),
            reason: format!("registry version must be string or number, found {other:?}"),
        }),
    }
}

fn parse_registry_credentials(
    chronicle: &str,
    phase: &str,
    value: JsonValue,
) -> Result<Option<RegistryCredentials>, ChronicleEngineError> {
    let map = match value {
        JsonValue::Null => return Ok(None),
        JsonValue::Object(map) => map,
        other => {
            return Err(ChronicleEngineError::InvalidPhaseOptions {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                reason: format!("registry credentials must be an object, found {other:?}"),
            });
        }
    };

    let username = map
        .get("username")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| ChronicleEngineError::InvalidPhaseOptions {
            chronicle: chronicle.to_string(),
            phase: phase.to_string(),
            reason: "registry credentials require `username`".to_string(),
        })?
        .to_string();

    let password = map
        .get("password")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| ChronicleEngineError::InvalidPhaseOptions {
            chronicle: chronicle.to_string(),
            phase: phase.to_string(),
            reason: "registry credentials require `password`".to_string(),
        })?
        .to_string();

    Ok(Some(RegistryCredentials { username, password }))
}

#[derive(Clone)]
enum ConnectorHandle {
    Kafka {
        name: String,
        config: KafkaConnector,
    },
    HttpServer {
        name: String,
        _config: HttpServerConnector,
    },
    HttpClient {
        name: String,
        config: HttpClientConnector,
    },
    Mariadb {
        name: String,
        config: MariadbConnector,
    },
    Postgres {
        name: String,
        config: PostgresConnector,
    },
    Rabbitmq {
        name: String,
        config: RabbitmqConnector,
    },
    Mqtt {
        name: String,
        config: MqttConnector,
    },
    Mongodb {
        name: String,
        config: MongodbConnector,
    },
    Redis {
        name: String,
        config: RedisConnector,
    },
    Grpc {
        name: String,
        config: GrpcConnector,
    },
    Smtp {
        name: String,
        config: SmtpConnector,
    },
    Missing {
        name: String,
        expected: &'static str,
    },
}

impl ConnectorHandle {
    fn name(&self) -> &str {
        match self {
            ConnectorHandle::Kafka { name, .. }
            | ConnectorHandle::HttpServer { name, .. }
            | ConnectorHandle::HttpClient { name, .. }
            | ConnectorHandle::Mariadb { name, .. }
            | ConnectorHandle::Postgres { name, .. }
            | ConnectorHandle::Rabbitmq { name, .. }
            | ConnectorHandle::Mqtt { name, .. }
            | ConnectorHandle::Mongodb { name, .. }
            | ConnectorHandle::Redis { name, .. }
            | ConnectorHandle::Grpc { name, .. }
            | ConnectorHandle::Smtp { name, .. }
            | ConnectorHandle::Missing { name, .. } => name,
        }
    }

    fn request_timeout_ms(&self) -> Option<u64> {
        match self {
            ConnectorHandle::Kafka { config, .. } => {
                config.timeouts.request.map(duration_to_millis)
            }
            ConnectorHandle::HttpClient { config, .. } => {
                config.timeouts.request.map(duration_to_millis)
            }
            ConnectorHandle::Mariadb { config, .. } => {
                config.timeouts.request.map(duration_to_millis)
            }
            ConnectorHandle::Postgres { config, .. } => {
                config.timeouts.request.map(duration_to_millis)
            }
            ConnectorHandle::Rabbitmq { config, .. } => {
                config.timeouts.request.map(duration_to_millis)
            }
            ConnectorHandle::Mqtt { config, .. } => config.timeouts.request.map(duration_to_millis),
            ConnectorHandle::Mongodb { config, .. } => config
                .timeouts
                .request
                .or(config.timeouts.query)
                .map(duration_to_millis),
            ConnectorHandle::Redis { config, .. } => {
                config.timeouts.request.map(duration_to_millis)
            }
            ConnectorHandle::Grpc { config, .. } => config.timeouts.request.map(duration_to_millis),
            ConnectorHandle::Smtp { config, .. } => config.timeouts.request.map(duration_to_millis),
            ConnectorHandle::HttpServer { .. } | ConnectorHandle::Missing { .. } => None,
        }
    }

    fn query_timeout_ms(&self) -> Option<u64> {
        match self {
            ConnectorHandle::Postgres { config, .. } => {
                config.timeouts.query.map(duration_to_millis)
            }
            ConnectorHandle::Mariadb { config, .. } => {
                config.timeouts.query.map(duration_to_millis)
            }
            ConnectorHandle::Mongodb { config, .. } => config
                .timeouts
                .query
                .or(config.timeouts.request)
                .map(duration_to_millis),
            _ => None,
        }
    }

    fn retry_budget(&self) -> Option<&RetryBudget> {
        match self {
            ConnectorHandle::Kafka { config, .. } => config.retry_budget.as_ref(),
            ConnectorHandle::HttpServer { .. } => None,
            ConnectorHandle::HttpClient { config, .. } => config.retry_budget.as_ref(),
            ConnectorHandle::Mariadb { config, .. } => config.retry_budget.as_ref(),
            ConnectorHandle::Postgres { config, .. } => config.retry_budget.as_ref(),
            ConnectorHandle::Rabbitmq { config, .. } => config.retry_budget.as_ref(),
            ConnectorHandle::Mqtt { config, .. } => config.retry_budget.as_ref(),
            ConnectorHandle::Mongodb { config, .. } => config.retry_budget.as_ref(),
            ConnectorHandle::Redis { config, .. } => config.retry_budget.as_ref(),
            ConnectorHandle::Grpc { config, .. } => config.retry_budget.as_ref(),
            ConnectorHandle::Smtp { config, .. } => config.retry_budget.as_ref(),
            ConnectorHandle::Missing { .. } => None,
        }
    }

    fn expect_kafka(
        self,
        chronicle: &str,
        phase: &str,
    ) -> Result<(String, KafkaConnector), ChronicleEngineError> {
        match self {
            ConnectorHandle::Kafka { name, config } => Ok((name, config)),
            ConnectorHandle::Missing { name, expected } => {
                Err(ChronicleEngineError::MissingConnector {
                    chronicle: chronicle.to_string(),
                    phase: phase.to_string(),
                    name,
                    expected,
                })
            }
            ConnectorHandle::HttpServer { name, .. }
            | ConnectorHandle::HttpClient { name, .. }
            | ConnectorHandle::Mariadb { name, .. }
            | ConnectorHandle::Postgres { name, .. }
            | ConnectorHandle::Rabbitmq { name, .. }
            | ConnectorHandle::Mqtt { name, .. }
            | ConnectorHandle::Mongodb { name, .. }
            | ConnectorHandle::Redis { name, .. }
            | ConnectorHandle::Grpc { name, .. }
            | ConnectorHandle::Smtp { name, .. } => Err(ChronicleEngineError::MissingConnector {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                name,
                expected: "kafka",
            }),
        }
    }

    fn expect_http_client(
        self,
        chronicle: &str,
        phase: &str,
    ) -> Result<(String, HttpClientConnector), ChronicleEngineError> {
        match self {
            ConnectorHandle::HttpClient { name, config } => Ok((name, config)),
            ConnectorHandle::Missing { name, expected } => {
                Err(ChronicleEngineError::MissingConnector {
                    chronicle: chronicle.to_string(),
                    phase: phase.to_string(),
                    name,
                    expected,
                })
            }
            ConnectorHandle::Kafka { name, .. }
            | ConnectorHandle::HttpServer { name, .. }
            | ConnectorHandle::Mariadb { name, .. }
            | ConnectorHandle::Postgres { name, .. }
            | ConnectorHandle::Rabbitmq { name, .. }
            | ConnectorHandle::Mqtt { name, .. }
            | ConnectorHandle::Mongodb { name, .. }
            | ConnectorHandle::Redis { name, .. }
            | ConnectorHandle::Grpc { name, .. }
            | ConnectorHandle::Smtp { name, .. } => Err(ChronicleEngineError::MissingConnector {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                name,
                expected: "http_client",
            }),
        }
    }

    fn expect_mariadb(
        self,
        chronicle: &str,
        phase: &str,
    ) -> Result<(String, MariadbConnector), ChronicleEngineError> {
        match self {
            ConnectorHandle::Mariadb { name, config } => Ok((name, config)),
            ConnectorHandle::Missing { name, expected } => {
                Err(ChronicleEngineError::MissingConnector {
                    chronicle: chronicle.to_string(),
                    phase: phase.to_string(),
                    name,
                    expected,
                })
            }
            ConnectorHandle::Kafka { name, .. }
            | ConnectorHandle::HttpServer { name, .. }
            | ConnectorHandle::HttpClient { name, .. }
            | ConnectorHandle::Postgres { name, .. }
            | ConnectorHandle::Rabbitmq { name, .. }
            | ConnectorHandle::Mqtt { name, .. }
            | ConnectorHandle::Mongodb { name, .. }
            | ConnectorHandle::Redis { name, .. }
            | ConnectorHandle::Grpc { name, .. }
            | ConnectorHandle::Smtp { name, .. } => Err(ChronicleEngineError::MissingConnector {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                name,
                expected: "mariadb",
            }),
        }
    }

    fn expect_postgres(
        self,
        chronicle: &str,
        phase: &str,
    ) -> Result<(String, PostgresConnector), ChronicleEngineError> {
        match self {
            ConnectorHandle::Postgres { name, config } => Ok((name, config)),
            ConnectorHandle::Missing { name, expected } => {
                Err(ChronicleEngineError::MissingConnector {
                    chronicle: chronicle.to_string(),
                    phase: phase.to_string(),
                    name,
                    expected,
                })
            }
            ConnectorHandle::Kafka { name, .. }
            | ConnectorHandle::HttpServer { name, .. }
            | ConnectorHandle::HttpClient { name, .. }
            | ConnectorHandle::Mariadb { name, .. }
            | ConnectorHandle::Rabbitmq { name, .. }
            | ConnectorHandle::Mqtt { name, .. }
            | ConnectorHandle::Mongodb { name, .. }
            | ConnectorHandle::Redis { name, .. }
            | ConnectorHandle::Grpc { name, .. }
            | ConnectorHandle::Smtp { name, .. } => Err(ChronicleEngineError::MissingConnector {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                name,
                expected: "postgres",
            }),
        }
    }

    fn expect_rabbitmq(
        self,
        chronicle: &str,
        phase: &str,
    ) -> Result<(String, RabbitmqConnector), ChronicleEngineError> {
        match self {
            ConnectorHandle::Rabbitmq { name, config } => Ok((name, config)),
            ConnectorHandle::Missing { name, expected } => {
                Err(ChronicleEngineError::MissingConnector {
                    chronicle: chronicle.to_string(),
                    phase: phase.to_string(),
                    name,
                    expected,
                })
            }
            ConnectorHandle::Kafka { name, .. }
            | ConnectorHandle::HttpServer { name, .. }
            | ConnectorHandle::HttpClient { name, .. }
            | ConnectorHandle::Mariadb { name, .. }
            | ConnectorHandle::Postgres { name, .. }
            | ConnectorHandle::Mqtt { name, .. }
            | ConnectorHandle::Mongodb { name, .. }
            | ConnectorHandle::Redis { name, .. }
            | ConnectorHandle::Grpc { name, .. }
            | ConnectorHandle::Smtp { name, .. } => Err(ChronicleEngineError::MissingConnector {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                name,
                expected: "rabbitmq",
            }),
        }
    }

    fn expect_mqtt(
        self,
        chronicle: &str,
        phase: &str,
    ) -> Result<(String, MqttConnector), ChronicleEngineError> {
        match self {
            ConnectorHandle::Mqtt { name, config } => Ok((name, config)),
            ConnectorHandle::Missing { name, expected } => {
                Err(ChronicleEngineError::MissingConnector {
                    chronicle: chronicle.to_string(),
                    phase: phase.to_string(),
                    name,
                    expected,
                })
            }
            ConnectorHandle::Kafka { name, .. }
            | ConnectorHandle::HttpServer { name, .. }
            | ConnectorHandle::HttpClient { name, .. }
            | ConnectorHandle::Mariadb { name, .. }
            | ConnectorHandle::Postgres { name, .. }
            | ConnectorHandle::Rabbitmq { name, .. }
            | ConnectorHandle::Mongodb { name, .. }
            | ConnectorHandle::Redis { name, .. }
            | ConnectorHandle::Grpc { name, .. }
            | ConnectorHandle::Smtp { name, .. } => Err(ChronicleEngineError::MissingConnector {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                name,
                expected: "mqtt",
            }),
        }
    }

    fn expect_mongodb(
        self,
        chronicle: &str,
        phase: &str,
    ) -> Result<(String, MongodbConnector), ChronicleEngineError> {
        match self {
            ConnectorHandle::Mongodb { name, config } => Ok((name, config)),
            ConnectorHandle::Missing { name, expected } => {
                Err(ChronicleEngineError::MissingConnector {
                    chronicle: chronicle.to_string(),
                    phase: phase.to_string(),
                    name,
                    expected,
                })
            }
            ConnectorHandle::Kafka { name, .. }
            | ConnectorHandle::HttpServer { name, .. }
            | ConnectorHandle::HttpClient { name, .. }
            | ConnectorHandle::Mariadb { name, .. }
            | ConnectorHandle::Postgres { name, .. }
            | ConnectorHandle::Rabbitmq { name, .. }
            | ConnectorHandle::Mqtt { name, .. }
            | ConnectorHandle::Redis { name, .. }
            | ConnectorHandle::Grpc { name, .. }
            | ConnectorHandle::Smtp { name, .. } => Err(ChronicleEngineError::MissingConnector {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                name,
                expected: "mongodb",
            }),
        }
    }

    fn expect_redis(
        self,
        chronicle: &str,
        phase: &str,
    ) -> Result<(String, RedisConnector), ChronicleEngineError> {
        match self {
            ConnectorHandle::Redis { name, config } => Ok((name, config)),
            ConnectorHandle::Missing { name, expected } => {
                Err(ChronicleEngineError::MissingConnector {
                    chronicle: chronicle.to_string(),
                    phase: phase.to_string(),
                    name,
                    expected,
                })
            }
            ConnectorHandle::Kafka { name, .. }
            | ConnectorHandle::HttpServer { name, .. }
            | ConnectorHandle::HttpClient { name, .. }
            | ConnectorHandle::Mariadb { name, .. }
            | ConnectorHandle::Postgres { name, .. }
            | ConnectorHandle::Rabbitmq { name, .. }
            | ConnectorHandle::Mqtt { name, .. }
            | ConnectorHandle::Mongodb { name, .. }
            | ConnectorHandle::Grpc { name, .. }
            | ConnectorHandle::Smtp { name, .. } => Err(ChronicleEngineError::MissingConnector {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                name,
                expected: "redis",
            }),
        }
    }

    fn expect_grpc(
        self,
        chronicle: &str,
        phase: &str,
    ) -> Result<(String, GrpcConnector), ChronicleEngineError> {
        match self {
            ConnectorHandle::Grpc { name, config } => Ok((name, config)),
            ConnectorHandle::Missing { name, expected } => {
                Err(ChronicleEngineError::MissingConnector {
                    chronicle: chronicle.to_string(),
                    phase: phase.to_string(),
                    name,
                    expected,
                })
            }
            ConnectorHandle::Kafka { name, .. }
            | ConnectorHandle::HttpServer { name, .. }
            | ConnectorHandle::HttpClient { name, .. }
            | ConnectorHandle::Mariadb { name, .. }
            | ConnectorHandle::Postgres { name, .. }
            | ConnectorHandle::Rabbitmq { name, .. }
            | ConnectorHandle::Mqtt { name, .. }
            | ConnectorHandle::Mongodb { name, .. }
            | ConnectorHandle::Redis { name, .. }
            | ConnectorHandle::Smtp { name, .. } => Err(ChronicleEngineError::MissingConnector {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                name,
                expected: "grpc",
            }),
        }
    }

    fn expect_smtp(
        self,
        chronicle: &str,
        phase: &str,
    ) -> Result<(String, SmtpConnector), ChronicleEngineError> {
        match self {
            ConnectorHandle::Smtp { name, config } => Ok((name, config)),
            ConnectorHandle::Missing { name, expected } => {
                Err(ChronicleEngineError::MissingConnector {
                    chronicle: chronicle.to_string(),
                    phase: phase.to_string(),
                    name,
                    expected,
                })
            }
            ConnectorHandle::Kafka { name, .. }
            | ConnectorHandle::HttpServer { name, .. }
            | ConnectorHandle::HttpClient { name, .. }
            | ConnectorHandle::Mariadb { name, .. }
            | ConnectorHandle::Postgres { name, .. }
            | ConnectorHandle::Rabbitmq { name, .. }
            | ConnectorHandle::Mqtt { name, .. }
            | ConnectorHandle::Mongodb { name, .. }
            | ConnectorHandle::Redis { name, .. }
            | ConnectorHandle::Grpc { name, .. } => Err(ChronicleEngineError::MissingConnector {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                name,
                expected: "smtp",
            }),
        }
    }
}

fn duration_to_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u64::MAX as u128) as u64
}

#[derive(Clone, Copy, Debug)]
enum MqttPayloadEncoding {
    Json,
    Base64,
}

impl MqttPayloadEncoding {
    fn as_str(&self) -> &'static str {
        match self {
            MqttPayloadEncoding::Json => "json",
            MqttPayloadEncoding::Base64 => "base64",
        }
    }
}

fn resolve_mqtt_encoding(
    chronicle: &str,
    phase: &str,
    template: Option<&JsonValue>,
    context: &ExecutionContext,
) -> Result<MqttPayloadEncoding, ChronicleEngineError> {
    if let Some(value) = template {
        let text = context
            .resolve_to_string(value)
            .map_err(|err| map_context_error(err, chronicle, Some(phase)))?;
        let normalised = text.trim().to_ascii_lowercase();

        if normalised.is_empty() || normalised == "json" {
            Ok(MqttPayloadEncoding::Json)
        } else if normalised == "base64" {
            Ok(MqttPayloadEncoding::Base64)
        } else {
            Err(ChronicleEngineError::InvalidPhaseOptions {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                reason: format!(
                    "mqtt phase option `payload_encoding` must be `json` or `base64`, found `{text}`"
                ),
            })
        }
    } else {
        Ok(MqttPayloadEncoding::Json)
    }
}

fn resolve_mqtt_qos(
    chronicle: &str,
    phase: &str,
    template: Option<&JsonValue>,
    context: &ExecutionContext,
) -> Result<u8, ChronicleEngineError> {
    if let Some(value) = template {
        let resolved = context
            .resolve_template(value)
            .map_err(|err| map_context_error(err, chronicle, Some(phase)))?;

        match resolved {
            JsonValue::Number(num) => {
                let qos = num
                    .as_i64()
                    .or_else(|| num.as_u64().map(|v| v as i64))
                    .ok_or_else(|| ChronicleEngineError::InvalidPhaseOptions {
                        chronicle: chronicle.to_string(),
                        phase: phase.to_string(),
                        reason: "mqtt qos must be an integer between 0 and 2".to_string(),
                    })?;
                validate_mqtt_qos(chronicle, phase, qos)
            }
            JsonValue::String(text) => {
                let parsed =
                    text.parse::<i64>()
                        .map_err(|_| ChronicleEngineError::InvalidPhaseOptions {
                            chronicle: chronicle.to_string(),
                            phase: phase.to_string(),
                            reason: format!(
                                "mqtt qos value `{text}` is not a valid integer between 0 and 2"
                            ),
                        })?;
                validate_mqtt_qos(chronicle, phase, parsed)
            }
            JsonValue::Null => Ok(0),
            other => Err(ChronicleEngineError::InvalidPhaseOptions {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                reason: format!("mqtt qos expected number or string template, found {other:?}"),
            }),
        }
    } else {
        Ok(0)
    }
}

fn validate_mqtt_qos(chronicle: &str, phase: &str, value: i64) -> Result<u8, ChronicleEngineError> {
    if (0..=2).contains(&value) {
        Ok(value as u8)
    } else {
        Err(ChronicleEngineError::InvalidPhaseOptions {
            chronicle: chronicle.to_string(),
            phase: phase.to_string(),
            reason: "mqtt qos must be between 0 and 2 (inclusive)".to_string(),
        })
    }
}

fn resolve_mqtt_retain(
    chronicle: &str,
    phase: &str,
    template: Option<&JsonValue>,
    context: &ExecutionContext,
) -> Result<bool, ChronicleEngineError> {
    if let Some(value) = template {
        let resolved = context
            .resolve_template(value)
            .map_err(|err| map_context_error(err, chronicle, Some(phase)))?;

        match resolved {
            JsonValue::Bool(flag) => Ok(flag),
            JsonValue::Number(num) => Ok(num.as_i64().unwrap_or_default() != 0),
            JsonValue::String(text) => match text.to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" | "on" => Ok(true),
                "false" | "0" | "no" | "off" => Ok(false),
                other => Err(ChronicleEngineError::InvalidPhaseOptions {
                    chronicle: chronicle.to_string(),
                    phase: phase.to_string(),
                    reason: format!("mqtt retain value `{other}` cannot be interpreted as boolean"),
                }),
            },
            JsonValue::Null => Ok(false),
            other => Err(ChronicleEngineError::InvalidPhaseOptions {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                reason: format!("mqtt retain expected boolean-compatible value, found {other:?}"),
            }),
        }
    } else {
        Ok(false)
    }
}

fn encode_mqtt_payload(
    chronicle: &str,
    phase: &str,
    payload: &JsonValue,
    encoding: MqttPayloadEncoding,
) -> Result<String, ChronicleEngineError> {
    match encoding {
        MqttPayloadEncoding::Json => serde_json::to_string(payload).map_err(|err| {
            ChronicleEngineError::InvalidPhaseOptions {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                reason: format!("failed to encode mqtt payload as json: {err}"),
            }
        }),
        MqttPayloadEncoding::Base64 => {
            let bytes = mqtt_payload_bytes(chronicle, phase, payload)?;
            Ok(BASE64_STANDARD.encode(bytes))
        }
    }
}

fn mqtt_payload_bytes(
    chronicle: &str,
    phase: &str,
    payload: &JsonValue,
) -> Result<Vec<u8>, ChronicleEngineError> {
    match payload {
        JsonValue::Null => Ok(Vec::new()),
        JsonValue::String(text) => match BASE64_STANDARD.decode(text) {
            Ok(decoded) => Ok(decoded),
            Err(_) => Ok(text.as_bytes().to_vec()),
        },
        JsonValue::Array(items) => {
            let mut bytes = Vec::with_capacity(items.len());
            for (index, item) in items.iter().enumerate() {
                let value = match item {
                    JsonValue::Number(num) => {
                        if let Some(u) = num.as_u64() {
                            if u <= u8::MAX as u64 {
                                u as u8
                            } else {
                                return Err(ChronicleEngineError::InvalidPhaseOptions {
                                    chronicle: chronicle.to_string(),
                                    phase: phase.to_string(),
                                    reason: format!(
                                        "mqtt payload byte at index {index} exceeds 255"
                                    ),
                                });
                            }
                        } else if let Some(i) = num.as_i64() {
                            if (0..=255).contains(&i) {
                                i as u8
                            } else {
                                return Err(ChronicleEngineError::InvalidPhaseOptions {
                                    chronicle: chronicle.to_string(),
                                    phase: phase.to_string(),
                                    reason: format!(
                                        "mqtt payload byte at index {index} exceeds range 0-255"
                                    ),
                                });
                            }
                        } else {
                            return Err(ChronicleEngineError::InvalidPhaseOptions {
                                chronicle: chronicle.to_string(),
                                phase: phase.to_string(),
                                reason: format!(
                                    "mqtt payload byte at index {index} must be numeric"
                                ),
                            });
                        }
                    }
                    other => {
                        return Err(ChronicleEngineError::InvalidPhaseOptions {
                            chronicle: chronicle.to_string(),
                            phase: phase.to_string(),
                            reason: format!(
                                "mqtt payload array expects numeric entries, found {other:?}"
                            ),
                        });
                    }
                };
                bytes.push(value);
            }
            Ok(bytes)
        }
        other => {
            serde_json::to_vec(other).map_err(|err| ChronicleEngineError::InvalidPhaseOptions {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                reason: format!("failed to serialise mqtt payload for base64 encoding: {err}"),
            })
        }
    }
}

fn next_packet_id(seq: &Arc<AtomicU16>) -> u64 {
    let previous = seq
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
            let next = if current == u16::MAX {
                1
            } else {
                current.saturating_add(1)
            };
            Some(next)
        })
        .unwrap_or(1);

    if previous == 0 {
        1
    } else {
        previous as u64
    }
}

fn resolve_optional_string_field(
    chronicle: &str,
    phase: &str,
    template: Option<&JsonValue>,
    context: &ExecutionContext,
) -> Result<Option<String>, ChronicleEngineError> {
    if let Some(value) = template {
        let resolved = context
            .resolve_to_string(value)
            .map_err(|err| map_context_error(err, chronicle, Some(phase)))?;
        let trimmed = resolved.trim();
        if trimmed.is_empty() {
            Ok(None)
        } else {
            Ok(Some(trimmed.to_string()))
        }
    } else {
        Ok(None)
    }
}

fn resolve_email_recipients(
    chronicle: &str,
    phase: &str,
    label: &str,
    template: Option<&JsonValue>,
    context: &ExecutionContext,
) -> Result<Vec<String>, ChronicleEngineError> {
    let Some(template) = template else {
        return Ok(Vec::new());
    };

    let resolved = context
        .resolve_template(template)
        .map_err(|err| map_context_error(err, chronicle, Some(phase)))?;

    let mut recipients = Vec::new();

    match resolved {
        JsonValue::Null => {}
        JsonValue::String(text) => {
            for entry in text.split([',', ';']) {
                let trimmed = entry.trim();
                if !trimmed.is_empty() {
                    recipients.push(trimmed.to_string());
                }
            }
        }
        JsonValue::Array(items) => {
            for item in items {
                match item {
                    JsonValue::Null => {}
                    JsonValue::String(text) => {
                        let trimmed = text.trim();
                        if !trimmed.is_empty() {
                            recipients.push(trimmed.to_string());
                        }
                    }
                    JsonValue::Number(num) => recipients.push(num.to_string()),
                    JsonValue::Bool(flag) => recipients.push(flag.to_string()),
                    other => {
                        return Err(ChronicleEngineError::InvalidPhaseOptions {
                            chronicle: chronicle.to_string(),
                            phase: phase.to_string(),
                            reason: format!(
                                "smtp recipient list `{label}` must contain strings, found {other:?}"
                            ),
                        });
                    }
                }
            }
        }
        other => {
            return Err(ChronicleEngineError::InvalidPhaseOptions {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                reason: format!(
                    "smtp recipient template `{label}` must resolve to string or array, found {other:?}"
                ),
            });
        }
    }

    Ok(recipients)
}

fn smtp_tls_mode_to_string(mode: &SmtpTlsMode) -> &'static str {
    match mode {
        SmtpTlsMode::Starttls => "starttls",
        SmtpTlsMode::Tls => "tls",
        SmtpTlsMode::None => "none",
    }
}

#[derive(Debug, Error)]
pub enum ChronicleEngineError {
    #[error("chronicle `{name}` not found")]
    UnknownChronicle { name: String },
    #[error("phase `{phase}` in chronicle `{chronicle}` missing option `{option}`")]
    MissingOption {
        chronicle: String,
        phase: String,
        option: String,
    },
    #[error("phase `{phase}` in chronicle `{chronicle}` has invalid options: {reason}")]
    InvalidPhaseOptions {
        chronicle: String,
        phase: String,
        reason: String,
    },
    #[error(
        "phase `{phase}` in chronicle `{chronicle}` requires connector `{name}` of type `{expected}`"
    )]
    MissingConnector {
        chronicle: String,
        phase: String,
        name: String,
        expected: &'static str,
    },
    #[error("phase `{phase}` in chronicle `{chronicle}` uses unsupported kind `{kind}`")]
    UnsupportedPhase {
        chronicle: String,
        phase: String,
        kind: String,
    },
    #[error("chronicle `{chronicle}` fallback references unknown phase `{phase}`")]
    UnknownFallbackPhase { chronicle: String, phase: String },
    #[error(
        "chronicle `{chronicle}` rejected due to route backpressure (policy: {policy:?}, limit: {limit})"
    )]
    RouteBackpressure {
        chronicle: String,
        policy: OverflowPolicy,
        limit: u32,
    },
    #[error("chronicle `{chronicle}` queue depth exceeded (max {max_queue_depth})")]
    RouteQueueOverflow {
        chronicle: String,
        max_queue_depth: u32,
    },
    #[error("phase `{phase}` in chronicle `{chronicle}` failed to resolve schema: {reason}")]
    SchemaResolution {
        chronicle: String,
        phase: String,
        reason: String,
    },
    #[error("phase `{phase}` in chronicle `{chronicle}` failed to encode payload: {reason}")]
    Serialization {
        chronicle: String,
        phase: String,
        reason: String,
    },
    #[error("failed to resolve `{expression}` in {location}: {reason}")]
    ContextResolution {
        location: String,
        expression: String,
        reason: String,
    },
}

fn clone_object(value: &JsonValue) -> Option<JsonMap<String, JsonValue>> {
    value.as_object().cloned()
}

fn resolve_template_to_object(
    chronicle: &str,
    phase: &str,
    template: Option<&JsonValue>,
    context: &ExecutionContext,
    label: &str,
) -> Result<JsonValue, ChronicleEngineError> {
    let Some(template) = template else {
        return Ok(JsonValue::Object(JsonMap::new()));
    };

    if let Some(map) = template.as_object() {
        let mut resolved = JsonMap::new();
        for (key, value) in map {
            let resolved_value = context
                .resolve_template(value)
                .map_err(|err| map_context_error(err, chronicle, Some(phase)))?;
            resolved.insert(key.clone(), resolved_value);
        }
        return Ok(JsonValue::Object(resolved));
    }

    let resolved = context
        .resolve_template(template)
        .map_err(|err| map_context_error(err, chronicle, Some(phase)))?;

    match resolved {
        JsonValue::Object(_) => Ok(resolved),
        other => Err(ChronicleEngineError::InvalidPhaseOptions {
            chronicle: chronicle.to_string(),
            phase: phase.to_string(),
            reason: format!(
                "rabbitmq phase option `{label}` must resolve to an object, got {other:?}"
            ),
        }),
    }
}

fn take_string(
    chronicle: &str,
    phase: &str,
    map: &mut JsonMap<String, JsonValue>,
    key: &str,
) -> Result<String, ChronicleEngineError> {
    match map.remove(key) {
        Some(JsonValue::String(inner)) => Ok(inner),
        Some(other) => Err(ChronicleEngineError::InvalidPhaseOptions {
            chronicle: chronicle.to_string(),
            phase: phase.to_string(),
            reason: format!("expected `{key}` to be a string, got {other:?}"),
        }),
        None => Err(ChronicleEngineError::MissingOption {
            chronicle: chronicle.to_string(),
            phase: phase.to_string(),
            option: key.to_string(),
        }),
    }
}

fn take_optional_string(map: &mut JsonMap<String, JsonValue>, key: &str) -> Option<String> {
    map.remove(key).and_then(|value| match value {
        JsonValue::String(inner) => Some(inner),
        JsonValue::Null => None,
        other => Some(other.to_string()),
    })
}

fn parse_timeout_value(
    chronicle: &str,
    phase: &str,
    label: &str,
    value: JsonValue,
) -> Result<Option<u64>, ChronicleEngineError> {
    match value {
        JsonValue::Null => Ok(None),
        JsonValue::Number(num) => num
            .as_u64()
            .ok_or_else(|| ChronicleEngineError::InvalidPhaseOptions {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                reason: format!("phase option `{label}` must be a positive integer"),
            })
            .map(Some),
        JsonValue::String(text) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }

            if let Ok(ms) = trimmed.parse::<u64>() {
                return Ok(Some(ms));
            }

            let duration = parse_duration(trimmed).map_err(|err| {
                ChronicleEngineError::InvalidPhaseOptions {
                    chronicle: chronicle.to_string(),
                    phase: phase.to_string(),
                    reason: format!(
                        "phase option `{label}` must be numeric milliseconds or duration, found `{trimmed}` ({err})"
                    ),
                }
            })?;
            Ok(Some(duration.as_millis() as u64))
        }
        other => Err(ChronicleEngineError::InvalidPhaseOptions {
            chronicle: chronicle.to_string(),
            phase: phase.to_string(),
            reason: format!(
                "phase option `{label}` must be numeric milliseconds or duration string, found {other:?}"
            ),
        }),
    }
}

fn take_optional_timeout_ms(
    chronicle: &str,
    phase: &str,
    map: &mut JsonMap<String, JsonValue>,
) -> Result<Option<u64>, ChronicleEngineError> {
    let timeout_value = map.remove("timeout");
    let timeout_ms_value = map.remove("timeout_ms");

    let timeout = if let Some(value) = timeout_value {
        parse_timeout_value(chronicle, phase, "timeout", value)?
    } else {
        None
    };

    let timeout_ms = if let Some(value) = timeout_ms_value {
        parse_timeout_value(chronicle, phase, "timeout_ms", value)?
    } else {
        None
    };

    if timeout.is_some() && timeout_ms.is_some() {
        return Err(ChronicleEngineError::InvalidPhaseOptions {
            chronicle: chronicle.to_string(),
            phase: phase.to_string(),
            reason: "phase options `timeout` and `timeout_ms` are mutually exclusive".to_string(),
        });
    }

    Ok(timeout.or(timeout_ms))
}

fn json_to_option_map(value: &JsonValue) -> OptionMap {
    let mut map = OptionMap::new();
    flatten_json_into("", value, &mut map);
    map
}

fn flatten_json_into(prefix: &str, value: &JsonValue, target: &mut OptionMap) {
    match value {
        JsonValue::Object(map) => {
            for (key, nested) in map {
                let combined = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                flatten_json_into(&combined, nested, target);
            }
        }
        JsonValue::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                let combined = if prefix.is_empty() {
                    index.to_string()
                } else {
                    format!("{prefix}.{index}")
                };
                flatten_json_into(&combined, item, target);
            }
        }
        JsonValue::Null => {
            target.insert(prefix.to_string(), ScalarValue::Null);
        }
        JsonValue::Bool(flag) => {
            target.insert(prefix.to_string(), ScalarValue::Bool(*flag));
        }
        JsonValue::Number(num) => {
            target.insert(prefix.to_string(), ScalarValue::Number(num.clone()));
        }
        JsonValue::String(text) => {
            target.insert(prefix.to_string(), ScalarValue::String(text.clone()));
        }
    }
}

fn parse_parallel_aggregation(
    chronicle: &str,
    phase: &str,
    options: &mut JsonMap<String, JsonValue>,
    child_count: usize,
) -> Result<ParallelAggregation, ChronicleEngineError> {
    let aggregation = match options.remove("aggregation") {
        Some(JsonValue::String(text)) => text,
        Some(JsonValue::Null) | None => "all".to_string(),
        Some(other) => {
            return Err(ChronicleEngineError::InvalidPhaseOptions {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                reason: format!(
                    "parallel phase option `aggregation` must be a string, found {other:?}"
                ),
            })
        }
    };

    let normalized = aggregation.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "all" => {
            if options.remove("quorum").is_some() {
                return Err(ChronicleEngineError::InvalidPhaseOptions {
                    chronicle: chronicle.to_string(),
                    phase: phase.to_string(),
                    reason: "`aggregation: all` does not accept `quorum`".to_string(),
                });
            }
            Ok(ParallelAggregation::All)
        }
        "any" => {
            if options.remove("quorum").is_some() {
                return Err(ChronicleEngineError::InvalidPhaseOptions {
                    chronicle: chronicle.to_string(),
                    phase: phase.to_string(),
                    reason: "`aggregation: any` does not accept `quorum`".to_string(),
                });
            }
            Ok(ParallelAggregation::Any)
        }
        "quorum" => {
            let quorum_value = match options.remove("quorum") {
                Some(JsonValue::Number(num)) => {
                    num.as_u64()
                        .ok_or_else(|| ChronicleEngineError::InvalidPhaseOptions {
                            chronicle: chronicle.to_string(),
                            phase: phase.to_string(),
                            reason: "`quorum` must be a positive integer".to_string(),
                        })?
                }
                Some(JsonValue::String(text)) => text.trim().parse::<u64>().map_err(|_| {
                    ChronicleEngineError::InvalidPhaseOptions {
                        chronicle: chronicle.to_string(),
                        phase: phase.to_string(),
                        reason: "`quorum` must be a positive integer".to_string(),
                    }
                })?,
                Some(other) => {
                    return Err(ChronicleEngineError::InvalidPhaseOptions {
                        chronicle: chronicle.to_string(),
                        phase: phase.to_string(),
                        reason: format!("`quorum` must be numeric, found {other:?}"),
                    })
                }
                None => child_count.div_ceil(2) as u64,
            };

            if quorum_value == 0 {
                return Err(ChronicleEngineError::InvalidPhaseOptions {
                    chronicle: chronicle.to_string(),
                    phase: phase.to_string(),
                    reason: "`quorum` must be at least 1".to_string(),
                });
            }

            if quorum_value as usize > child_count {
                return Err(ChronicleEngineError::InvalidPhaseOptions {
                    chronicle: chronicle.to_string(),
                    phase: phase.to_string(),
                    reason: format!(
                        "`quorum` value {quorum_value} exceeds child count {child_count}"
                    ),
                });
            }

            Ok(ParallelAggregation::Quorum {
                threshold: quorum_value as usize,
            })
        }
        other => Err(ChronicleEngineError::InvalidPhaseOptions {
            chronicle: chronicle.to_string(),
            phase: phase.to_string(),
            reason: format!("unsupported parallel aggregation `{other}`"),
        }),
    }
}

fn build_parallel_child(
    chronicle: &str,
    parent_phase: &str,
    index: usize,
    value: &JsonValue,
    registry: &ConnectorRegistry,
) -> Result<ParallelChildPlan, ChronicleEngineError> {
    let child_object = value.as_object().ok_or_else(|| {
        ChronicleEngineError::InvalidPhaseOptions {
            chronicle: chronicle.to_string(),
            phase: parent_phase.to_string(),
            reason: format!(
                "parallel child at index {index} must be an object with `name`, `type`, and `options`"
            ),
        }
    })?;

    let mut child_map = child_object.clone();

    let child_name = child_map
        .remove("name")
        .and_then(|value| value.as_str().map(str::to_string))
        .ok_or_else(|| ChronicleEngineError::InvalidPhaseOptions {
            chronicle: chronicle.to_string(),
            phase: parent_phase.to_string(),
            reason: format!("parallel child at index {index} requires a `name` string"),
        })?;

    let child_type = child_map
        .remove("type")
        .and_then(|value| value.as_str().map(str::to_string))
        .ok_or_else(|| ChronicleEngineError::InvalidPhaseOptions {
            chronicle: chronicle.to_string(),
            phase: parent_phase.to_string(),
            reason: format!(
                "parallel child `{child_name}` (index {index}) requires a `type` string"
            ),
        })?;

    let child_kind = PhaseKind::from_raw(&child_type);
    if matches!(child_kind, PhaseKind::Parallel) {
        return Err(ChronicleEngineError::InvalidPhaseOptions {
            chronicle: chronicle.to_string(),
            phase: parent_phase.to_string(),
            reason: "parallel phases do not support nested `parallel` children".to_string(),
        });
    }

    let optional = match child_map.remove("optional") {
        Some(JsonValue::Bool(flag)) => flag,
        Some(JsonValue::Null) | None => false,
        Some(other) => {
            return Err(ChronicleEngineError::InvalidPhaseOptions {
                chronicle: chronicle.to_string(),
                phase: parent_phase.to_string(),
                reason: format!(
                    "parallel child `{child_name}` option `optional` must be a boolean, found {other:?}"
                ),
            })
        }
    };

    let fallback = match child_map.remove("fallback") {
        Some(JsonValue::String(text)) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }
        Some(JsonValue::Null) | None => None,
        Some(other) => {
            return Err(ChronicleEngineError::InvalidPhaseOptions {
                chronicle: chronicle.to_string(),
                phase: parent_phase.to_string(),
                reason: format!(
                "parallel child `{child_name}` option `fallback` must be a string, found {other:?}"
            ),
            })
        }
    };

    let child_phase_label = format!("{parent_phase}.{child_name}");
    let timeout_ms = take_optional_timeout_ms(chronicle, &child_phase_label, &mut child_map)?;

    let options_value = child_map
        .remove("options")
        .unwrap_or_else(|| JsonValue::Object(JsonMap::new()));

    let options_map = match &options_value {
        JsonValue::Object(_) => json_to_option_map(&options_value),
        other => {
            return Err(ChronicleEngineError::InvalidPhaseOptions {
                chronicle: chronicle.to_string(),
                phase: parent_phase.to_string(),
                reason: format!(
                "parallel child `{child_name}` option `options` must be an object, found {other:?}"
            ),
            })
        }
    };

    if !child_map.is_empty() {
        let remaining = child_map
            .keys()
            .cloned()
            .collect::<Vec<String>>()
            .join(", ");
        return Err(ChronicleEngineError::InvalidPhaseOptions {
            chronicle: chronicle.to_string(),
            phase: parent_phase.to_string(),
            reason: format!("parallel child `{child_name}` has unsupported keys: {remaining}"),
        });
    }

    let child_phase = ChroniclePhase {
        name: child_name.clone(),
        kind: child_kind.clone(),
        options: options_map,
    };

    let plan = PhasePlan::from_definition(chronicle, &child_phase, registry)?;

    Ok(ParallelChildPlan {
        name: child_name,
        plan,
        timeout_ms,
        optional,
        fallback,
    })
}

fn normalize_template(value: Option<JsonValue>) -> Option<JsonValue> {
    match value {
        Some(JsonValue::Null) | None => None,
        Some(other) => Some(other),
    }
}

fn validate_mongodb_templates(
    chronicle: &str,
    phase: &str,
    operation: MongoOperation,
    document: &Option<JsonValue>,
    filter: &Option<JsonValue>,
    update: &Option<JsonValue>,
    pipeline: &Option<JsonValue>,
) -> Result<(), ChronicleEngineError> {
    let ensure_object = |label: &str,
                         value: &Option<JsonValue>|
     -> Result<(), ChronicleEngineError> {
        if let Some(JsonValue::Object(_)) = value {
            Ok(())
        } else {
            Err(ChronicleEngineError::InvalidPhaseOptions {
                chronicle: chronicle.to_string(),
                phase: phase.to_string(),
                reason: format!("mongodb phase option `{label}` must be an object when provided"),
            })
        }
    };

    match operation {
        MongoOperation::InsertOne => {
            ensure_object("document", document)?;
            if document.is_none() {
                return Err(ChronicleEngineError::InvalidPhaseOptions {
                    chronicle: chronicle.to_string(),
                    phase: phase.to_string(),
                    reason: "mongodb insert_one requires `document`".to_string(),
                });
            }
        }
        MongoOperation::ReplaceOne => {
            ensure_object("document", document)?;
            ensure_object("filter", filter)?;
            if document.is_none() || filter.is_none() {
                return Err(ChronicleEngineError::InvalidPhaseOptions {
                    chronicle: chronicle.to_string(),
                    phase: phase.to_string(),
                    reason: "mongodb replace_one requires both `document` and `filter`".to_string(),
                });
            }
        }
        MongoOperation::UpdateOne => {
            ensure_object("update", update)?;
            ensure_object("filter", filter)?;
            if update.is_none() || filter.is_none() {
                return Err(ChronicleEngineError::InvalidPhaseOptions {
                    chronicle: chronicle.to_string(),
                    phase: phase.to_string(),
                    reason: "mongodb update_one requires both `update` and `filter`".to_string(),
                });
            }
        }
        MongoOperation::DeleteOne => {
            ensure_object("filter", filter)?;
            if filter.is_none() {
                return Err(ChronicleEngineError::InvalidPhaseOptions {
                    chronicle: chronicle.to_string(),
                    phase: phase.to_string(),
                    reason: "mongodb delete_one requires `filter`".to_string(),
                });
            }
        }
        MongoOperation::Aggregate => match pipeline {
            Some(JsonValue::Array(_)) => {}
            Some(_) => {
                return Err(ChronicleEngineError::InvalidPhaseOptions {
                    chronicle: chronicle.to_string(),
                    phase: phase.to_string(),
                    reason: "mongodb aggregate requires `pipeline` to be an array".to_string(),
                })
            }
            None => {
                return Err(ChronicleEngineError::InvalidPhaseOptions {
                    chronicle: chronicle.to_string(),
                    phase: phase.to_string(),
                    reason: "mongodb aggregate requires `pipeline`".to_string(),
                })
            }
        },
    }

    Ok(())
}
