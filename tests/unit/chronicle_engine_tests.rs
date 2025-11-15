
use chronicle::chronicle::dispatcher::{ActionDispatcher, DeliveryContext};
use chronicle::chronicle::engine::*;
use chronicle::config::integration::{self, OverflowPolicy};
use chronicle::config::IntegrationConfig;
use chronicle::integration::factory::ConnectorFactoryRegistry;
use chronicle::integration::registry::ConnectorRegistry;
    use prost_types::FileDescriptorSet;
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::io::Write;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;
    use tempfile::NamedTempFile;

    #[path = "../support/mod.rs"]
    mod support;

    fn fixture_engine() -> ChronicleEngine {
        let config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/chronicle-integration.yaml");
        let config = IntegrationConfig::from_path(&config_path).expect("fixture config");
        let registry =
            ConnectorRegistry::build(&config, config_path.parent().expect("fixture directory"))
                .expect("registry build");

        ChronicleEngine::new(Arc::new(config), Arc::new(registry)).expect("engine build")
    }

    fn engine_from_yaml(yaml: &str) -> ChronicleEngine {
        let decorated = support::feature_flags::enable_optional_feature_flags(yaml);
        let config = IntegrationConfig::from_reader(decorated.as_bytes()).expect("config");
        let registry = ConnectorRegistry::build(&config, ".").expect("registry");
        ChronicleEngine::new(Arc::new(config), Arc::new(registry)).expect("engine build")
    }

    fn empty_dispatcher() -> ActionDispatcher {
        let registry = Arc::new(ConnectorRegistry::default());
        let factory = Arc::new(ConnectorFactoryRegistry::new(registry));
        ActionDispatcher::new(factory, None, None)
    }

    #[test]
    fn execute_collect_record_builds_actions_and_response() {
        let engine = fixture_engine();

        let trigger_payload = json!({
            "headers": {
                "trace_id": "trace-123",
                "content-type": "application/json"
            },
            "body": {
                "record": {
                    "id": "rec-123",
                    "attributes": {
                        "category": "telemetry",
                        "tier": "gold"
                    },
                    "metrics": {
                        "latency_ms": 42
                    },
                    "observed_at": "2024-01-01T12:00:00Z"
                }
            }
        });

        let execution = engine
            .execute("collect_record", trigger_payload)
            .expect("collect_record execution");

        assert_eq!(execution.actions.len(), 1);
        assert_eq!(execution.trace_id.as_deref(), Some("trace-123"));
        assert_eq!(execution.record_id.as_deref(), Some("rec-123"));
        assert!(execution.delivery.is_none());
        assert!(!execution.allow_partial_delivery);
        assert!(execution.fallback.is_none());

        match &execution.actions[0] {
            ChronicleAction::KafkaPublish {
                connector,
                topic,
                payload,
                trace_id,
                record_id,
                ..
            } => {
                assert_eq!(connector, "sample_kafka_cluster");
                assert_eq!(topic, "records.samples");
                assert_eq!(payload["summary"]["record_id"], json!("rec-123"));
                assert_eq!(payload["summary"]["latency_ms"], json!(42));
                assert_eq!(trace_id.as_deref(), Some("trace-123"));
                assert_eq!(record_id.as_deref(), Some("rec-123"));
            }
            other => panic!("unexpected action: {other:?}"),
        }

        let response = execution.response.expect("response is present");
        assert_eq!(response.status, 200);
        assert_eq!(response.content_type.as_deref(), Some("application/json"));
        assert_eq!(response.body["record_id"], json!("rec-123"));
        assert_eq!(response.body["trace_id"], json!("trace-123"));
    }

    #[test]
    fn execute_persist_record_produces_insert_and_ack() {
        let engine = fixture_engine();

        let trigger_payload = json!({
            "headers": {
                "message_id": "msg-1"
            },
            "body": {
                "record": {
                    "id": "rec-123",
                    "attributes": {
                        "category": "telemetry"
                    },
                    "metrics": {
                        "latency_ms": 42
                    }
                }
            }
        });

        let execution = engine
            .execute("persist_record", trigger_payload)
            .expect("persist_record execution");

        assert_eq!(execution.actions.len(), 1);
        assert_eq!(execution.trace_id.as_deref(), None);
        assert_eq!(execution.record_id.as_deref(), Some("rec-123"));

        match &execution.actions[0] {
            ChronicleAction::MariadbInsert {
                connector,
                key,
                values,
                trace_id,
                record_id,
                ..
            } => {
                assert_eq!(connector, "sample_state_store");
                assert_eq!(key, &JsonValue::String("rec-123".to_string()));
                assert_eq!(
                    values["payload"]["attributes"]["category"],
                    json!("telemetry")
                );
                assert_eq!(trace_id.as_deref(), None);
                assert_eq!(record_id.as_deref(), Some("rec-123"));
            }
            other => panic!("unexpected action: {other:?}"),
        }

        let response = execution.response.expect("ack response");
        assert_eq!(response.status, 200);
        assert_eq!(response.content_type.as_deref(), Some("application/json"));
        let body = response.body.as_object().expect("response body object");
        assert_eq!(
            body.get("record_id").and_then(|value| value.as_str()),
            Some("rec-123")
        );
        assert_eq!(
            body.get("inserted").and_then(|value| value.as_i64()),
            Some(1)
        );
    }

    #[test]
    fn parallel_phase_builds_children_outputs() {
        let yaml = r#"
api_version: v1
app: {}
connectors:
  - name: http_in
    type: http_server
    options:
      host: 127.0.0.1
      port: 8080
  - name: summary_client
    type: http_client
    options:
      base_url: http://summary
  - name: audit_client
    type: http_client
    options:
      base_url: http://audit
chronicles:
  - name: fanout
    trigger:
      connector: http_in
      options:
        method: POST
        path: /fanout
    phases:
      - name: fanout_parallel
        type: parallel
        options:
          phases:
            - name: summary
              type: http_client
              options:
                connector: summary_client
                method: POST
                path: /summary
                body:
                  id: .[0].body.id
            - name: audit
              type: http_client
              options:
                connector: audit_client
                method: POST
                path: /audit
                body:
                  id: .[0].body.id
"#;
        let engine = engine_from_yaml(yaml);
        let trigger_payload = json!({
            "body": {
                "id": "rec-42"
            }
        });

        let execution = engine
            .execute("fanout", trigger_payload)
            .expect("parallel execution");

        assert_eq!(execution.actions.len(), 2);

        match &execution.actions[0] {
            ChronicleAction::HttpRequest { connector, path, .. } => {
                assert_eq!(connector, "summary_client");
                assert_eq!(path, "/summary");
            }
            other => panic!("unexpected action: {other:?}"),
        }

        match &execution.actions[1] {
            ChronicleAction::HttpRequest { connector, path, .. } => {
                assert_eq!(connector, "audit_client");
                assert_eq!(path, "/audit");
            }
            other => panic!("unexpected action: {other:?}"),
        }

        assert_eq!(execution.context.len(), 2);
        let parallel_slot = execution.context[1]
            .as_object()
            .expect("parallel context object");
        assert_eq!(
            parallel_slot.get("aggregation_state"),
            Some(&json!("pending"))
        );
        assert_eq!(parallel_slot.get("aggregation"), Some(&json!("all")));

        let children = parallel_slot
            .get("children")
            .and_then(|value| value.as_object())
            .expect("children map");

        let summary_meta = children
            .get("summary")
            .and_then(|value| value.as_object())
            .expect("summary metadata");
        assert_eq!(summary_meta.get("status"), Some(&json!("pending")));
        assert_eq!(summary_meta.get("optional"), Some(&json!(false)));

        let audit_meta = children
            .get("audit")
            .and_then(|value| value.as_object())
            .expect("audit metadata");
        assert_eq!(audit_meta.get("status"), Some(&json!("pending")));
        assert_eq!(audit_meta.get("optional"), Some(&json!(false)));
    }

    #[test]
    fn parallel_optional_child_failure_sets_partial_state() {
        let yaml = r#"
api_version: v1
app: {}
connectors:
  - name: http_in
    type: http_server
    options:
      host: 127.0.0.1
      port: 8081
  - name: summary_client
    type: http_client
    options:
      base_url: http://summary
  - name: audit_client
    type: http_client
    options:
      base_url: http://audit
chronicles:
  - name: fanout_optional
    trigger:
      connector: http_in
      options:
        method: POST
        path: /fanout
    phases:
      - name: fanout_parallel
        type: parallel
        options:
          phases:
            - name: summary
              type: http_client
              options:
                connector: summary_client
                method: POST
                path: /summary
                body:
                  id: .[0].body.id
            - name: audit
              type: http_client
              optional: true
              options:
                connector: audit_client
                method: POST
                path: /audit
                body:
                  id: .[99].body.id
"#;
        let engine = engine_from_yaml(yaml);
        let trigger_payload = json!({
            "body": {
                "id": "rec-99"
            }
        });

        let execution = engine
            .execute("fanout_optional", trigger_payload)
            .expect("parallel execution");

        // only the successful child should produce an action
        assert_eq!(execution.actions.len(), 1);

        let parallel_slot = execution.context[1]
            .as_object()
            .expect("parallel context object");
        assert_eq!(
            parallel_slot.get("aggregation_state"),
            Some(&json!("partial"))
        );

        let children = parallel_slot
            .get("children")
            .and_then(|value| value.as_object())
            .expect("children map");

        let summary_meta = children
            .get("summary")
            .and_then(|value| value.as_object())
            .expect("summary metadata");
        assert_eq!(summary_meta.get("status"), Some(&json!("pending")));

        let audit_meta = children
            .get("audit")
            .and_then(|value| value.as_object())
            .expect("audit metadata");
        assert_eq!(audit_meta.get("status"), Some(&json!("skipped")));
        assert!(audit_meta
            .get("error")
            .and_then(|value| value.as_str())
            .is_some());
    }

    #[test]
    fn mongodb_phase_builds_action_and_context() {
        let yaml = r#"
api_version: v1
app: {}
connectors:
  - name: http_in
    type: http_server
    options:
      host: 127.0.0.1
      port: 9090
  - name: mongo_primary
    type: mongodb
    options:
      uri: mongodb://localhost:27017
      database: chronicle
chronicles:
  - name: mongo_route
    trigger:
      connector: http_in
      options:
        method: POST
        path: /mongo
    phases:
      - name: to_doc
        type: transform
        options:
          event:
            id: .[0].body.id
            value: .[0].body.value
      - name: store
        type: mongodb
        options:
          connector: mongo_primary
          collection: records
          operation: insert_one
          document:
            event: .[1].event
"#;
        let engine = engine_from_yaml(yaml);
        let trigger_payload = json!({
            "body": {
                "id": "doc-1",
                "value": "payload"
            }
        });

        let execution = engine
            .execute("mongo_route", trigger_payload)
            .expect("mongo execution");

        assert_eq!(execution.actions.len(), 1);

        match &execution.actions[0] {
            ChronicleAction::MongodbCommand {
                connector,
                collection,
                operation,
                document,
                trace_id,
                ..
            } => {
                assert_eq!(connector, "mongo_primary");
                assert_eq!(collection, "records");
                assert_eq!(operation, "insert_one");
                let doc = document.as_ref().expect("document present");
                assert_eq!(doc["event"]["id"], json!("doc-1"));
                assert_eq!(trace_id, &None);
            }
            other => panic!("unexpected action: {other:?}"),
        }

        assert_eq!(execution.context.len(), 3);
        let mongo_slot = execution.context[2]
            .as_object()
            .expect("mongo context object");
        assert_eq!(mongo_slot.get("status"), Some(&json!("pending")));
        assert_eq!(
            mongo_slot
                .get("document")
                .and_then(|value| value.get("event"))
                .and_then(|value| value.get("id")),
            Some(&json!("doc-1"))
        );
    }

    #[test]
    fn mongodb_phase_reports_context_resolution_failures() {
        let yaml = r#"
api_version: v1
app: {}
connectors:
  - name: http_in
    type: http_server
    options:
      host: 127.0.0.1
      port: 9091
  - name: mongo_primary
    type: mongodb
    options:
      uri: mongodb://localhost:27017
      database: chronicle
chronicles:
  - name: mongo_invalid
    trigger:
      connector: http_in
      options:
        method: POST
        path: /mongo
    phases:
      - name: store
        type: mongodb
        options:
          connector: mongo_primary
          collection: records
          operation: insert_one
          document:
            missing: .[0].missing
"#;
        let engine = engine_from_yaml(yaml);
        let trigger_payload = json!({
            "body": {
                "id": "doc-err"
            }
        });

        let err = engine
            .execute("mongo_invalid", trigger_payload)
            .expect_err("execution fails");
        assert!(matches!(
            err,
            ChronicleEngineError::ContextResolution { .. }
        ));
    }

    #[test]
    fn redis_phase_builds_action_and_context() {
        let yaml = r#"
api_version: v1
app: {}
connectors:
  - name: http_in
    type: http_server
    options:
      host: 127.0.0.1
      port: 7070
  - name: redis_primary
    type: redis
    options:
      url: redis://localhost:6379
chronicles:
  - name: redis_route
    trigger:
      connector: http_in
      options:
        method: POST
        path: /redis
    phases:
      - name: write_cache
        type: redis
        options:
          connector: redis_primary
          command: set
          key: .[0].body.id
          value: .[0].body.value
          ttl: 60
"#;
        let engine = engine_from_yaml(yaml);
        let trigger_payload = json!({
            "body": {
                "id": "cache-1",
                "value": "payload"
            }
        });

        let execution = engine
            .execute("redis_route", trigger_payload)
            .expect("redis execution");

        assert_eq!(execution.actions.len(), 1);

        match &execution.actions[0] {
            ChronicleAction::RedisCommand {
                connector,
                command,
                arguments,
                ..
            } => {
                assert_eq!(connector, "redis_primary");
                assert_eq!(command, "set");
                assert_eq!(arguments["key"], json!("cache-1"));
                assert_eq!(arguments["value"], json!("payload"));
                assert_eq!(arguments["ttl"], json!(60));
            }
            other => panic!("unexpected action: {other:?}"),
        }

        assert_eq!(execution.context.len(), 2);
        let redis_slot = execution.context[1]
            .as_object()
            .expect("redis context object");
        assert_eq!(redis_slot.get("status"), Some(&json!("pending")));
        assert_eq!(
            redis_slot
                .get("arguments")
                .and_then(|value| value.get("key")),
            Some(&json!("cache-1"))
        );
    }

    #[test]
    fn redis_phase_reports_resolution_errors() {
        let yaml = r#"
api_version: v1
app: {}
connectors:
  - name: http_in
    type: http_server
    options:
      host: 127.0.0.1
      port: 7071
  - name: redis_primary
    type: redis
    options:
      url: redis://localhost:6379
chronicles:
  - name: redis_invalid
    trigger:
      connector: http_in
      options:
        method: POST
        path: /redis
    phases:
      - name: write_cache
        type: redis
        options:
          connector: redis_primary
          command: set
          key: .[0].missing
"#;
        let engine = engine_from_yaml(yaml);
        let trigger_payload = json!({
            "body": {
                "id": "cache-err"
            }
        });

        let err = engine
            .execute("redis_invalid", trigger_payload)
            .expect_err("redis execution fails");
        assert!(matches!(
            err,
            ChronicleEngineError::ContextResolution { .. }
        ));
    }

    #[test]
    fn serialize_with_local_schema_produces_avro_payload() {
        let schema_path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/schemas/summary.avsc");
        let file_source = Arc::new(FileSchemaSource::new(schema_path));
        let schema = SchemaSource::File(file_source);
        let config = AvroPhaseConfig { schema };
        let plan = SerializePhasePlan {
            name: "encode_for_kafka".to_string(),
            codec: SerializeCodec::Avro(config),
            input_template: None,
        };

        let input = json!({
            "record_id": "rec-1",
            "category": "telemetry",
            "tier": "gold",
            "latency_ms": 42
        });

        let encoded = plan.encode("demo", input).expect("serialization succeeds");

        assert_eq!(encoded["codec"], json!("avro"));
        assert_eq!(encoded["content_type"], json!("application/avro"));
        assert_eq!(encoded["wire_format"], json!("avro-binary"));
        assert!(encoded["binary"].as_str().expect("base64 payload").len() > 8);
        assert!(encoded["schema_path"]
            .as_str()
            .expect("schema path")
            .ends_with("summary.avsc"));
    }

    #[test]
    fn serialize_with_protobuf_descriptor_produces_payload() {
        use prost_types::field_descriptor_proto::{Label, Type};
        use prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorProto};

        let fields = vec![
            FieldDescriptorProto {
                name: Some("record_id".to_string()),
                number: Some(1),
                label: Some(Label::Optional as i32),
                r#type: Some(Type::String as i32),
                ..Default::default()
            },
            FieldDescriptorProto {
                name: Some("category".to_string()),
                number: Some(2),
                label: Some(Label::Optional as i32),
                r#type: Some(Type::String as i32),
                ..Default::default()
            },
            FieldDescriptorProto {
                name: Some("tier".to_string()),
                number: Some(3),
                label: Some(Label::Optional as i32),
                r#type: Some(Type::String as i32),
                ..Default::default()
            },
            FieldDescriptorProto {
                name: Some("latency_ms".to_string()),
                number: Some(4),
                label: Some(Label::Optional as i32),
                r#type: Some(Type::Int32 as i32),
                ..Default::default()
            },
        ];

        let message = DescriptorProto {
            name: Some("SummaryRecord".to_string()),
            field: fields,
            ..Default::default()
        };

        let file = FileDescriptorProto {
            name: Some("summary.proto".to_string()),
            package: Some("com.nanocloud.chronicle".to_string()),
            message_type: vec![message],
            ..Default::default()
        };

        let descriptor_set = FileDescriptorSet { file: vec![file] };
        let mut bytes = Vec::new();
        descriptor_set
            .encode(&mut bytes)
            .expect("encode descriptor set");

        let mut temp = NamedTempFile::new().expect("temp descriptor");
        temp.write_all(&bytes).expect("write descriptor bytes");

        let config = ProtobufPhaseConfig {
            descriptor: Arc::new(DescriptorSource::new(temp.path().to_path_buf())),
            message: "com.nanocloud.chronicle.SummaryRecord".to_string(),
        };

        let plan = SerializePhasePlan {
            name: "encode_for_kafka_protobuf".to_string(),
            codec: SerializeCodec::Protobuf(config),
            input_template: None,
        };

        let input = json!({
            "record_id": "rec-1",
            "category": "telemetry",
            "tier": "gold",
            "latency_ms": 42,
        });

        let encoded = plan.encode("demo", input).expect("serialization succeeds");

        assert_eq!(encoded["codec"], json!("protobuf"));
        assert_eq!(encoded["content_type"], json!("application/x-protobuf"));
        assert_eq!(
            encoded["message"],
            json!("com.nanocloud.chronicle.SummaryRecord")
        );

        let binary = encoded["binary"].as_str().expect("base64 payload");
        let bytes = BASE64_STANDARD
            .decode(binary)
            .expect("decode protobuf bytes");
        assert!(!bytes.is_empty());
    }

    #[test]
    fn missing_context_produces_context_resolution_error() {
        let engine = fixture_engine();

        let trigger_payload = json!({
            "header": {
                "message_id": "abc123"
            },
            "body": {
                "user": {
                    "id": "user-123"
                }
            }
        });

        let err = engine
            .execute("collect_record", trigger_payload)
            .expect_err("missing data should error");

        match err {
            ChronicleEngineError::ContextResolution {
                location,
                expression,
                reason,
            } => {
                assert!(
                    location.contains("collect_record"),
                    "expected chronicle in location but got {location}"
                );
                assert!(
                    location.contains("summarize_record"),
                    "expected phase name in location but got {location}"
                );
                assert_eq!(expression, ".[0].body.record.attributes.category");
                assert!(
                    reason.contains("missing"),
                    "expected missing in reason but got {reason}"
                );
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn rabbitmq_phase_emits_actions_and_metadata() {
        let config = r#"
connectors:
  - name: ingest
    type: http_server
    options:
      host: 0.0.0.0
      port: 8080
  - name: rabbit
    type: rabbitmq
    options:
      url: amqp://guest:guest@localhost:5672/%2f

chronicles:
  - name: publish_rabbit
    trigger:
      connector: ingest
      options:
        method: POST
        path: /events
    phases:
      - name: build_message
        type: transform
        options:
          exchange: .[0].body.message.exchange
          routing_key: .[0].body.message.routing
          headers.correlation_id: .[0].header.correlation_id
          properties.app_id: "chronicle"
          properties.kind: .[0].body.message.kind
          body.payload: .[0].body.message.payload
      - name: send_message
        type: rabbitmq
        options:
          connector: rabbit
          exchange: .[1].exchange
          routing_key: .[1].routing_key
          headers: .[1].headers
          properties: .[1].properties
          body: .[1].body.payload
          mandatory: true
          confirm: true
          timeout_ms: 2000
"#;

        let engine = engine_from_yaml(config);

        let trigger_payload = json!({
            "header": {
                "correlation_id": "corr-123"
            },
            "body": {
                "message": {
                    "exchange": "events",
                    "routing": "sensor.update",
                    "kind": "sample",
                    "payload": {
                        "id": 42,
                        "value": "ok"
                    }
                }
            }
        });

        let execution = engine
            .execute("publish_rabbit", trigger_payload)
            .expect("rabbitmq execution");

        assert_eq!(execution.actions.len(), 1);

        match &execution.actions[0] {
            ChronicleAction::RabbitmqPublish {
                connector,
                url,
                exchange,
                routing_key,
                payload,
                headers,
                properties,
                mandatory,
                confirm,
                timeout_ms,
                ..
            } => {
                assert_eq!(connector, "rabbit");
                assert_eq!(url, "amqp://guest:guest@localhost:5672/%2f");
                assert_eq!(exchange.as_deref(), Some("events"));
                assert_eq!(routing_key.as_deref(), Some("sensor.update"));
                assert_eq!(payload["id"], json!(42));
                assert_eq!(payload["value"], json!("ok"));
                assert_eq!(headers["correlation_id"], json!("corr-123"));
                assert_eq!(properties["app_id"], json!("chronicle"));
                assert_eq!(properties["kind"], json!("sample"));
                assert_eq!(*mandatory, Some(true));
                assert_eq!(*confirm, Some(true));
                assert_eq!(*timeout_ms, Some(2000));
            }
            other => panic!("unexpected action: {other:?}"),
        }

        assert_eq!(execution.context.len(), 3);
        let metadata = execution.context[2].as_object().expect("metadata object");
        assert_eq!(metadata["connector"], json!("rabbit"));
        assert_eq!(metadata["exchange"], json!("events"));
        assert_eq!(metadata["routing_key"], json!("sensor.update"));
        assert_eq!(metadata["mandatory"], json!(true));
        assert_eq!(metadata["confirm"], json!(true));
        assert_eq!(metadata["timeout_ms"], json!(2000));
        assert_eq!(metadata["status"], json!("pending"));
        assert_eq!(metadata["count"], json!(1));
        assert_eq!(metadata["headers"]["correlation_id"], json!("corr-123"));
        assert_eq!(metadata["properties"]["app_id"], json!("chronicle"));
    }

    #[test]
    fn rabbitmq_phase_errors_when_headers_invalid() {
        let config = r#"
connectors:
  - name: ingest
    type: http_server
    options:
      host: 0.0.0.0
      port: 8080
  - name: rabbit
    type: rabbitmq
    options:
      url: amqp://guest:guest@localhost:5672/%2f

chronicles:
  - name: publish_rabbit
    trigger:
      connector: ingest
      options:
        method: POST
        path: /events
    phases:
      - name: build_message
        type: transform
        options:
          body.value: .[0].body.value
      - name: send_message
        type: rabbitmq
        options:
          connector: rabbit
          routing_key: "events"
          headers: true
          body: .[1].body.value
"#;

        let engine = engine_from_yaml(config);
        let trigger_payload = json!({ "body": { "value": { "id": 1 } } });

        let err = engine
            .execute("publish_rabbit", trigger_payload)
            .expect_err("invalid headers should cause failure");

        match err {
            ChronicleEngineError::InvalidPhaseOptions { phase, .. } => {
                assert_eq!(phase, "send_message");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn mqtt_phase_enqueues_publish_action() {
        let config = r#"
connectors:
  - name: ingest
    type: http_server
    options:
      host: 0.0.0.0
      port: 8081
  - name: telemetry
    type: mqtt
    options:
      url: mqtt://broker.example.com:1883
      client_id: chronicle-worker

chronicles:
  - name: publish_mqtt
    trigger:
      connector: ingest
      options:
        method: POST
        path: /telemetry
    phases:
      - name: normalise
        type: transform
        options:
          topic: .[0].body.event.topic
          payload: .[0].body.event.payload
      - name: publish_telemetry
        type: mqtt
        options:
          connector: telemetry
          topic: .[1].topic
          payload: .[1].payload
          qos: 1
          retain: false
          timeout_ms: 1500
"#;

        let engine = engine_from_yaml(config);

        let payload = json!({
            "body": {
                "event": {
                    "topic": "devices/alpha",
                    "payload": {
                        "reading": 42
                    }
                }
            }
        });

        let execution = engine
            .execute("publish_mqtt", payload)
            .expect("mqtt chronicle executes");

        assert_eq!(execution.actions.len(), 1);

        match &execution.actions[0] {
            ChronicleAction::MqttPublish {
                connector,
                topic,
                qos,
                retain,
                payload,
                payload_encoding,
                encoded_payload,
                timeout_ms,
                ..
            } => {
                assert_eq!(connector, "telemetry");
                assert_eq!(topic, "devices/alpha");
                assert_eq!(*qos, 1);
                assert!(!retain);
                assert_eq!(payload["reading"], json!(42));
                assert_eq!(payload_encoding, "json");
                assert!(encoded_payload.contains("reading"));
                assert_eq!(*timeout_ms, Some(1500));
            }
            other => panic!("unexpected action: {other:?}"),
        }

        let context = execution.context.last().expect("context slot present");
        assert_eq!(context["topic"], json!("devices/alpha"));
        assert_eq!(context["qos"], json!(1));
        assert_eq!(context["timeout_ms"], json!(1500));
    }

    #[test]
    fn postgres_phase_produces_query_action() {
        let config = r#"
connectors:
  - name: ingest
    type: http_server
    options:
      host: 0.0.0.0
      port: 8082
  - name: analytics
    type: postgres
    options:
      url: postgres://chronicle:chronicle@localhost:5432/analytics
      schema: public

chronicles:
  - name: record_event
    trigger:
      connector: ingest
      options:
        method: POST
        path: /events
    phases:
      - name: extract
        type: transform
        options:
          event.id: .[0].body.event.id
          event.kind: .[0].body.event.kind
      - name: store
        type: postgres_idempotent_insert
        options:
          connector: analytics
          sql: INSERT INTO events (event_id, kind) VALUES (:event_id, :kind)
          values.event_id: .[1].event.id
          values.kind: .[1].event.kind
          timeout_ms: 2000
"#;

        let engine = engine_from_yaml(config);

        let payload = json!({
            "body": {
                "event": {
                    "id": "evt-42",
                    "kind": "click"
                }
            }
        });

        let execution = engine
            .execute("record_event", payload)
            .expect("postgres chronicle executes");

        assert_eq!(execution.actions.len(), 1);

        match &execution.actions[0] {
            ChronicleAction::PostgresQuery {
                connector,
                sql,
                parameters,
                schema,
                timeout_ms,
                ..
            } => {
                assert_eq!(connector, "analytics");
                assert_eq!(schema.as_deref(), Some("public"));
                assert_eq!(
                    sql,
                    "INSERT INTO events (event_id, kind) VALUES (:event_id, :kind)"
                );
                assert_eq!(parameters["event_id"], json!("evt-42"));
                assert_eq!(parameters["kind"], json!("click"));
                assert_eq!(*timeout_ms, Some(2000));
            }
            other => panic!("unexpected action: {other:?}"),
        }

        let context = execution.context.last().expect("context slot present");
        assert_eq!(
            context["sql"],
            json!("INSERT INTO events (event_id, kind) VALUES (:event_id, :kind)")
        );
        assert_eq!(context["parameters"]["event_id"], json!("evt-42"));
        assert_eq!(context["timeout_ms"], json!(2000));
    }

    #[test]
    fn route_metadata_infers_dependencies_and_app_limits() {
        let config = r#"
api_version: v1
app:
  limits:
    routes:
      max_inflight: 10
      overflow_policy: queue
connectors:
  - name: ingest
    type: http_server
    options:
      host: 0.0.0.0
      port: 8081
  - name: outbound_kafka
    type: kafka
    options:
      brokers: ["localhost:9092"]
chronicles:
  - name: infer_dependencies
    trigger:
      connector: ingest
      options:
        method: POST
        path: /events
    phases:
      - name: map
        type: transform
        options:
          record.id: .[0].body.id
      - name: publish
        type: kafka_producer
        options:
          connector: outbound_kafka
          topic: events.updates
        "#;

        let engine = engine_from_yaml(config);
        let metadata = engine
            .route_metadata("infer_dependencies")
            .expect("route metadata");

        assert_eq!(metadata.dependencies, vec!["outbound_kafka"]);
        assert_eq!(metadata.inferred_dependencies, vec!["outbound_kafka"]);
        assert_eq!(metadata.dependency_source, DependencySource::Inferred);
        assert_eq!(metadata.limits.max_inflight, Some(10));
        assert_eq!(
            metadata.limits.overflow_policy,
            Some(OverflowPolicy::Queue)
        );
        assert_eq!(metadata.limits.max_queue_depth, None);
    }

    #[test]
    fn route_metadata_honours_explicit_requires_and_overrides_limits() {
        let config = r#"
api_version: v1
app:
  limits:
    routes:
      max_inflight: 12
      overflow_policy: queue
connectors:
  - name: ingest
    type: http_server
    options:
      host: 0.0.0.0
      port: 8082
  - name: primary_kafka
    type: kafka
    options:
      brokers: ["localhost:9092"]
  - name: dead_letter
    type: kafka
    options:
      brokers: ["localhost:9093"]
chronicles:
  - name: explicit_policy
    trigger:
      connector: ingest
      options:
        method: POST
        path: /records
    phases:
      - name: publish
        type: kafka_producer
        options:
          connector: primary_kafka
          topic: records.primary
    policy:
      requires: ["dead_letter", "primary_kafka"]
      limits:
        max_inflight: 5
      allow_partial_delivery: true
        "#;

        let engine = engine_from_yaml(config);
        let metadata = engine
            .route_metadata("explicit_policy")
            .expect("route metadata");

        assert_eq!(
            metadata.dependencies,
            vec!["dead_letter", "primary_kafka"]
        );
        assert_eq!(metadata.inferred_dependencies, vec!["primary_kafka"]);
        assert_eq!(metadata.dependency_source, DependencySource::Explicit);
        assert_eq!(metadata.allow_partial_delivery, true);
        assert_eq!(metadata.limits.max_inflight, Some(5));
        assert_eq!(
            metadata.limits.overflow_policy,
            Some(OverflowPolicy::Queue)
        );
    }


    #[test]
    fn route_concurrency_reject_policy_enforces_limit() {
        let limits = RouteRuntimeLimits {
            max_inflight: Some(1),
            overflow_policy: Some(OverflowPolicy::Reject),
            max_queue_depth: None,
        };
        let concurrency = RouteConcurrency::new(&limits);

        let permit = concurrency
            .acquire("limited-route")
            .expect("first permit succeeds");

        let err = concurrency
            .acquire("limited-route")
            .expect_err("second permit should be rejected");
        assert!(matches!(
            err,
            ChronicleEngineError::RouteBackpressure {
                policy: OverflowPolicy::Reject,
                limit: 1,
                ..
            }
        ));

        drop(permit);
    }

    #[test]
    fn route_concurrency_queue_blocks_until_capacity_available() {
        let limits = RouteRuntimeLimits {
            max_inflight: Some(1),
            overflow_policy: Some(OverflowPolicy::Queue),
            max_queue_depth: Some(8),
        };
        let concurrency = RouteConcurrency::new(&limits);

        let first = concurrency
            .acquire("queued-route")
            .expect("first permit succeeds");

        let concurrency_clone = concurrency.clone();
        let handle = thread::spawn(move || concurrency_clone.acquire("queued-route"));

        thread::sleep(Duration::from_millis(25));
        drop(first);

        let second = handle
            .join()
            .expect("thread join")
            .expect("second permit granted after release");
        drop(second);
    }

    #[test]
    fn route_concurrency_queue_depth_overflow_errors() {
        let limits = RouteRuntimeLimits {
            max_inflight: Some(1),
            overflow_policy: Some(OverflowPolicy::Queue),
            max_queue_depth: Some(0),
        };
        let concurrency = RouteConcurrency::new(&limits);

        let permit = concurrency
            .acquire("queue-overflow")
            .expect("first permit succeeds");

        let err = concurrency
            .acquire("queue-overflow")
            .expect_err("queue should overflow immediately");
        assert!(matches!(
            err,
            ChronicleEngineError::RouteQueueOverflow {
                max_queue_depth: 0,
                ..
            }
        ));

        drop(permit);
    }

    #[test]
    fn execution_exposes_delivery_policy_from_route() {
        let yaml = r#"
api_version: v1
app: {}
connectors:
  - name: http_in
    type: http_server
    options:
      host: 127.0.0.1
      port: 9092
  - name: http_out
    type: http_client
    options:
      base_url: http://example.com
chronicles:
  - name: deliver
    trigger:
      connector: http_in
      options:
        method: POST
        path: /deliver
    phases:
      - name: forward
        type: http_client
        options:
          connector: http_out
          method: POST
          path: /sink
          body:
            id: .[0].body.id
    policy:
      allow_partial_delivery: true
      delivery: { retries: 2, backoff: "100ms..1s", idempotent: true }
      fallback:
        http_out: forward
"#;

        let engine = engine_from_yaml(yaml);
        let payload = json!({ "body": { "id": 9 }});
        let execution = engine
            .execute("deliver", payload)
            .expect("route execution");

        let delivery = execution.delivery.expect("delivery policy present");
        assert_eq!(delivery.retries, Some(2));
        let backoff = delivery.backoff.expect("backoff present");
        assert_eq!(backoff.min, Duration::from_millis(100));
        assert_eq!(backoff.max, Duration::from_secs(1));
        assert_eq!(delivery.idempotent, Some(true));
        assert!(execution.allow_partial_delivery);
        let fallback = execution.fallback.expect("fallback map present");
        assert_eq!(fallback.get("http_out"), Some(&"forward".to_string()));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dispatcher_honours_fallback_when_partial_delivery_allowed() {
        let dispatcher = empty_dispatcher();
        let action = ChronicleAction::HttpRequest {
            connector: "missing".to_string(),
            base_url: "http://example.com".to_string(),
            method: "GET".to_string(),
            path: "/".to_string(),
            headers: BTreeMap::new(),
            body: JsonValue::Null,
            content_type: None,
            extra: JsonValue::Null,
            trace_id: None,
            record_id: None,
        };

        let actions = vec![action];
        let mut fallback = BTreeMap::new();
        fallback.insert("missing".to_string(), "noop".to_string());
        let mut fallback_actions = BTreeMap::new();
        fallback_actions.insert("noop".to_string(), Vec::new());

        let context = DeliveryContext {
            policy: None,
            fallback: Some(&fallback),
            fallback_actions: Some(&fallback_actions),
            allow_partial_delivery: true,
            retry_budget: None,
        };

        let result = dispatcher.dispatch("test-route", &actions, context).await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dispatcher_propagates_failure_without_fallback() {
        let dispatcher = empty_dispatcher();
        let action = ChronicleAction::HttpRequest {
            connector: "missing".to_string(),
            base_url: "http://example.com".to_string(),
            method: "GET".to_string(),
            path: "/".to_string(),
            headers: BTreeMap::new(),
            body: JsonValue::Null,
            content_type: None,
            extra: JsonValue::Null,
            trace_id: None,
            record_id: None,
        };

        let actions = vec![action];
        let context = DeliveryContext {
            policy: None,
            fallback: None,
            fallback_actions: None,
            allow_partial_delivery: false,
            retry_budget: None,
        };

        let result = dispatcher.dispatch("test-route", &actions, context).await;
        assert!(result.is_err());
    }

    #[test]
    fn execute_builds_fallback_action_map() {
        let yaml = r#"
api_version: v1
connectors:
  - name: ingest
    type: http_server
    options:
      host: 0.0.0.0
      port: 8080
  - name: kafka-primary
    type: kafka
    options:
      brokers: ["broker:9092"]
  - name: kafka-dlq
    type: kafka
    options:
      brokers: ["broker:9092"]
chronicles:
  - name: fallback-route
    trigger:
      connector: ingest
      options:
        path: /events
    phases:
      - name: primary_publish
        type: kafka_producer
        options:
          connector: kafka-primary
          topic: events
      - name: dlq_publish
        type: kafka_producer
        options:
          connector: kafka-dlq
          topic: events.dlq
    policy:
      allow_partial_delivery: true
      fallback:
        kafka-primary: dlq_publish
"#;

        let engine = engine_from_yaml(yaml);
        let payload = json!({ "body": { "id": 42 } });
        let execution = engine
            .execute("fallback-route", payload)
            .expect("route execution");

        assert_eq!(
            execution.actions.len(),
            1,
            "fallback phases should not run during primary execution"
        );
        let fallback_map = execution
            .fallback_actions
            .as_ref()
            .expect("fallback action map present");
        let dlq_actions = fallback_map
            .get("dlq_publish")
            .expect("dlq fallback actions present");
        assert_eq!(dlq_actions.len(), 1);
        match &dlq_actions[0] {
            ChronicleAction::KafkaPublish { connector, topic, .. } => {
                assert_eq!(connector, "kafka-dlq");
                assert_eq!(topic, "events.dlq");
            }
            other => panic!("unexpected fallback action: {other:?}"),
        }
    }
