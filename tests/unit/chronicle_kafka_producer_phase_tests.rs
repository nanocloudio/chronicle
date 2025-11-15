
use chronicle::chronicle::context::ExecutionContext;
use chronicle::chronicle::phase::kafka_producer::KafkaProducerPhaseExecutor;
use chronicle::chronicle::phase::PhaseExecutionError;
use chronicle::config::integration::{
    AppConfig, ApiVersion, ConnectorConfig, ConnectorKind, IntegrationConfig, OptionMap, PhaseKind,
    ScalarValue,
};
use chronicle::config::integration::ChroniclePhase;
use chronicle::integration::factory::ConnectorFactoryRegistry;
use chronicle::integration::registry::ConnectorRegistry;
use serde_json::json;
use std::path::Path;
use std::sync::Arc;

    fn build_phase(options: OptionMap) -> ChroniclePhase {
        ChroniclePhase {
            name: "kafka".to_string(),
            kind: PhaseKind::KafkaProducer,
            options,
        }
    }

    fn registry_with_kafka(name: &str) -> ConnectorFactoryRegistry {
        let mut connectors = Vec::new();
        let mut options = OptionMap::new();
        options.insert(
            "brokers".to_string(),
            ScalarValue::String("localhost:9092".to_string()),
        );

        connectors.push(ConnectorConfig {
            name: name.to_string(),
            kind: ConnectorKind::Kafka,
            options,
            warmup: false,
            warmup_timeout: None,
        });

        let config = IntegrationConfig {
            api_version: ApiVersion::V1,
            app: AppConfig::default(),
            connectors,
            chronicles: Vec::new(),
            management: None,
        };

        let registry = ConnectorRegistry::build(&config, Path::new(".")).expect("registry");
        ConnectorFactoryRegistry::new(Arc::new(registry))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn builds_message_from_context() {
        let mut options = OptionMap::new();
        options.insert(
            "connector".to_string(),
            ScalarValue::String("kafka".to_string()),
        );
        options.insert(
            "topic".to_string(),
            ScalarValue::String(".[0].topic".to_string()),
        );
        options.insert("acks".to_string(), ScalarValue::String("all".to_string()));
        options.insert(
            "header.correlation".to_string(),
            ScalarValue::String(".[0].correlation".to_string()),
        );
        options.insert(
            "payload.value".to_string(),
            ScalarValue::String(".[0].value".to_string()),
        );

        let phase = build_phase(options);
        let registry = registry_with_kafka("kafka");
        let executor = KafkaProducerPhaseExecutor::default();
        let mut context = ExecutionContext::new();
        context.insert_slot(
            0,
            json!({
                "topic": "events",
                "correlation": "abc123",
                "value": 42
            }),
        );

        let result = executor
            .execute(&phase, &mut context, &registry)
            .await
            .expect("kafka message");

        assert_eq!(result["topic"], json!("events"));
        assert_eq!(result["acks"], json!("all"));
        assert_eq!(result["header"]["correlation"], json!("abc123"));
        assert_eq!(result["payload"]["value"], json!(42));
        assert_eq!(result["brokers"][0], json!("localhost:9092"));
        assert_eq!(result["count"], json!(1));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn falls_back_to_previous_slot_payload() {
        let mut options = OptionMap::new();
        options.insert(
            "connector".to_string(),
            ScalarValue::String("kafka".to_string()),
        );
        options.insert(
            "topic".to_string(),
            ScalarValue::String("static.topic".to_string()),
        );

        let phase = build_phase(options);
        let registry = registry_with_kafka("kafka");
        let executor = KafkaProducerPhaseExecutor::default();
        let mut context = ExecutionContext::new();
        context.insert_slot(0, json!({ "message": "hello" }));
        context.insert_slot(1, json!({ "transformed": true }));

        let result = executor
            .execute(&phase, &mut context, &registry)
            .await
            .expect("fallback payload");

        assert_eq!(result["payload"], json!({ "transformed": true }));
        assert_eq!(result["topic"], json!("static.topic"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn error_when_connector_missing() {
        let mut options = OptionMap::new();
        options.insert(
            "connector".to_string(),
            ScalarValue::String("missing".to_string()),
        );
        options.insert(
            "topic".to_string(),
            ScalarValue::String("events".to_string()),
        );

        let phase = build_phase(options);
        let registry = ConnectorFactoryRegistry::new(Arc::new(Default::default()));
        let executor = KafkaProducerPhaseExecutor::default();
        let mut context = ExecutionContext::new();

        let err = executor
            .execute(&phase, &mut context, &registry)
            .await
            .expect_err("missing connector");

        match err {
            PhaseExecutionError::Message(message) => {
                assert!(message.contains("connector"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn header_template_must_be_object() {
        let mut options = OptionMap::new();
        options.insert(
            "connector".to_string(),
            ScalarValue::String("kafka".to_string()),
        );
        options.insert(
            "topic".to_string(),
            ScalarValue::String("events".to_string()),
        );
        options.insert(
            "header".to_string(),
            ScalarValue::String("not-an-object".to_string()),
        );

        let phase = build_phase(options);
        let registry = registry_with_kafka("kafka");
        let executor = KafkaProducerPhaseExecutor::default();
        let mut context = ExecutionContext::new();

        let err = executor
            .execute(&phase, &mut context, &registry)
            .await
            .expect_err("invalid header map");

        match err {
            PhaseExecutionError::Message(message) => {
                assert!(message.contains("header object"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
