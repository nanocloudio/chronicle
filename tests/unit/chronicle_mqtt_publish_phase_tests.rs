
use chronicle::chronicle::context::ExecutionContext;
use chronicle::chronicle::phase::mqtt_publish::MqttPublishPhaseExecutor;
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
            name: "publish".to_string(),
            kind: PhaseKind::Unknown("mqtt".to_string()),
            options,
        }
    }

    fn registry_with_mqtt(name: &str) -> ConnectorFactoryRegistry {
        let mut connectors = Vec::new();
        let mut connector_options = OptionMap::new();
        connector_options.insert(
            "url".to_string(),
            ScalarValue::String("mqtt://localhost:1883".to_string()),
        );

        connectors.push(ConnectorConfig {
            name: name.to_string(),
            kind: ConnectorKind::Mqtt,
            options: connector_options,
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

        let registry = ConnectorRegistry::build(&config, Path::new(".")).expect("registry build");
        ConnectorFactoryRegistry::new(Arc::new(registry))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn qos0_defaults_and_retains_metadata() {
        let mut options = OptionMap::new();
        options.insert(
            "connector".to_string(),
            ScalarValue::String("telemetry".to_string()),
        );
        options.insert(
            "topic".to_string(),
            ScalarValue::String("devices/alpha".to_string()),
        );
        options.insert(
            "payload.message".to_string(),
            ScalarValue::String(".[0].message".to_string()),
        );

        let phase = build_phase(options);
        let registry = registry_with_mqtt("telemetry");
        let executor = MqttPublishPhaseExecutor::default();
        let mut context = ExecutionContext::new();
        context.insert_slot(0, json!({ "message": { "id": "abc123" } }));

        let result = executor
            .execute(&phase, &mut context, &registry)
            .await
            .expect("qos0 publish");

        assert_eq!(result["qos"], json!(0));
        assert_eq!(result["retain"], json!(false));
        assert!(result["packet_id"].is_null());
        assert_eq!(
            result["payload_encoding"],
            json!(PayloadEncoding::Json.as_str())
        );
        assert_eq!(
            result["payload_encoded"],
            json!(r#"{"message":{"id":"abc123"}}"#)
        );
        let published_at = result["published_at"]
            .as_str()
            .expect("timestamp string present");
        chrono::DateTime::parse_from_rfc3339(published_at).expect("valid rfc3339 timestamp");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn qos1_assigns_packet_id_and_uses_context_fallback() {
        let mut options = OptionMap::new();
        options.insert(
            "connector".to_string(),
            ScalarValue::String("telemetry".to_string()),
        );
        options.insert(
            "topic".to_string(),
            ScalarValue::String("devices/beta".to_string()),
        );
        options.insert("qos".to_string(), ScalarValue::Number(JsonNumber::from(1)));

        let phase = build_phase(options);
        let registry = registry_with_mqtt("telemetry");
        let executor = MqttPublishPhaseExecutor::default();
        let mut context = ExecutionContext::new();
        context.insert_slot(0, json!({ "payload": { "reading": 42 } }));

        let result = executor
            .execute(&phase, &mut context, &registry)
            .await
            .expect("qos1 publish");

        assert_eq!(result["qos"], json!(1));
        assert!(result["packet_id"].as_u64().expect("packet id present") > 0);
        assert_eq!(
            result["payload_encoded"],
            json!(r#"{"payload":{"reading":42}}"#)
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn qos2_retained_payload_encodes_binary() {
        let mut options = OptionMap::new();
        options.insert(
            "connector".to_string(),
            ScalarValue::String("telemetry".to_string()),
        );
        options.insert(
            "topic".to_string(),
            ScalarValue::String("devices/gamma".to_string()),
        );
        options.insert("qos".to_string(), ScalarValue::Number(JsonNumber::from(2)));
        options.insert("retain".to_string(), ScalarValue::Bool(true));
        options.insert(
            "payload".to_string(),
            ScalarValue::String(".[0].binary".to_string()),
        );
        options.insert(
            "payload_encoding".to_string(),
            ScalarValue::String("base64".to_string()),
        );

        let phase = build_phase(options);
        let registry = registry_with_mqtt("telemetry");
        let executor = MqttPublishPhaseExecutor::default();
        let mut context = ExecutionContext::new();
        context.insert_slot(0, json!({ "binary": [1, 2, 3] }));

        let result = executor
            .execute(&phase, &mut context, &registry)
            .await
            .expect("qos2 publish");

        assert_eq!(result["qos"], json!(2));
        assert_eq!(result["retain"], json!(true));
        assert_eq!(result["payload_encoding"], json!("base64"));
        assert_eq!(result["payload_encoded"], json!("AQID"));
        assert!(result["packet_id"].as_u64().unwrap() > 0);
    }
