#![cfg(all(feature = "rabbitmq", feature = "mqtt"))]

#[path = "support/mod.rs"]
mod support;

use async_trait::async_trait;
use chronicle::app::{
    should_start_mongodb_runtime, should_start_mqtt_runtime, should_start_rabbitmq_runtime,
};
use chronicle::chronicle::engine::{ChronicleAction, ChronicleEngine};
use chronicle::chronicle::mqtt_triggers::{MqttSubscriber, MqttTriggerRuntime};
use chronicle::chronicle::rabbitmq_triggers::{
    RabbitmqAckMode, RabbitmqConsumer, RabbitmqConsumerConfig, RabbitmqTriggerRuntime,
};
use chronicle::config::{ConnectorFlags, IntegrationConfig};
use chronicle::integration::registry::ConnectorRegistry;
use chronicle::telemetry::runtime_counters;
use chronicle::transport::mqtt::MqttSubscriberConfig;
use serde_json::{json, Map as JsonMap, Value as JsonValue};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use support::mocks::{MockMqttBroker, MockRabbitmqBroker};

struct MessageBrokerHarness {
    config: Arc<IntegrationConfig>,
    registry: Arc<ConnectorRegistry>,
    engine: Arc<ChronicleEngine>,
}

impl MessageBrokerHarness {
    fn new() -> Self {
        let config_path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/message_brokers.yaml");
        let config =
            Arc::new(IntegrationConfig::from_path(&config_path).expect("message broker config"));
        let registry = Arc::new(
            ConnectorRegistry::build(&config, config_path.parent().expect("fixture dir"))
                .expect("registry build"),
        );
        let engine = Arc::new(
            ChronicleEngine::new(Arc::clone(&config), Arc::clone(&registry)).expect("engine build"),
        );

        Self {
            config,
            registry,
            engine,
        }
    }

    fn execute(&self, chronicle: &str, payload: JsonValue) -> ChronicleActionBundle {
        let execution = self
            .engine
            .execute(chronicle, payload)
            .unwrap_or_else(|err| panic!("chronicle `{chronicle}` failed: {err:?}"));
        ChronicleActionBundle {
            actions: execution.actions,
        }
    }

    fn config(&self) -> Arc<IntegrationConfig> {
        Arc::clone(&self.config)
    }

    fn registry(&self) -> Arc<ConnectorRegistry> {
        Arc::clone(&self.registry)
    }

    fn engine(&self) -> Arc<ChronicleEngine> {
        Arc::clone(&self.engine)
    }
}

struct ChronicleActionBundle {
    actions: Vec<ChronicleAction>,
}

impl ChronicleActionBundle {
    fn expect_actions(&self, expected: usize) {
        assert_eq!(
            self.actions.len(),
            expected,
            "expected {expected} actions but got {}",
            self.actions.len()
        );
    }
}

#[test]
fn rabbitmq_pipeline_emits_http_and_publish() {
    let harness = MessageBrokerHarness::new();
    let payload = json!({
        "routing_key": "alerts.high",
        "exchange": "alerts.incoming",
        "body": {
            "severity": "high",
            "message": "temperature exceeded threshold"
        },
        "redelivered": false
    });

    let bundle = harness.execute("rabbitmq_alert_pipeline", payload);
    bundle.expect_actions(2);

    let http_req = match &bundle.actions[0] {
        ChronicleAction::HttpRequest {
            connector,
            method,
            path,
            content_type,
            ..
        } => {
            assert_eq!(connector, "http_backend");
            assert_eq!(method, "POST");
            assert_eq!(path, "/alerts/handle");
            assert_eq!(content_type.as_deref(), Some("application/json"));
            true
        }
        other => panic!("expected HTTP request action, got {other:?}"),
    };
    assert!(http_req, "http request action missing");

    let publish_action = match &bundle.actions[1] {
        ChronicleAction::RabbitmqPublish {
            connector,
            exchange,
            routing_key,
            payload,
            headers,
            ..
        } => {
            assert_eq!(connector, "rabbitmq_core");
            assert_eq!(exchange.as_deref(), Some("alerts.audit"));
            assert_eq!(routing_key.as_deref(), Some("processed.alerts"));

            let broker = MockRabbitmqBroker::default();
            let header_map: JsonMap<String, JsonValue> =
                headers.as_object().cloned().unwrap_or_default();
            let confirmation = broker.publish(
                "alerts.audit",
                "processed.alerts",
                payload.clone(),
                header_map,
            );

            let delivery = broker.next_delivery().expect("delivery queued");
            assert_eq!(delivery.exchange(), "alerts.audit");
            assert_eq!(delivery.routing_key(), "processed.alerts");
            assert_eq!(delivery.payload(), payload);

            delivery.clone().ack(&broker);
            confirmation.ack();
            assert!(
                confirmation.is_acknowledged(),
                "mock confirmation should be acknowledged"
            );

            true
        }
        other => panic!("expected RabbitMQ publish action, got {other:?}"),
    };
    assert!(publish_action, "rabbitmq publish action missing");
}

#[test]
fn mqtt_pipeline_emits_publish_and_postgres_query() {
    let harness = MessageBrokerHarness::new();
    let payload = json!({
        "topic": "sensors/telemetry/node-1",
        "qos": 1,
        "retain": false,
        "payload": {
            "base64": "eyJ0ZW1wIjoxOC41fQ==",
            "text": "{\"temp\":18.5}",
            "json": { "temp": 18.5 }
        },
        "metadata": {
            "packet_id": 42,
            "timestamp": 1_720_000_000
        }
    });

    let bundle = harness.execute("mqtt_sensor_ingest", payload);
    bundle.expect_actions(2);

    let mqtt_publish = match &bundle.actions[0] {
        ChronicleAction::MqttPublish {
            connector,
            topic,
            qos,
            retain,
            payload,
            payload_encoding,
            encoded_payload,
            ..
        } => {
            assert_eq!(connector, "mqtt_iot");
            assert_eq!(topic, "sensors/processed");
            assert_eq!(*qos, 1);
            assert!(!retain);
            assert_eq!(payload["topic"], json!("sensors/telemetry/node-1"));
            assert_eq!(payload_encoding, "json");
            assert!(
                !encoded_payload.is_empty(),
                "encoded payload should not be empty"
            );

            let broker = MockMqttBroker::default();
            let publish_result = broker.push(topic.clone(), payload.clone(), *qos, *retain);
            assert_eq!(publish_result.qos(), 1);
            assert!(
                broker.next().is_some(),
                "mqtt broker should expose queued message"
            );
            assert!(publish_result.packet_id() >= 1);

            true
        }
        other => panic!("expected MQTT publish action, got {other:?}"),
    };
    assert!(mqtt_publish, "mqtt publish action missing");

    let postgres_query = match &bundle.actions[1] {
        ChronicleAction::PostgresQuery {
            connector,
            sql,
            parameters,
            ..
        } => {
            assert_eq!(connector, "postgres_ledger");
            assert!(
                sql.contains("INSERT INTO sensor_readings"),
                "unexpected SQL: {sql}"
            );
            assert_eq!(
                parameters["topic"],
                json!("sensors/telemetry/node-1"),
                "topic parameter mismatch"
            );
            assert_eq!(
                parameters["payload_base64"],
                json!("eyJ0ZW1wIjoxOC41fQ=="),
                "payload parameter mismatch"
            );
            true
        }
        other => panic!("expected Postgres query action, got {other:?}"),
    };
    assert!(postgres_query, "postgres query action missing");
}

#[test]
fn metrics_snapshot_records_publish_results() {
    let counters = runtime_counters();
    let before = counters.snapshot();

    let harness = MessageBrokerHarness::new();

    let rabbit_payload = json!({
        "routing_key": "alerts.high",
        "exchange": "alerts.incoming",
        "body": {
            "severity": "high",
            "message": "temperature exceeded threshold"
        },
        "redelivered": false
    });
    harness.execute("rabbitmq_alert_pipeline", rabbit_payload);

    let mqtt_payload = json!({
        "topic": "sensors/telemetry/node-2",
        "qos": 1,
        "retain": false,
        "payload": {
            "base64": "eyJ0ZW1wIjoyMC4wfQ==",
            "text": "{\"temp\":20.0}",
            "json": { "temp": 20.0 }
        }
    });
    harness.execute("mqtt_sensor_ingest", mqtt_payload);

    let after = counters.snapshot();

    assert!(
        after.rabbitmq_publish_success > before.rabbitmq_publish_success,
        "expected rabbitmq publish success counter to increase"
    );
    assert!(
        after.mqtt_publish_success > before.mqtt_publish_success,
        "expected mqtt publish success counter to increase"
    );
    assert_eq!(
        after.rabbitmq_publish_failure, before.rabbitmq_publish_failure,
        "rabbitmq publish failure counter should not change"
    );
    assert_eq!(
        after.mqtt_publish_failure, before.mqtt_publish_failure,
        "mqtt publish failure counter should not change"
    );
}

#[derive(Clone, Default)]
struct TestRabbitConsumer;

#[async_trait]
impl RabbitmqConsumer for TestRabbitConsumer {
    async fn next_delivery(
        &mut self,
    ) -> Result<
        Option<chronicle::chronicle::rabbitmq_triggers::RabbitmqDelivery>,
        chronicle::chronicle::rabbitmq_triggers::RabbitmqConsumerError,
    > {
        Ok(None)
    }

    async fn ack(
        &mut self,
        _delivery_tag: u64,
    ) -> Result<(), chronicle::chronicle::rabbitmq_triggers::RabbitmqConsumerError> {
        Ok(())
    }

    async fn nack(
        &mut self,
        _delivery_tag: u64,
        _requeue: bool,
    ) -> Result<(), chronicle::chronicle::rabbitmq_triggers::RabbitmqConsumerError> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn rabbitmq_trigger_runtime_captures_consumer_config() {
    let harness = MessageBrokerHarness::new();
    let captured = Arc::new(Mutex::new(Vec::<RabbitmqConsumerConfig>::new()));
    let capture_clone = Arc::clone(&captured);

    let runtime = RabbitmqTriggerRuntime::build_with(
        harness.config(),
        harness.registry(),
        harness.engine(),
        None,
        None,
        move |config: RabbitmqConsumerConfig| {
            let capture = Arc::clone(&capture_clone);
            async move {
                capture.lock().expect("capture lock").push(config.clone());
                Ok(TestRabbitConsumer)
            }
        },
    )
    .await
    .expect("rabbitmq runtime build");

    assert_eq!(runtime.consumer_count(), 1);
    let configs = captured.lock().expect("captured config");
    assert_eq!(configs.len(), 1);
    let cfg = &configs[0];
    assert_eq!(cfg.queue, "alerts.primary");
    assert_eq!(cfg.prefetch, Some(5));
    match cfg.ack_mode {
        RabbitmqAckMode::Manual => {}
        RabbitmqAckMode::Auto => panic!("expected manual ack mode"),
    }
}

#[derive(Clone, Default)]
struct TestMqttSubscriber;

#[async_trait]
impl MqttSubscriber for TestMqttSubscriber {
    async fn subscribe(
        &mut self,
        _topic: &str,
        _qos: u8,
        _retain_handling: chronicle::chronicle::mqtt_triggers::RetainHandling,
    ) -> Result<(), chronicle::chronicle::mqtt_triggers::MqttSubscriberError> {
        Ok(())
    }

    async fn next_message(
        &mut self,
    ) -> Result<
        Option<chronicle::chronicle::mqtt_triggers::MqttMessage>,
        chronicle::chronicle::mqtt_triggers::MqttSubscriberError,
    > {
        Ok(None)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn mqtt_trigger_runtime_registers_subscriber() {
    let harness = MessageBrokerHarness::new();
    let registrations = Arc::new(Mutex::new(Vec::<(String, u8)>::new()));
    let registrations_clone = Arc::clone(&registrations);

    let runtime = MqttTriggerRuntime::build_with(
        harness.config(),
        harness.registry(),
        harness.engine(),
        None,
        None,
        move |config: MqttSubscriberConfig| {
            let registrations = Arc::clone(&registrations_clone);
            async move {
                registrations
                    .lock()
                    .expect("registration lock")
                    .push((config.topic.clone(), config.qos));
                Ok(TestMqttSubscriber)
            }
        },
    )
    .await
    .expect("mqtt runtime build");

    assert_eq!(runtime.subscriber_count(), 1);
    let entries = registrations.lock().expect("registrations");
    assert_eq!(
        entries.as_slice(),
        &[("sensors/telemetry/+".to_string(), 1)]
    );
}

#[test]
fn connector_flags_gate_trigger_runtimes() {
    let integration_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/message_brokers.yaml");
    let config = IntegrationConfig::from_path(&integration_path).expect("load integration");
    let registry = ConnectorRegistry::build(&config, integration_path.parent().unwrap())
        .expect("registry build");

    let disabled = ConnectorFlags {
        rabbitmq: false,
        mqtt: false,
        mongodb: false,
        redis: false,
    };
    assert!(!should_start_rabbitmq_runtime(&disabled, &registry));
    assert!(!should_start_mqtt_runtime(&disabled, &registry));
    assert!(!should_start_mongodb_runtime(&disabled, &registry));

    let enabled = ConnectorFlags {
        rabbitmq: true,
        mqtt: true,
        mongodb: true,
        redis: true,
    };
    assert!(should_start_rabbitmq_runtime(&enabled, &registry));
    assert!(should_start_mqtt_runtime(&enabled, &registry));
    let expect_mongodb = cfg!(feature = "db-mongodb") && registry.has_mongodb_connectors();
    assert_eq!(
        should_start_mongodb_runtime(&enabled, &registry),
        expect_mongodb
    );
}
