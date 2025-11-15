#![cfg(feature = "rabbitmq")]


use async_trait::async_trait;
use chronicle::chronicle::engine::ChronicleEngine;
use chronicle::config::integration::IntegrationConfig;
use chronicle::integration::registry::ConnectorRegistry;
use chronicle::transport::rabbitmq::{
    RabbitmqAckMode, RabbitmqConsumer, RabbitmqConsumerConfig, RabbitmqDelivery,
    RabbitmqTriggerRuntime,
};
use chronicle::transport::rabbitmq::RabbitmqConsumerError;
use chronicle::transport::TransportRuntime;
use serde_json::{json, Map as JsonMap};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout, Duration};
use tokio_util::sync::CancellationToken;

#[path = "../support/mod.rs"]
mod support;

type DeliveryAction = Result<Option<RabbitmqDelivery>, RabbitmqConsumerError>;

#[derive(Clone)]
struct MockConsumer {
    deliveries: Arc<Mutex<VecDeque<DeliveryAction>>>,
    acks: Arc<Mutex<Vec<u64>>>,
    nacks: Arc<Mutex<Vec<(u64, bool)>>>,
    prefetches: Arc<Mutex<Vec<u16>>>,
}

impl MockConsumer {
    fn new(
        deliveries: Arc<Mutex<VecDeque<DeliveryAction>>>,
        acks: Arc<Mutex<Vec<u64>>>,
        nacks: Arc<Mutex<Vec<(u64, bool)>>>,
        prefetches: Arc<Mutex<Vec<u16>>>,
    ) -> Self {
        Self {
            deliveries,
            acks,
            nacks,
            prefetches,
        }
    }
}

#[async_trait]
impl RabbitmqConsumer for MockConsumer {
    async fn configure_prefetch(&mut self, prefetch: u16) -> Result<(), RabbitmqConsumerError> {
        self.prefetches
            .lock()
            .expect("prefetch guard")
            .push(prefetch);
        Ok(())
    }

    async fn next_delivery(
        &mut self,
    ) -> Result<Option<RabbitmqDelivery>, RabbitmqConsumerError> {
        let mut guard = self.deliveries.lock().expect("delivery guard");
        guard.pop_front().unwrap_or_else(|| Ok(None))
    }

    async fn ack(&mut self, delivery_tag: u64) -> Result<(), RabbitmqConsumerError> {
        self.acks.lock().expect("ack guard").push(delivery_tag);
        Ok(())
    }

    async fn nack(
        &mut self,
        delivery_tag: u64,
        requeue: bool,
    ) -> Result<(), RabbitmqConsumerError> {
        self.nacks
            .lock()
            .expect("nack guard")
            .push((delivery_tag, requeue));
        Ok(())
    }
}

fn sample_delivery(tag: u64) -> RabbitmqDelivery {
    RabbitmqDelivery {
        body: serde_json::to_vec(&json!({"record": {"id": tag}})).expect("serialize body"),
        routing_key: "test.queue".to_string(),
        exchange: "events".to_string(),
        delivery_tag: tag,
        redelivered: false,
        headers: JsonMap::new(),
        properties: JsonMap::new(),
        timestamp: Some(1_700_000_000),
    }
}

fn build_config(phases: &str) -> Arc<IntegrationConfig> {
    let yaml = format!(
        r#"
connectors:
  - name: rabbit
type: rabbitmq
options:
  url: amqp://guest:guest@localhost:5672/%2f

chronicles:
  - name: test_rabbit
trigger:
  connector: rabbit
  options:
    type: rabbitmq
    queue: test.queue
    ack_mode: manual
    prefetch: 9
phases:
{phases}
"#
    );

    let yaml = support::feature_flags::enable_optional_feature_flags(&yaml);

    Arc::new(IntegrationConfig::from_reader(yaml.as_bytes()).expect("config"))
}

fn build_registry(config: &Arc<IntegrationConfig>) -> Arc<ConnectorRegistry> {
    Arc::new(ConnectorRegistry::build(config, ".").expect("registry build"))
}

async fn wait_for_condition<F>(timeout_ms: u64, mut predicate: F)
where
    F: FnMut() -> bool,
{
    timeout(Duration::from_millis(timeout_ms), async {
        loop {
            if predicate() {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("condition within timeout");
}

async fn start_runtime<R>(
    mut runtime: R,
) -> (
    R,
    CancellationToken,
    JoinHandle<chronicle::error::Result<()>>,
)
where
    R: TransportRuntime + Send + 'static,
{
    runtime
        .prepare()
        .await
        .expect("runtime preparation should succeed");
    let shutdown = CancellationToken::new();
    runtime
        .start(shutdown.clone())
        .await
        .expect("runtime start should succeed");
    let run = runtime.run();
    let task = tokio::spawn(async move { run.wait().await });
    (runtime, shutdown, task)
}

#[tokio::test(flavor = "multi_thread")]
async fn provisions_consumer_and_configures_prefetch() {
    let config = build_config("    - name: noop\n      type: transform\n      options: {}\n");
    let registry = build_registry(&config);
    let engine =
        Arc::new(ChronicleEngine::new(config.clone(), registry.clone()).expect("engine build"));

    let deliveries: Arc<Mutex<VecDeque<DeliveryAction>>> =
        Arc::new(Mutex::new(VecDeque::new()));
    let acks: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let nacks: Arc<Mutex<Vec<(u64, bool)>>> = Arc::new(Mutex::new(Vec::new()));
    let prefetches: Arc<Mutex<Vec<u16>>> = Arc::new(Mutex::new(Vec::new()));
    let recorded_configs: Arc<Mutex<Vec<RabbitmqConsumerConfig>>> =
        Arc::new(Mutex::new(Vec::new()));

    let runtime =
        RabbitmqTriggerRuntime::build_with(config.clone(), registry.clone(), engine.clone(), {
            let deliveries = deliveries.clone();
            let acks = acks.clone();
            let nacks = nacks.clone();
            let prefetches = prefetches.clone();
            let configs = recorded_configs.clone();
            move |cfg: RabbitmqConsumerConfig| {
                let deliveries = deliveries.clone();
                let acks = acks.clone();
                let nacks = nacks.clone();
                let prefetches = prefetches.clone();
                let configs = configs.clone();
                async move {
                    configs.lock().expect("config guard").push(cfg.clone());
                    Ok(MockConsumer::new(deliveries, acks, nacks, prefetches))
                }
            }
        })
        .await
        .expect("runtime build");

    assert_eq!(runtime.consumer_count(), 1);

    let configs = recorded_configs.lock().expect("config guard");
    assert_eq!(configs.len(), 1);
    let cfg = &configs[0];
    assert_eq!(cfg.queue, "test.queue");
    assert_eq!(cfg.prefetch, Some(9));
    drop(configs);

    let prefetch_values = prefetches.lock().expect("prefetch guard").clone();
    assert_eq!(prefetch_values, vec![9]);
    assert!(acks.lock().expect("acks guard").is_empty());
    assert!(nacks.lock().expect("nacks guard").is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn acknowledges_messages_on_success() {
    let config = build_config("    - name: noop\n      type: transform\n      options: {}\n");
    let registry = build_registry(&config);
    let engine =
        Arc::new(ChronicleEngine::new(config.clone(), registry.clone()).expect("engine build"));

    let deliveries: Arc<Mutex<VecDeque<DeliveryAction>>> =
        Arc::new(Mutex::new(VecDeque::new()));
    let acks: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let nacks: Arc<Mutex<Vec<(u64, bool)>>> = Arc::new(Mutex::new(Vec::new()));

    let runtime =
        RabbitmqTriggerRuntime::build_with(config.clone(), registry.clone(), engine.clone(), {
            let deliveries = deliveries.clone();
            let acks = acks.clone();
            let nacks = nacks.clone();
            move |_cfg: RabbitmqConsumerConfig| {
                let deliveries = deliveries.clone();
                let acks = acks.clone();
                let nacks = nacks.clone();
                async move {
                    Ok(MockConsumer::new(
                        deliveries,
                        acks,
                        nacks,
                        Arc::new(Mutex::new(Vec::new())),
                    ))
                }
            }
        })
        .await
        .expect("runtime build");

    assert_eq!(runtime.consumer_count(), 1);

    deliveries
        .lock()
        .expect("delivery guard")
        .push_back(Ok(Some(sample_delivery(42))));

    let (mut runtime, shutdown, task) = start_runtime(runtime).await;

    wait_for_condition(500, || !acks.lock().expect("acks guard").is_empty()).await;

    shutdown.cancel();
    runtime
        .shutdown()
        .await
        .expect("runtime shutdown should succeed");
    task.await
        .expect("runtime join should succeed")
        .expect("runtime run should succeed");

    assert_eq!(acks.lock().expect("acks guard").as_slice(), &[42]);
    assert!(nacks.lock().expect("nacks guard").is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn negatively_acknowledges_on_failure() {
    let config = build_config(
        "    - name: explode\n      type: transform\n      options:\n        missing: .[0].body.missing\n",
    );
    let registry = build_registry(&config);
    let engine =
        Arc::new(ChronicleEngine::new(config.clone(), registry.clone()).expect("engine build"));

    let deliveries: Arc<Mutex<VecDeque<DeliveryAction>>> =
        Arc::new(Mutex::new(VecDeque::new()));
    let acks: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let nacks: Arc<Mutex<Vec<(u64, bool)>>> = Arc::new(Mutex::new(Vec::new()));

    let runtime =
        RabbitmqTriggerRuntime::build_with(config.clone(), registry.clone(), engine.clone(), {
            let deliveries = deliveries.clone();
            let acks = acks.clone();
            let nacks = nacks.clone();
            move |_cfg: RabbitmqConsumerConfig| {
                let deliveries = deliveries.clone();
                let acks = acks.clone();
                let nacks = nacks.clone();
                async move {
                    Ok(MockConsumer::new(
                        deliveries,
                        acks,
                        nacks,
                        Arc::new(Mutex::new(Vec::new())),
                    ))
                }
            }
        })
        .await
        .expect("runtime build");

    deliveries
        .lock()
        .expect("delivery guard")
        .push_back(Ok(Some(sample_delivery(7))));

    let (mut runtime, shutdown, task) = start_runtime(runtime).await;

    wait_for_condition(500, || !nacks.lock().expect("nacks guard").is_empty()).await;

    shutdown.cancel();
    runtime
        .shutdown()
        .await
        .expect("runtime shutdown should succeed");
    task.await
        .expect("runtime join should succeed")
        .expect("runtime run should succeed");

    assert!(acks.lock().expect("acks guard").is_empty());
    let recorded = nacks.lock().expect("nacks guard").clone();
    assert_eq!(recorded, vec![(7, true)]);
}

#[test]
fn retry_settings_defaults_when_not_specified() {
    let trigger = JsonMap::new();
    let connector = JsonMap::new();
    let settings = RetrySettings::from_extras(&trigger, &connector);

    assert_eq!(settings.initial(), Duration::from_millis(200));
    assert_eq!(settings.max(), Duration::from_secs(5));
    assert!((settings.multiplier() - 2.0).abs() < f64::EPSILON);
}

#[test]
fn retry_backoff_progresses_and_resets() {
    let mut trigger = JsonMap::new();
    trigger.insert(
        "retry_initial".into(),
        JsonValue::String("100ms".to_string()),
    );
    trigger.insert(
        "retry_max".into(),
        JsonValue::String("400ms".to_string()),
    );
    trigger.insert(
        "retry_multiplier".into(),
        JsonValue::Number(JsonNumber::from_f64(2.0).unwrap()),
    );

    let settings = RetrySettings::from_extras(&trigger, &JsonMap::new());
    let mut backoff = RetryBackoff::new(settings);

    let first = backoff.on_failure();
    let second = backoff.on_failure();
    let third = backoff.on_failure();

    assert_eq!(first, Duration::from_millis(100));
    assert_eq!(second, Duration::from_millis(200));
    assert_eq!(third, Duration::from_millis(400));

    backoff.on_success();
    let reset = backoff.on_failure();
    assert_eq!(reset, Duration::from_millis(100));
}

#[test]
fn retry_settings_prefer_trigger_over_connector() {
    let mut connector = JsonMap::new();
    connector.insert(
        "retry_initial".into(),
        JsonValue::String("250ms".to_string()),
    );
    connector.insert(
        "retry_max".into(),
        JsonValue::String("750ms".to_string()),
    );

    let mut trigger = JsonMap::new();
    trigger.insert(
        "retry_initial".into(),
        JsonValue::String("125ms".to_string()),
    );
    trigger.insert(
        "retry_multiplier".into(),
        JsonValue::Number(JsonNumber::from_f64(1.5).unwrap()),
    );

    let settings = RetrySettings::from_extras(&trigger, &connector);

    assert_eq!(settings.initial(), Duration::from_millis(125));
    assert_eq!(settings.max(), Duration::from_millis(750));
    assert!((settings.multiplier() - 1.5).abs() < f64::EPSILON);
}

#[test]
fn converts_headers_between_json_and_amqp() {
    let mut headers = JsonMap::new();
    headers.insert("count".into(), JsonValue::Number(JsonNumber::from(3)));
    headers.insert("nested".into(), json!({"flag": true}));
    headers.insert("list".into(), json!([1, "two"]));

    let field_table = json_to_field_table(&JsonValue::Object(headers.clone())).expect("field table");
    let roundtrip = field_table_to_json(&field_table);

    assert_eq!(roundtrip.get("count"), headers.get("count"));
    assert_eq!(roundtrip.get("nested"), headers.get("nested"));
    assert_eq!(roundtrip.get("list"), headers.get("list"));
}

#[test]
fn converts_basic_properties_roundtrip() {
    let properties_json = json!({
        "content_type": "application/json",
        "delivery_mode": 2,
        "priority": 7,
        "correlation_id": "abcd-1234",
        "reply_to": "response.queue",
        "expiration": "60000",
        "message_id": "msg-1",
        "user_id": "guest",
        "app_id": "chronicle",
        "cluster_id": "cluster",
        "type": "event",
        "timestamp": 1_700_000_500u64,
    });

    let props = json_to_basic_properties(&properties_json).expect("basic properties");
    let roundtrip = basic_properties_to_json(&props);

    for key in properties_json.as_object().expect("map").keys() {
        assert!(roundtrip.contains_key(key), "missing key {key}");
    }
    assert_eq!(roundtrip.get("content_type"), Some(&JsonValue::String("application/json".into())));
    assert_eq!(roundtrip.get("delivery_mode"), Some(&JsonValue::Number(JsonNumber::from(2))));
    assert_eq!(roundtrip.get("priority"), Some(&JsonValue::Number(JsonNumber::from(7))));
    assert_eq!(roundtrip.get("timestamp"), Some(&JsonValue::Number(JsonNumber::from(1_700_000_500u64))));
}
