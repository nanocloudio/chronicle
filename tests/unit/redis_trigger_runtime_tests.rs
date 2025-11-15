#![cfg(feature = "db-redis")]

use async_trait::async_trait;
use base64::Engine as _;
use chronicle::chronicle::engine::ChronicleEngine;
use chronicle::config::integration::IntegrationConfig;
use chronicle::integration::registry::ConnectorRegistry;
use chronicle::transport::redis::{
    RedisDelivery, RedisDriverError, RedisTriggerConfig, RedisTriggerDriver, RedisTriggerRuntime,
};
use chronicle::transport::TransportRuntime;
#[path = "../support/mod.rs"]
mod support;

use support::redis::build_trigger_payload;
use serde_json::{json, Value as JsonValue};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct MockDriver {
polls: Arc<AtomicU32>,
}

impl MockDriver {
fn new(polls: Arc<AtomicU32>) -> Self {
    Self { polls }
}
}

#[async_trait]
impl RedisTriggerDriver for MockDriver {
async fn next_delivery(&mut self) -> Result<Option<RedisDelivery>, RedisDriverError> {
    self.polls.fetch_add(1, Ordering::SeqCst);
    sleep(Duration::from_millis(10)).await;
    Ok(None)
}

async fn ack(&mut self, _: &RedisDelivery) -> Result<(), RedisDriverError> {
    Ok(())
}

async fn reconnect(&mut self) -> Result<(), RedisDriverError> {
    Ok(())
}
}

#[derive(Clone)]
struct StreamDriver {
queue: Arc<Mutex<VecDeque<RedisDelivery>>>,
acks: Arc<AtomicU32>,
}

impl StreamDriver {
fn new(queue: Arc<Mutex<VecDeque<RedisDelivery>>>, acks: Arc<AtomicU32>) -> Self {
    Self { queue, acks }
}
}

#[async_trait]
impl RedisTriggerDriver for StreamDriver {
async fn next_delivery(&mut self) -> Result<Option<RedisDelivery>, RedisDriverError> {
    let delivery = {
        let mut guard = self.queue.lock().expect("queue guard");
        guard.pop_front()
    };

    if let Some(delivery) = delivery {
        Ok(Some(delivery))
    } else {
        sleep(Duration::from_millis(10)).await;
        Ok(None)
    }
}

async fn ack(&mut self, delivery: &RedisDelivery) -> Result<(), RedisDriverError> {
    if matches!(delivery, RedisDelivery::Stream { .. }) {
        self.acks.fetch_add(1, Ordering::SeqCst);
    }
    Ok(())
}

async fn reconnect(&mut self) -> Result<(), RedisDriverError> {
    Ok(())
}
}

fn build_config(yaml: &str) -> Arc<IntegrationConfig> {
let decorated = support::feature_flags::enable_optional_feature_flags(yaml);
Arc::new(
    IntegrationConfig::from_reader(decorated.as_bytes()).expect("config to parse successfully"),
)
}

fn build_registry(config: &Arc<IntegrationConfig>) -> Arc<ConnectorRegistry> {
Arc::new(
    ConnectorRegistry::build(config, ".").expect("connector registry to build successfully"),
)
}

async fn spawn_runtime<R>(
    mut runtime: R,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<chronicle::error::Result<()>>
where
    R: TransportRuntime + Send + 'static,
{
    runtime
        .prepare()
        .await
        .expect("runtime preparation should succeed");
    runtime
        .start(shutdown.clone())
        .await
        .expect("runtime start should succeed");
    tokio::spawn(async move { runtime.run().wait().await })
}

#[tokio::test(flavor = "multi_thread")]
async fn build_invokes_factory_for_pubsub_triggers() {
let yaml = r#"
connectors:
  - name: redis_main
type: redis
options:
  url: redis://localhost:6379

chronicles:
  - name: redis_events
trigger:
  connector: redis_main
  options:
    type: redis
    mode: pubsub
    channels: ["events"]
phases:
  - name: noop
    type: transform
    options: {}
"#;

let config = build_config(yaml);
let registry = build_registry(&config);
let engine =
    Arc::new(ChronicleEngine::new(config.clone(), registry.clone()).expect("engine build"));

let polls = Arc::new(AtomicU32::new(0));
let captured: Arc<Mutex<Vec<RedisTriggerConfig>>> = Arc::new(Mutex::new(Vec::new()));

let runtime = RedisTriggerRuntime::build_with(
    config.clone(),
    registry.clone(),
    engine.clone(),
    {
        let captured = captured.clone();
        let polls = polls.clone();
        move |cfg: RedisTriggerConfig| {
            captured.lock().expect("capture guard").push(cfg.clone());
            let polls = polls.clone();
            async move {
                Ok(Box::new(MockDriver::new(polls)) as Box<dyn RedisTriggerDriver + Send>)
            }
        }
    },
)
.await
.expect("runtime build");

assert_eq!(runtime.listener_count(), 1, "expected single redis listener");

let captured = captured.lock().expect("captured configs guard");
assert_eq!(captured.len(), 1);
let cfg = &captured[0];
assert_eq!(cfg.chronicle, "redis_events");
assert_eq!(cfg.options.channels, vec!["events"]);
assert!(cfg.options.stream.is_none());
assert!(cfg.options.group.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn runtime_polls_driver_until_shutdown() {
let yaml = r#"
connectors:
  - name: redis_main
type: redis
options:
  url: redis://localhost:6379

chronicles:
  - name: redis_stream
trigger:
  connector: redis_main
  options:
    type: redis
    mode: stream
    stream: pending
    group: workers
phases:
  - name: noop
    type: transform
    options: {}
"#;

let config = build_config(yaml);
let registry = build_registry(&config);
let engine =
    Arc::new(ChronicleEngine::new(config.clone(), registry.clone()).expect("engine build"));

let polls = Arc::new(AtomicU32::new(0));

let runtime = RedisTriggerRuntime::build_with(
    config.clone(),
    registry.clone(),
    engine.clone(),
    {
        let polls = polls.clone();
        move |_cfg: RedisTriggerConfig| {
            let polls = polls.clone();
            async move {
                Ok(Box::new(MockDriver::new(polls)) as Box<dyn RedisTriggerDriver + Send>)
            }
        }
    },
)
.await
.expect("runtime build");

let shutdown = CancellationToken::new();
let run_task = spawn_runtime(runtime, shutdown.clone()).await;
tokio::time::sleep(Duration::from_millis(60)).await;
shutdown.cancel();
let _ = run_task.await;

assert!(
    polls.load(Ordering::SeqCst) > 0,
    "expected driver to be polled at least once"
);
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_delivery_invokes_ack_after_success() {
let yaml = r#"
connectors:
  - name: redis_main
type: redis
options:
  url: redis://localhost:6379

chronicles:
  - name: redis_stream
trigger:
  connector: redis_main
  options:
    type: redis
    mode: stream
    stream: pending
    group: workers
phases:
  - name: noop
    type: transform
    options: {}
"#;

let config = build_config(yaml);
let registry = build_registry(&config);
let engine =
    Arc::new(ChronicleEngine::new(config.clone(), registry.clone()).expect("engine build"));

let mut fields = HashMap::new();
fields.insert(
    "payload".to_string(),
    br#"{"foo":"bar"}"#.to_vec(),
);
fields.insert("status".to_string(), b"ok".to_vec());
fields.insert("timestamp".to_string(), b"12345".to_vec());

let queue = Arc::new(Mutex::new(VecDeque::from([RedisDelivery::Stream {
    stream: "pending".to_string(),
    id: "1-0".to_string(),
    fields,
}])));
let acks = Arc::new(AtomicU32::new(0));

let runtime = RedisTriggerRuntime::build_with(
    config.clone(),
    registry.clone(),
    engine.clone(),
    {
        let queue = queue.clone();
        let acks = acks.clone();
        move |_cfg: RedisTriggerConfig| {
            let driver = StreamDriver::new(queue.clone(), acks.clone());
            async move { Ok(Box::new(driver) as Box<dyn RedisTriggerDriver + Send>) }
        }
    },
)
.await
.expect("runtime build");

let shutdown = CancellationToken::new();
let run_task = spawn_runtime(runtime, shutdown.clone()).await;
tokio::time::sleep(Duration::from_millis(120)).await;
shutdown.cancel();
let _ = run_task.await;

assert_eq!(acks.load(Ordering::SeqCst), 1, "expected ack to be invoked once");
}

#[test]
fn pubsub_payload_maps_payload_and_metadata() {
let delivery = RedisDelivery::PubSub {
    channel: "events".to_string(),
    payload: b"hello".to_vec(),
};

let value = build_trigger_payload(&delivery).expect("payload");
let root = value.as_object().expect("object root");
assert_eq!(
    root.get("channel"),
    Some(&JsonValue::String("events".to_string()))
);

let payload = root
    .get("payload")
    .and_then(JsonValue::as_object)
    .expect("payload object");
assert_eq!(
    payload.get("base64"),
    Some(&JsonValue::String(
        base64::engine::general_purpose::STANDARD.encode(b"hello"),
    ))
);
assert_eq!(payload.get("text"), Some(&JsonValue::String("hello".to_string())));

let metadata = root
    .get("metadata")
    .and_then(JsonValue::as_object)
    .expect("metadata object");
assert!(metadata.get("timestamp").and_then(JsonValue::as_i64).is_some());
}

#[test]
fn stream_payload_maps_attributes_and_timestamp() {
let mut fields = HashMap::new();
fields.insert(
    "payload".to_string(),
    br#"{"foo":42}"#.to_vec(),
);
fields.insert("count".to_string(), b"7".to_vec());
fields.insert("timestamp".to_string(), b"9876".to_vec());

let delivery = RedisDelivery::Stream {
    stream: "pending".to_string(),
    id: "1-0".to_string(),
    fields,
};

let value = build_trigger_payload(&delivery).expect("payload");
let root = value.as_object().expect("object root");
assert_eq!(
    root.get("stream"),
    Some(&JsonValue::String("pending".to_string()))
);
assert_eq!(root.get("id"), Some(&JsonValue::String("1-0".to_string())));

let payload = root
    .get("payload")
    .and_then(JsonValue::as_object)
    .expect("payload object");
assert_eq!(payload.get("json"), Some(&json!({"foo": 42})));

let attributes = root
    .get("attributes")
    .and_then(JsonValue::as_object)
    .expect("attributes object");
assert!(attributes.contains_key("count"));

let metadata = root
    .get("metadata")
    .and_then(JsonValue::as_object)
    .expect("metadata object");
assert_eq!(
    metadata.get("timestamp").and_then(JsonValue::as_i64),
    Some(9876)
);
}
