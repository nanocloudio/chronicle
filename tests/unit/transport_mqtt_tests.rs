#![cfg(feature = "mqtt")]


use async_trait::async_trait;
use chronicle::chronicle::engine::ChronicleEngine;
use chronicle::config::integration::IntegrationConfig;
use chronicle::integration::registry::ConnectorRegistry;
use chronicle::transport::mqtt::{
    MqttMessage, MqttSubscriber, MqttSubscriberError, MqttTriggerRuntime, RetainHandling,
};
use chronicle::transport::TransportRuntime;
use serde_json::json;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout, Duration};
use tokio_util::sync::CancellationToken;

#[path = "../support/mod.rs"]
mod support;

type MessageAction = Result<Option<MqttMessage>, MqttSubscriberError>;

#[derive(Clone)]
struct MockSubscriber {
    messages: Arc<Mutex<VecDeque<MessageAction>>>,
    acks: Arc<Mutex<Vec<Option<u16>>>>,
    subscriptions: Arc<Mutex<Vec<(String, u8, RetainHandling)>>>,
    reconnects: Arc<Mutex<u32>>,
}

impl MockSubscriber {
    fn new(
        messages: Arc<Mutex<VecDeque<MessageAction>>>,
        acks: Arc<Mutex<Vec<Option<u16>>>>,
        subscriptions: Arc<Mutex<Vec<(String, u8, RetainHandling)>>>,
        reconnects: Arc<Mutex<u32>>,
    ) -> Self {
        Self {
            messages,
            acks,
            subscriptions,
            reconnects,
        }
    }
}

#[async_trait]
impl MqttSubscriber for MockSubscriber {
    async fn subscribe(
        &mut self,
        topic: &str,
        qos: u8,
        retain_handling: RetainHandling,
    ) -> Result<(), MqttSubscriberError> {
        self.subscriptions
            .lock()
            .expect("subscription guard")
            .push((topic.to_string(), qos, retain_handling));
        Ok(())
    }

    async fn next_message(&mut self) -> Result<Option<MqttMessage>, MqttSubscriberError> {
        let mut guard = self.messages.lock().expect("message guard");
        guard.pop_front().unwrap_or_else(|| Ok(None))
    }

    async fn ack(&mut self, packet_id: Option<u16>) -> Result<(), MqttSubscriberError> {
        self.acks.lock().expect("ack guard").push(packet_id);
        Ok(())
    }

    async fn reconnect(&mut self) -> Result<(), MqttSubscriberError> {
        let mut guard = self.reconnects.lock().expect("reconnect guard");
        *guard += 1;
        Ok(())
    }
}

fn build_config(qos: u8, retain: Option<u8>, phases: &str) -> Arc<IntegrationConfig> {
    let retain_line = retain
        .map(|value| format!("        retain_handling: {value}\n"))
        .unwrap_or_default();

    let yaml = format!(
        r#"
connectors:
  - name: mqtt
type: mqtt
options:
  url: mqtt://localhost:1883
  client_id: chronicle-tests

chronicles:
  - name: mqtt_trigger
trigger:
  connector: mqtt
  options:
    type: mqtt
    topic: sensors/+/state
    qos: {qos}
{retain_line}    phases:
{phases}
"#
    );

    let yaml = support::feature_flags::enable_optional_feature_flags(&yaml);

    Arc::new(IntegrationConfig::from_reader(yaml.as_bytes()).expect("config"))
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

fn build_registry(config: &Arc<IntegrationConfig>) -> Arc<ConnectorRegistry> {
    Arc::new(ConnectorRegistry::build(config, ".").expect("registry build"))
}

async fn wait_for<F>(timeout_ms: u64, mut predicate: F)
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
    .expect("condition to complete within timeout");
}

fn sample_message(packet: u16, qos: u8, retain: bool) -> MqttMessage {
    let body = serde_json::to_vec(&json!({ "reading": 42 })).expect("payload");
    MqttMessage::new(
        "sensors/room/temp",
        body,
        qos,
        retain,
        if qos > 0 { Some(packet) } else { None },
        Some(1_700_000_123),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn subscribes_with_declared_qos_and_retain_policy() {
    let config = build_config(
        1,
        Some(1),
        "      - name: noop\n        type: transform\n        options: {}\n",
    );
    let registry = build_registry(&config);
    let engine =
        Arc::new(ChronicleEngine::new(config.clone(), registry.clone()).expect("engine build"));

    let messages: Arc<Mutex<VecDeque<MessageAction>>> = Arc::new(Mutex::new(VecDeque::new()));
    let acks: Arc<Mutex<Vec<Option<u16>>>> = Arc::new(Mutex::new(Vec::new()));
    let subscriptions: Arc<Mutex<Vec<(String, u8, RetainHandling)>>> =
        Arc::new(Mutex::new(Vec::new()));
    let reconnects: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));

    let runtime =
        MqttTriggerRuntime::build_with(config.clone(), registry.clone(), engine.clone(), {
            let messages = messages.clone();
            let acks = acks.clone();
            let subs = subscriptions.clone();
            let reconnects = reconnects.clone();
            move |cfg: MqttSubscriberConfig| {
                let messages = messages.clone();
                let acks = acks.clone();
                let subs_ref = subs.clone();
                let reconnects = reconnects.clone();
                async move {
                    subs_ref.lock().expect("config guard").push((
                        cfg.topic.clone(),
                        cfg.qos,
                        cfg.retain_handling,
                    ));
                    Ok(MockSubscriber::new(messages, acks, subs_ref, reconnects))
                }
            }
        })
        .await
        .expect("runtime build");

    assert_eq!(runtime.subscriber_count(), 1);

    let subs = subscriptions.lock().expect("subscription guard");
    assert_eq!(subs.len(), 2);
    let last = subs.last().expect("subscription recorded");
    assert_eq!(last.0, "sensors/+/state");
    assert_eq!(last.1, 1);
    assert_eq!(last.2, RetainHandling::Ignore);
}

#[tokio::test(flavor = "multi_thread")]
async fn acknowledges_qos_messages_on_success() {
    let config = build_config(
        1,
        None,
        "      - name: noop\n        type: transform\n        options: {}\n",
    );
    let registry = build_registry(&config);
    let engine =
        Arc::new(ChronicleEngine::new(config.clone(), registry.clone()).expect("engine build"));

    let messages: Arc<Mutex<VecDeque<MessageAction>>> = Arc::new(Mutex::new(VecDeque::new()));
    let acks: Arc<Mutex<Vec<Option<u16>>>> = Arc::new(Mutex::new(Vec::new()));
    let subscriptions: Arc<Mutex<Vec<(String, u8, RetainHandling)>>> =
        Arc::new(Mutex::new(Vec::new()));
    let reconnects: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));

    let runtime =
        MqttTriggerRuntime::build_with(config.clone(), registry.clone(), engine.clone(), {
            let messages = messages.clone();
            let acks = acks.clone();
            let subs = subscriptions.clone();
            let reconnects = reconnects.clone();
            move |_cfg: MqttSubscriberConfig| {
                let messages = messages.clone();
                let acks = acks.clone();
                let subs = subs.clone();
                let reconnects = reconnects.clone();
                async move { Ok(MockSubscriber::new(messages, acks, subs, reconnects)) }
            }
        })
        .await
        .expect("runtime build");

    messages
        .lock()
        .expect("message guard")
        .push_back(Ok(Some(sample_message(99, 1, false))));

    let (mut runtime, shutdown, task) = start_runtime(runtime).await;

    wait_for(500, || !acks.lock().expect("ack guard").is_empty()).await;

    shutdown.cancel();
    runtime
        .shutdown()
        .await
        .expect("runtime shutdown should succeed");
    task.await
        .expect("runtime join should succeed")
        .expect("runtime run should succeed");

    assert_eq!(acks.lock().expect("ack guard").as_slice(), &[Some(99)]);
    assert_eq!(*reconnects.lock().expect("reconnect guard"), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn does_not_ack_qos0_messages() {
    let config = build_config(
        0,
        None,
        "      - name: noop\n        type: transform\n        options: {}\n",
    );
    let registry = build_registry(&config);
    let engine =
        Arc::new(ChronicleEngine::new(config.clone(), registry.clone()).expect("engine build"));

    let messages: Arc<Mutex<VecDeque<MessageAction>>> = Arc::new(Mutex::new(VecDeque::new()));
    let acks: Arc<Mutex<Vec<Option<u16>>>> = Arc::new(Mutex::new(Vec::new()));
    let subscriptions: Arc<Mutex<Vec<(String, u8, RetainHandling)>>> =
        Arc::new(Mutex::new(Vec::new()));
    let reconnects: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));

    let runtime =
        MqttTriggerRuntime::build_with(config.clone(), registry.clone(), engine.clone(), {
            let messages = messages.clone();
            let acks = acks.clone();
            let subs = subscriptions.clone();
            let reconnects = reconnects.clone();
            move |_cfg: MqttSubscriberConfig| {
                let messages = messages.clone();
                let acks = acks.clone();
                let subs = subs.clone();
                let reconnects = reconnects.clone();
                async move { Ok(MockSubscriber::new(messages, acks, subs, reconnects)) }
            }
        })
        .await
        .expect("runtime build");

    messages
        .lock()
        .expect("message guard")
        .push_back(Ok(Some(sample_message(55, 0, false))));

    let (mut runtime, shutdown, task) = start_runtime(runtime).await;

    wait_for(500, || !subscriptions.lock().expect("sub guard").is_empty()).await;

    shutdown.cancel();
    runtime
        .shutdown()
        .await
        .expect("runtime shutdown should succeed");
    task.await
        .expect("runtime join should succeed")
        .expect("runtime run should succeed");

    assert!(acks.lock().expect("ack guard").is_empty());
    assert_eq!(*reconnects.lock().expect("reconnect guard"), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn ignores_retained_messages_when_requested() {
    let config = build_config(
        1,
        Some(1),
        "      - name: noop\n        type: transform\n        options: {}\n",
    );
    let registry = build_registry(&config);
    let engine =
        Arc::new(ChronicleEngine::new(config.clone(), registry.clone()).expect("engine build"));

    let messages: Arc<Mutex<VecDeque<MessageAction>>> = Arc::new(Mutex::new(VecDeque::new()));
    let acks: Arc<Mutex<Vec<Option<u16>>>> = Arc::new(Mutex::new(Vec::new()));
    let subscriptions: Arc<Mutex<Vec<(String, u8, RetainHandling)>>> =
        Arc::new(Mutex::new(Vec::new()));
    let reconnects: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));

    let runtime =
        MqttTriggerRuntime::build_with(config.clone(), registry.clone(), engine.clone(), {
            let messages = messages.clone();
            let acks = acks.clone();
            let subs = subscriptions.clone();
            let reconnects = reconnects.clone();
            move |_cfg: MqttSubscriberConfig| {
                let messages = messages.clone();
                let acks = acks.clone();
                let subs = subs.clone();
                let reconnects = reconnects.clone();
                async move { Ok(MockSubscriber::new(messages, acks, subs, reconnects)) }
            }
        })
        .await
        .expect("runtime build");

    messages
        .lock()
        .expect("message guard")
        .push_back(Ok(Some(sample_message(7, 1, true))));

    let (mut runtime, shutdown, task) = start_runtime(runtime).await;

    wait_for(500, || !acks.lock().expect("ack guard").is_empty()).await;

    shutdown.cancel();
    runtime
        .shutdown()
        .await
        .expect("runtime shutdown should succeed");
    task.await
        .expect("runtime join should succeed")
        .expect("runtime run should succeed");

    assert_eq!(acks.lock().expect("ack guard").as_slice(), &[Some(7)]);
    assert_eq!(*reconnects.lock().expect("reconnect guard"), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn processes_retained_messages_when_included() {
    let config = build_config(
        1,
        Some(0),
        "      - name: noop\n        type: transform\n        options: {}\n",
    );
    let registry = build_registry(&config);
    let engine =
        Arc::new(ChronicleEngine::new(config.clone(), registry.clone()).expect("engine build"));

    let messages: Arc<Mutex<VecDeque<MessageAction>>> = Arc::new(Mutex::new(VecDeque::new()));
    let acks: Arc<Mutex<Vec<Option<u16>>>> = Arc::new(Mutex::new(Vec::new()));
    let subscriptions: Arc<Mutex<Vec<(String, u8, RetainHandling)>>> =
        Arc::new(Mutex::new(Vec::new()));
    let reconnects: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));

    let runtime =
        MqttTriggerRuntime::build_with(config.clone(), registry.clone(), engine.clone(), {
            let messages = messages.clone();
            let acks = acks.clone();
            let subs = subscriptions.clone();
            let reconnects = reconnects.clone();
            move |_cfg: MqttSubscriberConfig| {
                let messages = messages.clone();
                let acks = acks.clone();
                let subs = subs.clone();
                let reconnects = reconnects.clone();
                async move { Ok(MockSubscriber::new(messages, acks, subs, reconnects)) }
            }
        })
        .await
        .expect("runtime build");

    messages
        .lock()
        .expect("message guard")
        .push_back(Ok(Some(sample_message(88, 1, true))));

    let (mut runtime, shutdown, task) = start_runtime(runtime).await;

    wait_for(500, || !acks.lock().expect("ack guard").is_empty()).await;

    shutdown.cancel();
    runtime
        .shutdown()
        .await
        .expect("runtime shutdown should succeed");
    task.await
        .expect("runtime join should succeed")
        .expect("runtime run should succeed");

    assert_eq!(acks.lock().expect("ack guard").as_slice(), &[Some(88)]);
    assert_eq!(*reconnects.lock().expect("reconnect guard"), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn attempts_reconnect_on_receive_error() {
    let config = build_config(
        0,
        None,
        "      - name: noop\n        type: transform\n        options: {}\n",
    );
    let registry = build_registry(&config);
    let engine =
        Arc::new(ChronicleEngine::new(config.clone(), registry.clone()).expect("engine build"));

    let messages: Arc<Mutex<VecDeque<MessageAction>>> = Arc::new(Mutex::new(VecDeque::new()));
    messages
        .lock()
        .expect("message guard")
        .push_back(Err(MqttSubscriberError::new("connection reset")));

    let acks: Arc<Mutex<Vec<Option<u16>>>> = Arc::new(Mutex::new(Vec::new()));
    let subscriptions: Arc<Mutex<Vec<(String, u8, RetainHandling)>>> =
        Arc::new(Mutex::new(Vec::new()));
    let reconnects: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));

    let runtime =
        MqttTriggerRuntime::build_with(config.clone(), registry.clone(), engine.clone(), {
            let messages = messages.clone();
            let acks = acks.clone();
            let subs = subscriptions.clone();
            let reconnects = reconnects.clone();
            move |_cfg: MqttSubscriberConfig| {
                let messages = messages.clone();
                let acks = acks.clone();
                let subs = subs.clone();
                let reconnects = reconnects.clone();
                async move { Ok(MockSubscriber::new(messages, acks, subs, reconnects)) }
            }
        })
        .await
        .expect("runtime build");

    let (mut runtime, shutdown, task) = start_runtime(runtime).await;

    wait_for(500, || *reconnects.lock().expect("reconnect guard") > 0).await;

    shutdown.cancel();
    runtime
        .shutdown()
        .await
        .expect("runtime shutdown should succeed");
    task.await
        .expect("runtime join should succeed")
        .expect("runtime run should succeed");

    assert_eq!(acks.lock().expect("ack guard").len(), 0);
    assert!(*reconnects.lock().expect("reconnect guard") >= 1);
    let subs = subscriptions.lock().expect("subscription guard");
    assert!(!subs.is_empty());
}

#[test]
fn retry_settings_defaults_for_mqtt() {
    let trigger = JsonMap::new();
    let connector = JsonMap::new();
    let settings = RetrySettings::from_extras(&trigger, &connector);

    assert_eq!(settings.initial(), Duration::from_millis(200));
    assert_eq!(settings.max(), Duration::from_secs(5));
    assert!((settings.multiplier() - 2.0).abs() < f64::EPSILON);
}

#[test]
fn retry_backoff_advances_until_max() {
    let mut trigger = JsonMap::new();
    trigger.insert(
        "retry_initial".into(),
        JsonValue::String("150ms".to_string()),
    );
    trigger.insert(
        "retry_max".into(),
        JsonValue::String("450ms".to_string()),
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

    assert_eq!(first, Duration::from_millis(150));
    assert_eq!(second, Duration::from_millis(300));
    assert_eq!(third, Duration::from_millis(450));

    backoff.on_success();
    let reset = backoff.on_failure();
    assert_eq!(reset, Duration::from_millis(150));
}
