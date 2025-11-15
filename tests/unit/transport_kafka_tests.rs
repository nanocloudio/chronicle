#![cfg(feature = "kafka")]


use chronicle::chronicle::engine::ChronicleEngine;
use chronicle::config::integration::IntegrationConfig;
use chronicle::integration::registry::ConnectorRegistry;
use chronicle::transport::kafka::{
    KafkaConsumer, KafkaConsumerConfig, KafkaConsumerError, KafkaHeader, KafkaRecord,
    KafkaTriggerRuntime,
};
use chronicle::transport::TransportRuntime;
use serde_json::json;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout, Duration};
use tokio_util::sync::CancellationToken;

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
async fn provisions_consumer_for_each_kafka_chronicle() {
    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/chronicle-integration.yaml");
    let config =
        Arc::new(IntegrationConfig::from_path(&fixture).expect("integration fixture to load"));
    let registry = Arc::new(
        ConnectorRegistry::build(&config, fixture.parent().expect("fixture dir"))
            .expect("registry build"),
    );
    let engine =
        Arc::new(ChronicleEngine::new(config.clone(), registry.clone()).expect("engine build"));

    let recorded_configs: Arc<Mutex<Vec<KafkaConsumerConfig>>> =
        Arc::new(Mutex::new(Vec::new()));
    let subscriptions: Arc<Mutex<Vec<Vec<String>>>> = Arc::new(Mutex::new(Vec::new()));
    let poll_actions = Arc::new(Mutex::new(VecDeque::new()));
    let commits: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(Vec::new()));

    let runtime =
        KafkaTriggerRuntime::build_with(config.clone(), registry.clone(), engine.clone(), {
            let configs = recorded_configs.clone();
            let subs = subscriptions.clone();
            let actions = poll_actions.clone();
            let commit_offsets = commits.clone();

            move |cfg: KafkaConsumerConfig| {
                {
                    let mut guard = configs.lock().expect("config guard");
                    guard.push(cfg.clone());
                }

                Ok(MockConsumer::new(
                    subs.clone(),
                    actions.clone(),
                    commit_offsets.clone(),
                ))
            }
        })
        .await
        .expect("runtime to build");

    let expected_consumers = config
        .chronicles
        .iter()
        .filter(|chronicle| chronicle.trigger.kafka.is_some())
        .count();
    assert_eq!(runtime.consumer_count(), expected_consumers);

    let configs = recorded_configs.lock().expect("config guard");
    assert_eq!(configs.len(), expected_consumers);
    let relay_cfg = configs
        .iter()
        .find(|cfg| cfg.chronicle == "relay_record")
        .expect("relay_record consumer config present");
    assert_eq!(relay_cfg.connector, "sample_kafka_cluster");
    assert_eq!(relay_cfg.topic, "records.samples");
    assert_eq!(relay_cfg.group_id.as_deref(), Some("relay-service"));
    assert_eq!(relay_cfg.auto_offset_reset.as_deref(), Some("earliest"));

    let subs = subscriptions.lock().expect("subs guard");
    assert_eq!(subs.len(), expected_consumers);
    assert!(
        subs.iter()
            .any(|topics| topics == &vec!["records.samples".to_string()]),
        "expected records.samples subscription"
    );

    // ensure no commits happened during provisioning
    assert!(commits.lock().expect("commit guard").is_empty());
}

#[test]
fn payload_decodes_headers_and_json_body() {
    let record = KafkaRecord {
        topic: "test-topic".to_string(),
        partition: 1,
        offset: 42,
        timestamp: Some(1_695_000_000),
        key: Some(b"key-123".to_vec()),
        payload: Some(
            br#"{
                "header": { "message_id": "abc123" },
                "body": { "email": "user@example.com" }
            }"#
            .to_vec(),
        ),
        headers: vec![
            KafkaHeader {
                key: "message_id".to_string(),
                value: b"abc123".to_vec(),
            },
            KafkaHeader {
                key: "content-type".to_string(),
                value: b"application/json".to_vec(),
            },
        ],
    };

    let payload = build_trigger_payload(&record);
    assert_eq!(payload["topic"], json!("test-topic"));
    assert_eq!(payload["partition"], json!(1));
    assert_eq!(payload["offset"], json!(42));
    assert_eq!(payload["header"]["message_id"], json!("abc123"));
    assert_eq!(payload["header"]["content-type"], json!("application/json"));
    assert_eq!(payload["body"]["email"], json!("user@example.com"));
    assert_eq!(payload["key"]["text"], json!("key-123"));
    assert_eq!(
        payload["payload"]["json"]["header"]["message_id"],
        json!("abc123")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn commits_offsets_only_on_success() {
    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/chronicle-integration.yaml");
    let config =
        Arc::new(IntegrationConfig::from_path(&fixture).expect("integration fixture to load"));
    let registry = Arc::new(
        ConnectorRegistry::build(&config, fixture.parent().expect("fixture dir"))
            .expect("registry build"),
    );
    let engine =
        Arc::new(ChronicleEngine::new(config.clone(), registry.clone()).expect("engine build"));

    let subscriptions: Arc<Mutex<Vec<Vec<String>>>> = Arc::new(Mutex::new(Vec::new()));
    let commits: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(Vec::new()));

    let success_actions = Arc::new(Mutex::new(VecDeque::from(vec![
        PollAction::Record(success_record()),
        PollAction::None,
    ])));

    let runtime =
        KafkaTriggerRuntime::build_with(config.clone(), registry.clone(), engine.clone(), {
            let subs = subscriptions.clone();
            let actions = success_actions.clone();
            let commit_offsets = commits.clone();
            move |cfg: KafkaConsumerConfig| {
                let _ = cfg;
                Ok(MockConsumer::new(
                    subs.clone(),
                    actions.clone(),
                    commit_offsets.clone(),
                ))
            }
        })
        .await
        .expect("runtime to build");

    let (mut runtime, shutdown, task) = start_runtime(runtime).await;

    timeout(Duration::from_secs(1), async {
        loop {
            if !commits.lock().expect("commit guard").is_empty() {
                break;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("commit to occur within timeout");

    assert_eq!(commits.lock().expect("commit guard")[0], 7);

    shutdown.cancel();
    runtime
        .shutdown()
        .await
        .expect("runtime shutdown should succeed");
    task.await
        .expect("runtime join should succeed")
        .expect("runtime run should succeed");
}

#[tokio::test(flavor = "multi_thread")]
async fn leaves_offset_uncommitted_on_failure() {
    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/chronicle-integration.yaml");
    let config =
        Arc::new(IntegrationConfig::from_path(&fixture).expect("integration fixture to load"));
    let registry = Arc::new(
        ConnectorRegistry::build(&config, fixture.parent().expect("fixture dir"))
            .expect("registry build"),
    );
    let engine =
        Arc::new(ChronicleEngine::new(config.clone(), registry.clone()).expect("engine build"));

    let subscriptions: Arc<Mutex<Vec<Vec<String>>>> = Arc::new(Mutex::new(Vec::new()));
    let commits: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(Vec::new()));

    let failure_actions = Arc::new(Mutex::new(VecDeque::from(vec![
        PollAction::Record(failing_record()),
        PollAction::None,
    ])));

    let runtime =
        KafkaTriggerRuntime::build_with(config.clone(), registry.clone(), engine.clone(), {
            let subs = subscriptions.clone();
            let actions = failure_actions.clone();
            let commit_offsets = commits.clone();
            move |cfg: KafkaConsumerConfig| {
                let _ = cfg;
                Ok(MockConsumer::new(
                    subs.clone(),
                    actions.clone(),
                    commit_offsets.clone(),
                ))
            }
        })
        .await
        .expect("runtime to build");

    let (mut runtime, shutdown, task) = start_runtime(runtime).await;

    sleep(Duration::from_millis(200)).await;
    assert!(commits.lock().expect("commit guard").is_empty());

    shutdown.cancel();
    runtime
        .shutdown()
        .await
        .expect("runtime shutdown should succeed");
    task.await
        .expect("runtime join should succeed")
        .expect("runtime run should succeed");
}

#[derive(Clone)]
struct MockConsumer {
    subscriptions: Arc<Mutex<Vec<Vec<String>>>>,
    poll_actions: Arc<Mutex<VecDeque<PollAction>>>,
    commits: Arc<Mutex<Vec<i64>>>,
}

impl MockConsumer {
    fn new(
        subscriptions: Arc<Mutex<Vec<Vec<String>>>>,
        poll_actions: Arc<Mutex<VecDeque<PollAction>>>,
        commits: Arc<Mutex<Vec<i64>>>,
    ) -> Self {
        Self {
            subscriptions,
            poll_actions,
            commits,
        }
    }
}

#[async_trait]
impl KafkaConsumer for MockConsumer {
    async fn subscribe(&mut self, topics: &[String]) -> Result<(), KafkaConsumerError> {
        self.subscriptions
            .lock()
            .expect("subs guard")
            .push(topics.to_vec());
        Ok(())
    }

    async fn poll(&mut self) -> Result<Option<KafkaRecord>, KafkaConsumerError> {
        let action = self.poll_actions.lock().expect("actions guard").pop_front();

        match action {
            Some(PollAction::Record(record)) => Ok(Some(record)),
            Some(PollAction::None) | None => Ok(None),
        }
    }

    async fn commit(&mut self, record: &KafkaRecord) -> Result<(), KafkaConsumerError> {
        self.commits
            .lock()
            .expect("commits guard")
            .push(record.offset);
        Ok(())
    }
}

#[derive(Clone)]
enum PollAction {
    Record(KafkaRecord),
    None,
}

fn success_record() -> KafkaRecord {
    KafkaRecord {
        topic: "records.samples".to_string(),
        partition: 0,
        offset: 7,
        timestamp: Some(1_695_000_000),
        key: Some(b"success".to_vec()),
        payload: Some(
            br#"{
                "body": {
                    "summary": {
                        "record_id": "rec-123",
                        "latency_ms": 42
                    },
                    "trace": {
                        "trace_id": "trace-123"
                    }
                }
            }"#
            .to_vec(),
        ),
        headers: vec![KafkaHeader {
            key: "message_id".to_string(),
            value: b"abc123".to_vec(),
        }],
    }
}

fn failing_record() -> KafkaRecord {
    KafkaRecord {
        topic: "records.samples".to_string(),
        partition: 0,
        offset: 99,
        timestamp: Some(1_695_000_000),
        key: Some(b"failure".to_vec()),
        payload: Some(
            br#"{
                "body": {
                    "trace": {
                        "trace_id": "trace-123"
                    }
                }
            }"#
            .to_vec(),
        ),
        headers: vec![],
    }
}
