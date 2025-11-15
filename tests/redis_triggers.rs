#![cfg(feature = "db-redis")]

use async_trait::async_trait;
use chronicle::chronicle::engine::ChronicleEngine;
use chronicle::config::integration::IntegrationConfig;
use chronicle::integration::registry::ConnectorRegistry;
use chronicle::transport::redis::{
    RedisDelivery, RedisDriverError, RedisTriggerConfig, RedisTriggerDriver, RedisTriggerRuntime,
};
use chronicle::transport::TransportRuntime;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout, Duration};
use tokio_util::sync::CancellationToken;

#[path = "support/mod.rs"]
mod support;

#[derive(Clone)]
struct TestDriverState {
    deliveries: Arc<Mutex<VecDeque<RedisDelivery>>>,
    acked: Arc<Mutex<Vec<DeliverySnapshot>>>,
    ack_count: Arc<AtomicUsize>,
}

impl TestDriverState {
    fn new(initial: Vec<RedisDelivery>) -> Self {
        Self {
            deliveries: Arc::new(Mutex::new(initial.into())),
            acked: Arc::new(Mutex::new(Vec::new())),
            ack_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn make_driver(&self) -> TestRedisDriver {
        TestRedisDriver {
            state: self.clone(),
        }
    }

    fn ack_count(&self) -> usize {
        self.ack_count.load(Ordering::SeqCst)
    }

    async fn snapshots(&self) -> Vec<DeliverySnapshot> {
        self.acked.lock().await.clone()
    }
}

struct TestRedisDriver {
    state: TestDriverState,
}

#[async_trait]
impl RedisTriggerDriver for TestRedisDriver {
    async fn next_delivery(&mut self) -> Result<Option<RedisDelivery>, RedisDriverError> {
        let mut guard = self.state.deliveries.lock().await;
        Ok(guard.pop_front())
    }

    async fn ack(&mut self, delivery: &RedisDelivery) -> Result<(), RedisDriverError> {
        self.state.ack_count.fetch_add(1, Ordering::SeqCst);
        self.state
            .acked
            .lock()
            .await
            .push(DeliverySnapshot::from(delivery));
        Ok(())
    }

    async fn reconnect(&mut self) -> Result<(), RedisDriverError> {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DeliverySnapshot {
    PubSub { channel: String },
    Stream { stream: String, id: String },
}

impl DeliverySnapshot {
    fn from(delivery: &RedisDelivery) -> Self {
        match delivery {
            RedisDelivery::PubSub { channel, .. } => DeliverySnapshot::PubSub {
                channel: channel.clone(),
            },
            RedisDelivery::Stream { stream, id, .. } => DeliverySnapshot::Stream {
                stream: stream.clone(),
                id: id.clone(),
            },
        }
    }
}

fn build_config(yaml: &str) -> Arc<IntegrationConfig> {
    let decorated = support::feature_flags::enable_optional_feature_flags(yaml);
    Arc::new(
        IntegrationConfig::from_reader(decorated.as_bytes())
            .expect("integration config to parse successfully"),
    )
}

fn build_registry(config: &Arc<IntegrationConfig>) -> Arc<ConnectorRegistry> {
    Arc::new(
        ConnectorRegistry::build(config, ".")
            .expect("connector registry to construct successfully"),
    )
}

async fn wait_for_acks(state: &TestDriverState, expected: usize) {
    timeout(Duration::from_secs(1), async {
        loop {
            if state.ack_count() >= expected {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("timed out waiting for redis trigger acknowledgements");
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
async fn redis_pubsub_runtime_fans_out_messages() {
    let yaml = r#"api_version: v1
connectors:
  - name: redis_events
    type: redis
    options:
      url: redis://localhost:6379

chronicles:
  - name: redis_pubsub_pipeline
    trigger:
      connector: redis_events
      options:
        mode: pubsub
        channels: ["events.alpha", "events.beta"]
    phases:
      - name: capture
        type: transform
        options:
          event.channel: .[0].channel
          event.payload: .[0].payload
"#;

    let config = build_config(yaml);
    let registry = build_registry(&config);
    let engine =
        Arc::new(ChronicleEngine::new(config.clone(), registry.clone()).expect("engine build"));

    let driver_state = TestDriverState::new(vec![
        RedisDelivery::PubSub {
            channel: "events.alpha".to_string(),
            payload: br#"{"foo":"alpha"}"#.to_vec(),
        },
        RedisDelivery::PubSub {
            channel: "events.beta".to_string(),
            payload: br#"{"foo":"beta"}"#.to_vec(),
        },
        RedisDelivery::PubSub {
            channel: "events.alpha".to_string(),
            payload: br#"{"foo":"again"}"#.to_vec(),
        },
    ]);

    let runtime = RedisTriggerRuntime::build_with(
        config.clone(),
        registry.clone(),
        engine.clone(),
        None,
        None,
        {
            let driver_state = driver_state.clone();
            move |cfg: RedisTriggerConfig| {
                assert_eq!(cfg.chronicle, "redis_pubsub_pipeline");
                let driver = driver_state.make_driver();
                async move { Ok(Box::new(driver) as Box<dyn RedisTriggerDriver + Send>) }
            }
        },
    )
    .await
    .expect("runtime build");

    let shutdown = CancellationToken::new();
    let run_task = spawn_runtime(runtime, shutdown.clone()).await;

    wait_for_acks(&driver_state, 3).await;

    let snapshots = driver_state.snapshots().await;
    assert_eq!(
        snapshots,
        vec![
            DeliverySnapshot::PubSub {
                channel: "events.alpha".to_string(),
            },
            DeliverySnapshot::PubSub {
                channel: "events.beta".to_string(),
            },
            DeliverySnapshot::PubSub {
                channel: "events.alpha".to_string(),
            },
        ]
    );

    shutdown.cancel();
    run_task
        .await
        .expect("redis trigger join handle")
        .expect("redis trigger runtime completes without error");
}

#[tokio::test(flavor = "multi_thread")]
async fn redis_stream_runtime_replays_duplicate_entries() {
    let yaml = r#"api_version: v1
connectors:
  - name: redis_streams
    type: redis
    options:
      url: redis://localhost:6379

chronicles:
  - name: redis_stream_pipeline
    trigger:
      connector: redis_streams
      options:
        mode: stream
        stream: backlog.items
        group: workers
    phases:
      - name: materialise
        type: transform
        options:
          entry.stream: .[0].stream
          entry.id: .[0].id
          entry.payload: .[0].payload
"#;

    let config = build_config(yaml);
    let registry = build_registry(&config);
    let engine =
        Arc::new(ChronicleEngine::new(config.clone(), registry.clone()).expect("engine build"));

    let deliveries = vec![
        RedisDelivery::Stream {
            stream: "backlog.items".to_string(),
            id: "1-0".to_string(),
            fields: stream_fields(r#"{"kind":"create"}"#, Some("12345")),
        },
        RedisDelivery::Stream {
            stream: "backlog.items".to_string(),
            id: "1-0".to_string(),
            fields: stream_fields(r#"{"kind":"create"}"#, Some("12345")),
        },
        RedisDelivery::Stream {
            stream: "backlog.items".to_string(),
            id: "1-1".to_string(),
            fields: stream_fields(r#"{"kind":"update"}"#, Some("12346")),
        },
    ];

    let driver_state = TestDriverState::new(deliveries);

    let runtime = RedisTriggerRuntime::build_with(
        config.clone(),
        registry.clone(),
        engine.clone(),
        None,
        None,
        {
            let driver_state = driver_state.clone();
            move |cfg: RedisTriggerConfig| {
                assert_eq!(cfg.chronicle, "redis_stream_pipeline");
                let driver = driver_state.make_driver();
                async move { Ok(Box::new(driver) as Box<dyn RedisTriggerDriver + Send>) }
            }
        },
    )
    .await
    .expect("runtime build");

    let shutdown = CancellationToken::new();
    let run_task = spawn_runtime(runtime, shutdown.clone()).await;

    wait_for_acks(&driver_state, 3).await;

    let snapshots = driver_state.snapshots().await;
    assert_eq!(
        snapshots,
        vec![
            DeliverySnapshot::Stream {
                stream: "backlog.items".to_string(),
                id: "1-0".to_string(),
            },
            DeliverySnapshot::Stream {
                stream: "backlog.items".to_string(),
                id: "1-0".to_string(),
            },
            DeliverySnapshot::Stream {
                stream: "backlog.items".to_string(),
                id: "1-1".to_string(),
            },
        ]
    );

    shutdown.cancel();
    run_task
        .await
        .expect("redis trigger join handle")
        .expect("redis trigger runtime completes without error");
}

fn stream_fields(payload: &str, timestamp: Option<&str>) -> HashMap<String, Vec<u8>> {
    let mut fields = HashMap::new();
    fields.insert("payload".to_string(), payload.as_bytes().to_vec());
    if let Some(ts) = timestamp {
        fields.insert("timestamp".to_string(), ts.as_bytes().to_vec());
    }
    fields
}
