#![cfg(all(
    feature = "rabbitmq",
    feature = "mqtt",
    feature = "db-redis",
    feature = "db-mongodb",
    feature = "db-postgres"
))]

use anyhow::{anyhow, Context, Result};
use chronicle::chronicle::dispatcher::{ActionDispatcher, DeliveryContext};
use chronicle::chronicle::engine::{ChronicleAction, ChronicleEngine, ChronicleExecution};
use chronicle::config::IntegrationConfig;
use chronicle::integration::factory::ConnectorFactoryRegistry;
use chronicle::integration::registry::ConnectorRegistry;
use chronicle::readiness::warmup::warmup_connectors;
use chronicle::telemetry::runtime_counters;
use futures_util::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, QueueDeclareOptions, QueuePurgeOptions,
};
use lapin::types::FieldTable;
use lapin::{Channel as AmqpChannel, Connection as AmqpConnection, ConnectionProperties};
use mongodb::{bson::doc, Client as MongoClient};
use redis::aio::ConnectionManager as RedisConnection;
use redis::AsyncCommands;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use serde_json::{json, Value as JsonValue};
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex, OnceCell};
use tokio::time::{timeout, Duration};
use tokio_executor_trait::Tokio as TokioExecutor;
use uuid::Uuid;

const AMQP_URL: &str = "amqp://guest:guest@localhost:5672/%2f";
const REDIS_URL: &str = "redis://localhost:6379/0";
const MQTT_HOST: &str = "localhost";
const MQTT_PORT: u16 = 1883;
const POSTGRES_URL: &str = "postgres://chronicle:chronicle@localhost:5432/chronicle";
const MONGODB_URI: &str = "mongodb://localhost:27017";
const RABBITMQ_QUEUE: &str = "chronicle.records";
const REDIS_STATUS_KEY: &str = "chronicle:record_status";

static HARNESS_CELL: OnceCell<Result<DocumentedChronicleHarness, anyhow::Error>> =
    OnceCell::const_new();
static TEST_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
static HARNESS_READY: OnceCell<bool> = OnceCell::const_new();

async fn harness_guard() -> tokio::sync::MutexGuard<'static, ()> {
    TEST_MUTEX.get_or_init(|| Mutex::new(())).lock().await
}

async fn harness() -> Result<&'static DocumentedChronicleHarness> {
    let cell = HARNESS_CELL
        .get_or_init(|| async { DocumentedChronicleHarness::new().await })
        .await;

    match cell {
        Ok(harness) => Ok(harness),
        Err(err) => Err(anyhow::anyhow!(err.to_string())),
    }
}

async fn harness_available() -> bool {
    *HARNESS_READY
        .get_or_init(|| async {
            let probes = [
                ("127.0.0.1", 5672),
                ("127.0.0.1", 1883),
                ("127.0.0.1", 5432),
                ("127.0.0.1", 27017),
                ("127.0.0.1", 6379),
            ];

            for (host, port) in probes {
                let addr = format!("{host}:{port}");
                match timeout(Duration::from_millis(200), TcpStream::connect(&addr)).await {
                    Ok(Ok(stream)) => {
                        // Connection succeeded; drop the probe socket to avoid lingering handles.
                        drop(stream);
                    }
                    _ => return false,
                }
            }
            true
        })
        .await
}

struct DocumentedChronicleHarness {
    engine: Arc<ChronicleEngine>,
    dispatcher: ActionDispatcher,
}

impl DocumentedChronicleHarness {
    async fn new() -> Result<Self> {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let config_path = manifest_dir.join("tests/fixtures/documented-chronicles.yaml");

        let raw_config =
            IntegrationConfig::from_path(&config_path).context("failed to load fixtures")?;
        let registry = Arc::new(
            ConnectorRegistry::build(&raw_config, config_path.parent().unwrap())
                .context("failed to build connector registry")?,
        );
        let factory = ConnectorFactoryRegistry::new(Arc::clone(&registry));

        warmup_connectors(&raw_config.app, &raw_config.connectors, &factory)
            .await
            .context("connector warm-up failed")?;

        let config = Arc::new(raw_config);
        let engine = Arc::new(
            ChronicleEngine::new(Arc::clone(&config), Arc::clone(&registry))
                .context("failed to build chronicle engine")?,
        );
        let dispatcher = ActionDispatcher::new(Arc::new(factory), None, None);

        Ok(Self { engine, dispatcher })
    }

    async fn dispatch(&self, execution: &ChronicleExecution) -> Result<()> {
        let context = DeliveryContext {
            policy: execution.delivery.as_ref(),
            fallback: execution.fallback.as_ref(),
            fallback_actions: execution.fallback_actions.as_ref(),
            allow_partial_delivery: execution.allow_partial_delivery,
            retry_budget: execution.retry_budget.as_ref(),
        };

        self.dispatcher
            .dispatch(&execution.name, &execution.actions, context)
            .await
            .map_err(|err| anyhow!("dispatch failed: {err:?}"))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn collect_record_publishes_and_caches_status() -> Result<()> {
    let _guard = harness_guard().await;
    if !harness_available().await {
        eprintln!("skipping collect_record_publishes_and_caches_status (harness offline)");
        return Ok(());
    }
    let harness = match harness().await {
        Ok(handle) => handle,
        Err(err) => {
            eprintln!(
                "skipping collect_record_publishes_and_caches_status (harness unavailable): {err}"
            );
            return Ok(());
        }
    };

    let record_id = format!("rec-{}", Uuid::new_v4());

    let (amqp_conn, amqp_channel) = prepare_rabbitmq().await?;
    purge_queue(&amqp_channel, RABBITMQ_QUEUE).await?;

    let mut redis = redis_connection().await?;
    let _: u64 = redis.del(&record_id).await?;

    let payload = json!({
        "headers": {
            "trace_id": "trace-collect"
        },
        "body": {
            "record": {
                "id": record_id,
                "attributes": {
                    "category": "metrics",
                    "tier": "gold"
                },
                "metrics": {
                    "latency_ms": 42
                },
                "observed_at": "2024-04-09T21:01:23Z"
            }
        }
    });

    let execution = harness
        .engine
        .execute("collect_record", payload)
        .context("collect_record execution failed")?;
    assert_eq!(execution.actions.len(), 3);
    for action in &execution.actions {
        match action {
            ChronicleAction::RabbitmqPublish { trace_id, .. }
            | ChronicleAction::RedisCommand { trace_id, .. } => {
                assert_eq!(trace_id.as_deref(), Some("trace-collect"));
            }
            _ => {}
        }
    }
    let counters_before = runtime_counters().snapshot();
    harness.dispatch(&execution).await?;
    let counters_after = runtime_counters().snapshot();
    assert_eq!(
        counters_after.rabbitmq_publish_success,
        counters_before.rabbitmq_publish_success + 1
    );

    let published = consume_rabbitmq_message(&amqp_channel, RABBITMQ_QUEUE).await?;
    let message: JsonValue =
        serde_json::from_slice(&published).context("decode rabbitmq payload")?;
    assert_eq!(
        message["summary"]["record_id"]
            .as_str()
            .context("missing summary.record_id")?,
        execution
            .context
            .get(1)
            .and_then(|value| value.get("summary"))
            .and_then(|value| value.get("record_id"))
            .and_then(JsonValue::as_str)
            .context("expected cached summary in execution context")?
    );

    let cached: Option<String> = redis.get(&record_id).await?;
    assert_eq!(cached.as_deref(), Some("accepted"));

    if let Some(response) = execution.response {
        assert_eq!(response.status, 200);
        assert_eq!(
            response.body.get("record_id").and_then(JsonValue::as_str),
            Some(record_id.as_str())
        );
    } else {
        anyhow::bail!("collect_record response missing");
    }

    drop(redis);
    drop(amqp_channel);
    drop(amqp_conn);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn relay_record_updates_redis_and_broadcasts_mqtt() -> Result<()> {
    let _guard = harness_guard().await;
    if !harness_available().await {
        eprintln!("skipping relay_record_updates_redis_and_broadcasts_mqtt (harness offline)");
        return Ok(());
    }
    let harness = match harness().await {
        Ok(handle) => handle,
        Err(err) => {
            eprintln!(
                "skipping relay_record_updates_redis_and_broadcasts_mqtt (harness unavailable): {err}"
            );
            return Ok(());
        }
    };

    let record_id = format!("relay-{}", Uuid::new_v4());
    let mut redis = redis_connection().await?;
    let _: u64 = redis.hdel(REDIS_STATUS_KEY, &record_id).await?;

    let (mqtt_client, mqtt_events, mut mqtt_receiver) = prepare_mqtt().await?;
    mqtt_client
        .subscribe("records/summary", QoS::AtLeastOnce)
        .await
        .context("mqtt subscribe failed")?;

    let payload = json!({
        "body": {
            "summary": {
                "record_id": &record_id,
                "category": "metrics",
                "tier": "gold",
                "observed_at": "2024-04-09T21:01:23Z"
            },
            "trace": {
                "trace_id": "relay-trace"
            }
        }
    });

    let execution = harness
        .engine
        .execute("relay_record", payload)
        .context("relay_record execution failed")?;
    assert_eq!(execution.actions.len(), 2);
    for action in &execution.actions {
        match action {
            ChronicleAction::RedisCommand { trace_id, .. }
            | ChronicleAction::MqttPublish { trace_id, .. } => {
                assert_eq!(trace_id.as_deref(), Some("relay-trace"));
            }
            _ => {}
        }
    }
    let counters_before = runtime_counters().snapshot();
    harness.dispatch(&execution).await?;
    let counters_after = runtime_counters().snapshot();
    assert_eq!(
        counters_after.mqtt_publish_success,
        counters_before.mqtt_publish_success + 1
    );

    let status: Option<String> = redis.hget(REDIS_STATUS_KEY, &record_id).await?;
    assert_eq!(status.as_deref(), Some("processing"));

    let mqtt_payload = timeout(Duration::from_secs(5), mqtt_receiver.recv())
        .await
        .context("mqtt payload wait timeout")?
        .context("mqtt receiver closed before message")?;
    let mqtt_json: JsonValue =
        serde_json::from_slice(&mqtt_payload).context("decode mqtt payload")?;
    assert_eq!(
        mqtt_json
            .get("record_id")
            .and_then(JsonValue::as_str)
            .context("mqtt payload missing record_id")?,
        record_id
    );

    drop(redis);
    mqtt_client.disconnect().await.ok();
    let _ = mqtt_events.await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn persist_record_writes_postgres_and_mongodb() -> Result<()> {
    let _guard = harness_guard().await;
    if !harness_available().await {
        eprintln!("skipping persist_record_writes_postgres_and_mongodb (harness offline)");
        return Ok(());
    }
    let harness = match harness().await {
        Ok(handle) => handle,
        Err(err) => {
            eprintln!(
                "skipping persist_record_writes_postgres_and_mongodb (harness unavailable): {err}"
            );
            return Ok(());
        }
    };

    let record_id = format!("persist-{}", Uuid::new_v4());
    let pool = prepare_postgres().await?;
    sqlx::query("DELETE FROM records WHERE record_id = $1")
        .bind(&record_id)
        .execute(&pool)
        .await?;

    let mongo = MongoClient::with_uri_str(MONGODB_URI)
        .await
        .context("connect mongodb")?;
    let audit_collection = mongo
        .database("chronicle")
        .collection::<mongodb::bson::Document>("record_audit");
    audit_collection
        .delete_many(doc! { "record_id": &record_id }, None)
        .await?;

    let payload = json!({
        "headers": {
            "trace_id": "persist-trace"
        },
        "body": {
            "record": {
                "id": &record_id,
                "attributes": {
                    "category": "metrics",
                    "tier": "platinum"
                },
                "metrics": {
                    "latency_ms": 84
                },
                "observed_at": "2024-04-09T22:15:00Z"
            }
        }
    });

    let execution = harness
        .engine
        .execute("persist_record", payload)
        .context("persist_record execution failed")?;
    assert_eq!(execution.actions.len(), 3);
    for action in &execution.actions {
        match action {
            ChronicleAction::PostgresQuery { trace_id, .. }
            | ChronicleAction::MongodbCommand { trace_id, .. } => {
                assert_eq!(trace_id.as_deref(), Some("persist-trace"));
            }
            _ => {}
        }
    }
    harness.dispatch(&execution).await?;

    let row =
        sqlx::query("SELECT attributes, metrics, observed_at FROM records WHERE record_id = $1")
            .bind(&record_id)
            .fetch_one(&pool)
            .await
            .context("missing postgres row")?;
    let attrs: JsonValue = row
        .try_get("attributes")
        .context("failed to read attributes column")?;
    assert_eq!(
        attrs
            .get("category")
            .and_then(JsonValue::as_str)
            .context("attributes.category missing")?,
        "metrics"
    );

    let audit_doc = audit_collection
        .find_one(doc! { "record_id": &record_id }, None)
        .await?
        .context("mongodb audit document missing")?;
    let trace = audit_doc
        .get_str("trace_id")
        .context("mongodb trace_id missing")?;
    assert_eq!(trace, "persist-trace");

    if let Some(response) = execution.response {
        assert_eq!(response.status, 200);
        assert_eq!(
            response.body.get("persisted").and_then(JsonValue::as_bool),
            Some(true)
        );
    } else {
        anyhow::bail!("persist_record response missing");
    }

    pool.close().await;
    Ok(())
}

async fn prepare_rabbitmq() -> Result<(AmqpConnection, AmqpChannel)> {
    let properties = ConnectionProperties::default().with_executor(TokioExecutor::current());
    let connection = AmqpConnection::connect(AMQP_URL, properties)
        .await
        .context("connect rabbitmq")?;
    let channel = connection
        .create_channel()
        .await
        .context("create rabbitmq channel")?;
    channel
        .queue_declare(
            RABBITMQ_QUEUE,
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .context("declare queue")?;
    Ok((connection, channel))
}

async fn purge_queue(channel: &AmqpChannel, queue: &str) -> Result<()> {
    channel
        .queue_purge(queue, QueuePurgeOptions::default())
        .await
        .context("purge queue")?;
    Ok(())
}

async fn consume_rabbitmq_message(channel: &AmqpChannel, queue: &str) -> Result<Vec<u8>> {
    let mut consumer = channel
        .basic_consume(
            queue,
            &format!("chronicle-test-{}", Uuid::new_v4()),
            BasicConsumeOptions {
                no_ack: false,
                exclusive: true,
                ..BasicConsumeOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .context("create consumer")?;

    let maybe_delivery = timeout(Duration::from_secs(5), consumer.next())
        .await
        .context("rabbitmq delivery timeout")?;
    let delivery_result = maybe_delivery.ok_or_else(|| anyhow!("consumer closed unexpectedly"))?;
    let delivery = delivery_result.context("consumer returned error")?;

    let payload = delivery.data.clone();
    delivery
        .ack(BasicAckOptions::default())
        .await
        .context("ack delivery")?;
    Ok(payload)
}

async fn redis_connection() -> Result<RedisConnection> {
    let client = redis::Client::open(REDIS_URL).context("redis client")?;
    redis::aio::ConnectionManager::new(client)
        .await
        .context("redis connection")
}

async fn prepare_mqtt() -> Result<(
    AsyncClient,
    tokio::task::JoinHandle<()>,
    mpsc::Receiver<Vec<u8>>,
)> {
    let client_id = format!("chronicle-test-{}", Uuid::new_v4());
    let mut options = MqttOptions::new(client_id, MQTT_HOST, MQTT_PORT);
    options.set_keep_alive(Duration::from_secs(5));

    let (client, mut eventloop) = AsyncClient::new(options, 10);
    let (tx, rx) = mpsc::channel(1);
    let handle = tokio::spawn(async move {
        while let Ok(event) = eventloop.poll().await {
            if let Event::Incoming(Packet::Publish(publish)) = event {
                let _ = tx.send(publish.payload.to_vec()).await;
                break;
            }
        }
    });
    Ok((client, handle, rx))
}

async fn prepare_postgres() -> Result<sqlx::Pool<sqlx::Postgres>> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(POSTGRES_URL)
        .await
        .context("connect postgres")?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS records (
            record_id TEXT PRIMARY KEY,
            attributes JSONB NOT NULL,
            metrics JSONB NOT NULL,
            observed_at TIMESTAMPTZ NOT NULL
        );
        "#,
    )
    .execute(&pool)
    .await
    .context("create records table")?;

    Ok(pool)
}
