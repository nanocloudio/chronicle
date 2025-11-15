use crate::app_state::AppState;
#[cfg(feature = "kafka")]
use crate::chronicle::phase::model::PhaseMessage;
use crate::config::KafkaConfig;
#[cfg(feature = "kafka")]
use crate::error::{Context, Result};
#[cfg(feature = "kafka")]
use crate::transport::kafka_context::{KafkaClientContext, KafkaClientRole};
#[cfg(feature = "kafka")]
use crate::transport::kafka_util::{commit_with_logging, wait_backoff};
#[cfg(feature = "kafka")]
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
#[cfg(feature = "kafka")]
use rdkafka::message::BorrowedMessage;
#[cfg(feature = "kafka")]
use serde::Deserialize;
#[cfg(feature = "kafka")]
use serde_json::Value as JsonValue;
#[cfg(feature = "kafka")]
use std::time::Duration;
use tokio_util::sync::CancellationToken;
#[cfg(feature = "kafka")]
use uuid::Uuid;

pub struct KafkaConsumerLoop {
    config: KafkaConfig,
}

impl KafkaConsumerLoop {
    pub fn new(config: &KafkaConfig) -> Result<Self> {
        Ok(Self {
            config: config.clone(),
        })
    }

    #[allow(clippy::needless_return)]
    pub async fn run(self, state: AppState, shutdown: CancellationToken) -> Result<()> {
        tracing::info!(
            brokers = %self.config.brokers.join(","),
            group_id = %self.config.group_id,
            topics = %self.config.topics.join(","),
            "starting Kafka consumer loop"
        );

        #[cfg(feature = "kafka")]
        {
            return self.run_kafka_loop(state, shutdown).await;
        }

        #[cfg(not(feature = "kafka"))]
        {
            let _ = (state, shutdown);
            Err(crate::err!(
                "Kafka support requested but the binary was built without the `kafka` feature"
            ))
        }
    }

    #[cfg(feature = "kafka")]
    async fn run_kafka_loop(self, state: AppState, shutdown: CancellationToken) -> Result<()> {
        use rdkafka::error::KafkaError;

        let brokers = self.config.brokers.join(",");
        let group_id = self.config.group_id.clone();
        let topics = self.config.topics.clone();
        let retry_delay = Duration::from_secs(self.config.poll_interval_secs.unwrap_or(5));

        'outer: loop {
            if shutdown.is_cancelled() {
                tracing::info!("Kafka consumer loop shutting down");
                break;
            }

            let consumer = loop {
                match Self::create_consumer(&self.config, &brokers, &topics, retry_delay) {
                    Ok(consumer) => break consumer,
                    Err(err) => {
                        tracing::warn!(
                            %err,
                            backoff_ms = retry_delay.as_millis(),
                            "failed to create Kafka consumer; retrying after backoff"
                        );
                        if wait_backoff(retry_delay, &shutdown).await {
                            tracing::info!("Kafka consumer loop shutting down");
                            return Ok(());
                        }
                    }
                }
            };

            tracing::info!(
                brokers = %brokers,
                group_id = %group_id,
                topics = ?topics,
                "Kafka consumer subscribed and ready"
            );

            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        tracing::info!("Kafka consumer loop shutting down");
                        commit_with_logging(
                            || async { consumer.commit_consumer_state(CommitMode::Sync) },
                            |err| {
                                tracing::warn!(
                                    %err,
                                    "failed to commit Kafka consumer state during shutdown"
                                );
                            },
                        )
                        .await;
                        return Ok(());
                    }
                    polled = consumer.recv() => {
                        match polled {
                            Ok(message) => {
                                let permit = state.backpressure.kafka.acquire().await;
                                if let Err(err) = Self::handle_message(&state, &consumer, message).await {
                                    tracing::error!(error = %err, "failed to handle Kafka message");
                                }
                                drop(permit);
                            }
                            Err(KafkaError::PartitionEOF(partition)) => {
                                tracing::debug!(partition, "reached end of Kafka partition");
                            }
                            Err(err) => {
                            tracing::warn!(
                                %err,
                                backoff_ms = retry_delay.as_millis(),
                                "Kafka poll failed; will attempt to reconnect after backoff"
                            );
                                commit_with_logging(
                                    || async { consumer.commit_consumer_state(CommitMode::Sync) },
                                    |commit_err| {
                                        tracing::debug!(
                                            %commit_err,
                                            "failed to commit Kafka consumer state before reconnect"
                                        );
                                    },
                                )
                                .await;
                                if wait_backoff(retry_delay, &shutdown).await {
                                    tracing::info!("Kafka consumer loop shutting down");
                                    return Ok(());
                                } else {
                                    continue 'outer;
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    #[cfg(feature = "kafka")]
    fn create_consumer(
        config: &KafkaConfig,
        brokers: &str,
        topics: &[String],
        retry_delay: Duration,
    ) -> Result<StreamConsumer<KafkaClientContext>> {
        use rdkafka::config::ClientConfig;
        use rdkafka::consumer::{Consumer, StreamConsumer};

        let backoff_ms = retry_delay.as_millis().clamp(0, u128::from(u32::MAX)) as u64;
        let backoff_ms = backoff_ms.to_string();
        let topic_names = topics.join(",");
        let context_label = format!(
            "kafka_consumer_loop[group={},topics={}]",
            config.group_id.as_str(),
            topic_names
        );
        let context = KafkaClientContext::new(context_label, KafkaClientRole::Consumer);

        let consumer: StreamConsumer<KafkaClientContext> = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", config.group_id.as_str())
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("session.timeout.ms", "10000")
            .set("reconnect.backoff.ms", backoff_ms.as_str())
            .set("reconnect.backoff.max.ms", backoff_ms.as_str())
            .set("retry.backoff.ms", backoff_ms.as_str())
            .create_with_context(context)
            .with_context(|| {
                format!(
                    "failed to create Kafka consumer (brokers={brokers}, group={})",
                    config.group_id
                )
            })?;

        let topic_refs: Vec<&str> = topics.iter().map(|topic| topic.as_str()).collect();
        consumer
            .subscribe(&topic_refs)
            .with_context(|| format!("failed to subscribe to topics {topics:?}"))?;

        Ok(consumer)
    }

    #[cfg(feature = "kafka")]
    async fn handle_message(
        state: &AppState,
        consumer: &StreamConsumer<KafkaClientContext>,
        message: BorrowedMessage<'_>,
    ) -> Result<()> {
        use rdkafka::message::Message;

        let payload = message
            .payload_view::<str>()
            .transpose()
            .map_err(|err| crate::err!("failed to decode Kafka payload as UTF-8: {err}"))?;

        let payload = match payload {
            Some(text) => text,
            None => {
                tracing::warn!(
                    topic = message.topic(),
                    partition = message.partition(),
                    offset = message.offset(),
                    "received Kafka message without payload"
                );
                consumer
                    .store_offset_from_message(&message)
                    .context("failed to store offset for empty payload message")?;
                consumer
                    .commit_message(&message, CommitMode::Async)
                    .context("failed to commit empty payload message")?;
                return Ok(());
            }
        };

        let trigger: PhaseTriggerEvent = serde_json::from_str(payload).map_err(|err| {
            crate::err!(
                "failed to parse Kafka payload as PhaseTriggerEvent: {err}; payload={payload}"
            )
        })?;

        let phase_message = PhaseMessage::Trigger {
            phase_id: trigger.phase_id,
            payload: trigger.payload,
        };

        state
            .phase_executor
            .handle_message(phase_message)
            .await
            .context("phase executor failed to process Kafka message")?;

        consumer
            .store_offset_from_message(&message)
            .context("failed to store Kafka offset")?;
        consumer
            .commit_message(&message, CommitMode::Async)
            .context("failed to commit Kafka offset")?;

        Ok(())
    }
}

#[cfg(feature = "kafka")]
#[derive(Debug, Deserialize)]
struct PhaseTriggerEvent {
    phase_id: Uuid,
    #[serde(default)]
    payload: JsonValue,
}
