#![forbid(unsafe_code)]

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use rdkafka::client::ClientContext;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::ConsumerContext;
use rdkafka::error::KafkaError;

#[derive(Clone, Copy, Debug)]
pub enum KafkaClientRole {
    Producer,
    Consumer,
}

impl KafkaClientRole {
    fn as_str(&self) -> &'static str {
        match self {
            KafkaClientRole::Producer => "producer",
            KafkaClientRole::Consumer => "consumer",
        }
    }
}

#[derive(Debug)]
pub struct KafkaConnectivityState {
    disconnected: AtomicBool,
}

impl KafkaConnectivityState {
    pub fn new() -> Self {
        Self {
            disconnected: AtomicBool::new(false),
        }
    }

    pub fn mark_disconnected(&self) -> bool {
        !self.disconnected.swap(true, Ordering::SeqCst)
    }

    pub fn mark_connected(&self) -> bool {
        self.disconnected.swap(false, Ordering::SeqCst)
    }
}

impl Default for KafkaConnectivityState {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)]
pub struct KafkaClientContext {
    connector: String,
    role: KafkaClientRole,
    state: Arc<KafkaConnectivityState>,
    emit_transition_events: bool,
}

impl KafkaClientContext {
    pub fn new(connector: impl Into<String>, role: KafkaClientRole) -> Self {
        Self {
            connector: connector.into(),
            role,
            state: Arc::new(KafkaConnectivityState::new()),
            emit_transition_events: true,
        }
    }

    pub fn with_state(
        connector: impl Into<String>,
        role: KafkaClientRole,
        state: Arc<KafkaConnectivityState>,
        emit_transition_events: bool,
    ) -> Self {
        Self {
            connector: connector.into(),
            role,
            state,
            emit_transition_events,
        }
    }
}

impl ClientContext for KafkaClientContext {
    fn log(&self, _level: RDKafkaLogLevel, _facility: &str, _message: &str) {
        // librdkafka already emits connection events via the `error` callback,
        // so avoid logging every log-line to keep the output clean.
    }

    fn error(&self, error: KafkaError, reason: &str) {
        if self.emit_transition_events && self.state.mark_disconnected() {
            tracing::warn!(
                target = "chronicle::kafka",
                event = "rdkafka_client_error",
                connector = %self.connector,
                role = %self.role.as_str(),
                error = %error,
                reason = %reason,
            );
        } else if self.emit_transition_events {
            tracing::debug!(
                target = "chronicle::kafka",
                event = "rdkafka_client_error",
                connector = %self.connector,
                role = %self.role.as_str(),
                error = %error,
                reason = %reason,
            );
        }
    }
}

impl ConsumerContext for KafkaClientContext {}
