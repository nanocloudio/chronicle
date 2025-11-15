#![forbid(unsafe_code)]

use crate::error::Result;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use metrics::{Key, Label, Level, Metadata};
use serde_json::Value as JsonValue;
use std::future::Future;
use std::time::{Duration, Instant};

pub fn payload_to_bytes(payload: &JsonValue) -> Result<Vec<u8>> {
    match payload {
        JsonValue::Null => Ok(Vec::new()),
        JsonValue::String(text) => Ok(text.as_bytes().to_vec()),
        other => serde_json::to_vec(other)
            .map_err(|err| crate::err!("failed to serialise payload: {err}")),
    }
}

pub fn decode_payload(encoded: &str, encoding: &str) -> Result<Vec<u8>> {
    if encoding.eq_ignore_ascii_case("base64") {
        BASE64_STANDARD
            .decode(encoded)
            .map_err(|err| crate::err!("invalid base64 payload: {err}"))
    } else {
        Ok(encoded.as_bytes().to_vec())
    }
}

pub async fn publish_with_timeout<F, Fut, E>(
    transport: &'static str,
    timeout_ms: Option<u64>,
    publish: F,
) -> std::result::Result<(), E>
where
    F: FnOnce(Option<Duration>) -> Fut,
    Fut: Future<Output = std::result::Result<(), E>>,
{
    let timeout = timeout_ms.map(Duration::from_millis);
    let start = Instant::now();
    let result = publish(timeout).await;
    match result.as_ref() {
        Ok(_) => record_publish_metrics(transport, "success", start.elapsed()),
        Err(_) => record_publish_metrics(transport, "error", start.elapsed()),
    }
    result
}

fn record_publish_metrics(transport: &str, status: &str, elapsed: Duration) {
    let labels = vec![
        Label::new("transport", transport.to_owned()),
        Label::new("status", status.to_owned()),
    ];

    let counter_key = Key::from_parts("chronicle_broker_publish_total", labels.clone());
    let histogram_key = Key::from_parts("chronicle_broker_publish_elapsed_ms", labels);
    let metadata = Metadata::new(module_path!(), Level::INFO, Some(module_path!()));

    metrics::with_recorder(|recorder| {
        recorder
            .register_counter(&counter_key, &metadata)
            .increment(1);
        recorder
            .register_histogram(&histogram_key, &metadata)
            .record(elapsed.as_secs_f64() * 1000.0);
    });
}
