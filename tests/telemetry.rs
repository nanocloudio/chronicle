use chronicle::chronicle::engine::ChronicleEngine;
use chronicle::config::IntegrationConfig;
use chronicle::integration::registry::ConnectorRegistry;
use serde_json::json;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tracing::subscriber::with_default;
use tracing_subscriber::fmt::MakeWriter;

struct BufferWriter {
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl<'a> MakeWriter<'a> for BufferWriter {
    type Writer = BufferGuard;

    fn make_writer(&'a self) -> Self::Writer {
        BufferGuard {
            buffer: self.buffer.clone(),
        }
    }
}

struct BufferGuard {
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl std::io::Write for BufferGuard {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut guard = self.buffer.lock().expect("log buffer lock");
        guard.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn fixture_engine() -> ChronicleEngine {
    let config_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/chronicle-integration.yaml");
    let config = IntegrationConfig::from_path(&config_path).expect("fixture config");
    let registry = ConnectorRegistry::build(&config, config_path.parent().expect("fixture dir"))
        .expect("registry build");

    ChronicleEngine::new(Arc::new(config), Arc::new(registry)).expect("engine build")
}

fn capture_logs<F>(action: F) -> String
where
    F: FnOnce(),
{
    let buffer = Arc::new(Mutex::new(Vec::new()));
    let writer = BufferWriter {
        buffer: buffer.clone(),
    };

    let subscriber = tracing_subscriber::fmt()
        .with_writer(writer)
        .with_ansi(false)
        .without_time()
        .with_target(true)
        .finish();

    with_default(subscriber, action);

    let contents = buffer.lock().expect("log buffer lock");
    String::from_utf8(contents.clone()).expect("utf8 logs")
}

mod tests {
    use super::*;

    #[test]
    fn engine_emits_phase_lifecycle_events() {
        let engine = fixture_engine();

        let output = capture_logs(|| {
            let payload = json!({
                "headers": {
                    "trace_id": "trace-telemetry",
                    "content-type": "application/json"
                },
                "body": {
                    "record": {
                        "id": "rec-telemetry",
                        "attributes": {
                            "category": "telemetry",
                            "tier": "gold"
                        },
                        "metrics": {
                            "latency_ms": 42
                        },
                        "observed_at": "2024-01-01T12:00:00Z"
                    }
                }
            });

            engine
                .execute("collect_record", payload)
                .expect("chronicle execution succeeds");
        });

        assert!(
            output.contains("event=\"chronicle_started\""),
            "logs: {output}"
        );
        assert!(output.contains("event=\"phase_started\""), "logs: {output}");
        assert!(output.contains("phase=summarize_record"), "logs: {output}");
        assert!(
            output.contains("event=\"phase_completed\""),
            "logs: {output}"
        );
        assert!(
            output.contains("event=\"chronicle_completed\""),
            "logs: {output}"
        );
        assert!(
            output.contains("trace_id=\"trace-telemetry\""),
            "logs: {output}"
        );
        assert!(
            output.contains("record_id=\"rec-telemetry\""),
            "logs: {output}"
        );
    }
}
