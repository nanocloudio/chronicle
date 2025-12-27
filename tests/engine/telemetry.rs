use chronicle::chronicle::engine::ChronicleEngine;
use chronicle::config::IntegrationConfig;
use chronicle::integration::registry::ConnectorRegistry;
use serde_json::json;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tracing::subscriber::with_default;
use tracing_subscriber::fmt::MakeWriter;

type TestError = Box<dyn std::error::Error + Send + Sync>;
type TestResult<T = ()> = Result<T, TestError>;

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
        let mut guard = self.buffer.lock().unwrap_or_else(|e| {
            eprintln!("log buffer lock poisoned: {e}");
            std::process::abort()
        });
        guard.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn fixture_engine() -> TestResult<ChronicleEngine> {
    let config_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/chronicle-integration.yaml");
    let config = IntegrationConfig::from_path(&config_path)?;
    let fixture_dir = config_path.parent().ok_or("fixture path has no parent")?;
    let registry = ConnectorRegistry::build(&config, fixture_dir)?;

    Ok(ChronicleEngine::new(Arc::new(config), Arc::new(registry))?)
}

fn capture_logs<F>(action: F) -> TestResult<String>
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

    let contents = buffer
        .lock()
        .map_err(|e| format!("log buffer lock poisoned: {e}"))?;
    Ok(String::from_utf8(contents.clone())?)
}

mod tests {
    use super::*;

    #[test]
    fn engine_emits_phase_lifecycle_events() -> TestResult {
        let engine = fixture_engine()?;

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

            // Note: We ignore the execution result in the closure since we're testing telemetry output
            let _ = engine.execute("collect_record", payload);
        })?;

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
        Ok(())
    }
}
