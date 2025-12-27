use chronicle::chronicle::engine::{ChronicleAction, ChronicleEngine};
use chronicle::config::IntegrationConfig;
use chronicle::integration::registry::ConnectorRegistry;
use serde_json::json;
use std::path::PathBuf;
use std::sync::Arc;

type TestError = Box<dyn std::error::Error + Send + Sync>;
type TestResult<T = ()> = Result<T, TestError>;

fn fixture_engine() -> TestResult<ChronicleEngine> {
    let config_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/chronicle-integration.yaml");
    let fixture_dir = config_path
        .parent()
        .ok_or("fixture config path has no parent")?;
    let config = IntegrationConfig::from_path(&config_path)?;
    let registry = ConnectorRegistry::build(&config, fixture_dir)?;
    Ok(ChronicleEngine::new(Arc::new(config), Arc::new(registry))?)
}

#[test]
fn smoke_collect_relay_persist_pipeline() -> TestResult {
    let engine = fixture_engine()?;

    // Step 1: HTTP collect_record
    let collect_payload = json!({
        "headers": { "trace_id": "trace-smoke" },
        "body": {
            "record": {
                "id": "rec-smoke",
                "attributes": {
                    "category": "telemetry",
                    "tier": "gold"
                },
                "metrics": {
                    "latency_ms": 10
                },
                "observed_at": "2024-01-01T00:00:00Z"
            }
        }
    });
    let collect_exec = engine.execute("collect_record", collect_payload.clone())?;
    assert_eq!(collect_exec.trace_id.as_deref(), Some("trace-smoke"));
    let ChronicleAction::KafkaPublish { payload, .. } = &collect_exec.actions[0] else {
        return Err(format!("unexpected action: {:?}", collect_exec.actions[0]).into());
    };
    let kafka_action = payload.clone();

    // Step 2: Feed Kafka payload into relay_record
    let relay_payload = json!({
        "body": kafka_action
    });
    let relay_exec = engine.execute("relay_record", relay_payload)?;
    assert_eq!(relay_exec.trace_id.as_deref(), Some("trace-smoke"));
    let ChronicleAction::HttpRequest {
        connector, path, ..
    } = &relay_exec.actions[0]
    else {
        return Err(format!("unexpected action: {:?}", relay_exec.actions[0]).into());
    };
    assert_eq!(connector, "sample_processing_api");
    assert_eq!(path, "/api/v1/records/relay");

    // Step 3: Persist record to MariaDB
    let persist_exec = engine.execute("persist_record", collect_payload)?;
    assert_eq!(persist_exec.record_id.as_deref(), Some("rec-smoke"));
    let ChronicleAction::MariadbInsert { connector, key, .. } = &persist_exec.actions[0] else {
        return Err(format!("unexpected action: {:?}", persist_exec.actions[0]).into());
    };
    assert_eq!(connector, "sample_state_store");
    assert_eq!(key, "rec-smoke");
    let response = persist_exec.response.ok_or("persist response missing")?;
    assert_eq!(response.status, 200);
    Ok(())
}
