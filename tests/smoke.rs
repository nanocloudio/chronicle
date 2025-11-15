use chronicle::chronicle::engine::{ChronicleAction, ChronicleEngine};
use chronicle::config::IntegrationConfig;
use chronicle::integration::registry::ConnectorRegistry;
use serde_json::json;
use std::path::PathBuf;
use std::sync::Arc;

fn fixture_engine() -> ChronicleEngine {
    let config_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/chronicle-integration.yaml");
    let config = IntegrationConfig::from_path(&config_path).expect("fixture config");
    let registry = ConnectorRegistry::build(&config, config_path.parent().expect("fixture dir"))
        .expect("registry build");
    ChronicleEngine::new(Arc::new(config), Arc::new(registry)).expect("engine build")
}

#[test]
fn smoke_collect_relay_persist_pipeline() {
    let engine = fixture_engine();

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
    let collect_exec = engine
        .execute("collect_record", collect_payload.clone())
        .expect("collect_record succeeds");
    assert_eq!(collect_exec.trace_id.as_deref(), Some("trace-smoke"));
    let kafka_action = match &collect_exec.actions[0] {
        ChronicleAction::KafkaPublish { payload, .. } => payload.clone(),
        other => panic!("unexpected action: {other:?}"),
    };

    // Step 2: Feed Kafka payload into relay_record
    let relay_payload = json!({
        "body": kafka_action
    });
    let relay_exec = engine
        .execute("relay_record", relay_payload)
        .expect("relay_record succeeds");
    assert_eq!(relay_exec.trace_id.as_deref(), Some("trace-smoke"));
    match &relay_exec.actions[0] {
        ChronicleAction::HttpRequest {
            connector, path, ..
        } => {
            assert_eq!(connector, "sample_processing_api");
            assert_eq!(path, "/api/v1/records/relay");
        }
        other => panic!("unexpected action: {other:?}"),
    }

    // Step 3: Persist record to MariaDB
    let persist_exec = engine
        .execute("persist_record", collect_payload)
        .expect("persist_record succeeds");
    assert_eq!(persist_exec.record_id.as_deref(), Some("rec-smoke"));
    match &persist_exec.actions[0] {
        ChronicleAction::MariadbInsert { connector, key, .. } => {
            assert_eq!(connector, "sample_state_store");
            assert_eq!(key, "rec-smoke");
        }
        other => panic!("unexpected action: {other:?}"),
    }
    let response = persist_exec.response.expect("persist response");
    assert_eq!(response.status, 200);
}
