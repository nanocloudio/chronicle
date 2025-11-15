#![cfg(feature = "db-mariadb")]

#[path = "common/mod.rs"]
mod common;

use chronicle::chronicle::engine::ChronicleAction;
use serde_json::json;

#[test]
fn persist_record_phase_writes_to_mariadb_and_acks() {
    let engine = common::build_engine();

    let payload = json!({
        "header": {
            "message_id": "abc123"
        },
        "body": {
            "record": {
                "id": "rec-123",
                "attributes": {
                    "category": "telemetry"
                },
                "metrics": {
                    "latency_ms": 42
                }
            }
        }
    });

    let execution = engine
        .execute("persist_record", payload)
        .expect("persist_record executes");

    assert_eq!(execution.actions.len(), 1);
    assert_eq!(execution.record_id.as_deref(), Some("rec-123"));
    assert_eq!(
        execution.context.len(),
        3,
        "persist_record should capture trigger, transform, and acknowledgement frames"
    );
    let ack_frame = execution.context[2]
        .as_object()
        .expect("ack frame should be an object");
    assert_eq!(
        ack_frame
            .get("body")
            .and_then(|value| value.get("record_id"))
            .and_then(|value| value.as_str()),
        Some("rec-123")
    );

    match &execution.actions[0] {
        ChronicleAction::MariadbInsert {
            connector,
            key,
            values,
            record_id,
            ..
        } => {
            assert_eq!(connector, "sample_state_store");
            assert_eq!(key, "rec-123");
            assert_eq!(
                values["payload"]["attributes"]["category"],
                json!("telemetry")
            );
            assert_eq!(record_id.as_deref(), Some("rec-123"));
        }
        other => panic!("unexpected action: {other:?}"),
    }

    let response = execution.response.expect("ack response");
    assert_eq!(response.status, 200);
    assert_eq!(response.content_type.as_deref(), Some("application/json"));
    assert_eq!(response.body["record_id"], json!("rec-123"));
    assert_eq!(response.body["inserted"], json!(1));
}
