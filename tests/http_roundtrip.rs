#[path = "common/mod.rs"]
mod common;

use chronicle::chronicle::engine::{ChronicleAction, HttpResponse};
use chronicle::codec::http::encode_http_response;
use serde_json::json;

#[test]
fn collect_record_roundtrip_produces_kafka_publish_and_http_response() {
    let engine = common::build_engine();

    let payload = json!({
        "headers": {
            "trace_id": "trace-123",
            "content-type": "application/json"
        },
        "body": {
            "record": {
                "id": "rec-123",
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

    let execution = engine
        .execute("collect_record", payload)
        .expect("collect_record executes");
    assert_eq!(execution.actions.len(), 1);
    assert_eq!(execution.trace_id.as_deref(), Some("trace-123"));
    assert_eq!(execution.record_id.as_deref(), Some("rec-123"));
    assert_eq!(
        execution.context.len(),
        4,
        "pipeline should produce four context frames"
    );

    let trigger_frame = execution.context[0]
        .as_object()
        .expect("trigger frame must be an object");
    assert_eq!(
        trigger_frame
            .get("headers")
            .and_then(|headers| headers.get("trace_id"))
            .and_then(|value| value.as_str()),
        Some("trace-123")
    );

    let summary_frame = execution.context[1]
        .as_object()
        .expect("summary frame must be an object");
    assert_eq!(
        summary_frame
            .get("summary")
            .and_then(|value| value.get("record_id"))
            .and_then(|value| value.as_str()),
        Some("rec-123")
    );

    let response_frame = execution.context[3]
        .as_object()
        .expect("response frame must be an object");
    assert_eq!(
        response_frame
            .get("body")
            .and_then(|value| value.get("trace_id"))
            .and_then(|value| value.as_str()),
        Some("trace-123")
    );

    match &execution.actions[0] {
        ChronicleAction::KafkaPublish {
            connector,
            topic,
            payload,
            trace_id,
            record_id,
            ..
        } => {
            assert_eq!(connector, "sample_kafka_cluster");
            assert_eq!(topic, "records.samples");
            assert_eq!(payload["summary"]["record_id"], json!("rec-123"));
            assert_eq!(payload["summary"]["latency_ms"], json!(42));
            assert_eq!(payload["trace"]["trace_id"], json!("trace-123"));
            assert_eq!(trace_id.as_deref(), Some("trace-123"));
            assert_eq!(record_id.as_deref(), Some("rec-123"));
        }
        other => panic!("unexpected action: {other:?}"),
    }

    let response = execution.response.expect("http response");
    assert_eq!(response.status, 200);
    assert_eq!(response.content_type.as_deref(), Some("application/json"));
    assert_eq!(response.body["record_id"], json!("rec-123"));
    assert_eq!(response.body["trace_id"], json!("trace-123"));
}

#[test]
fn http_response_encoding_serialises_structured_bodies() {
    let response = HttpResponse {
        status: 201,
        content_type: Some("application/json".to_string()),
        body: json!({ "status": "created" }),
    };

    let encoded = encode_http_response(&response).expect("http response encoded");
    assert_eq!(encoded.status, 201);
    assert_eq!(encoded.content_type.as_deref(), Some("application/json"));

    let decoded: serde_json::Value = serde_json::from_slice(&encoded.body).expect("decoded body");
    assert_eq!(
        decoded.get("status").and_then(|v| v.as_str()),
        Some("created")
    );
}
