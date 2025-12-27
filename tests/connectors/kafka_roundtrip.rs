#[path = "../common/mod.rs"]
mod common;

use chronicle::chronicle::engine::ChronicleAction;
use common::TestResult;
use serde_json::json;

#[test]
fn relay_record_kafka_trigger_emits_http_request() -> TestResult {
    let engine = common::build_engine();

    let payload = json!({
        "headers": {
            "message_id": "abc123"
        },
        "body": {
            "summary": {
                "record_id": "rec-123",
                "latency_ms": 42
            },
            "trace": {
                "trace_id": "trace-123"
            }
        }
    });

    let execution = engine.execute("relay_record", payload)?;

    assert_eq!(execution.actions.len(), 1);
    assert!(execution.response.is_none());
    assert_eq!(execution.trace_id.as_deref(), Some("trace-123"));
    assert_eq!(execution.record_id.as_deref(), Some("rec-123"));
    assert_eq!(
        execution.context.len(),
        2,
        "relay_record should produce trigger and transform frames"
    );

    let ChronicleAction::HttpRequest {
        connector,
        method,
        path,
        headers,
        body,
        trace_id,
        record_id,
        ..
    } = &execution.actions[0]
    else {
        return Err(format!("unexpected action: {:?}", execution.actions[0]).into());
    };

    assert_eq!(connector, "sample_processing_api");
    assert_eq!(method, "POST");
    assert_eq!(path, "/api/v1/records/relay");
    assert_eq!(headers.get("trace_id"), Some(&"trace-123".to_string()));
    assert_eq!(body["summary"]["record_id"], json!("rec-123"));
    assert_eq!(body["trace"]["trace_id"], json!("trace-123"));
    assert_eq!(trace_id.as_deref(), Some("trace-123"));
    assert_eq!(record_id.as_deref(), Some("rec-123"));
    Ok(())
}

#[test]
fn notify_summary_kafka_trigger_sends_smtp_email() -> TestResult {
    let engine = common::build_engine();

    let payload = json!({
        "headers": {
            "trace_id": "trace-123",
            "message_id": "msg-42"
        },
        "body": {
            "summary": {
                "record_id": "rec-123",
                "category": "telemetry",
                "latency_ms": 42
            },
            "trace": {
                "trace_id": "trace-123",
                "observed_at": "2024-01-01T12:00:00Z"
            }
        }
    });

    let execution = engine.execute("notify_summary", payload)?;

    assert_eq!(execution.actions.len(), 1);
    assert!(execution.response.is_none());
    assert_eq!(execution.trace_id.as_deref(), Some("trace-123"));
    assert_eq!(
        execution.context.len(),
        3,
        "notify_summary should capture trigger, transform, and email frames"
    );
    let email_frame = execution
        .context
        .last()
        .and_then(|frame| frame.as_object())
        .ok_or("email frame must be present")?;
    assert_eq!(
        email_frame.get("subject").and_then(|value| value.as_str()),
        Some("Record rec-123 latency alert")
    );

    let ChronicleAction::SmtpEmail {
        connector, message, ..
    } = &execution.actions[0]
    else {
        return Err(format!("unexpected action: {:?}", execution.actions[0]).into());
    };

    assert_eq!(connector, "notification_smtp");

    let message = message.as_object().ok_or("smtp message must be object")?;
    assert_eq!(
        message.get("subject").and_then(|v| v.as_str()),
        Some("Record rec-123 latency alert")
    );
    let body = message
        .get("body")
        .and_then(|value| value.as_object())
        .ok_or("smtp body object")?;
    let text = body
        .get("text")
        .and_then(|value| value.as_str())
        .ok_or("smtp text body")?;
    assert!(text.contains("rec-123"));
    assert!(text.contains("42"));
    Ok(())
}
