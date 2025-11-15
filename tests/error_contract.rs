#[path = "common/mod.rs"]
mod common;

use serde_json::json;

#[test]
fn unknown_chronicle_reports_name() {
    let engine = common::build_engine();

    let error = engine
        .execute("nonexistent", json!({}))
        .expect_err("unknown chronicle should error");

    let message = error.to_string();
    assert!(
        message.contains("chronicle `nonexistent`"),
        "error should include chronicle name: {message}"
    );
}

#[test]
fn missing_context_field_returns_context_error() {
    let engine = common::build_engine();

    let payload = json!({
        "headers": {},
        "body": {}
    });

    let error = engine
        .execute("collect_record", payload)
        .expect_err("missing fields should fail context resolution");

    let message = error.to_string();
    assert!(
        message.contains("failed to resolve"),
        "error should mention resolution failure: {message}"
    );
    assert!(
        message.contains("collect_record"),
        "error should include chronicle context: {message}"
    );
}
