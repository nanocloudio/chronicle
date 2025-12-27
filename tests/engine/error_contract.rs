#[path = "../common/mod.rs"]
mod common;

use common::TestResult;
use serde_json::json;

#[test]
fn unknown_chronicle_reports_name() -> TestResult {
    let engine = common::build_engine();

    let result = engine.execute("nonexistent", json!({}));
    let error = result
        .err()
        .ok_or("unknown chronicle should error but succeeded")?;

    let message = error.to_string();
    assert!(
        message.contains("chronicle `nonexistent`"),
        "error should include chronicle name: {message}"
    );
    Ok(())
}

#[test]
fn missing_context_field_returns_context_error() -> TestResult {
    let engine = common::build_engine();

    let payload = json!({
        "headers": {},
        "body": {}
    });

    let result = engine.execute("collect_record", payload);
    let error = result
        .err()
        .ok_or("missing fields should fail context resolution but succeeded")?;

    let message = error.to_string();
    assert!(
        message.contains("failed to resolve"),
        "error should mention resolution failure: {message}"
    );
    assert!(
        message.contains("collect_record"),
        "error should include chronicle context: {message}"
    );
    Ok(())
}
