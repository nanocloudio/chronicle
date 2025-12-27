//! Startup/shutdown smoke test for chronicle engine.
//!
//! This test ensures the README "Quick Start" fixture (tests/fixtures/chronicle-integration.yaml)
//! can be loaded, the engine can be started, and it shuts down cleanly.

#[path = "../common/mod.rs"]
mod common;

use chronicle::chronicle::engine::ChronicleEngine;
use chronicle::config::IntegrationConfig;
use chronicle::integration::registry::ConnectorRegistry;
use common::TestResult;
use std::sync::Arc;

/// Verify the fixture config loads successfully.
#[test]
fn fixture_config_loads() -> TestResult {
    let config_path = common::fixture_config_path();
    assert!(
        config_path.exists(),
        "fixture config should exist at tests/fixtures/chronicle-integration.yaml"
    );

    let config = IntegrationConfig::from_path(&config_path)?;

    assert!(
        !config.chronicles.is_empty(),
        "fixture should define at least one chronicle"
    );
    assert!(
        !config.connectors.is_empty(),
        "fixture should define at least one connector"
    );
    Ok(())
}

/// Verify the engine can be constructed from the fixture config.
#[test]
fn engine_starts_from_fixture() -> TestResult {
    let config_path = common::fixture_config_path();
    let config = IntegrationConfig::from_path(&config_path)?;
    let fixture_dir = config_path.parent().ok_or("fixture path has no parent")?;
    let registry = ConnectorRegistry::build(&config, fixture_dir)?;

    let engine = ChronicleEngine::new(Arc::new(config), Arc::new(registry))?;

    // Verify engine has loaded the expected chronicles by checking route metadata
    assert!(
        engine.route_metadata("collect_record").is_some(),
        "engine should have collect_record route from fixture"
    );
    Ok(())
}

/// Verify the engine can execute a chronicle from the fixture.
#[test]
fn engine_executes_fixture_chronicle() -> TestResult {
    let engine = common::build_engine();

    // Payload must match fixture expectations (includes latency_ms for summarize phase)
    let payload = serde_json::json!({
        "headers": { "trace_id": "startup-test" },
        "body": {
            "record": {
                "id": "test-record",
                "attributes": { "category": "test", "tier": "gold" },
                "metrics": { "latency_ms": 42 },
                "observed_at": "2024-01-01T00:00:00Z"
            }
        }
    });

    let execution = engine.execute("collect_record", payload)?;

    assert_eq!(
        execution.trace_id.as_deref(),
        Some("startup-test"),
        "trace_id should be extracted from payload"
    );
    assert!(
        !execution.actions.is_empty(),
        "execution should produce at least one action"
    );
    Ok(())
}

/// Verify engine drops cleanly (no panic, no leak).
#[test]
fn engine_shuts_down_cleanly() {
    let engine = common::build_engine();

    // Execute something to ensure engine is "active"
    let payload = serde_json::json!({
        "body": { "record": { "id": "shutdown-test" } }
    });
    let _ = engine.execute("collect_record", payload);

    // Drop the engine - this should complete without panic
    drop(engine);

    // If we reach here, shutdown was clean
}
