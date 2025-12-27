#![allow(
    dead_code,
    reason = "Test helper functions may not be used by all test files"
)]

use chronicle::chronicle::engine::ChronicleEngine;
use chronicle::config::IntegrationConfig;
use chronicle::integration::registry::ConnectorRegistry;
use std::path::PathBuf;
use std::sync::Arc;

/// Error type for test fixtures.
pub type TestError = Box<dyn std::error::Error + Send + Sync>;

/// Result type for test fixtures.
pub type TestResult<T = ()> = Result<T, TestError>;

pub fn fixture_config_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/chronicle-integration.yaml")
}

pub fn load_integration_config() -> IntegrationConfig {
    IntegrationConfig::from_path(fixture_config_path()).unwrap_or_else(|e| {
        eprintln!("Test setup failed - integration config: {e}");
        std::process::exit(1)
    })
}

pub fn build_engine() -> ChronicleEngine {
    let config_path = fixture_config_path();
    let Some(fixture_dir) = config_path.parent() else {
        eprintln!("Test setup failed - fixture path has no parent");
        std::process::exit(1)
    };
    let config = IntegrationConfig::from_path(&config_path).unwrap_or_else(|e| {
        eprintln!("Test setup failed - integration config: {e}");
        std::process::exit(1)
    });
    let registry = ConnectorRegistry::build(&config, fixture_dir).unwrap_or_else(|e| {
        eprintln!("Test setup failed - registry build: {e}");
        std::process::exit(1)
    });

    ChronicleEngine::new(Arc::new(config), Arc::new(registry)).unwrap_or_else(|e| {
        eprintln!("Test setup failed - engine build: {e}");
        std::process::exit(1)
    })
}

pub fn build_engine_handles() -> (
    ChronicleEngine,
    Arc<IntegrationConfig>,
    Arc<ConnectorRegistry>,
) {
    let config_path = fixture_config_path();
    let Some(fixture_dir) = config_path.parent() else {
        eprintln!("Test setup failed - fixture path has no parent");
        std::process::exit(1)
    };
    let config = Arc::new(
        IntegrationConfig::from_path(&config_path).unwrap_or_else(|e| {
            eprintln!("Test setup failed - integration config: {e}");
            std::process::exit(1)
        }),
    );
    let registry = Arc::new(
        ConnectorRegistry::build(&config, fixture_dir).unwrap_or_else(|e| {
            eprintln!("Test setup failed - registry build: {e}");
            std::process::exit(1)
        }),
    );

    let engine =
        ChronicleEngine::new(Arc::clone(&config), Arc::clone(&registry)).unwrap_or_else(|e| {
            eprintln!("Test setup failed - engine build: {e}");
            std::process::exit(1)
        });

    (engine, config, registry)
}
