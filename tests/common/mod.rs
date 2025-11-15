#![allow(dead_code)]

use chronicle::chronicle::engine::ChronicleEngine;
use chronicle::config::IntegrationConfig;
use chronicle::integration::registry::ConnectorRegistry;
use std::path::PathBuf;
use std::sync::Arc;

pub fn fixture_config_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/chronicle-integration.yaml")
}

pub fn load_integration_config() -> IntegrationConfig {
    IntegrationConfig::from_path(fixture_config_path()).expect("integration config should load")
}

pub fn build_engine() -> ChronicleEngine {
    let config_path = fixture_config_path();
    let config = IntegrationConfig::from_path(&config_path).expect("integration config");
    let registry = ConnectorRegistry::build(&config, config_path.parent().expect("fixture dir"))
        .expect("registry build");

    ChronicleEngine::new(Arc::new(config), Arc::new(registry)).expect("engine build")
}

pub fn build_engine_handles() -> (
    ChronicleEngine,
    Arc<IntegrationConfig>,
    Arc<ConnectorRegistry>,
) {
    let config_path = fixture_config_path();
    let config = Arc::new(
        IntegrationConfig::from_path(&config_path).expect("integration config should load"),
    );
    let registry = Arc::new(
        ConnectorRegistry::build(&config, config_path.parent().expect("fixture dir"))
            .expect("registry build"),
    );

    let engine =
        ChronicleEngine::new(Arc::clone(&config), Arc::clone(&registry)).expect("engine build");

    (engine, config, registry)
}
