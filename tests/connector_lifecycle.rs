#![cfg(any(
    feature = "http-out",
    feature = "kafka",
    feature = "db-mariadb",
    feature = "smtp"
))]

use chronicle::config::integration::IntegrationConfig;
use chronicle::integration::factory::ConnectorFactoryRegistry;
use chronicle::integration::registry::ConnectorRegistry;
use std::path::PathBuf;
use std::sync::Arc;

#[cfg(any(feature = "kafka", feature = "db-mariadb"))]
use tokio::runtime::Runtime;

#[cfg(any(
    feature = "http-out",
    feature = "kafka",
    feature = "db-mariadb",
    feature = "smtp"
))]
fn fixture_factory() -> ConnectorFactoryRegistry {
    let fixture =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/chronicle-integration.yaml");
    let directory = fixture
        .parent()
        .expect("fixture path should include a directory");
    let config =
        IntegrationConfig::from_path(&fixture).expect("integration config snapshot should parse");
    let registry =
        Arc::new(ConnectorRegistry::build(&config, directory).expect("connector registry build"));
    ConnectorFactoryRegistry::new(registry)
}

#[cfg(feature = "http-out")]
#[test]
fn http_client_handles_reuse_registry_cache() {
    let factory = fixture_factory();

    let first = factory
        .http_client("sample_processing_api")
        .expect("http client connector available");
    let second = factory
        .http_client("sample_processing_api")
        .expect("http client connector available");

    assert!(Arc::ptr_eq(&first, &second));
    drop(second);
    let third = factory
        .http_client("sample_processing_api")
        .expect("http client connector available");
    assert!(Arc::ptr_eq(&first, &third));
    assert_eq!(first.base_url(), "https://processor.example.com");
}

#[cfg(feature = "kafka")]
#[test]
fn kafka_producer_handles_reuse_registry_cache() {
    let runtime = Runtime::new().expect("tokio runtime");
    let _guard = runtime.enter();
    let factory = fixture_factory();

    let first = factory
        .kafka_producer("sample_kafka_cluster")
        .expect("kafka producer connector available");
    let second = factory
        .kafka_producer("sample_kafka_cluster")
        .expect("kafka producer connector available");

    assert!(Arc::ptr_eq(&first, &second));
    drop(second);
    let third = factory
        .kafka_producer("sample_kafka_cluster")
        .expect("kafka producer connector available");
    assert!(Arc::ptr_eq(&first, &third));
    assert_eq!(first.brokers(), &["localhost:9092".to_string()]);
}

#[cfg(feature = "db-mariadb")]
#[test]
fn mariadb_pool_handles_reuse_registry_cache() {
    let runtime = Runtime::new().expect("tokio runtime");
    let _guard = runtime.enter();
    let factory = fixture_factory();

    let first = factory
        .mariadb_pool("sample_state_store")
        .expect("mariadb connector available");
    let second = factory
        .mariadb_pool("sample_state_store")
        .expect("mariadb connector available");

    assert!(Arc::ptr_eq(&first, &second));
    drop(second);
    let third = factory
        .mariadb_pool("sample_state_store")
        .expect("mariadb connector available");
    assert!(Arc::ptr_eq(&first, &third));
    assert_eq!(first.schema(), Some("records"));
}

#[cfg(feature = "smtp")]
#[test]
fn smtp_mailer_handles_reuse_registry_cache() {
    let factory = fixture_factory();

    let first = factory
        .smtp_mailer("notification_smtp")
        .expect("smtp connector available");
    let second = factory
        .smtp_mailer("notification_smtp")
        .expect("smtp connector available");

    assert!(Arc::ptr_eq(&first, &second));
    drop(second);
    let third = factory
        .smtp_mailer("notification_smtp")
        .expect("smtp connector available");
    assert!(Arc::ptr_eq(&first, &third));
    assert_eq!(first.host(), "smtp.example.com");
}
