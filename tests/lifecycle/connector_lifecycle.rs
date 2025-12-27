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

type TestError = Box<dyn std::error::Error + Send + Sync>;
type TestResult<T = ()> = Result<T, TestError>;

#[cfg(any(
    feature = "http-out",
    feature = "kafka",
    feature = "db-mariadb",
    feature = "smtp"
))]
fn fixture_factory() -> TestResult<ConnectorFactoryRegistry> {
    let fixture =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/chronicle-integration.yaml");
    let directory = fixture
        .parent()
        .ok_or("fixture path should include a directory")?;
    let config = IntegrationConfig::from_path(&fixture)?;
    let registry = Arc::new(ConnectorRegistry::build(&config, directory)?);
    Ok(ConnectorFactoryRegistry::new(registry))
}

#[cfg(feature = "http-out")]
#[test]
fn http_client_handles_reuse_registry_cache() -> TestResult {
    let factory = fixture_factory()?;

    let first = factory.http_client("sample_processing_api")?;
    let second = factory.http_client("sample_processing_api")?;

    assert!(Arc::ptr_eq(&first, &second));
    drop(second);
    let third = factory.http_client("sample_processing_api")?;
    assert!(Arc::ptr_eq(&first, &third));
    assert_eq!(first.base_url(), "https://processor.example.com");
    Ok(())
}

#[cfg(feature = "kafka")]
#[test]
fn kafka_producer_handles_reuse_registry_cache() -> TestResult {
    let runtime = Runtime::new()?;
    let _guard = runtime.enter();
    let factory = fixture_factory()?;

    let first = factory.kafka_producer("sample_kafka_cluster")?;
    let second = factory.kafka_producer("sample_kafka_cluster")?;

    assert!(Arc::ptr_eq(&first, &second));
    drop(second);
    let third = factory.kafka_producer("sample_kafka_cluster")?;
    assert!(Arc::ptr_eq(&first, &third));
    assert_eq!(first.brokers(), &["localhost:9092".to_string()]);
    Ok(())
}

#[cfg(feature = "db-mariadb")]
#[test]
fn mariadb_pool_handles_reuse_registry_cache() -> TestResult {
    let runtime = Runtime::new()?;
    let _guard = runtime.enter();
    let factory = fixture_factory()?;

    let first = factory.mariadb_pool("sample_state_store")?;
    let second = factory.mariadb_pool("sample_state_store")?;

    assert!(Arc::ptr_eq(&first, &second));
    drop(second);
    let third = factory.mariadb_pool("sample_state_store")?;
    assert!(Arc::ptr_eq(&first, &third));
    assert_eq!(first.schema(), Some("records"));
    Ok(())
}

#[cfg(feature = "smtp")]
#[test]
fn smtp_mailer_handles_reuse_registry_cache() -> TestResult {
    let factory = fixture_factory()?;

    let first = factory.smtp_mailer("notification_smtp")?;
    let second = factory.smtp_mailer("notification_smtp")?;

    assert!(Arc::ptr_eq(&first, &second));
    drop(second);
    let third = factory.smtp_mailer("notification_smtp")?;
    assert!(Arc::ptr_eq(&first, &third));
    assert_eq!(first.host(), "smtp.example.com");
    Ok(())
}
