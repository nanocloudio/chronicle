
use chronicle::chronicle::context::ExecutionContext;
use chronicle::chronicle::phase::mariadb_insert::MariaDbInsertPhaseExecutor;
use chronicle::chronicle::phase::PhaseExecutionError;
use chronicle::config::integration::{
    AppConfig, ApiVersion, ConnectorConfig, ConnectorKind, IntegrationConfig, OptionMap, PhaseKind,
    ScalarValue,
};
use chronicle::config::integration::ChroniclePhase;
use chronicle::integration::factory::ConnectorFactoryRegistry;
use chronicle::integration::registry::ConnectorRegistry;
use serde_json::json;
use std::path::Path;
use std::sync::Arc;

    fn build_phase(options: OptionMap) -> ChroniclePhase {
        ChroniclePhase {
            name: "mariadb".to_string(),
            kind: PhaseKind::MariadbIdempotentInsert,
            options,
        }
    }

    fn registry_with_mariadb(name: &str) -> ConnectorFactoryRegistry {
        let mut connectors = Vec::new();
        let mut options = OptionMap::new();
        options.insert(
            "url".to_string(),
            ScalarValue::String("mysql://user:pass@localhost:3306/db".to_string()),
        );
        options.insert(
            "schema".to_string(),
            ScalarValue::String("chronicle".to_string()),
        );

        connectors.push(ConnectorConfig {
            name: name.to_string(),
            kind: ConnectorKind::Mariadb,
            options,
            warmup: false,
            warmup_timeout: None,
        });

        let config = IntegrationConfig {
            api_version: ApiVersion::V1,
            app: AppConfig::default(),
            connectors,
            chronicles: Vec::new(),
            management: None,
        };

        let registry = ConnectorRegistry::build(&config, Path::new(".")).expect("registry");
        ConnectorFactoryRegistry::new(Arc::new(registry))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn inserts_first_time_and_skips_duplicates() {
        let mut options = OptionMap::new();
        options.insert(
            "connector".to_string(),
            ScalarValue::String("mariadb".to_string()),
        );
        options.insert(
            "key".to_string(),
            ScalarValue::String(".[0].id".to_string()),
        );
        options.insert(
            "value.payload".to_string(),
            ScalarValue::String(".[0].payload".to_string()),
        );

        let phase = build_phase(options);
        let registry = registry_with_mariadb("mariadb");
        let executor = MariaDbInsertPhaseExecutor::default();
        let mut context = ExecutionContext::new();
        context.insert_slot(0, json!({ "id": "abc123", "payload": { "x": 1 } }));

        let first = executor
            .execute(&phase, &mut context, &registry)
            .await
            .expect("first insert");

        assert_eq!(first["inserted"], json!(true));
        assert_eq!(first["count"], json!(1));

        let second = executor
            .execute(&phase, &mut context, &registry)
            .await
            .expect("second insert");

        assert_eq!(second["inserted"], json!(false));
        assert_eq!(second["count"], json!(0));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn error_when_connector_missing() {
        let mut options = OptionMap::new();
        options.insert(
            "connector".to_string(),
            ScalarValue::String("missing".to_string()),
        );
        options.insert(
            "key".to_string(),
            ScalarValue::String(".[0].id".to_string()),
        );

        let phase = build_phase(options);
        let registry = ConnectorFactoryRegistry::new(Arc::new(Default::default()));
        let executor = MariaDbInsertPhaseExecutor::default();
        let mut context = ExecutionContext::new();

        let err = executor
            .execute(&phase, &mut context, &registry)
            .await
            .expect_err("missing connector");

        match err {
            PhaseExecutionError::Message(message) => {
                assert!(message.contains("connector"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
#![cfg(feature = "db-mariadb")]
