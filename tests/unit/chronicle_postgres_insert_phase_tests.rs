
#![cfg(feature = "db-postgres")]

use chronicle::chronicle::context::ExecutionContext;
use chronicle::chronicle::phase::postgres_insert::PostgresInsertPhaseExecutor;
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
            name: "postgres".to_string(),
            kind: PhaseKind::PostgresIdempotentInsert,
            options,
        }
    }

    fn registry_with_postgres(name: &str) -> ConnectorFactoryRegistry {
        let mut connectors = Vec::new();
        let mut options = OptionMap::new();
        options.insert(
            "url".to_string(),
            ScalarValue::String("postgres://user:pass@localhost:5432/db".to_string()),
        );
        options.insert(
            "schema".to_string(),
            ScalarValue::String("public".to_string()),
        );

        connectors.push(ConnectorConfig {
            name: name.to_string(),
            kind: ConnectorKind::Postgres,
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

        let registry = ConnectorRegistry::build(&config, Path::new(".")).expect("registry build");
        ConnectorFactoryRegistry::new(Arc::new(registry))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn builds_query_payload_with_resolved_parameters() {
        let mut options = OptionMap::new();
        options.insert(
            "connector".to_string(),
            ScalarValue::String("analytics".to_string()),
        );
        options.insert(
            "sql".to_string(),
            ScalarValue::String("INSERT INTO events (id, kind) VALUES (:id, :kind)".to_string()),
        );
        options.insert(
            "values.id".to_string(),
            ScalarValue::String(".[0].event.id".to_string()),
        );
        options.insert(
            "values.kind".to_string(),
            ScalarValue::String(".[0].event.kind".to_string()),
        );

        let phase = build_phase(options);
        let registry = registry_with_postgres("analytics");
        let executor = PostgresInsertPhaseExecutor::default();
        let mut context = ExecutionContext::new();
        context.insert_slot(0, json!({ "event": { "id": "abc", "kind": "click" } }));

        let result = executor
            .execute(&phase, &mut context, &registry)
            .await
            .expect("postgres payload");

        assert_eq!(result["connector"], json!("analytics"));
        assert_eq!(
            result["sql"],
            json!("INSERT INTO events (id, kind) VALUES (:id, :kind)")
        );
        assert_eq!(result["parameters"]["id"], json!("abc"));
        assert_eq!(result["parameters"]["kind"], json!("click"));
        assert_eq!(result["schema"], json!("public"));
        assert_eq!(result["count"], json!(1));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn values_must_be_object() {
        let mut options = OptionMap::new();
        options.insert(
            "connector".to_string(),
            ScalarValue::String("analytics".to_string()),
        );
        options.insert(
            "sql".to_string(),
            ScalarValue::String("SELECT 1".to_string()),
        );
        options.insert(
            "values".to_string(),
            ScalarValue::String("not-an-object".to_string()),
        );

        let phase = build_phase(options);
        let registry = registry_with_postgres("analytics");
        let executor = PostgresInsertPhaseExecutor::default();
        let mut context = ExecutionContext::new();

        let err = executor
            .execute(&phase, &mut context, &registry)
            .await
            .expect_err("invalid parameters");

        match err {
            PhaseExecutionError::Message(message) => {
                assert!(message.contains("values must be an object"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
