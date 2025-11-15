
use chronicle::chronicle::context::ExecutionContext;
use chronicle::chronicle::phase::transform::TransformPhaseExecutor;
use chronicle::chronicle::phase::PhaseExecutionError;
use chronicle::config::integration::{ChroniclePhase, OptionMap, PhaseKind, ScalarValue};
use chronicle::integration::factory::ConnectorFactoryRegistry;
use serde_json::json;
use std::sync::Arc;

    fn build_phase(options: OptionMap) -> ChroniclePhase {
        ChroniclePhase {
            name: "transform".to_string(),
            kind: PhaseKind::Transform,
            options,
        }
    }

    fn empty_factory() -> ConnectorFactoryRegistry {
        ConnectorFactoryRegistry::new(Arc::new(Default::default()))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn resolves_context_references() {
        let mut options = OptionMap::new();
        options.insert(
            "email".to_string(),
            ScalarValue::String(".[0].body.email".to_string()),
        );
        options.insert(
            "id".to_string(),
            ScalarValue::String(".[0].body.user_id".to_string()),
        );

        let phase = build_phase(options);
        let executor = TransformPhaseExecutor::default();
        let registry = empty_factory();
        let mut context = ExecutionContext::new();
        context.insert_slot(
            0,
            json!({
                "body": {
                    "email": "user@example.com",
                    "user_id": "user-123"
                }
            }),
        );

        let result = executor
            .execute(&phase, &mut context, &registry)
            .await
            .expect("transform success");

        assert_eq!(result["email"], json!("user@example.com"));
        assert_eq!(result["id"], json!("user-123"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn resolves_jq_expressions() {
        let mut options = OptionMap::new();
        options.insert(
            "summary".to_string(),
            ScalarValue::String(".[0].record.id".to_string()),
        );

        let phase = build_phase(options);
        let executor = TransformPhaseExecutor::default();
        let registry = empty_factory();
        let mut context = ExecutionContext::new();
        context.insert_slot(
            0,
            json!({
                "record": {
                    "id": "rec-42"
                }
            }),
        );

        let result = executor
            .execute(&phase, &mut context, &registry)
            .await
            .expect("transform success");

        assert_eq!(result["summary"], json!("rec-42"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn missing_context_returns_error() {
        let mut options = OptionMap::new();
        options.insert(
            "email".to_string(),
            ScalarValue::String(".[0].body.email".to_string()),
        );

        let phase = build_phase(options);
        let executor = TransformPhaseExecutor::default();
        let registry = empty_factory();
        let mut context = ExecutionContext::new();

        let err = executor
            .execute(&phase, &mut context, &registry)
            .await
            .expect_err("missing context");

        match err {
            PhaseExecutionError::Context(_) => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }

    use std::sync::Arc;
