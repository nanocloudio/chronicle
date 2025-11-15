
use chronicle::chronicle::context::ExecutionContext;
use chronicle::chronicle::phase::{
    PhaseExecutionError, PhaseHandler, PhaseKind, PhaseRegistry, PhaseRegistryError,
};
use chronicle::config::integration::{ChronicleDefinition, ChroniclePolicy, ChronicleTrigger, OptionMap};
use chronicle::integration::factory::ConnectorFactoryRegistry;
use chronicle::integration::registry::ConnectorRegistry;
use async_trait::async_trait;
use serde_json::{json, Value as JsonValue};
use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct RecordingHandler {
        outputs: Arc<Mutex<Vec<JsonValue>>>,
    }

    #[async_trait]
    impl PhaseHandler for RecordingHandler {
        async fn execute(
            &self,
            phase: &ChroniclePhase,
            _context: &mut ExecutionContext,
            _connectors: &ConnectorFactoryRegistry,
        ) -> Result<JsonValue, PhaseExecutionError> {
            let value = json!({ "phase": phase.name.clone() });
            self.outputs.lock().unwrap().push(value.clone());
            Ok(value)
        }
    }

    fn build_phase(name: &str, kind: PhaseKind) -> ChroniclePhase {
        ChroniclePhase {
            name: name.to_string(),
            kind,
            options: OptionMap::new(),
        }
    }

    fn build_chronicle(phases: Vec<ChroniclePhase>) -> ChronicleDefinition {
        ChronicleDefinition {
            name: "test".to_string(),
            trigger: ChronicleTrigger {
                connector: "dummy".to_string(),
                options: OptionMap::new(),
                kafka: None,
            },
            phases,
            policy: ChroniclePolicy::default(),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn registry_executes_registered_phases() {
        let mut registry = PhaseRegistry::new();
        let handler = RecordingHandler::default();
        let recorder = handler.outputs.clone();
        registry.register_handler(PhaseKind::Transform, handler);

        let chronicle = build_chronicle(vec![
            build_phase("phase1", PhaseKind::Transform),
            build_phase("phase2", PhaseKind::Transform),
        ]);

        let connectors = ConnectorFactoryRegistry::new(Arc::new(ConnectorRegistry::default()));
        let mut context = ExecutionContext::new();
        context.insert_slot(0, json!({ "input": true }));

        registry
            .execute(&chronicle, &connectors, &mut context)
            .await
            .expect("registry executes");

        let records = recorder.lock().unwrap().clone();
        assert_eq!(records.len(), 2);
        assert_eq!(context.slot_value(1).unwrap()["phase"], json!("phase1"));
        assert_eq!(context.slot_value(2).unwrap()["phase"], json!("phase2"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn missing_handler_reports_error() {
        let registry = PhaseRegistry::new();
        let chronicle = build_chronicle(vec![build_phase("phase1", PhaseKind::Transform)]);
        let connectors = ConnectorFactoryRegistry::new(Arc::new(ConnectorRegistry::default()));
        let mut context = ExecutionContext::new();

        let err = registry
            .execute(&chronicle, &connectors, &mut context)
            .await
            .expect_err("missing handler");

        match err {
            PhaseRegistryError::MissingHandler { phase, .. } => {
                assert_eq!(phase, "phase1");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
