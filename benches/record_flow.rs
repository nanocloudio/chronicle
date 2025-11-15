use chronicle::chronicle::engine::ChronicleEngine;
use chronicle::config::IntegrationConfig;
use chronicle::integration::registry::ConnectorRegistry;
use criterion::{criterion_group, criterion_main, Criterion};
use serde_json::json;
use std::path::PathBuf;
use std::sync::Arc;

fn load_engine() -> ChronicleEngine {
    let config_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/chronicle-integration.yaml");
    let config = IntegrationConfig::from_path(&config_path).expect("fixture config");
    let registry = ConnectorRegistry::build(&config, config_path.parent().expect("fixture dir"))
        .expect("registry build");
    ChronicleEngine::new(Arc::new(config), Arc::new(registry)).expect("engine build")
}

fn bench_collect_record(c: &mut Criterion) {
    let engine = load_engine();
    let payload_template = json!({
        "header": {
            "trace_id": "trace-bench",
            "content-type": "application/json"
        },
        "body": {
            "record": {
                "id": "rec-bench",
                "attributes": {
                    "category": "telemetry",
                    "tier": "gold"
                },
                "metrics": {
                    "latency_ms": 42
                },
                "observed_at": "2024-01-01T00:00:00Z"
            }
        }
    });

    c.bench_function("collect_record_pipeline", |b| {
        b.iter(|| {
            let payload = payload_template.clone();
            let result = engine
                .execute("collect_record", payload)
                .expect("collect_record benchmark success");
            assert_eq!(result.actions.len(), 1);
        });
    });
}

criterion_group!(benches, bench_collect_record);
criterion_main!(benches);
