#![cfg(feature = "db-postgres")]

use chronicle::chronicle::phase::postgres_insert::PostgresInsertPhaseExecutor;
use chronicle::chronicle::phase::PhaseHandler;
use chronicle::config::IntegrationConfig;
use chronicle::context::ExecutionContext;
use chronicle::integration::factory::ConnectorFactoryRegistry;
use chronicle::integration::registry::ConnectorRegistry;
use serde_json::json;
use std::sync::Arc;

fn build_registry(yaml: &str) -> (IntegrationConfig, ConnectorFactoryRegistry) {
    let config = IntegrationConfig::from_reader(yaml.as_bytes()).expect("config");
    let registry = ConnectorRegistry::build(&config, ".").expect("registry");
    let factory = ConnectorFactoryRegistry::new(Arc::new(registry));
    (config, factory)
}

#[tokio::test(flavor = "multi_thread")]
async fn postgres_phase_from_config_resolves_parameters() {
    let yaml = r#"api_version: v1
connectors:
  - name: analytics_pg
    type: postgres
    options:
      url: postgres://chronicle:chronicle@localhost:5432/analytics
      schema: public
  - name: ingest_http
    type: http_server
    options:
      host: 0.0.0.0
      port: 8100
      path: /health
chronicles:
  - name: store_event
    trigger:
      connector: ingest_http
      options:
        method: POST
        path: /events
        contentType: application/json
    phases:
      - name: write_event
        type: postgres_idempotent_insert
        options:
          connector: analytics_pg
          sql: INSERT INTO events (event_id, payload) VALUES (:event_id, :payload)
          values.event_id: .[0].body.event.id
          values.payload: .[0].body.event
"#;

    let (config, factory) = build_registry(yaml);
    let chronicle = config
        .chronicles
        .iter()
        .find(|c| c.name == "store_event")
        .expect("chronicle exists");
    let phase = chronicle
        .phases
        .iter()
        .find(|p| p.name == "write_event")
        .expect("phase exists");

    let executor = PostgresInsertPhaseExecutor;
    let mut context = ExecutionContext::new();
    context.insert_slot(
        0,
        json!({
            "body": {
                "event": {
                    "id": "evt-123",
                    "kind": "click"
                }
            }
        }),
    );

    let result = executor
        .execute(phase, &mut context, &factory)
        .await
        .expect("postgres executor");

    assert_eq!(result["connector"], json!("analytics_pg"));
    assert_eq!(
        result["sql"],
        json!("INSERT INTO events (event_id, payload) VALUES (:event_id, :payload)")
    );
    assert_eq!(result["parameters"]["event_id"], json!("evt-123"));
    assert_eq!(result["parameters"]["payload"]["kind"], json!("click"));
    assert_eq!(result["schema"], json!("public"));
}
