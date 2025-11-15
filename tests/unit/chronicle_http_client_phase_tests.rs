
use chronicle::chronicle::context::ExecutionContext;
use chronicle::chronicle::phase::http_client::HttpClientPhaseExecutor;
use chronicle::chronicle::phase::PhaseExecutionError;
use chronicle::config::integration::{
    AppConfig, ApiVersion, ConnectorConfig, ConnectorKind, IntegrationConfig, OptionMap, PhaseKind,
    ScalarValue,
};
use chronicle::config::integration::ChroniclePhase;
use chronicle::integration::factory::ConnectorFactoryRegistry;
use chronicle::integration::registry::ConnectorRegistry;
use httpmock::{Method::POST, MockServer};
use serde_json::json;
use std::path::Path;
use std::sync::Arc;

    fn build_phase(options: OptionMap) -> ChroniclePhase {
        ChroniclePhase {
            name: "http".to_string(),
            kind: PhaseKind::HttpClient,
            options,
        }
    }

    fn registry_with_client(
        name: &str,
        base_url: &str,
        default_headers: Option<Vec<(&str, &str)>>,
    ) -> ConnectorFactoryRegistry {
        let mut connectors = Vec::new();
        let mut options = OptionMap::new();
        options.insert(
            "base_url".to_string(),
            ScalarValue::String(base_url.to_string()),
        );
        if let Some(headers) = default_headers {
            for (key, value) in headers {
                options.insert(
                    format!("default_headers.{key}"),
                    ScalarValue::String(value.to_string()),
                );
            }
        }

        connectors.push(ConnectorConfig {
            name: name.to_string(),
            kind: ConnectorKind::HttpClient,
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
    async fn sends_http_request_and_returns_response() {
        let server = MockServer::start_async().await;

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/api/v1/webhook")
                .header("x-trace", "abc123")
                .json_body(json!({
                    "email": "user@example.com",
                    "user_id": "user-123"
                }));

            then.status(201)
                .header("content-type", "application/json")
                .json_body(json!({ "status": "ok" }));
        });

        let mut phase_options = OptionMap::new();
        phase_options.insert(
            "connector".to_string(),
            ScalarValue::String("client".to_string()),
        );
        phase_options.insert(
            "method".to_string(),
            ScalarValue::String("POST".to_string()),
        );
        phase_options.insert(
            "path".to_string(),
            ScalarValue::String("/api/v1/webhook".to_string()),
        );
        phase_options.insert(
            "contentType".to_string(),
            ScalarValue::String("application/json".to_string()),
        );
        phase_options.insert(
            "header.x-trace".to_string(),
            ScalarValue::String(".[0].trace_id".to_string()),
        );
        phase_options.insert(
            "body.email".to_string(),
            ScalarValue::String(".[0].user.email".to_string()),
        );
        phase_options.insert(
            "body.user_id".to_string(),
            ScalarValue::String(".[0].user.id".to_string()),
        );

        let phase = build_phase(phase_options);
        let executor = HttpClientPhaseExecutor::default();
        let base_url = server.base_url();
        let registry = registry_with_client("client", &base_url, None);
        let mut context = ExecutionContext::new();
        context.insert_slot(
            0,
            json!({
                "trace_id": "abc123",
                "user": { "email": "user@example.com", "id": "user-123" }
            }),
        );

        let result = executor
            .execute(&phase, &mut context, &registry)
            .await
            .expect("http client success");

        mock.assert();

        assert_eq!(result["status"], json!(201));
        assert_eq!(result["body"]["status"], json!("ok"));
        assert_eq!(
            result["header"]["content-type"][0],
            json!("application/json")
        );
        assert!(result["durationMs"].as_u64().is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn default_headers_are_applied() {
        let server = MockServer::start_async().await;

        let mock = server.mock(|when, then| {
            when.method("GET")
                .path("/defaults")
                .header("x-client", "chronicle");
            then.status(200);
        });

        let mut phase_options = OptionMap::new();
        phase_options.insert(
            "connector".to_string(),
            ScalarValue::String("client".to_string()),
        );
        phase_options.insert(
            "path".to_string(),
            ScalarValue::String("/defaults".to_string()),
        );

        let phase = build_phase(phase_options);
        let executor = HttpClientPhaseExecutor::default();
        let base_url = server.base_url();
        let registry = registry_with_client(
            "client",
            &base_url,
            Some(vec![("x-client", "chronicle")]),
        );
        let mut context = ExecutionContext::new();

        executor
            .execute(&phase, &mut context, &registry)
            .await
            .expect("http client success");

        mock.assert();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn binary_responses_are_base64_encoded() {
        let server = MockServer::start_async().await;

        let binary_payload = vec![0xFF_u8, 0xFE, 0xFD];

        let mock = server.mock(|when, then| {
            when.method("GET").path("/binary");
            then.status(200)
                .header("content-type", "application/octet-stream")
                .body(binary_payload.clone());
        });

        let mut phase_options = OptionMap::new();
        phase_options.insert(
            "connector".to_string(),
            ScalarValue::String("client".to_string()),
        );
        phase_options.insert(
            "path".to_string(),
            ScalarValue::String("/binary".to_string()),
        );

        let phase = build_phase(phase_options);
        let executor = HttpClientPhaseExecutor::default();
        let base_url = server.base_url();
        let registry = registry_with_client("client", &base_url, None);
        let mut context = ExecutionContext::new();

        let result = executor
            .execute(&phase, &mut context, &registry)
            .await
            .expect("binary response");

        mock.assert();

        assert_eq!(result["status"], json!(200));
        let encoded = result["body"].as_str().expect("body string");
        assert_eq!(encoded, BASE64.encode(&binary_payload));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn missing_connector_returns_error() {
        let mut options = OptionMap::new();
        options.insert(
            "connector".to_string(),
            ScalarValue::String("missing".to_string()),
        );

        let phase = build_phase(options);
        let executor = HttpClientPhaseExecutor::default();
        let registry = ConnectorFactoryRegistry::new(Arc::new(Default::default()));
        let mut context = ExecutionContext::new();

        let err = executor
            .execute(&phase, &mut context, &registry)
            .await
            .expect_err("missing connector should error");

        match err {
            PhaseExecutionError::Message(message) => {
                assert!(message.contains("connector"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
