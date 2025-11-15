use chronicle::config::integration::IntegrationConfig;
use std::path::PathBuf;
use std::time::Duration;

#[path = "../support/mod.rs"]
mod support;

macro_rules! flagged_yaml {
    ($body:expr) => {
        support::feature_flags::enable_optional_feature_flags($body)
    };
}

use support::config::sanitise_document;

#[test]
fn sanitiser_converts_backticks_and_equals() {
    let raw = "body: `a literal`\nkey = value";
    let expected = "body: \"a literal\"\nkey: value";
    assert_eq!(sanitise_document(raw), expected);
}

#[test]
fn fixture_config_parses() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/chronicle-integration.yaml");

    let config = IntegrationConfig::from_path(path).expect("fixture should load");

    assert_eq!(config.connectors.len(), 6);
    assert_eq!(config.chronicles.len(), 5);
    let management = config.management.expect("management block expected");
    assert_eq!(management.host, "0.0.0.0");
    assert_eq!(management.port, 9100);
    assert_eq!(
        management
            .live
            .as_ref()
            .expect("live endpoint expected")
            .path,
        "/healthz"
    );
    assert_eq!(
        management
            .metrics
            .as_ref()
            .expect("metrics endpoint expected")
            .path,
        "/metrics"
    );
}

#[test]
fn missing_api_version_reports_error() {
    let yaml = r#"
connectors: []
chronicles: []
"#;

    let error =
        IntegrationConfig::from_reader(yaml.as_bytes()).expect_err("missing api_version");

    assert!(format!("{error}").contains("api_version is required"));
}

#[test]
fn unknown_top_level_key_is_rejected() {
    let yaml = r#"
api_version: v1
apps: {}
connectors: []
chronicles: []
"#;

    let error =
        IntegrationConfig::from_reader(yaml.as_bytes()).expect_err("unknown top-level key");

    assert!(format!("{error}").contains("unknown top-level key"));
}

#[test]
fn multiple_yaml_documents_not_supported() {
    let yaml = r#"
api_version: v1
connectors: []
chronicles: []
---
api_version: v1
connectors: []
chronicles: []
"#;

    let error =
        IntegrationConfig::from_reader(yaml.as_bytes()).expect_err("multi-doc yaml rejected");

    assert!(format!("{error}").contains("multiple YAML documents are not supported"));
}

#[test]
fn retry_budget_max_backoff_cannot_exceed_max_elapsed() {
    let yaml = r#"
api_version: v1
app:
  retry_budget:
    max_elapsed: 5s
    max_backoff: 10s
connectors: []
chronicles: []
"#;

    let error = IntegrationConfig::from_reader(yaml.as_bytes())
        .expect_err("max_backoff greater than max_elapsed should fail");

    assert!(format!("{error}")
        .contains("app.retry_budget.max_backoff must be less than or equal to app.retry_budget.max_elapsed"));
}

#[test]
fn tls_inline_private_keys_rejected() {
    let yaml = r#"
api_version: v1
connectors:
  - name: ingest
    type: http_server
    options:
      host: 0.0.0.0
      port: 8080
      tls:
        cert: certs/cert.pem
        key: |
          -----BEGIN PRIVATE KEY-----
          MIIBVwIBADANBgkqhkiG9w0BAQEFAASCAT8wggE7AgEAAkEAu
chronicles: []
"#;

    let error =
        IntegrationConfig::from_reader(yaml.as_bytes()).expect_err("inline private key rejected");

    assert!(format!("{error}").contains("tls.key must reference a filesystem path"));
}

    #[test]
    fn duplicate_connectors_fail_validation() {
        let yaml = flagged_yaml!(r#"
connectors:
  - name: one
    type: http_server
  - name: one
    type: http_server
"#);

        let error = IntegrationConfig::from_reader(yaml.as_bytes())
            .expect_err("duplicate connector should fail");

        assert!(format!("{error}").contains("duplicate connector"));
    }

    #[test]
    fn trigger_referencing_unknown_connector_fails() {
        let yaml = flagged_yaml!(r#"
connectors:
  - name: good
    type: http_server
chronicles:
  - name: bad-trigger
    trigger:
      connector: missing
      options: {}
    phases: []
"#);

        let error = IntegrationConfig::from_reader(yaml.as_bytes())
            .expect_err("unknown connector should fail");

        assert!(format!("{error}").contains("unknown trigger connector"));
    }

    #[test]
    fn phase_referencing_wrong_connector_type_fails() {
        let yaml = flagged_yaml!(r#"
connectors:
  - name: http
    type: http_client
  - name: kafka
    type: kafka
chronicles:
  - name: bad-phase
    trigger:
      connector: http
      options: {}
    phases:
      - name: publish
        type: kafka_producer
        options:
          connector: http
"#);

        let error = IntegrationConfig::from_reader(yaml.as_bytes())
            .expect_err("wrong connector type should fail");

        assert!(format!("{error}").contains("expects connector `http` to be `kafka`"));
    }

    #[test]
    fn tls_cert_and_key_must_be_paired() {
        let yaml = flagged_yaml!(r#"
connectors:
  - name: rest
    type: http_server
    options:
      host: 0.0.0.0
      port: 8080
      tls.cert: cert.pem
chronicles: []
"#);

        let error = IntegrationConfig::from_reader(yaml.as_bytes())
            .expect_err("tls pair validation should fail");

        assert!(format!("{error}").contains("tls.cert"));
    }

    #[test]
    fn https_http_client_requires_tls_block() {
        let yaml = flagged_yaml!(r#"
connectors:
  - name: outbound
    type: http
    options:
      role: client
      base_url: https://api.example.com
chronicles: []
"#);

        let error = IntegrationConfig::from_reader(yaml.as_bytes())
            .expect_err("https clients must configure tls block");

        assert!(
            format!("{error}").contains("uses an https base_url but is missing the `tls` section")
        );
    }

    #[test]
    fn rabbitmq_client_cert_requires_amqps() {
        let yaml = flagged_yaml!(r#"
connectors:
  - name: rabbit
    type: rabbitmq
    options:
      url: amqp://localhost:5672/%2f
      tls:
        cert: certs/client.pem
        key: certs/client.key
chronicles: []
"#);

        let error = IntegrationConfig::from_reader(yaml.as_bytes())
            .expect_err("client certificates without amqps should fail");

        assert!(format!("{error}")
            .contains("configures TLS client certificates but `url` does not start with amqps://"));
    }

    #[test]
    fn rediss_url_requires_tls_block() {
        let yaml = flagged_yaml!(r#"
connectors:
  - name: cache
    type: redis
    options:
      url: rediss://cache.example.com:6380
chronicles: []
"#);

        let error = IntegrationConfig::from_reader(yaml.as_bytes())
            .expect_err("rediss connectors need tls block");

        assert!(format!("{error}").contains("uses rediss:// but is missing the `tls` section"));
    }

    #[test]
    fn kafka_client_cert_requires_tls_enabled() {
        let yaml = flagged_yaml!(r#"
connectors:
  - name: kafka
    type: kafka
    options:
      brokers:
        - "broker:9092"
      security:
        tls:
          client_cert: certs/client.pem
          client_key: certs/client.key
chronicles: []
"#);

        let error = IntegrationConfig::from_reader(yaml.as_bytes())
            .expect_err("mutual TLS without enabled flag should fail");

        assert!(format!("{error}")
            .contains("configures `security.tls.client_cert`/`client_key` but `security.tls.enabled` is not true"));
    }

    #[test]
    fn kafka_tls_enabled_requires_ca() {
        let yaml = flagged_yaml!(r#"
connectors:
  - name: kafka
    type: kafka
    options:
      brokers:
        - "broker:9092"
      security:
        tls:
          enabled: true
chronicles: []
"#);

        let error = IntegrationConfig::from_reader(yaml.as_bytes())
            .expect_err("tls enabled without ca should fail");

        assert!(format!("{error}").contains("enables `security.tls` but is missing `security.tls.ca`"));
    }

    #[test]
    fn mariadb_tls_disable_with_cert_fails() {
        let yaml = flagged_yaml!(r#"
connectors:
  - name: mariadb
    type: mariadb
    options:
      url: mysql://user:pass@localhost:3306/db
      tls:
        mode: disable
        cert: certs/client.pem
        key: certs/client.key
chronicles: []
"#);

        let error = IntegrationConfig::from_reader(yaml.as_bytes())
            .expect_err("disabled tls should not accept credentials");

        assert!(format!("{error}")
            .contains("sets `tls.mode` to `disable` but also configures TLS credentials"));
    }

    #[test]
    fn postgres_tls_disable_with_cert_fails() {
        let yaml = flagged_yaml!(r#"
connectors:
  - name: postgres
    type: postgres
    options:
      url: postgres://user:pass@localhost:5432/db
      tls:
        mode: disable
        cert: certs/client.pem
        key: certs/client.key
chronicles: []
"#);

        let error = IntegrationConfig::from_reader(yaml.as_bytes())
            .expect_err("disabled tls should not accept postgres credentials");

        assert!(format!("{error}")
            .contains("sets `tls.mode` to `disable` but also configures TLS credentials"));
    }

    #[test]
    fn rabbitmq_retry_max_must_exceed_initial() {
        let yaml = flagged_yaml!(r#"
api_version: v1
connectors:
  - name: rabbit
    type: rabbitmq
    options:
      url: amqp://user:pass@localhost:5672/%2f
chronicles:
  - name: invalid-retry
    trigger:
      connector: rabbit
      options:
        queue: jobs
        retry_initial: 5s
        retry_max: 1s
    phases: []
"#);

        let error = IntegrationConfig::from_reader(yaml.as_bytes())
            .expect_err("retry_max lower than retry_initial should fail");

        assert!(format!("{error}")
            .contains("rabbitmq trigger option `retry_max` must be greater than or equal to `retry_initial`"));
    }

    #[test]
    fn rabbitmq_retry_fields_require_valid_values() {
        let yaml = flagged_yaml!(r#"
api_version: v1
connectors:
  - name: rabbit
    type: rabbitmq
    options:
      url: amqp://user:pass@localhost:5672/%2f
chronicles:
  - name: bad-rabbit
    trigger:
      connector: rabbit
      options:
        queue: events
        retry_initial: 200
        retry_max: invalid
        retry_multiplier: 1.0
    phases: []
"#);

        let error = IntegrationConfig::from_reader(yaml.as_bytes())
            .expect_err("invalid retry fields should fail");

        let message = format!("{error}");
        assert!(message.contains("rabbitmq trigger option `retry_initial` must be provided as a duration string"));
        assert!(message.contains("rabbitmq trigger option `retry_max` must be a valid duration"));
        assert!(message.contains("rabbitmq trigger option `retry_multiplier` must be between 1.1 and 10.0"));
    }

    #[test]
    fn rabbitmq_ack_mode_must_be_known_value() {
        let yaml = flagged_yaml!(r#"
api_version: v1
connectors:
  - name: rabbit
    type: rabbitmq
    options:
      url: amqp://user:pass@localhost:5672/%2f
chronicles:
  - name: bad-ack
    trigger:
      connector: rabbit
      options:
        queue: work
        ack_mode: eager
    phases: []
"#);

        let error = IntegrationConfig::from_reader(yaml.as_bytes())
            .expect_err("invalid ack mode should fail");

        assert!(format!("{error}")
            .contains("rabbitmq trigger option `ack_mode` must be `auto` or `manual`"));
    }

    #[test]
    fn rabbitmq_legacy_retry_keys_rejected_in_v1() {
        let yaml = flagged_yaml!(r#"
api_version: v1
connectors:
  - name: rabbit
    type: rabbitmq
    options:
      url: amqp://user:pass@localhost:5672/%2f
chronicles:
  - name: legacy-keys
    trigger:
      connector: rabbit
      options:
        queue: audit
        retry_initial_ms: 100
        retry_max_ms: 500
    phases: []
"#);

        let error = IntegrationConfig::from_reader(yaml.as_bytes())
            .expect_err("legacy retry keys should fail");

        let message = format!("{error}");
        assert!(message.contains("rabbitmq trigger uses deprecated option `retry_initial_ms`") );
        assert!(message.contains("rabbitmq trigger uses deprecated option `retry_max_ms`"));
    }

    #[test]
    fn mqtt_qos_and_retain_handling_are_bounded() {
        let yaml = flagged_yaml!(r#"
api_version: v1
connectors:
  - name: mqtt
    type: mqtt
    options:
      url: mqtt://broker.local:1883
chronicles:
  - name: invalid-mqtt
    trigger:
      connector: mqtt
      options:
        topic: devices/#
        qos: 3
        retain_handling: -1
    phases: []
"#);

        let error = IntegrationConfig::from_reader(yaml.as_bytes())
            .expect_err("invalid qos/retain handling should fail");

        let message = format!("{error}");
        assert!(message.contains("mqtt trigger option `qos` must be 0, 1, or 2"));
        assert!(message.contains("mqtt trigger option `retain_handling` must be 0, 1, or 2"));
    }

    #[test]
    fn mqtt_retry_multiplier_requires_range() {
        let yaml = flagged_yaml!(r#"
api_version: v1
connectors:
  - name: mqtt
    type: mqtt
    options:
      url: mqtt://broker.local:1883
chronicles:
  - name: bad-multiplier
    trigger:
      connector: mqtt
      options:
        topic: sensors/#
        retry_initial: whoops
        retry_multiplier: 10.5
    phases: []
"#);

        let error = IntegrationConfig::from_reader(yaml.as_bytes())
            .expect_err("retry multiplier outside bounds should fail");

        let message = format!("{error}");
        assert!(message.contains("mqtt trigger option `retry_initial` must be a valid duration"));
        assert!(message.contains("mqtt trigger option `retry_multiplier` must be between 1.1 and 10.0"));
    }

    #[test]
    fn mongodb_pipeline_must_be_array() {
        let yaml = flagged_yaml!(r#"
api_version: v1
connectors:
  - name: mongo
    type: mongodb
    options:
      uri: mongodb://localhost:27017
      database: telemetry
chronicles:
  - name: bad-mongo
    trigger:
      connector: mongo
      options:
        pipeline: select *
    phases: []
"#);

        let error = IntegrationConfig::from_reader(yaml.as_bytes())
            .expect_err("pipeline must be array");

        assert!(format!("{error}")
            .contains("mongodb trigger option `pipeline` must be an array"));
    }

    #[test]
    fn redis_trigger_requires_mode_and_fields() {
        let yaml_missing_mode = flagged_yaml!(r#"
api_version: v1
connectors:
  - name: redis
    type: redis
    options:
      url: redis://localhost:6379
chronicles:
  - name: missing-mode
    trigger:
      connector: redis
      options:
        channels:
          - events
    phases: []
"#);

        let error = IntegrationConfig::from_reader(yaml_missing_mode.as_bytes())
            .expect_err("mode is required");
        let message = format!("{error}");
        assert!(message
            .contains("redis trigger requires option `mode` set to `pubsub` or `stream`"));

        let yaml_pubsub = flagged_yaml!(r#"
api_version: v1
connectors:
  - name: redis
    type: redis
    options:
      url: redis://localhost:6379
chronicles:
  - name: missing-channels
    trigger:
      connector: redis
      options:
        mode: pubsub
    phases: []
"#);

        let error = IntegrationConfig::from_reader(yaml_pubsub.as_bytes())
            .expect_err("pubsub requires channels");
        assert!(format!("{error}")
            .contains("redis trigger in `pubsub` mode requires option `channels`"));

        let yaml_stream = flagged_yaml!(r#"
api_version: v1
connectors:
  - name: redis
    type: redis
    options:
      url: redis://localhost:6379
chronicles:
  - name: missing-stream
    trigger:
      connector: redis
      options:
        mode: stream
    phases: []
"#);

        let error = IntegrationConfig::from_reader(yaml_stream.as_bytes())
            .expect_err("stream mode requires stream name");
        assert!(format!("{error}")
            .contains("redis trigger in `stream` mode requires option `stream`"));
    }

    #[test]
    fn http_trigger_requires_path() {
        let yaml = flagged_yaml!(r#"
connectors:
  - name: rest
    type: http_server
    options:
      port: 8080
chronicles:
  - name: missing-path
    trigger:
      connector: rest
      options: {}
    phases: []
"#);

        let error =
            IntegrationConfig::from_reader(yaml.as_bytes()).expect_err("missing path should fail");

        assert!(format!("{error}").contains("http trigger requires a `path` option"));
    }

    #[test]
    fn api_version_defaults_and_app_defaults_apply() {
        let yaml = flagged_yaml!(r#"
api_version: v1
connectors: []
chronicles: []
"#);

        let config =
            IntegrationConfig::from_reader(yaml.as_bytes()).expect("defaults should parse");

        assert!(matches!(config.api_version, ApiVersion::V1));
        assert!(matches!(
            config.app.min_ready_routes,
            MinReadyRoutes::All
        ));
        assert_eq!(config.app.half_open_counts_as_ready, HalfOpenPolicy::Route);
        assert_eq!(config.app.warmup_timeout, Duration::from_secs(30));
        assert_eq!(config.app.readiness_cache, Duration::from_millis(250));
        assert_eq!(config.app.drain_timeout, Duration::from_secs(30));
        assert!(config.app.retry_budget.is_none());
        assert!(config.app.feature_flags.is_empty());
    }

    #[test]
    fn unsupported_api_version_fails_validation() {
        let yaml = flagged_yaml!(r#"
api_version: v2
connectors: []
chronicles: []
"#);

        let error = IntegrationConfig::from_reader(yaml.as_bytes())
            .expect_err("unsupported api_version should fail");

        assert!(format!("{error}")
            .contains("api_version `v2` is not supported (supported versions: v1)"));
    }

    #[test]
    fn chronicle_policy_parses_structured_fields() {
        let yaml = flagged_yaml!(r#"
api_version: v1
connectors:
  - name: rest
    type: http_server
    options:
      port: 8080
chronicles:
  - name: policy-route
    trigger:
      connector: rest
      options:
        path: /
    phases:
      - name: dead-letter
        type: transform
        options: {}
    policy:
      requires: ["rest"]
      allow_partial_delivery: true
      readiness_priority: 7
      limits:
        max_inflight: 42
        overflow_policy: queue
        max_queue_depth: 128
      retry_budget:
        max_attempts: 3
        base_backoff: 25ms
      half_open_counts_as_ready: endpoint
      delivery:
        retries: 5
        backoff: 50ms..2s
        idempotent: true
      fallback:
        rest: dead-letter
"#);

        let config = IntegrationConfig::from_reader(yaml.as_bytes())
            .expect("policy configuration should parse");

        let policy = &config.chronicles[0].policy;
        assert_eq!(policy.requires, vec!["rest".to_string()]);
        assert!(policy.allow_partial_delivery);
        assert_eq!(policy.readiness_priority, Some(7));

        let route_limits = policy.limits.as_ref().expect("route limits present");
        assert_eq!(route_limits.max_inflight, Some(42));
        assert_eq!(route_limits.max_queue_depth, Some(128));
        assert_eq!(route_limits.overflow_policy, Some(OverflowPolicy::Queue));

        let retry_budget = policy.retry_budget.as_ref().expect("retry budget present");
        assert_eq!(retry_budget.max_attempts, Some(3));
        assert_eq!(
            retry_budget.base_backoff,
            Some(Duration::from_millis(25))
        );

        assert_eq!(
            policy.half_open_counts_as_ready,
            Some(HalfOpenPolicy::Endpoint)
        );

        let delivery = policy.delivery.as_ref().expect("delivery policy present");
        assert_eq!(delivery.retries, Some(5));
        let backoff = delivery.backoff.as_ref().expect("backoff range present");
        assert_eq!(backoff.min, Duration::from_millis(50));
        assert_eq!(backoff.max, Duration::from_secs(2));
        assert_eq!(delivery.idempotent, Some(true));

        let fallback = policy.fallback.as_ref().expect("fallback mapping present");
        assert_eq!(fallback.get("rest"), Some(&"dead-letter".to_string()));
    }

    #[test]
    fn invalid_app_duration_is_rejected() {
        let yaml = flagged_yaml!(r#"
api_version: v1
app:
  warmup_timeout: not-a-duration
connectors: []
chronicles: []
"#);

        let error = IntegrationConfig::from_reader(yaml.as_bytes())
            .expect_err("invalid duration should fail");

        assert!(format!("{error}")
            .contains("app.warmup_timeout must be a valid duration (got `not-a-duration`)"));
    }

    #[test]
    fn readiness_cache_enforces_minimum_duration() {
        let yaml = flagged_yaml!(r#"
api_version: v1
app:
  readiness_cache: 25ms
connectors: []
chronicles: []
"#);

        let error = IntegrationConfig::from_reader(yaml.as_bytes())
            .expect_err("readiness cache minimum should fail");

        assert!(format!("{error}").contains("app.readiness_cache must be at least 50ms"));
    }

    #[test]
    fn kafka_trigger_requires_topic() {
        let yaml = flagged_yaml!(r#"
connectors:
  - name: rest
    type: http_server
    options:
      port: 8080
  - name: kafka
    type: kafka
    options:
      brokers: localhost:9092
chronicles:
  - name: missing-topic
    trigger:
      connector: kafka
      kafka:
        group_id: g1
    phases: []
"#);

        let error =
            IntegrationConfig::from_reader(yaml.as_bytes()).expect_err("missing topic should fail");

        assert!(format!("{error}").contains("kafka trigger requires option `topic`"));
    }

    #[test]
    fn kafka_producer_requires_topic_option() {
        let yaml = flagged_yaml!(r#"
connectors:
  - name: rest
    type: http_server
    options:
      port: 8080
  - name: kafka
    type: kafka
    options:
      brokers: localhost:9092
chronicles:
  - name: pipeline
    trigger:
      connector: rest
      options:
        path: /ingest
    phases:
      - name: publish
        type: kafka_producer
        options:
          connector: kafka
"#);

        let error =
            IntegrationConfig::from_reader(yaml.as_bytes()).expect_err("missing topic should fail");

        assert!(format!("{error}").contains("phase `publish` (index 0) requires option `topic`"));
    }

    #[test]
    fn mariadb_insert_requires_key_option() {
        let yaml = flagged_yaml!(r#"
connectors:
  - name: rest
    type: http_server
    options:
      port: 8080
  - name: mariadb
    type: mariadb
    options:
      url: mysql://user:pass@localhost:3306/db
chronicles:
  - name: store
    trigger:
      connector: rest
      options:
        path: /ingest
    phases:
      - name: persist
        type: mariadb_idempotent_insert
        options:
          connector: mariadb
"#);

        let error =
            IntegrationConfig::from_reader(yaml.as_bytes()).expect_err("missing key should fail");

        assert!(format!("{error}").contains("phase `persist` (index 0) requires option `key`"));
    }

    #[test]
    fn http_trigger_using_http_client_fails() {
        let yaml = flagged_yaml!(r#"
connectors:
  - name: client
    type: http_client
    options:
      base_url: https://example.com
chronicles:
  - name: invalid-http-trigger
    trigger:
      connector: client
      options:
        path: /ingest
    phases: []
"#);

        let error = IntegrationConfig::from_reader(yaml.as_bytes())
            .expect_err("http trigger bound to client connector should fail");

        assert!(format!("{error}").contains("expects HTTP server connector"));
    }

    #[test]
    fn http_client_tls_requires_cert_and_key_pair() {
        let yaml = flagged_yaml!(r#"
connectors:
  - name: api-client
    type: http_client
    options:
      base_url: https://example.com
      tls.cert: client.crt
chronicles: []
"#);

        let error = IntegrationConfig::from_reader(yaml.as_bytes())
            .expect_err("http client tls cert without key should fail");

        assert!(format!("{error}")
            .contains("connector `api-client` must provide both `tls.cert` and `tls.key`"));
    }

    #[test]
    fn validation_error_renders_all_messages() {
        let yaml = flagged_yaml!(r#"
connectors:
  - name: rest
    type: http_server
    options:
      port: 8080
      tls.cert: cert.pem
chronicles:
  - name: sample
    trigger:
      connector: rest
      options: {}
    phases: []
"#);

        let error = IntegrationConfig::from_reader(yaml.as_bytes())
            .expect_err("multiple validation failures expected");

        let rendered = format!("{error}");
        assert!(
            rendered.contains("connector `rest` must provide both `tls.cert` and `tls.key`"),
            "rendered error should mention tls pair: {rendered}"
        );
        assert!(
            rendered.contains("chronicle `sample` http trigger requires a `path` option"),
            "rendered error should mention trigger path: {rendered}"
        );
    }
