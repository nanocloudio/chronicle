
use chronicle::config::integration::IntegrationConfig;
use chronicle::integration::registry::ConnectorRegistry;
use std::path::PathBuf;

    #[path = "../support/mod.rs"]
    mod support;

    #[test]
    fn registry_builds_from_fixture() {
        let config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/chronicle-integration.yaml");
        let config = IntegrationConfig::from_path(&config_path).expect("fixture config");

        let registry =
            ConnectorRegistry::build(&config, config_path.parent().expect("config has parent"))
                .expect("registry should build");

        let http_server = registry
            .http_server("sample_ingest_service")
            .expect("http server connector");
        assert_eq!(http_server.host, "0.0.0.0");
        assert_eq!(http_server.port, 8443);

        let http_tls = http_server.tls.as_ref().expect("tls should be present");
        assert_eq!(
            http_tls
                .key
                .as_ref()
                .and_then(|path| path.file_name())
                .and_then(|name| name.to_str()),
            Some("key.pem")
        );

        let kafka = registry
            .kafka("sample_kafka_cluster")
            .expect("kafka connector");
        assert_eq!(kafka.brokers, vec!["localhost:9092"]);
        assert!(registry.has_kafka_connectors());
        assert!(!registry.has_postgres_connectors());

        let http_client = registry
            .http_client("sample_processing_api")
            .expect("http client connector");
        assert_eq!(http_client.base_url, "https://processor.example.com");
        let pool = http_client.pool.as_ref().expect("http client pool");
        assert_eq!(pool.max_idle_per_host, Some(32));
        assert_eq!(pool.idle_timeout_secs, Some(30));

        assert!(
            registry.unknown().next().is_none(),
            "no unknown connectors expected"
        );
    }

    #[test]
    fn mongodb_and_redis_connectors_are_indexed() {
        let yaml = support::feature_flags::enable_optional_feature_flags(
            r#"
connectors:
  - name: mongo
    type: mongodb
    options:
      uri: mongodb://localhost:27017
      database: records
      tls:
        ca: certs/ca.pem
  - name: redis-cache
    type: redis
    options:
      url: redis://localhost:6379
      cluster: true
      pool:
        max_connections: 32
      tls:
        ca: tls/ca.pem
"#;
        );

        let config = IntegrationConfig::from_reader(yaml.as_bytes()).expect("config");
        let base = PathBuf::from("/configs");

        let registry = ConnectorRegistry::build(&config, &base).expect("registry");

        let mongodb = registry.mongodb("mongo").expect("mongodb connector");
        assert_eq!(mongodb.uri, "mongodb://localhost:27017");
        assert_eq!(mongodb.database, "records");
        let mongo_tls = mongodb.tls.as_ref().expect("mongodb tls");
        assert_eq!(
            mongo_tls.ca.as_ref().map(|path| path.display().to_string()),
            Some("/configs/certs/ca.pem".to_string())
        );

        let redis = registry.redis("redis-cache").expect("redis connector");
        assert_eq!(redis.url, "redis://localhost:6379");
        assert_eq!(redis.cluster, Some(true));
        let redis_pool = redis.pool.as_ref().expect("redis pool");
        assert_eq!(redis_pool.max_connections, Some(32));
        let redis_tls = redis.tls.as_ref().expect("redis tls");
        assert_eq!(
            redis_tls.ca.as_ref().map(|path| path.display().to_string()),
            Some("/configs/tls/ca.pem".to_string())
        );
    }

    #[test]
    fn http_connectors_with_role_are_split() {
        let yaml = r#"
connectors:
  - name: ingest
    type: http
    options:
      role: server
      host: 0.0.0.0
      port: 8080
  - name: outbound
    type: http
    options:
      role: client
      base_url: http://processor.example.com
"#;

        let config = IntegrationConfig::from_reader(yaml.as_bytes()).expect("config");
        let registry = ConnectorRegistry::build(&config, ".").expect("registry");

        let server = registry.http_server("ingest").expect("http server");
        assert_eq!(server.host, "0.0.0.0");
        assert_eq!(server.port, 8080);

        let client = registry.http_client("outbound").expect("http client");
        assert_eq!(client.base_url, "http://processor.example.com");
    }
