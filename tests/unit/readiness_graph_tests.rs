use chronicle::chronicle::engine::DependencySource;
use chronicle::config::IntegrationConfig;
use chronicle::readiness::RouteGraph;

    #[test]
    fn graph_infers_dependencies_without_policy_overrides() {
        let yaml = r#"
api_version: v1
app:
  limits:
    routes:
      max_inflight: 9
connectors:
  - name: ingest
    type: http_server
    options:
      host: 0.0.0.0
      port: 8080
  - name: kafka
    type: kafka
    options:
      brokers:
        - "broker:9092"
  - name: db
    type: postgres
    options:
      url: postgres://user:pass@localhost:5432/app
chronicles:
  - name: sample
    trigger:
      connector: ingest
      options:
        path: /events
    phases:
      - name: enrich
        type: transform
        options:
          payload: .[0].body
      - name: publish
        type: kafka_producer
        options:
          connector: kafka
          topic: events
      - name: store
        type: postgres_idempotent_insert
        options:
          connector: db
          sql: INSERT INTO events (body) VALUES (:body)
"#;

        let config = IntegrationConfig::from_reader(yaml.as_bytes()).expect("config must parse");
        let graph = RouteGraph::build(&config);

        assert_eq!(graph.routes.len(), 1);
        let route = &graph.routes[0];
        assert_eq!(route.name, "sample");
        assert_eq!(route.trigger.name, "endpoint.ingest");
        assert_eq!(route.dependencies.len(), 2);
        assert_eq!(route.dependencies[0].source, DependencySource::Inferred);
        assert_eq!(route.dependencies[1].source, DependencySource::Inferred);
        let dependency_names: Vec<_> = route
            .dependencies
            .iter()
            .map(|edge| edge.endpoint.name.as_str())
            .collect();
        assert_eq!(
            dependency_names,
            vec!["endpoint.db", "endpoint.kafka"]
        );
        assert_eq!(route.inferred_dependencies, vec!["db", "kafka"]);
        assert_eq!(route.policy.limits.effective.max_inflight, Some(9));
        assert!(route.policy.limits.policy.is_none());
    }

    #[test]
    fn graph_respects_explicit_policy_requires() {
        let yaml = r#"
api_version: v1
app:
  limits:
    routes:
      max_inflight: 12
connectors:
  - name: ingest
    type: http_server
    options:
      host: 0.0.0.0
      port: 8080
  - name: kafka
    type: kafka
    options:
      brokers:
        - "broker:9092"
  - name: dead-letter
    type: kafka
    options:
      brokers:
        - "broker:9092"
chronicles:
  - name: sample
    trigger:
      connector: ingest
      options:
        path: /events
    phases:
      - name: publish
        type: kafka_producer
        options:
          connector: kafka
          topic: events
      - name: dead-letter
        type: kafka_producer
        options:
          connector: dead-letter
          topic: events.dlq
    policy:
      requires: ["dead-letter", "kafka"]
      allow_partial_delivery: true
      limits:
        max_inflight: 4
      fallback:
        kafka: dead-letter
"#;

        let config = IntegrationConfig::from_reader(yaml.as_bytes()).expect("config must parse");
        let graph = RouteGraph::build(&config);

        assert_eq!(graph.routes.len(), 1);
        let route = &graph.routes[0];
        assert_eq!(route.dependencies.len(), 2);
        let dependency_names: Vec<_> = route
            .dependencies
            .iter()
            .map(|edge| edge.endpoint.name.as_str())
            .collect();
        assert_eq!(
            dependency_names,
            vec!["endpoint.dead-letter", "endpoint.kafka"]
        );
        for edge in &route.dependencies {
            assert_eq!(edge.source, DependencySource::Explicit);
        }
        assert_eq!(route.inferred_dependencies, vec!["dead-letter", "kafka"]);
        assert!(route.policy.allow_partial_delivery);
        assert_eq!(route.policy.limits.effective.max_inflight, Some(4));
        assert_eq!(route.policy.limits.app.as_ref().and_then(|l| l.max_inflight), Some(12));
        assert_eq!(route.policy.limits.policy.as_ref().and_then(|l| l.max_inflight), Some(4));
        let fallback = route.policy.fallback.as_ref().expect("fallback present");
        assert_eq!(fallback.get("kafka").map(String::as_str), Some("dead-letter"));
    }
