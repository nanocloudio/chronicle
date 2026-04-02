use chronicle::config::integration::StateProvider;
use chronicle::config::IntegrationConfig;

fn parse_config(yaml: &str) -> Result<IntegrationConfig, String> {
    IntegrationConfig::from_reader(yaml.as_bytes()).map_err(|e| e.to_string())
}

fn minimal_yaml(state_block: &str) -> String {
    format!(
        r#"
api_version: v1
app:
  min_ready_routes: all
  readiness_cache: 250ms
  limits:
    routes:
      max_inflight: 1024
      overflow_policy: reject
  retry_budget:
    max_attempts: 5
    max_elapsed: 30s
    base_backoff: 50ms
    max_backoff: 5s
    jitter: full
{state_block}
connectors:
  - name: http-in
    type: http
    options:
      role: server
      listen_addr: ":8080"
chronicles:
  - name: test
    trigger:
      connector: http-in
      options:
        method: POST
        path: /test
    phases:
      - name: echo
        type: transform
        options:
          body: .[0].body
"#
    )
}

// ---------------------------------------------------------------------------
// Omitted state section — defaults apply
// ---------------------------------------------------------------------------

#[test]
fn omitted_state_section_uses_defaults() {
    let yaml = minimal_yaml("");
    let config = parse_config(&yaml).expect("should parse without state section");
    assert!(matches!(config.app.state.provider, StateProvider::Memory));
    assert!(config.app.state.retention.is_zero());
}

// ---------------------------------------------------------------------------
// Memory provider
// ---------------------------------------------------------------------------

#[test]
fn memory_provider_with_retention() {
    let yaml = minimal_yaml(
        "  state:\n    provider: memory\n    retention: 5m",
    );
    let config = parse_config(&yaml).expect("should parse");
    assert!(matches!(config.app.state.provider, StateProvider::Memory));
    assert_eq!(config.app.state.retention, std::time::Duration::from_secs(300));
}

#[test]
fn memory_provider_zero_retention() {
    let yaml = minimal_yaml(
        "  state:\n    provider: memory\n    retention: 0s",
    );
    let config = parse_config(&yaml).expect("should parse");
    assert!(config.app.state.retention.is_zero());
}

#[test]
fn memory_provider_implicit_when_only_retention_set() {
    let yaml = minimal_yaml("  state:\n    retention: 1h");
    let config = parse_config(&yaml).expect("should parse");
    assert!(matches!(config.app.state.provider, StateProvider::Memory));
    assert_eq!(
        config.app.state.retention,
        std::time::Duration::from_secs(3600)
    );
}

// ---------------------------------------------------------------------------
// Clustor provider
// ---------------------------------------------------------------------------

#[test]
fn clustor_provider_valid() {
    let yaml = minimal_yaml(
        r#"  state:
    provider: clustor
    retention: 1h
    node_id: node-1
    peer_addrs:
      - "node-2@10.0.0.2:9400"
    data_dir: /var/lib/chronicle/raft
    raft_bind: "0.0.0.0:9400"
    tls_cert: /etc/tls/cert.pem
    tls_key: /etc/tls/key.pem
    tls_ca: /etc/tls/ca.pem
    trust_domain: chronicle.local"#,
    );
    let config = parse_config(&yaml).expect("should parse clustor config");
    match &config.app.state.provider {
        StateProvider::Clustor {
            node_id,
            peer_addrs,
            data_dir,
            raft_bind,
            ..
        } => {
            assert_eq!(node_id, "node-1");
            assert_eq!(peer_addrs.len(), 1);
            assert_eq!(data_dir.to_str().unwrap(), "/var/lib/chronicle/raft");
            assert_eq!(raft_bind, "0.0.0.0:9400");
        }
        other => panic!("expected Clustor, got {other:?}"),
    }
}

#[test]
fn clustor_provider_missing_required_fields() {
    let yaml = minimal_yaml(
        "  state:\n    provider: clustor\n    retention: 1h",
    );
    let err = parse_config(&yaml).expect_err("should fail with missing fields");
    assert!(err.contains("node_id"), "should mention node_id: {err}");
    assert!(err.contains("peer_addrs"), "should mention peer_addrs: {err}");
    assert!(err.contains("data_dir"), "should mention data_dir: {err}");
    assert!(err.contains("raft_bind"), "should mention raft_bind: {err}");
    assert!(err.contains("tls_cert"), "should mention tls_cert: {err}");
}

// ---------------------------------------------------------------------------
// Lattice provider
// ---------------------------------------------------------------------------

#[test]
fn lattice_provider_valid() {
    let yaml = minimal_yaml(
        "  state:\n    provider: lattice\n    endpoint: localhost:2379\n    retention: 1h",
    );
    let config = parse_config(&yaml).expect("should parse lattice config");
    match &config.app.state.provider {
        StateProvider::Lattice { endpoint, prefix } => {
            assert_eq!(endpoint, "localhost:2379");
            assert_eq!(prefix, "/chronicle/exec/");
        }
        other => panic!("expected Lattice, got {other:?}"),
    }
}

#[test]
fn lattice_provider_missing_endpoint() {
    let yaml = minimal_yaml(
        "  state:\n    provider: lattice\n    retention: 1h",
    );
    let err = parse_config(&yaml).expect_err("should fail with missing endpoint");
    assert!(err.contains("endpoint"), "should mention endpoint: {err}");
}

// ---------------------------------------------------------------------------
// Invalid provider
// ---------------------------------------------------------------------------

#[test]
fn unknown_provider_rejected() {
    let yaml = minimal_yaml(
        "  state:\n    provider: redis\n    retention: 1h",
    );
    let err = parse_config(&yaml).expect_err("should reject unknown provider");
    assert!(
        err.contains("memory") && err.contains("lattice") && err.contains("clustor"),
        "should list valid providers: {err}"
    );
}

// ---------------------------------------------------------------------------
// Unknown keys
// ---------------------------------------------------------------------------

#[test]
fn unknown_key_in_state_rejected() {
    let yaml = minimal_yaml(
        "  state:\n    provider: memory\n    retention: 1h\n    bogus_key: true",
    );
    let err = parse_config(&yaml).expect_err("should reject unknown key");
    assert!(
        err.contains("bogus_key") || err.contains("unknown"),
        "should mention the unknown key: {err}"
    );
}
