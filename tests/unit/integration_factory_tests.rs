
use chronicle::config::integration::IntegrationConfig;
use chronicle::integration::factory::{ConnectorFactoryError, ConnectorFactoryRegistry};
use chronicle::integration::registry::ConnectorRegistry;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::tempdir;

    #[path = "../support/mod.rs"]
    mod support;

    fn build_registry() -> (Arc<ConnectorRegistry>, ConnectorFactoryRegistry) {
        let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/chronicle-integration.yaml");
        let config = IntegrationConfig::from_path(&fixture).expect("integration config");
        let registry = Arc::new(
            ConnectorRegistry::build(&config, fixture.parent().expect("fixture dir"))
                .expect("registry build"),
        );
        let factory = ConnectorFactoryRegistry::new(registry.clone());
        (registry, factory)
    }

    fn factory_from_yaml(yaml: &str) -> ConnectorFactoryRegistry {
        let decorated = support::feature_flags::enable_optional_feature_flags(yaml);
        let config = IntegrationConfig::from_reader(decorated.as_bytes()).expect("config");
        let registry = Arc::new(ConnectorRegistry::build(&config, ".").expect("registry"));
        ConnectorFactoryRegistry::new(registry)
    }

    #[test]
    fn http_client_handles_are_cached() {
        let (_registry, factory) = build_registry();

        let first = factory
            .http_client("sample_processing_api")
            .expect("http client");
        let second = factory
            .http_client("sample_processing_api")
            .expect("http client");

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(first.base_url(), "https://processor.example.com");
    }

    #[cfg(feature = "kafka")]
    #[test]
    fn kafka_handles_include_brokers() {
        let (_registry, factory) = build_registry();

        let handle = factory
            .kafka_producer("sample_kafka_cluster")
            .expect("kafka handle");

        assert_eq!(handle.brokers(), &["localhost:9092".to_string()]);
        let again = factory
            .kafka_producer("sample_kafka_cluster")
            .expect("kafka handle");
        assert!(Arc::ptr_eq(&handle, &again));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn mariadb_pools_are_cached() {
        let (_registry, factory) = build_registry();

        let pool = factory
            .mariadb_pool("sample_state_store")
            .expect("mariadb pool");
        let again = factory
            .mariadb_pool("sample_state_store")
            .expect("mariadb pool");

        assert!(Arc::ptr_eq(&pool, &again));
        assert_eq!(pool.schema(), Some("records"));
    }

    #[test]
    fn missing_connector_emits_error() {
        let (_registry, factory) = build_registry();
        let err = factory.http_client("missing").expect_err("should fail");
        match err {
            ConnectorFactoryError::MissingConnector { name, expected } => {
                assert_eq!(name, "missing");
                assert_eq!(expected, "http_client");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn rabbitmq_handles_are_cached() {
        let factory = factory_from_yaml(
            r#"
connectors:
  - name: primary_broker
    type: rabbitmq
    options:
      url: amqps://broker.example.com/%2f
      prefetch: 32
      tls:
        ca: tls/ca.pem
  - name: telemetry
    type: mqtt
    options:
      url: mqtts://mqtt.example.com:8883
      client_id: chronicle-worker
      tls:
        ca: tls/ca.pem
chronicles: []
"#,
        );

        let first = factory.rabbitmq("primary_broker").expect("rabbitmq handle");
        let second = factory.rabbitmq("primary_broker").expect("rabbitmq handle");

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(first.url(), "amqps://broker.example.com/%2f");
        assert_eq!(first.prefetch(), Some(32));
        let tls = first.tls().expect("tls config");
        let ca_path = tls.ca().expect("ca path available");
        assert!(ca_path.ends_with("tls/ca.pem"));
    }

    #[test]
    fn mqtt_handles_are_cached() {
        let factory = factory_from_yaml(
            r#"
connectors:
  - name: telemetry
    type: mqtt
    options:
      url: mqtts://mqtt.example.com:8883
      client_id: chronicle-worker
      username: chronicle
      password: secret
      keep_alive: 30
      tls:
        ca: tls/ca.pem
chronicles: []
"#,
        );

        let first = factory.mqtt_client("telemetry").expect("mqtt handle");
        let second = factory.mqtt_client("telemetry").expect("mqtt handle");

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(first.url(), "mqtts://mqtt.example.com:8883");
        assert_eq!(first.client_id(), Some("chronicle-worker"));
        assert_eq!(first.username(), Some("chronicle"));
        assert_eq!(first.keep_alive(), Some(30));
        assert!(first.tls().is_some());
    }

    #[test]
    fn http_client_supports_mutual_tls_configuration() {
        let dir = tempdir().expect("tempdir");
        let ca_path = dir.path().join("ca.pem");
        let cert_path = dir.path().join("client.crt");
        let key_path = dir.path().join("client.key");

        let (ca_pem, client_cert_pem, client_key_pem) = sample_http_client_identity();

        std::fs::write(&ca_path, ca_pem).expect("write ca");
        std::fs::write(&cert_path, client_cert_pem).expect("write cert");
        std::fs::write(&key_path, client_key_pem).expect("write key");

        let yaml = format!(
            r#"
connectors:
  - name: mtls
    type: http_client
    options:
      base_url: https://example.com
      tls:
        ca: "{}"
        cert: "{}"
        key: "{}"
chronicles: []
"#,
            ca_path.display(),
            cert_path.display(),
            key_path.display()
        );

        let factory = factory_from_yaml(&yaml);
        let handle = factory.http_client("mtls").expect("http client with mTLS");
        assert_eq!(handle.base_url(), "https://example.com");
    }

    #[cfg(feature = "kafka")]
    #[test]
    fn kafka_security_sets_tls_and_sasl() {
        let yaml = r#"
connectors:
  - name: secure
    type: kafka
    options:
      brokers: ["broker:9092"]
      security:
        tls:
          ca: /tmp/ca.pem
          cert: /tmp/client.crt
          key: /tmp/client.key
        sasl:
          username: user
          password: secret
          mechanism: PLAIN
chronicles: []
"#;

        let factory = factory_from_yaml(yaml);
        let handle = factory.kafka_producer("secure").expect("kafka handle");
        let security = handle
            .extra()
            .get("security")
            .and_then(|value| value.as_object())
            .expect("security section present");
        assert!(security.contains_key("tls"));
        assert!(security.contains_key("sasl"));
    }

    #[cfg(feature = "kafka")]
    #[test]
    fn kafka_security_requires_cert_and_key_pair() {
        let yaml = r#"
connectors:
  - name: broken
    type: kafka
    options:
      brokers: ["broker:9092"]
      security:
        tls:
          cert: /tmp/client.crt
chronicles: []
"#;

        let factory = factory_from_yaml(yaml);
        let err = factory
            .kafka_producer("broken")
            .expect_err("should fail without key");
        match err {
            ConnectorFactoryError::KafkaProducer { reason, .. } => {
                assert!(reason.contains("tls.cert"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    fn sample_http_client_identity() -> (&'static str, &'static str, &'static str) {
        (SAMPLE_CA_PEM, SAMPLE_CLIENT_CERT, SAMPLE_CLIENT_KEY)
    }

    const SAMPLE_CA_PEM: &str = "-----BEGIN CERTIFICATE-----
MIIDGTCCAgGgAwIBAgIUR+oK6qFmjB80ScnIoSKwgJaxQYgwDQYJKoZIhvcNAQEL
BQAwHDEaMBgGA1UEAwwRQ2hyb25pY2xlIFRlc3QgQ0EwHhcNMjUxMDMwMjExOTAy
WhcNMjYxMDMwMjExOTAyWjAcMRowGAYDVQQDDBFDaHJvbmljbGUgVGVzdCBDQTCC
ASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALZ9kkGzunCdseBjMnC+nRPE
Ti/XsDD4gtK79iFBh+/XW3n/9tz+rW3Qrq+tI/zS9Ep86/Q1laXYwbkqYE9s+Hzo
jXeKWe/asY/uJT+YOrS78a93h5dPRBZMuBZEDeugJ23Zo/dt20VsRc96SblikGOz
a4oU+EQ8Qd+rTkVb+kvp+qa7Noe8JNKkMJEGHyXpH+dpPQ0lmlr8R5AOwM/oa1Aa
2ZNnlm4k50CpnfL0U2Il8AlPn1b1JThIEtwGfASTIM7H65fdyJAZoR7QATnPBbfA
koW+LWeETIhNESpWYkb/SzaqtxhNDWdd1E2VhlSrZZdcIgjgo8cPyYe6KeNa/EMC
AwEAAaNTMFEwHQYDVR0OBBYEFA0A5vodvLSRaTt8vhTAkLh+pOeDMB8GA1UdIwQY
MBaAFA0A5vodvLSRaTt8vhTAkLh+pOeDMA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZI
hvcNAQELBQADggEBAIx9dS5kqKMc3uwHjhlG8VtMcVyLodxWPHTYdSx0a0pVBC/o
cuGsP1dYLHjsjWzA2h46Ri92A2ykbBkTmfISOYZet1Gnsw111hjMyGTnaezmciWM
ne9NgGw7dT4XUPt83pFnT5DzkkHT9rCTCUqaWGFPmCrkOSHLaehRDAhV3gmDtFwP
obUZfxseDHRJZTR+KPuxiihi7RVZhJsLomngY+qjdGXga8fTQzpnU4FItSaWnBGQ
DG7vhMyv/IjPOkVYbwBCGOV/Qx2Ln846a6GUTyrulyNaRtP/SsVvW3HoqFkAmh/O
At1dFtIW4fR4Pg7FpT1nQ4rTqUweYJ68AUSlwjA=
-----END CERTIFICATE-----
";

    const SAMPLE_CLIENT_CERT: &str = "-----BEGIN CERTIFICATE-----
MIIDUzCCAjugAwIBAgIUcNt8COHKUIOxzjOBEdtAVoM88FEwDQYJKoZIhvcNAQEL
BQAwHDEaMBgGA1UEAwwRQ2hyb25pY2xlIFRlc3QgQ0EwHhcNMjUxMDMwMjExOTAz
WhcNMjYxMDMwMjExOTAzWjAbMRkwFwYDVQQDDBBjaHJvbmljbGUtY2xpZW50MIIB
IjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyJhVsxyUKRLIKOFtVge5l70G
qTZk1x4+AgIMsmalTEjx9VaRIsyPsFpqFdJBkqV20M+H3SsccEft1EgiVi0C+Pmz
W0m8SqJ3+NlXbQIc7tBpmcmhhlgSSVCN6VzoLJBob+zbDeICR26q/5pCIGaL/Dk5
VDxCm6jHd+emanVt5fv7/qpwNj1UP8S5nt6h+9/dn7vYcYWSWOv0WpUN7K9StfQ1
Zs2oJ3tc2AkNbOGnZTMNXsxOQnAjA5ECxWCIWZYeyQEJBxFFNhZT3b6J0eS/OInC
FauG1krh6lYIZZbgzSqiV3CQCtG6f/QOMY/qzkblX3JdJfpajLXTX3WCOAWJJwID
AQABo4GNMIGKMBMGA1UdJQQMMAoGCCsGAQUFBwMCMBsGA1UdEQQUMBKCEGNocm9u
aWNsZS1jbGllbnQwCQYDVR0TBAIwADALBgNVHQ8EBAMCB4AwHQYDVR0OBBYEFJNO
TLeDbda9EDYK63JlJ6oz3tCrMB8GA1UdIwQYMBaAFA0A5vodvLSRaTt8vhTAkLh+
pOeDMA0GCSqGSIb3DQEBCwUAA4IBAQAMMcqkZEW0n891zgm821R2+titHIJBE8pN
Nt9oDssGTQ76yKlAp+3FnpQAWa0Bbq7dna59tz93c/ZmS1/YMot35bMCsWV3n9me
jl5pyQBD95flxbBnKa0VnIpIXx8NgVs4fmLKhhgLKP+uXVbG+tFtxNVjetBOsHJA
AnvGhckRFyBGsOpa2PX4Jh5hY9HAKzwOZK65WxGYnTOo/dhSBxUGRC0R7yW1bGlj
t7FLpKLjTI55RSMQdDIZu9nAGpaHuqZSqIY0DDfLc7anJYIgFNkWxLqMGTEonlrh
d4YLSDaKSfNKaf3cqg6EcIL952vJE4K0WChz2w7Aj7TeihcZkupM
-----END CERTIFICATE-----
";

    const SAMPLE_CLIENT_KEY: &str = "-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEAyJhVsxyUKRLIKOFtVge5l70GqTZk1x4+AgIMsmalTEjx9VaR
IsyPsFpqFdJBkqV20M+H3SsccEft1EgiVi0C+PmzW0m8SqJ3+NlXbQIc7tBpmcmh
hlgSSVCN6VzoLJBob+zbDeICR26q/5pCIGaL/Dk5VDxCm6jHd+emanVt5fv7/qpw
Nj1UP8S5nt6h+9/dn7vYcYWSWOv0WpUN7K9StfQ1Zs2oJ3tc2AkNbOGnZTMNXsxO
QnAjA5ECxWCIWZYeyQEJBxFFNhZT3b6J0eS/OInCFauG1krh6lYIZZbgzSqiV3CQ
CtG6f/QOMY/qzkblX3JdJfpajLXTX3WCOAWJJwIDAQABAoIBAAWrVy7atSeK0rd0
zKPYUB00H43FWNFlFwywFDVR6De85HDyFX2uyk9fjkR30OU45kqZYVhVklp9XWdu
WUevwpXY473XDbDbPtnJaN/oZeFoAg0cNoOhvJcamH8byMy6byvdkq+/QoZ/CoSl
+RaJzlnCubzVS9VMeRmr0ARJgljv1pfiuxd93nAcX/Ug9IvheX15H8rlRYJ0Tfx3
vEejahxkgyYp7UMKxSGcGvsQXMuOd89FQK2v70El25nVxLhx2hKtmW/teriFPNqK
KTR0sDY9zGN0X3G4NNLEGZvS2TSu+a/EOfjeY6tge6pfU8qvuf+NguoAcoucmkeJ
/na7clECgYEA6cEZ91GvjvsNlcwYNaHPJ9iAeljy5UeqRxtNV3NjKjYCObnitnDP
8K8gttf1oHvVI8QLl6RrDM4Iq0mD7OWXmj6K/hyBo3Qw++xz+aq5ybxfaGpA6Vbc
H9rt94OLNDITYE8O8fiFMuwqNv0nu1fPZc7pzjvtyB336kdO2jjFnPECgYEA269h
/9ptppaYQQgTr9qc6BqLOMH5faqfkUSNXWU/loUQqQmYtZrk1tkakMqcdtsOzg7o
BxssV59Ye0bpnCDmkV4LMsqJAA8kt5MAV4oLluE14Kml6amgUcM1D2fMf5AYr4b/
7Jlt169Mka5nFazZujONdvllWkKFdigO5uJhZ5cCgYEAvpfQUDOubWqN/SHYa4Jv
ohGJUDjOc9wnHqtIOJHAvV4kGmVSUWdSZPCmP+9+O0g81Vi4CwDouBwWPXNHuhTJ
s95i/ibIHTpT5lU5isyFh9OsBzr7ikZkXSTo+vOqwPhDjDdp/CmikY62LPflOX+z
f1Nil+GNU6n7xm42AQBhQEECgYA/jVINYRQdgC1Vis+fLN/9aUhjSAIz73sv3CQF
I7gshBwYupT53HBdEvtTbbmrzJ1Q0REglSTTyF4hc5c7Om1ZGlqk++B8KuVVUepk
aKQHpukeMBW2LgMaBB3CciW/tWDeznAU8yHKpoTBFDHwHEv0SNug0m8WmG3hi58b
/dttywKBgQC0kE3fVTztUGIgHc3UO9Jwfh3+KMOPfDK4hQ5nLwNryiowW64wTQIS
bXLkOU5Tqc9BcBdMYztQdMb/xTGtGrB4eD0paGEgPZ4RxVtjJ6DUVXj/Nu2kZgzd
ljcEft+vOVBgwUKRYYVWmrBMJLCwc4DgRiV8wvEHgHx/ZrjWT3KxWQ==
-----END RSA PRIVATE KEY-----
";
