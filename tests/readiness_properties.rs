use chronicle::config::IntegrationConfig;
use chronicle::readiness::controller::ReadinessController;
use chronicle::readiness::state::{ApplicationState, RouteState};
use proptest::prelude::*;
use std::time::Duration;

const ROUTE_YAML: &str = r#"
api_version: v1
app:
  min_ready_routes: all
connectors:
  - name: http_ingest
    type: http
    options:
      role: server
      host: 0.0.0.0
      port: 8080
chronicles:
  - name: route_a
    trigger:
      connector: http_ingest
      options:
        method: GET
        path: /ready
    phases: []
"#;

const MULTI_ROUTE_YAML: &str = r#"
api_version: v1
app:
  min_ready_routes: 2
connectors:
  - name: ingress
    type: http
    options:
      role: server
      host: 0.0.0.0
      port: 8080
  - name: upstream
    type: http
    options:
      role: client
      base_url: https://upstream.example.com
      tls:
        ca: tls/upstream-ca.pem
  - name: audit
    type: http
    options:
      role: client
      base_url: https://audit.example.com
      tls:
        ca: tls/audit-ca.pem
chronicles:
  - name: alpha
    trigger:
      connector: ingress
      options:
        method: GET
        path: /alpha
    phases:
      - name: forward_alpha
        type: http_client
        options:
          connector: upstream
          method: POST
          path: /alpha
    policy:
      readiness_priority: 10
  - name: beta
    trigger:
      connector: ingress
      options:
        method: GET
        path: /beta
    phases:
      - name: audit_beta
        type: http_client
        options:
          connector: audit
          method: POST
          path: /audit
    policy:
      readiness_priority: 5
      allow_partial_delivery: true
  - name: gamma
    trigger:
      connector: ingress
      options:
        method: GET
        path: /gamma
    phases:
      - name: forward_gamma
        type: http_client
        options:
          connector: upstream
          method: POST
          path: /gamma
"#;

const PRIORITY_ROUTE_YAML: &str = r#"
api_version: v1
app:
  min_ready_routes: 1
connectors:
  - name: ingress
    type: http
    options:
      role: server
      host: 0.0.0.0
      port: 8080
  - name: upstream
    type: http
    options:
      role: client
      base_url: https://upstream.example.com
      tls:
        ca: tls/upstream-ca.pem
chronicles:
  - name: alpha
    trigger:
      connector: ingress
      options:
        method: GET
        path: /alpha
    phases: []
    policy:
      readiness_priority: 10
  - name: beta
    trigger:
      connector: ingress
      options:
        method: GET
        path: /beta
    phases: []
    policy:
      readiness_priority: 1
"#;

proptest! {
    #[test]
    fn cached_snapshot_tracks_route_transitions(ttl_ms in 0u64..50, sequence in route_state_sequence()) {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        runtime.block_on(async {
            let controller = build_controller();
            let ttl = Duration::from_millis(ttl_ms);

            for state in sequence {
                let applied = controller
                    .set_route_state("route_a", state)
                    .await
                    .expect("valid transition");
                assert_eq!(applied, state);

                if !ttl.is_zero() {
                    tokio::time::sleep(ttl + Duration::from_millis(1)).await;
                }

                let snapshot = controller.cached_snapshot(ttl).await;
                let application_state = controller.application_state().await;

                let route_state = snapshot
                    .routes
                    .iter()
                    .find(|route| route.name == "route_a")
                    .expect("route snapshot")
                    .state;

                assert_eq!(route_state, state);
                assert_eq!(snapshot.application_state, application_state);
                if matches!(application_state, ApplicationState::Draining) {
                    assert!(
                        matches!(state, RouteState::Draining),
                        "application state cannot drain before the route does"
                    );
                }
            }
        });
    }
}

#[test]
fn multi_route_readiness_and_degraded_states() {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    runtime.block_on(async {
        let controller = build_multi_route_controller();
        // Inspect priorities surfaced in snapshots.
        let snapshot = controller.routes_snapshot().await;
        let alpha_policy = snapshot
            .iter()
            .find(|route| route.name == "alpha")
            .expect("alpha route present")
            .policy
            .clone();
        assert_eq!(alpha_policy.readiness_priority, Some(10));
        let beta_policy = snapshot
            .iter()
            .find(|route| route.name == "beta")
            .expect("beta route present")
            .policy
            .clone();
        assert_eq!(beta_policy.readiness_priority, Some(5));

        // Initial state should be warming up.
        assert_eq!(
            controller.application_state().await,
            ApplicationState::WarmingUp
        );

        controller
            .set_route_state("alpha", RouteState::Ready)
            .await
            .expect("alpha transitions to READY");
        controller
            .set_route_state("beta", RouteState::Ready)
            .await
            .expect("beta transitions to READY");
        assert_eq!(
            controller.application_state().await,
            ApplicationState::NotReady,
            "gamma still warming up should block readiness even when other routes are ready"
        );

        controller
            .set_route_state("gamma", RouteState::Ready)
            .await
            .expect("gamma transitions to READY");
        assert_eq!(
            controller.application_state().await,
            ApplicationState::Ready
        );

        controller
            .set_route_state("beta", RouteState::Degraded)
            .await
            .expect("beta transitions to DEGRADED");
        assert_eq!(
            controller.application_state().await,
            ApplicationState::Degraded,
            "presence of a degraded route should downgrade the overall application state"
        );

        controller
            .set_route_state("alpha", RouteState::NotReady)
            .await
            .expect("alpha transitions to NOT_READY");
        assert_eq!(
            controller.application_state().await,
            ApplicationState::Degraded,
            "priority routes that are degraded should continue to report DEGRADED even if lower-priority routes fail"
        );

        controller
            .set_route_state("beta", RouteState::Ready)
            .await
            .expect("beta returns to READY");
        assert_eq!(
            controller.application_state().await,
            ApplicationState::Ready
        );
    });
}

#[test]
fn readiness_priority_respected_for_numeric_thresholds() {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    runtime.block_on(async {
        let config = IntegrationConfig::from_reader(PRIORITY_ROUTE_YAML.as_bytes())
            .expect("priority config loads");
        let controller = ReadinessController::initialise(&config);

        controller
            .set_route_state("alpha", RouteState::Ready)
            .await
            .expect("alpha transitions to READY");
        controller
            .set_route_state("beta", RouteState::NotReady)
            .await
            .expect("beta transitions to NOT_READY");

        assert_eq!(
            controller.application_state().await,
            ApplicationState::NotReady,
            "highest priority route must be READY before numeric thresholds are satisfied"
        );

        controller
            .set_route_state("beta", RouteState::Ready)
            .await
            .expect("beta transitions to READY");
        assert_eq!(
            controller.application_state().await,
            ApplicationState::Ready,
            "once the top priority route is READY the threshold should be satisfied"
        );

        controller
            .set_route_state("alpha", RouteState::NotReady)
            .await
            .expect("alpha transitions to NOT_READY");
        assert_eq!(
            controller.application_state().await,
            ApplicationState::Ready,
            "lower priority routes may become NOT_READY without tripping the global threshold"
        );
    });
}

fn build_controller() -> ReadinessController {
    let config =
        IntegrationConfig::from_reader(ROUTE_YAML.as_bytes()).expect("fixture config loads");
    ReadinessController::initialise(&config)
}

fn build_multi_route_controller() -> ReadinessController {
    let config = IntegrationConfig::from_reader(MULTI_ROUTE_YAML.as_bytes())
        .expect("multi-route config loads");
    ReadinessController::initialise(&config)
}

fn route_state_sequence() -> impl Strategy<Value = Vec<RouteState>> {
    prop::collection::vec(any::<u8>(), 1..16).prop_map(|choices| {
        let mut current = RouteState::WarmingUp;
        let mut sequence = Vec::with_capacity(choices.len());
        for choice in choices {
            let options = allowed_next(current);
            let idx = (choice as usize) % options.len();
            current = options[idx];
            sequence.push(current);
        }
        sequence
    })
}

fn allowed_next(state: RouteState) -> &'static [RouteState] {
    use RouteState::*;
    match state {
        WarmingUp => WARMING_UP_NEXT,
        Ready => READY_NEXT,
        NotReady => NOT_READY_NEXT,
        Degraded => DEGRADED_NEXT,
        Draining => DRAINING_NEXT,
    }
}

const WARMING_UP_NEXT: &[RouteState] = &[
    RouteState::WarmingUp,
    RouteState::Ready,
    RouteState::NotReady,
];
const READY_NEXT: &[RouteState] = &[
    RouteState::Ready,
    RouteState::NotReady,
    RouteState::Degraded,
    RouteState::Draining,
];
const NOT_READY_NEXT: &[RouteState] = &[
    RouteState::NotReady,
    RouteState::Ready,
    RouteState::Degraded,
    RouteState::Draining,
];
const DEGRADED_NEXT: &[RouteState] = &[
    RouteState::Degraded,
    RouteState::Ready,
    RouteState::NotReady,
    RouteState::Draining,
];
const DRAINING_NEXT: &[RouteState] = &[RouteState::Draining];
