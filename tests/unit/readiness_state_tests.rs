use chronicle::readiness::{ApplicationState, MinReadyRoutes, ReadinessStateMachine, RouteState};

fn machine_two_routes(min_ready_routes: MinReadyRoutes) -> ReadinessStateMachine {
    ReadinessStateMachine::new(
        min_ready_routes,
        vec!["route-a".to_string(), "route-b".to_string()],
    )
}

#[test]
fn transitions_enforce_state_machine() {
    let mut machine = machine_two_routes(MinReadyRoutes::All);
    assert_eq!(
        machine
            .set_route_state("route-a", RouteState::Ready)
            .unwrap(),
        RouteState::Ready
    );
    assert!(
        machine
            .set_route_state("route-a", RouteState::WarmingUp)
            .is_err()
    );
    assert_eq!(
        machine
            .set_route_state("route-a", RouteState::Degraded)
            .unwrap(),
        RouteState::Degraded
    );
    assert_eq!(
        machine
            .set_route_state("route-a", RouteState::Draining)
            .unwrap(),
        RouteState::Draining
    );
    assert!(
        machine
            .set_route_state("route-a", RouteState::Ready)
            .is_err()
    );
}

#[test]
fn application_state_honours_min_ready_routes_all() {
    let mut machine = machine_two_routes(MinReadyRoutes::All);
    assert_eq!(machine.application_state(), ApplicationState::WarmingUp);

    machine
        .set_route_state("route-a", RouteState::Ready)
        .unwrap();
    assert_eq!(machine.application_state(), ApplicationState::NotReady);

    machine
        .set_route_state("route-b", RouteState::Ready)
        .unwrap();
    assert_eq!(machine.application_state(), ApplicationState::Ready);

    machine
        .set_route_state("route-b", RouteState::Degraded)
        .unwrap();
    assert_eq!(machine.application_state(), ApplicationState::Degraded);
}

#[test]
fn application_state_honours_min_ready_routes_count() {
    let mut machine = machine_two_routes(MinReadyRoutes::Count(1));
    assert_eq!(machine.application_state(), ApplicationState::WarmingUp);

    machine
        .set_route_state("route-a", RouteState::Ready)
        .unwrap();
    assert_eq!(machine.application_state(), ApplicationState::Ready);

    machine
        .set_route_state("route-b", RouteState::NotReady)
        .unwrap();
    assert_eq!(machine.application_state(), ApplicationState::Ready);

    machine
        .set_route_state("route-a", RouteState::NotReady)
        .unwrap();
    assert_eq!(machine.application_state(), ApplicationState::NotReady);
}
