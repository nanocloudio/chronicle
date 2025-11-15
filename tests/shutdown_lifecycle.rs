#[path = "common/mod.rs"]
mod common;

use chronicle::integration::factory::ConnectorFactoryRegistry;
use chronicle::readiness::{
    retry_after_hint_seconds, warmup_connectors, ApplicationState, ReadinessController, RouteState,
};
use std::{sync::Arc, time::Duration};

#[tokio::test(flavor = "multi_thread")]
async fn warmup_and_drain_transition_application_state() {
    let (_engine, config, registry) = common::build_engine_handles();
    let factory = ConnectorFactoryRegistry::new(Arc::clone(&registry));

    warmup_connectors(&config.app, &config.connectors, &factory)
        .await
        .expect("warm-up should succeed for fixture connectors");

    let controller = ReadinessController::initialise(&config);
    assert_eq!(controller.route_count(), config.chronicles.len());
    assert_eq!(
        controller.application_state().await,
        ApplicationState::WarmingUp
    );

    for chronicle in &config.chronicles {
        controller
            .set_route_state(&chronicle.name, RouteState::Ready)
            .await
            .expect("transition to ready succeeds");
    }

    assert_eq!(
        controller.application_state().await,
        ApplicationState::Ready
    );

    let cache_ttl = config.app.readiness_cache;
    assert!(
        cache_ttl > Duration::from_millis(0),
        "fixture config should set a non-zero readiness cache"
    );
    let retry_after_hint = retry_after_hint_seconds(cache_ttl);
    assert_eq!(
        retry_after_hint, 1,
        "sub-second cache must clamp Retry-After hints to one second"
    );

    let target_route = config
        .chronicles
        .first()
        .expect("fixture chronicle present")
        .name
        .clone();

    controller
        .set_route_state(&target_route, RouteState::NotReady)
        .await
        .expect("route transitions to NOT_READY");
    let snapshot = controller.cached_snapshot(cache_ttl).await;
    assert_eq!(
        snapshot.application_state,
        ApplicationState::NotReady,
        "fresh snapshot reflects the NOT_READY transition"
    );

    controller
        .set_route_state(&target_route, RouteState::Ready)
        .await
        .expect("route transitions back to READY");
    let cached_snapshot = controller.cached_snapshot(cache_ttl).await;
    assert_eq!(
        cached_snapshot.application_state,
        ApplicationState::NotReady,
        "cached snapshot should remain NOT_READY until the readiness cache TTL elapses"
    );

    tokio::time::sleep(cache_ttl + Duration::from_millis(25)).await;
    let refreshed_snapshot = controller.cached_snapshot(cache_ttl).await;
    assert_eq!(
        refreshed_snapshot.application_state,
        ApplicationState::Ready,
        "after the cache window expires the snapshot reflects the latest READY state"
    );

    // Simulate graceful shutdown by draining routes.
    controller.enter_draining().await;

    assert_eq!(
        controller.application_state().await,
        ApplicationState::Draining
    );
}
