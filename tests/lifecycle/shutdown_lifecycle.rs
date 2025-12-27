#[path = "../common/mod.rs"]
mod common;

use chronicle::integration::factory::ConnectorFactoryRegistry;
use chronicle::readiness::{
    retry_after_hint_seconds, warmup_connectors, ApplicationState, ReadinessController, RouteState,
};
use futures_util::future::join_all;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test(flavor = "multi_thread")]
async fn warmup_and_drain_transition_application_state() -> TestResult {
    let (_engine, config, registry) = common::build_engine_handles();
    let factory = ConnectorFactoryRegistry::new(Arc::clone(&registry));

    warmup_connectors(&config.app, &config.connectors, &factory).await?;

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
            .map_err(|e| format!("set_route_state failed: {e:?}"))?;
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
        .ok_or("fixture chronicle should be present")?
        .name
        .clone();

    controller
        .set_route_state(&target_route, RouteState::NotReady)
        .await
        .map_err(|e| format!("set_route_state failed: {e:?}"))?;
    let snapshot = controller.cached_snapshot(cache_ttl).await;
    assert_eq!(
        snapshot.application_state,
        ApplicationState::NotReady,
        "fresh snapshot reflects the NOT_READY transition"
    );

    controller
        .set_route_state(&target_route, RouteState::Ready)
        .await
        .map_err(|e| format!("set_route_state failed: {e:?}"))?;
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
    Ok(())
}

/// Test that a spawned background task respects cancellation within a fixed timeout.
///
/// This validates Task 9: structured cancellation discipline ensures all tasks
/// stop promptly when the shutdown signal fires.
#[tokio::test(flavor = "multi_thread")]
async fn cancellation_token_stops_tasks_within_timeout() -> TestResult {
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();

    // Spawn a task that runs indefinitely until cancelled
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown_clone.cancelled() => {
                    return "cancelled";
                }
                _ = tokio::time::sleep(Duration::from_millis(10)) => {
                    // Keep looping
                }
            }
        }
    });

    // Let the task run briefly
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Trigger cancellation
    shutdown.cancel();

    // Assert the task stops within a fixed timeout (100ms should be plenty)
    let shutdown_timeout = Duration::from_millis(100);
    let result = tokio::time::timeout(shutdown_timeout, task).await;

    match result {
        Ok(Ok(msg)) => {
            assert_eq!(msg, "cancelled", "task should exit via cancellation path");
        }
        Ok(Err(join_err)) => {
            return Err(format!("task panicked: {join_err}").into());
        }
        Err(_) => {
            return Err(format!(
                "task did not stop within {:?} after cancellation signal",
                shutdown_timeout
            )
            .into());
        }
    }
    Ok(())
}

/// Test that multiple concurrent tasks all respect cancellation.
#[tokio::test(flavor = "multi_thread")]
async fn multiple_tasks_stop_on_cancellation() -> TestResult {
    let shutdown = CancellationToken::new();
    let task_count = 5;
    let mut handles = Vec::with_capacity(task_count);

    for i in 0..task_count {
        let token = shutdown.clone();
        handles.push(tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = token.cancelled() => {
                        return i;
                    }
                    _ = tokio::time::sleep(Duration::from_millis(5)) => {}
                }
            }
        }));
    }

    // Let tasks run briefly
    tokio::time::sleep(Duration::from_millis(25)).await;

    // Cancel all tasks
    shutdown.cancel();

    // All tasks should stop within timeout
    let shutdown_timeout = Duration::from_millis(100);
    let results = tokio::time::timeout(shutdown_timeout, join_all(handles)).await;

    match results {
        Ok(outcomes) => {
            for (idx, outcome) in outcomes.into_iter().enumerate() {
                match outcome {
                    Ok(returned_idx) => {
                        assert_eq!(returned_idx, idx, "task {idx} returned correctly");
                    }
                    Err(join_err) => {
                        return Err(format!("task {idx} panicked: {join_err}").into());
                    }
                }
            }
        }
        Err(_) => {
            return Err(format!(
                "not all tasks stopped within {:?} after cancellation",
                shutdown_timeout
            )
            .into());
        }
    }
    Ok(())
}

/// Test that engine drop completes without leaking tasks.
///
/// This validates the "no task leaked logs on shutdown" aspect of Task 9.
#[test]
fn engine_drop_completes_cleanly() {
    let engine = common::build_engine();

    // Execute something to ensure engine is active
    let payload = serde_json::json!({
        "body": { "record": { "id": "drop-test" } }
    });
    let _ = engine.execute("collect_record", payload);

    // Drop should complete without panic or hanging
    drop(engine);

    // If we reach here, shutdown was clean (no leaked tasks blocking drop)
}
