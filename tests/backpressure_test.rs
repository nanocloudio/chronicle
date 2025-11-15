use chronicle::backpressure::BackpressureController;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread")]
async fn tracks_throttled_and_inflight() {
    let controller = BackpressureController::new(Some(1));

    let permit_one = controller.acquire().await;
    let snapshot = controller.snapshot();
    assert_eq!(snapshot.limit, Some(1));
    assert_eq!(snapshot.inflight, 1);
    assert_eq!(snapshot.throttled, 0);

    let controller_clone = controller.clone();
    let waiter = tokio::spawn(async move { controller_clone.acquire().await });

    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    let snapshot = controller.snapshot();
    assert_eq!(snapshot.inflight, 1, "second permit waits for first");
    assert_eq!(snapshot.throttled, 1, "second acquire increments throttled");

    drop(permit_one);
    let permit_two = waiter.await.expect("waiter task");
    drop(permit_two);

    let snapshot = controller.snapshot();
    assert_eq!(snapshot.inflight, 0);
    assert_eq!(snapshot.throttled, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn no_limit_means_no_throttling() {
    let controller = BackpressureController::new(None);

    let _permit = controller.acquire().await;
    let snapshot = controller.snapshot();
    assert_eq!(snapshot.limit, None);
    assert_eq!(snapshot.inflight, 0);
    assert_eq!(snapshot.throttled, 0);
}
