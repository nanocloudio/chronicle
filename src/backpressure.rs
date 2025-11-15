use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Aggregates the configured concurrency gates for subsystems (HTTP entrypoints, Kafka consumers).
#[derive(Clone, Default)]
pub struct BackpressureManager {
    pub http: BackpressureController,
    pub kafka: BackpressureController,
}

impl BackpressureManager {
    pub fn new(
        config: &crate::config::BackpressureConfig,
        app_config: Option<&crate::config::integration::AppConfig>,
    ) -> Self {
        let http_limit = app_config
            .and_then(|app| {
                app.limits
                    .http
                    .as_ref()
                    .and_then(|http| http.max_concurrency)
            })
            .map(|value| value as usize)
            .or(config.http_max_concurrency);

        let kafka_limit = app_config
            .and_then(|app| {
                app.limits
                    .kafka
                    .as_ref()
                    .and_then(|kafka| kafka.max_inflight_per_partition)
            })
            .map(|value| value as usize)
            .or(config.kafka_max_inflight);

        Self {
            http: BackpressureController::new(http_limit),
            kafka: BackpressureController::new(kafka_limit),
        }
    }
}

#[derive(Clone, Default)]
pub struct BackpressureController {
    semaphore: Option<Arc<Semaphore>>,
    metrics: Arc<ControllerMetrics>,
}

impl BackpressureController {
    pub fn new(limit: Option<usize>) -> Self {
        if let Some(limit) = limit {
            Self {
                semaphore: Some(Arc::new(Semaphore::new(limit))),
                metrics: Arc::new(ControllerMetrics {
                    limit: Some(limit),
                    throttled: AtomicU64::new(0),
                    inflight: AtomicU64::new(0),
                }),
            }
        } else {
            Self::default()
        }
    }

    pub async fn acquire(&self) -> BackpressurePermit {
        if let Some(semaphore) = &self.semaphore {
            match semaphore.clone().try_acquire_owned() {
                Ok(permit) => {
                    self.metrics.inflight.fetch_add(1, Ordering::Relaxed);
                    BackpressurePermit {
                        inner: Some(permit),
                        metrics: Arc::clone(&self.metrics),
                    }
                }
                Err(_) => {
                    self.metrics.throttled.fetch_add(1, Ordering::Relaxed);
                    let permit = semaphore
                        .clone()
                        .acquire_owned()
                        .await
                        .expect("backpressure semaphore closed");
                    self.metrics.inflight.fetch_add(1, Ordering::Relaxed);
                    BackpressurePermit {
                        inner: Some(permit),
                        metrics: Arc::clone(&self.metrics),
                    }
                }
            }
        } else {
            BackpressurePermit {
                inner: None,
                metrics: Arc::clone(&self.metrics),
            }
        }
    }

    pub fn try_acquire_now(&self) -> Option<BackpressurePermit> {
        if let Some(semaphore) = &self.semaphore {
            match semaphore.clone().try_acquire_owned() {
                Ok(permit) => {
                    self.metrics.inflight.fetch_add(1, Ordering::Relaxed);
                    Some(BackpressurePermit {
                        inner: Some(permit),
                        metrics: Arc::clone(&self.metrics),
                    })
                }
                Err(_) => {
                    self.metrics.throttled.fetch_add(1, Ordering::Relaxed);
                    None
                }
            }
        } else {
            Some(BackpressurePermit {
                inner: None,
                metrics: Arc::clone(&self.metrics),
            })
        }
    }

    pub fn snapshot(&self) -> ControllerSnapshot {
        ControllerSnapshot {
            limit: self.metrics.limit,
            inflight: self.metrics.inflight.load(Ordering::Relaxed),
            throttled: self.metrics.throttled.load(Ordering::Relaxed),
        }
    }
}

pub struct BackpressurePermit {
    inner: Option<OwnedSemaphorePermit>,
    metrics: Arc<ControllerMetrics>,
}

impl Drop for BackpressurePermit {
    fn drop(&mut self) {
        if self.inner.is_some() {
            self.metrics.inflight.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

#[derive(Default)]
struct ControllerMetrics {
    limit: Option<usize>,
    throttled: AtomicU64,
    inflight: AtomicU64,
}

#[derive(Debug, Clone, Copy)]
pub struct ControllerSnapshot {
    pub limit: Option<usize>,
    pub inflight: u64,
    pub throttled: u64,
}

impl ControllerSnapshot {
    pub fn paused(&self) -> bool {
        if let Some(limit) = self.limit {
            self.inflight >= limit as u64
        } else {
            false
        }
    }
}
