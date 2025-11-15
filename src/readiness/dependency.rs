use crate::config::integration::CircuitBreakerConfig;
use crate::readiness::controller::ReadinessController;
use crate::readiness::graph::canonical_endpoint_name;
use crate::readiness::state::EndpointState;
use crate::retry::jitter_between;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

const MAINTENANCE_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Clone)]
pub struct DependencyHealth {
    inner: Arc<DependencyHealthInner>,
}

struct DependencyHealthInner {
    readiness: ReadinessController,
    dependencies: BTreeMap<String, Vec<String>>,
    breakers: RwLock<HashMap<String, EndpointBreaker>>,
    configs: HashMap<String, CircuitBreakerConfig>,
}

impl DependencyHealth {
    pub fn new(
        readiness: ReadinessController,
        dependencies: BTreeMap<String, Vec<String>>,
        configs: HashMap<String, CircuitBreakerConfig>,
    ) -> Self {
        let inner = DependencyHealthInner {
            readiness,
            dependencies,
            breakers: RwLock::new(HashMap::new()),
            configs,
        };

        Self {
            inner: Arc::new(inner),
        }
    }

    pub async fn record_success(&self, connector: &str) {
        let endpoint = canonical_endpoint_name(connector);
        let update = {
            let mut guard = self.inner.breakers.write().await;
            let breaker = guard.entry(endpoint.clone()).or_insert_with(|| {
                EndpointBreaker::new(self.inner.configs.get(&endpoint).cloned())
            });
            breaker.record_success(Instant::now())
        };

        if let Some(state) = update {
            self.inner.apply_state(&endpoint, state).await;
        }
    }

    pub async fn record_failure(&self, connector: &str) {
        let endpoint = canonical_endpoint_name(connector);
        let update = {
            let mut guard = self.inner.breakers.write().await;
            let breaker = guard.entry(endpoint.clone()).or_insert_with(|| {
                EndpointBreaker::new(self.inner.configs.get(&endpoint).cloned())
            });
            breaker.record_failure(Instant::now())
        };

        if let Some(state) = update {
            self.inner.apply_state(&endpoint, state).await;
        }
    }

    pub async fn report_outcome(&self, connector: &str, success: bool) {
        if success {
            self.record_success(connector).await;
        } else {
            self.record_failure(connector).await;
        }
    }

    pub fn spawn_maintenance(&self, shutdown: CancellationToken) {
        let inner = self.inner.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = sleep(MAINTENANCE_INTERVAL) => {
                        inner.tick().await;
                    }
                }
            }
        });
    }
}

impl DependencyHealthInner {
    async fn tick(&self) {
        let mut pending = Vec::new();
        {
            let now = Instant::now();
            let mut guard = self.breakers.write().await;
            for (endpoint, breaker) in guard.iter_mut() {
                if let Some(state) = breaker.poll_transition(now) {
                    pending.push((endpoint.clone(), state));
                }
            }
        }

        for (endpoint, state) in pending {
            self.apply_state(&endpoint, state).await;
        }
    }

    async fn apply_state(&self, endpoint: &str, state: EndpointState) {
        let Some(routes) = self.dependencies.get(endpoint) else {
            return;
        };

        for route in routes {
            if let Err(err) = self
                .readiness
                .set_dependency_state(route, endpoint, state)
                .await
            {
                tracing::warn!(
                    route = route,
                    endpoint = endpoint,
                    state = state.as_str(),
                    error = ?err,
                    "failed to apply dependency state transition"
                );
            }
        }
    }
}

struct EndpointBreaker {
    config: Option<CircuitBreakerConfig>,
    state: EndpointState,
    open_deadline: Option<Instant>,
    backoff: Option<Duration>,
    events: VecDeque<Event>,
}

impl EndpointBreaker {
    fn new(config: Option<CircuitBreakerConfig>) -> Self {
        Self {
            config,
            state: EndpointState::Healthy,
            open_deadline: None,
            backoff: None,
            events: VecDeque::new(),
        }
    }

    fn record_success(&mut self, now: Instant) -> Option<EndpointState> {
        self.push_event(now, true);

        let new_state = match self.config.clone() {
            None => EndpointState::Healthy,
            Some(config) => match self.state {
                EndpointState::CircuitOpen => EndpointState::CircuitOpen,
                EndpointState::CircuitHalfOpen => {
                    self.reset_backoff(&config);
                    EndpointState::Healthy
                }
                _ => EndpointState::Healthy,
            },
        };

        self.set_state(new_state)
    }

    fn record_failure(&mut self, now: Instant) -> Option<EndpointState> {
        self.push_event(now, false);

        let new_state = match self.config.clone() {
            None => EndpointState::Unhealthy,
            Some(config) => match self.state {
                EndpointState::CircuitOpen => EndpointState::CircuitOpen,
                EndpointState::CircuitHalfOpen => self.trip(now, &config),
                _ => {
                    if self.should_trip(now, &config) {
                        self.trip(now, &config)
                    } else {
                        EndpointState::Unhealthy
                    }
                }
            },
        };

        self.set_state(new_state)
    }

    fn poll_transition(&mut self, now: Instant) -> Option<EndpointState> {
        if !matches!(self.state, EndpointState::CircuitOpen) {
            return None;
        }

        let deadline = self.open_deadline?;

        if now >= deadline {
            self.open_deadline = None;
            return self.set_state(EndpointState::CircuitHalfOpen);
        }

        None
    }

    fn reset_backoff(&mut self, config: &CircuitBreakerConfig) {
        self.backoff = Some(config.open_base);
        self.open_deadline = None;
    }

    fn should_trip(&mut self, now: Instant, config: &CircuitBreakerConfig) -> bool {
        self.prune_events(now, config.window);
        if self.events.is_empty() {
            return false;
        }

        let failures = self.events.iter().filter(|event| !event.success).count();
        let ratio = failures as f64 / self.events.len() as f64;
        ratio >= config.failure_rate
    }

    fn trip(&mut self, now: Instant, config: &CircuitBreakerConfig) -> EndpointState {
        let delay = self.next_backoff(config);
        self.open_deadline = Some(now + delay);
        EndpointState::CircuitOpen
    }

    fn next_backoff(&mut self, config: &CircuitBreakerConfig) -> Duration {
        let current = self
            .backoff
            .unwrap_or_else(|| std::cmp::max(config.open_base, Duration::from_millis(1)));
        let jittered = if current <= config.open_base {
            config.open_base
        } else {
            jitter_between(config.open_base, current)
        };

        let next = current.mul_f64(2.0);
        self.backoff = Some(std::cmp::min(next, config.open_max));
        jittered
    }

    fn set_state(&mut self, next: EndpointState) -> Option<EndpointState> {
        if self.state == next {
            return None;
        }

        if matches!(next, EndpointState::Healthy) {
            self.events.clear();
        }

        self.state = next;
        Some(next)
    }

    fn push_event(&mut self, now: Instant, success: bool) {
        self.events.push_back(Event { at: now, success });
        if let Some(ref config) = self.config {
            self.prune_events(now, config.window);
        } else {
            while self.events.len() > 32 {
                self.events.pop_front();
            }
        }
    }

    fn prune_events(&mut self, now: Instant, window: Duration) {
        while let Some(front) = self.events.front() {
            if now.duration_since(front.at) > window {
                self.events.pop_front();
            } else {
                break;
            }
        }
    }
}

struct Event {
    at: Instant,
    success: bool,
}
