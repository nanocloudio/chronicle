use crate::config::integration::{
    AppConfig, ConnectorKind, HalfOpenPolicy, IntegrationConfig, MinReadyRoutes, RetryBudget,
};
use crate::metrics::metrics;
use crate::readiness::{
    ApplicationState, DependencyEdge, DependencySource, EndpointState, ReadinessStateMachine,
    RouteGraph, RouteNode, RoutePolicyInfo, RouteSeed, RouteState, TransitionError,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

#[derive(Clone)]
struct RouteDescriptor {
    node: RouteNode,
}

impl RouteDescriptor {
    fn new(node: RouteNode) -> Self {
        Self { node }
    }

    fn snapshot(&self, state: RouteState) -> RouteSnapshot {
        RouteSnapshot {
            name: self.node.name.clone(),
            state,
            trigger: self.node.trigger.clone(),
            dependencies: self.node.dependencies.clone(),
            inferred_dependencies: self.node.inferred_dependencies.clone(),
            policy: self.node.policy.clone(),
        }
    }
}

#[derive(Clone)]
struct RouteHealth {
    dependency_states: BTreeMap<String, EndpointState>,
    retry_budget: RetryBudgetTracker,
    half_open_counts_as_ready: HalfOpenPolicy,
    allow_partial_delivery: bool,
}

impl RouteHealth {
    fn new(node: &RouteNode, app: &AppConfig) -> Self {
        let dependency_states = node
            .dependencies
            .iter()
            .map(|edge| (edge.endpoint.name.clone(), EndpointState::WarmingUp))
            .collect::<BTreeMap<_, _>>();

        let half_open = node
            .policy
            .half_open_counts_as_ready
            .unwrap_or(app.half_open_counts_as_ready);

        let effective_budget = node
            .policy
            .retry_budget
            .as_ref()
            .or(app.retry_budget.as_ref());
        let retry_budget = RetryBudgetTracker::new(None, effective_budget);

        Self {
            dependency_states,
            retry_budget,
            half_open_counts_as_ready: half_open,
            allow_partial_delivery: node.policy.allow_partial_delivery,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RouteSnapshot {
    pub name: String,
    pub state: RouteState,
    pub trigger: crate::readiness::ConnectorEndpoint,
    pub dependencies: Vec<DependencyEdge>,
    pub inferred_dependencies: Vec<String>,
    pub policy: RoutePolicyInfo,
}

#[derive(Clone)]
pub struct ReadinessController {
    machine: Arc<RwLock<ReadinessStateMachine>>,
    routes: Arc<BTreeMap<String, RouteDescriptor>>,
    health: Arc<RwLock<BTreeMap<String, RouteHealth>>>,
    cache: Arc<RwLock<Option<CachedSnapshot>>>,
    _min_ready_routes: MinReadyRoutes,
    default_half_open: HalfOpenPolicy,
}

impl ReadinessController {
    pub fn initialise(config: &IntegrationConfig) -> Self {
        let graph = RouteGraph::build(config);
        Self::from_graph(graph, &config.app)
    }

    pub fn from_graph(graph: RouteGraph, app: &AppConfig) -> Self {
        let mut route_map = BTreeMap::new();
        let mut health_map = BTreeMap::new();
        let mut machine_routes = Vec::new();

        for (ordinal, node) in graph.routes.into_iter().enumerate() {
            let route_name = node.name.clone();
            let readiness_priority = node.policy.readiness_priority.unwrap_or(ordinal as i32);
            machine_routes.push(RouteSeed {
                name: route_name.clone(),
                readiness_priority,
                ordinal,
            });
            health_map.insert(route_name.clone(), RouteHealth::new(&node, app));
            route_map.insert(route_name, RouteDescriptor::new(node));
        }

        let machine = ReadinessStateMachine::new(app.min_ready_routes.clone(), machine_routes);

        Self {
            machine: Arc::new(RwLock::new(machine)),
            routes: Arc::new(route_map),
            health: Arc::new(RwLock::new(health_map)),
            cache: Arc::new(RwLock::new(None)),
            _min_ready_routes: app.min_ready_routes.clone(),
            default_half_open: app.half_open_counts_as_ready,
        }
    }

    pub fn route_count(&self) -> usize {
        self.routes.len()
    }

    pub async fn route_state(&self, route: &str) -> Option<RouteState> {
        let guard = self.machine.read().await;
        guard.route_state(route)
    }

    pub async fn enter_draining(&self) {
        let mut guard = self.machine.write().await;
        let transitions = guard.enter_draining();
        drop(guard);

        for (route, from) in transitions {
            tracing::info!(
                route = route.as_str(),
                state_from = from.as_str(),
                state_to = RouteState::Draining.as_str(),
                reason = "application_shutdown",
                "route state transition"
            );
        }
    }

    pub async fn application_state(&self) -> ApplicationState {
        let guard = self.machine.read().await;
        guard.application_state()
    }

    pub async fn set_route_state(
        &self,
        route: &str,
        state: RouteState,
    ) -> Result<RouteState, TransitionError> {
        let mut guard = self.machine.write().await;
        let previous = guard.route_state(route);
        let result = guard.set_route_state(route, state);
        let transition = match (previous, result.as_ref()) {
            (Some(from), Ok(&to)) if from != to => Some((from, to)),
            _ => None,
        };
        drop(guard);

        if let Some((from, to)) = transition {
            tracing::info!(
                route = route,
                state_from = from.as_str(),
                state_to = to.as_str(),
                reason = "external_set_route_state",
                "route state transition"
            );
        }

        result
    }

    pub async fn routes_snapshot(&self) -> Vec<RouteSnapshot> {
        let states = {
            let guard = self.machine.read().await;
            guard.route_states()
        };

        let mut snapshots = Vec::with_capacity(states.len());
        for (name, state) in states {
            if let Some(descriptor) = self.routes.get(&name) {
                snapshots.push(descriptor.snapshot(state));
            }
        }
        snapshots.sort_by(|a, b| a.name.cmp(&b.name));
        snapshots
    }

    pub async fn set_dependency_state(
        &self,
        route: &str,
        dependency: &str,
        state: EndpointState,
    ) -> Result<RouteState, TransitionError> {
        let mut health_guard = self.health.write().await;
        let route_health =
            health_guard
                .get_mut(route)
                .ok_or_else(|| TransitionError::RouteUnknown {
                    route: route.to_string(),
                })?;

        let previous = route_health
            .dependency_states
            .insert(dependency.to_string(), state);
        let endpoint_transition = if previous != Some(state) {
            Some((previous, state))
        } else {
            None
        };
        let new_state = evaluate_route_state(route_health);
        drop(health_guard);

        let mut machine_guard = self.machine.write().await;
        let previous_route_state = machine_guard.route_state(route);
        let result = machine_guard.set_route_state(route, new_state);
        let route_transition = match (previous_route_state, result.as_ref()) {
            (Some(from), Ok(&to)) if from != to => Some((from, to)),
            _ => None,
        };
        drop(machine_guard);
        if let Some((previous, current)) = endpoint_transition {
            let from_label = previous.map(EndpointState::as_str).unwrap_or("UNKNOWN");
            tracing::info!(
                route = route,
                endpoint = dependency,
                state_from = from_label,
                state_to = current.as_str(),
                reason = "dependency_update",
                "endpoint state transition"
            );
        }
        if let Some((from, to)) = route_transition {
            tracing::info!(
                route = route,
                endpoint = dependency,
                state_from = from.as_str(),
                state_to = to.as_str(),
                reason = "dependency_update",
                "route state transition"
            );
        }
        result
    }

    pub async fn record_retry_failure(&self, route: &str) -> Result<bool, TransitionError> {
        let mut health_guard = self.health.write().await;
        let route_health =
            health_guard
                .get_mut(route)
                .ok_or_else(|| TransitionError::RouteUnknown {
                    route: route.to_string(),
                })?;

        let allowed = route_health.retry_budget.record_failure();
        drop(health_guard);

        if allowed {
            Ok(true)
        } else {
            metrics().record_retry_budget_exhausted(route, None);
            let mut machine_guard = self.machine.write().await;
            machine_guard.set_route_state(route, RouteState::NotReady)?;
            Ok(false)
        }
    }

    pub async fn reset_retry_budget(&self, route: &str) -> Result<(), TransitionError> {
        let mut health_guard = self.health.write().await;
        let route_health =
            health_guard
                .get_mut(route)
                .ok_or_else(|| TransitionError::RouteUnknown {
                    route: route.to_string(),
                })?;
        route_health.retry_budget.reset();
        drop(health_guard);
        Ok(())
    }

    async fn build_snapshot(&self) -> ReadinessSnapshot {
        let app_state = self.application_state().await;
        let route_states = {
            let guard = self.machine.read().await;
            guard.route_states()
        };

        let health_guard = self.health.read().await;
        let mut routes = Vec::with_capacity(route_states.len());
        let mut endpoint_map: BTreeMap<String, EndpointAggregate> = BTreeMap::new();

        for (name, state) in route_states {
            if let Some(descriptor) = self.routes.get(&name) {
                let mut dependencies = Vec::new();
                let mut unhealthy = Vec::new();
                let dependency_lookup: BTreeMap<_, _> = descriptor
                    .node
                    .dependencies
                    .iter()
                    .map(|edge| (edge.endpoint.name.clone(), edge.endpoint.kind.clone()))
                    .collect();

                if let Some(route_health) = health_guard.get(&name) {
                    for (dependency, dep_state) in &route_health.dependency_states {
                        dependencies.push(dependency.clone());
                        let kind = dependency_lookup
                            .get(dependency)
                            .cloned()
                            .unwrap_or_else(|| ConnectorKind::Unknown(dependency.clone()));
                        merge_endpoint_state(&mut endpoint_map, dependency, kind, *dep_state);
                        if !matches!(dep_state, EndpointState::Healthy) {
                            unhealthy.push(dependency.clone());
                        }
                    }
                } else {
                    for edge in &descriptor.node.dependencies {
                        let dep_name = edge.endpoint.name.clone();
                        dependencies.push(dep_name.clone());
                        merge_endpoint_state(
                            &mut endpoint_map,
                            &dep_name,
                            edge.endpoint.kind.clone(),
                            EndpointState::Healthy,
                        );
                    }
                }

                dependencies.sort();
                unhealthy.sort();

                routes.push(RouteStatusSnapshot {
                    name: name.clone(),
                    state,
                    dependencies,
                    unhealthy,
                });
            }
        }

        routes.sort_by(|a, b| a.name.cmp(&b.name));

        let endpoints = endpoint_map
            .into_iter()
            .map(|(name, aggregate)| EndpointStatusSnapshot {
                name,
                state: aggregate.state,
                kind: aggregate.kind,
            })
            .collect();

        ReadinessSnapshot {
            application_state: app_state,
            routes,
            endpoints,
        }
    }

    pub async fn status_snapshot(&self) -> ReadinessSnapshot {
        self.build_snapshot().await
    }

    pub async fn cached_snapshot(&self, ttl: Duration) -> ReadinessSnapshot {
        if ttl.as_nanos() == 0 {
            return self.build_snapshot().await;
        }

        let now = Instant::now();
        {
            let cache_guard = self.cache.read().await;
            if let Some(cached) = cache_guard.as_ref() {
                if now.duration_since(cached.observed_at) <= ttl {
                    return cached.snapshot.clone();
                }
            }
        }

        let snapshot = self.build_snapshot().await;
        let mut cache_guard = self.cache.write().await;
        *cache_guard = Some(CachedSnapshot {
            snapshot: snapshot.clone(),
            observed_at: now,
        });
        snapshot
    }

    pub fn describe_route(&self, route: &str) -> Option<RouteDescriptorSummary> {
        self.routes
            .get(route)
            .map(|descriptor| RouteDescriptorSummary {
                name: descriptor.node.name.clone(),
                trigger: descriptor.node.trigger.clone(),
                dependencies: descriptor.node.dependencies.clone(),
                inferred_dependencies: descriptor.node.inferred_dependencies.clone(),
                dependency_source: descriptor
                    .node
                    .dependencies
                    .first()
                    .map(|edge| edge.source)
                    .unwrap_or(DependencySource::Inferred),
                allow_partial_delivery: descriptor.node.policy.allow_partial_delivery,
                half_open_counts_as_ready: descriptor
                    .node
                    .policy
                    .half_open_counts_as_ready
                    .unwrap_or(self.default_half_open),
            })
    }

    pub fn dependencies_by_connector(&self) -> BTreeMap<String, Vec<String>> {
        let mut map: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (route, descriptor) in self.routes.iter() {
            for edge in &descriptor.node.dependencies {
                map.entry(edge.endpoint.name.clone())
                    .or_default()
                    .push(route.clone());
            }
        }
        map
    }
}

/// Convert the readiness cache window into the Retry-After hint advertised by the management API.
pub fn retry_after_hint_seconds(cache: Duration) -> u64 {
    let secs = cache.as_secs();
    if secs == 0 {
        1
    } else {
        secs
    }
}

#[derive(Clone, Debug)]
pub struct RouteDescriptorSummary {
    pub name: String,
    pub trigger: crate::readiness::ConnectorEndpoint,
    pub dependencies: Vec<DependencyEdge>,
    pub inferred_dependencies: Vec<String>,
    pub dependency_source: DependencySource,
    pub allow_partial_delivery: bool,
    pub half_open_counts_as_ready: HalfOpenPolicy,
}

#[derive(Clone, Debug)]
pub struct ReadinessSnapshot {
    pub application_state: ApplicationState,
    pub routes: Vec<RouteStatusSnapshot>,
    pub endpoints: Vec<EndpointStatusSnapshot>,
}

#[derive(Clone, Debug)]
pub struct RouteStatusSnapshot {
    pub name: String,
    pub state: RouteState,
    pub dependencies: Vec<String>,
    pub unhealthy: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct EndpointStatusSnapshot {
    pub name: String,
    pub state: EndpointState,
    pub kind: ConnectorKind,
}

struct CachedSnapshot {
    snapshot: ReadinessSnapshot,
    observed_at: Instant,
}

fn evaluate_route_state(route: &RouteHealth) -> RouteState {
    if route.dependency_states.is_empty() {
        return RouteState::Ready;
    }

    if route
        .dependency_states
        .values()
        .any(|state| matches!(state, EndpointState::WarmingUp))
    {
        return RouteState::WarmingUp;
    }

    let mut has_unhealthy = false;
    let mut has_half_open = false;

    for state in route.dependency_states.values() {
        match state {
            EndpointState::Healthy => {}
            EndpointState::CircuitHalfOpen => {
                has_half_open = true;
            }
            EndpointState::Unhealthy | EndpointState::CircuitOpen => {
                has_unhealthy = true;
            }
            EndpointState::WarmingUp => {}
        }
    }

    if has_unhealthy {
        if route.allow_partial_delivery {
            RouteState::Degraded
        } else {
            RouteState::NotReady
        }
    } else if has_half_open {
        match route.half_open_counts_as_ready {
            HalfOpenPolicy::Route => RouteState::Degraded,
            HalfOpenPolicy::Endpoint | HalfOpenPolicy::Never => RouteState::NotReady,
        }
    } else {
        RouteState::Ready
    }
}

#[derive(Clone, Debug)]
struct EndpointAggregate {
    kind: ConnectorKind,
    state: EndpointState,
}

fn merge_endpoint_state(
    map: &mut BTreeMap<String, EndpointAggregate>,
    name: &str,
    kind: ConnectorKind,
    state: EndpointState,
) {
    map.entry(name.to_string())
        .and_modify(|existing| {
            if endpoint_state_priority(state) > endpoint_state_priority(existing.state) {
                existing.state = state;
            }
        })
        .or_insert(EndpointAggregate { kind, state });
}

fn endpoint_state_priority(state: EndpointState) -> u8 {
    match state {
        EndpointState::Healthy => 0,
        EndpointState::CircuitHalfOpen => 1,
        EndpointState::WarmingUp => 2,
        EndpointState::Unhealthy => 3,
        EndpointState::CircuitOpen => 4,
    }
}

fn min_option<T>(a: Option<T>, b: Option<T>) -> Option<T>
where
    T: Ord,
{
    match (a, b) {
        (Some(lhs), Some(rhs)) => Some(std::cmp::min(lhs, rhs)),
        (Some(lhs), None) => Some(lhs),
        (None, Some(rhs)) => Some(rhs),
        (None, None) => None,
    }
}

#[derive(Clone, Debug)]
struct EffectiveRetryBudget {
    max_attempts: Option<u32>,
    max_elapsed: Option<Duration>,
}

#[derive(Clone, Debug)]
struct RetryBudgetTracker {
    effective: EffectiveRetryBudget,
    attempts: u32,
    window_started: Option<Instant>,
}

impl RetryBudgetTracker {
    fn new(app_budget: Option<&RetryBudget>, route_budget: Option<&RetryBudget>) -> Self {
        let effective = EffectiveRetryBudget {
            max_attempts: min_option(
                app_budget.and_then(|budget| budget.max_attempts),
                route_budget.and_then(|budget| budget.max_attempts),
            ),
            max_elapsed: min_option(
                app_budget.and_then(|budget| budget.max_elapsed),
                route_budget.and_then(|budget| budget.max_elapsed),
            ),
        };

        Self {
            effective,
            attempts: 0,
            window_started: None,
        }
    }

    fn record_failure(&mut self) -> bool {
        if self.is_unbounded() {
            return true;
        }

        let now = Instant::now();

        if let Some(max_elapsed) = self.effective.max_elapsed {
            match self.window_started {
                Some(start) if now.duration_since(start) > max_elapsed => {
                    self.window_started = Some(now);
                    self.attempts = 0;
                }
                Some(_) => {}
                None => self.window_started = Some(now),
            }
        }

        if let Some(max_attempts) = self.effective.max_attempts {
            self.attempts += 1;
            if self.attempts > max_attempts {
                return false;
            }
        }

        true
    }

    fn reset(&mut self) {
        self.attempts = 0;
        self.window_started = None;
    }

    fn is_unbounded(&self) -> bool {
        self.effective.max_attempts.is_none() && self.effective.max_elapsed.is_none()
    }
}
