use crate::config::integration::MinReadyRoutes;
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouteState {
    WarmingUp,
    Ready,
    NotReady,
    Degraded,
    Draining,
}

impl RouteState {
    fn is_ready_like(self) -> bool {
        matches!(self, RouteState::Ready | RouteState::Degraded)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            RouteState::WarmingUp => "WARMING_UP",
            RouteState::Ready => "READY",
            RouteState::NotReady => "NOT_READY",
            RouteState::Degraded => "DEGRADED",
            RouteState::Draining => "DRAINING",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EndpointState {
    WarmingUp,
    Healthy,
    Unhealthy,
    CircuitOpen,
    CircuitHalfOpen,
}

impl EndpointState {
    pub fn as_str(self) -> &'static str {
        match self {
            EndpointState::WarmingUp => "WARMING_UP",
            EndpointState::Healthy => "HEALTHY",
            EndpointState::Unhealthy => "UNHEALTHY",
            EndpointState::CircuitOpen => "CB_OPEN",
            EndpointState::CircuitHalfOpen => "CB_HALF_OPEN",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplicationState {
    WarmingUp,
    Ready,
    NotReady,
    Degraded,
    Draining,
}

impl ApplicationState {
    pub fn as_str(self) -> &'static str {
        match self {
            ApplicationState::WarmingUp => "WARMING_UP",
            ApplicationState::Ready => "READY",
            ApplicationState::NotReady => "NOT_READY",
            ApplicationState::Degraded => "DEGRADED",
            ApplicationState::Draining => "DRAINING",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransitionError {
    RouteUnknown {
        route: String,
    },
    InvalidTransition {
        route: String,
        from: RouteState,
        to: RouteState,
    },
}

pub struct RouteSeed {
    pub name: String,
    pub readiness_priority: i32,
    pub ordinal: usize,
}

struct RouteEntry {
    state: RouteState,
    readiness_priority: i32,
    ordinal: usize,
}

pub struct ReadinessStateMachine {
    routes: BTreeMap<String, RouteEntry>,
    min_ready_routes: MinReadyRoutes,
}

impl ReadinessStateMachine {
    pub fn new(
        min_ready_routes: MinReadyRoutes,
        routes: impl IntoIterator<Item = RouteSeed>,
    ) -> Self {
        let mut entries = BTreeMap::new();
        for seed in routes {
            entries.insert(
                seed.name,
                RouteEntry {
                    state: RouteState::WarmingUp,
                    readiness_priority: seed.readiness_priority,
                    ordinal: seed.ordinal,
                },
            );
        }

        Self {
            routes: entries,
            min_ready_routes,
        }
    }

    pub fn route_state(&self, route: &str) -> Option<RouteState> {
        self.routes.get(route).map(|entry| entry.state)
    }

    pub fn set_route_state(
        &mut self,
        route: &str,
        next: RouteState,
    ) -> Result<RouteState, TransitionError> {
        let name = route.to_string();
        let entry = self
            .routes
            .get_mut(route)
            .ok_or_else(|| TransitionError::RouteUnknown {
                route: name.clone(),
            })?;

        if !Self::is_valid_transition(entry.state, next) {
            return Err(TransitionError::InvalidTransition {
                route: name,
                from: entry.state,
                to: next,
            });
        }

        entry.state = next;
        Ok(entry.state)
    }

    fn is_valid_transition(current: RouteState, next: RouteState) -> bool {
        match current {
            RouteState::WarmingUp => matches!(
                next,
                RouteState::WarmingUp
                    | RouteState::Ready
                    | RouteState::NotReady
                    | RouteState::Draining
            ),
            RouteState::Ready => matches!(
                next,
                RouteState::Ready
                    | RouteState::NotReady
                    | RouteState::Degraded
                    | RouteState::Draining
            ),
            RouteState::NotReady => matches!(
                next,
                RouteState::NotReady
                    | RouteState::Ready
                    | RouteState::Degraded
                    | RouteState::Draining
            ),
            RouteState::Degraded => matches!(
                next,
                RouteState::Degraded
                    | RouteState::Ready
                    | RouteState::NotReady
                    | RouteState::Draining
            ),
            RouteState::Draining => matches!(next, RouteState::Draining),
        }
    }

    pub fn enter_draining(&mut self) -> Vec<(String, RouteState)> {
        let mut transitions = Vec::new();
        for (name, entry) in self.routes.iter_mut() {
            if entry.state == RouteState::Draining {
                continue;
            }
            if Self::is_valid_transition(entry.state, RouteState::Draining) {
                let previous = entry.state;
                entry.state = RouteState::Draining;
                transitions.push((name.clone(), previous));
            }
        }
        transitions
    }

    pub fn application_state(&self) -> ApplicationState {
        if self.routes.is_empty() {
            return ApplicationState::Ready;
        }

        if self
            .routes
            .values()
            .any(|entry| entry.state == RouteState::Draining)
        {
            return ApplicationState::Draining;
        }

        let total = self.routes.len();
        let any_ready_like = self
            .routes
            .values()
            .any(|entry| entry.state.is_ready_like());
        let any_warming = self
            .routes
            .values()
            .any(|entry| entry.state == RouteState::WarmingUp);
        let any_degraded = self
            .routes
            .values()
            .any(|entry| entry.state == RouteState::Degraded);

        match self.min_ready_routes {
            MinReadyRoutes::All => {
                if self
                    .routes
                    .values()
                    .all(|entry| entry.state.is_ready_like())
                {
                    if any_degraded {
                        return ApplicationState::Degraded;
                    } else {
                        return ApplicationState::Ready;
                    }
                }
            }
            MinReadyRoutes::Count(target) => {
                let required = std::cmp::min(target as usize, total);
                if required == 0 {
                    if any_degraded {
                        return ApplicationState::Degraded;
                    } else {
                        return ApplicationState::Ready;
                    }
                }

                let mut ordered: Vec<&RouteEntry> = self.routes.values().collect();
                ordered.sort_by(|lhs, rhs| {
                    lhs.readiness_priority
                        .cmp(&rhs.readiness_priority)
                        .then_with(|| lhs.ordinal.cmp(&rhs.ordinal))
                });

                let ready_required = ordered
                    .into_iter()
                    .take(required)
                    .filter(|entry| entry.state.is_ready_like())
                    .count();

                if ready_required == required {
                    if any_degraded {
                        return ApplicationState::Degraded;
                    } else {
                        return ApplicationState::Ready;
                    }
                }
            }
        }

        if !any_ready_like && any_warming {
            ApplicationState::WarmingUp
        } else {
            ApplicationState::NotReady
        }
    }

    pub fn route_states(&self) -> Vec<(String, RouteState)> {
        self.routes
            .iter()
            .map(|(name, entry)| (name.clone(), entry.state))
            .collect()
    }
}
