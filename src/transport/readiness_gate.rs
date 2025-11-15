use crate::readiness::{ReadinessController, RouteState};
use crate::transport::runtime::sleep_with_shutdown;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;

const DEFAULT_BACKOFF: Duration = Duration::from_millis(200);

#[derive(Clone)]
pub struct RouteReadinessGate {
    controller: Option<ReadinessController>,
    route: String,
    backoff: Duration,
}

impl RouteReadinessGate {
    pub fn new(controller: Option<ReadinessController>, route: impl Into<String>) -> Self {
        Self {
            controller,
            route: route.into(),
            backoff: DEFAULT_BACKOFF,
        }
    }

    pub async fn wait(&self, shutdown: &CancellationToken) -> bool {
        let Some(controller) = self.controller.as_ref() else {
            return true;
        };

        loop {
            match controller.route_state(&self.route).await {
                Some(RouteState::Ready | RouteState::Degraded) | None => return true,
                _ => {}
            }

            if sleep_with_shutdown(self.backoff, shutdown).await {
                return false;
            }
        }
    }
}
