use crate::backpressure::BackpressureManager;
use crate::chronicle::engine::ChronicleEngine;
use crate::chronicle::phase::PhaseExecutor;
use crate::config::integration::AppConfig;
use crate::config::IntegrationConfig;
use crate::connectors::database::Database;
use crate::integration::registry::ConnectorRegistry;
use crate::readiness::{DependencyHealth, ReadinessController};
use std::sync::Arc;

/// Shared state exposed to management endpoints and background workers.
#[derive(Clone)]
pub struct AppState {
    pub db: Option<Database>,
    pub phase_executor: PhaseExecutor,
    pub integration: Option<Arc<IntegrationConfig>>,
    pub connectors: Option<Arc<ConnectorRegistry>>,
    pub chronicle_engine: Option<Arc<ChronicleEngine>>,
    pub backpressure: BackpressureManager,
    pub app_policy: AppConfig,
    pub readiness: Option<ReadinessController>,
    pub dependency_health: Option<DependencyHealth>,
}
