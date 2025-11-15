use crate::config::integration::{AppConfig, ConnectorConfig, ConnectorKind, JitterMode};
#[allow(unused_imports)]
use crate::error::Context;
use crate::error::{ChronicleError, Result as ChronicleResult};
use crate::integration::factory::ConnectorFactoryRegistry;
use crate::metrics::metrics;
use crate::readiness::graph::canonical_endpoint_name;
use crate::readiness::{EndpointState, ReadinessController};
use crate::retry::{jitter_between, merge_retry_budgets};
use std::collections::BTreeMap;
use std::fmt;
use std::time::{Duration, Instant};
use tokio::time::{sleep, timeout};
#[cfg(feature = "grpc")]
use tower::ServiceExt;

#[cfg(feature = "kafka")]
use rdkafka::producer::Producer;

const DEFAULT_MAX_ATTEMPTS: u32 = 3;
const DEFAULT_BASE_BACKOFF: Duration = Duration::from_millis(250);
const DEFAULT_MAX_BACKOFF: Duration = Duration::from_secs(2);

#[derive(Clone)]
struct ReadinessHandle {
    controller: ReadinessController,
    dependencies: BTreeMap<String, Vec<String>>,
}

#[derive(Debug)]
pub struct WarmupFailure {
    pub connector: String,
    pub attempts: u32,
    pub error: ChronicleError,
}

#[derive(Debug)]
pub struct WarmupAggregateError {
    pub failures: Vec<WarmupFailure>,
}

impl fmt::Display for WarmupAggregateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "warm-up failed for {} connector(s)", self.failures.len())?;
        for failure in &self.failures {
            writeln!(
                f,
                "  {} (attempts: {}): {}",
                failure.connector, failure.attempts, failure.error
            )?;
        }
        Ok(())
    }
}

impl std::error::Error for WarmupAggregateError {}

pub struct WarmupCoordinator<'a> {
    app: &'a AppConfig,
    connectors: &'a [ConnectorConfig],
    readiness: Option<ReadinessHandle>,
}

impl<'a> WarmupCoordinator<'a> {
    pub fn new(app: &'a AppConfig, connectors: &'a [ConnectorConfig]) -> Self {
        Self {
            app,
            connectors,
            readiness: None,
        }
    }

    pub fn with_readiness(mut self, controller: ReadinessController) -> Self {
        let dependencies = controller.dependencies_by_connector();
        self.readiness = Some(ReadinessHandle {
            controller,
            dependencies,
        });
        self
    }

    pub async fn run(
        &self,
        factory: &ConnectorFactoryRegistry,
    ) -> std::result::Result<(), WarmupAggregateError> {
        warmup_connectors_inner(self.app, self.connectors, factory, self.readiness.as_ref()).await
    }
}

pub async fn warmup_connectors(
    app: &AppConfig,
    connectors: &[ConnectorConfig],
    factory: &ConnectorFactoryRegistry,
) -> std::result::Result<(), WarmupAggregateError> {
    WarmupCoordinator::new(app, connectors).run(factory).await
}

async fn warmup_connectors_inner(
    app: &AppConfig,
    connectors: &[ConnectorConfig],
    factory: &ConnectorFactoryRegistry,
    readiness: Option<&ReadinessHandle>,
) -> std::result::Result<(), WarmupAggregateError> {
    let mut failures = Vec::new();
    let counters = metrics();
    for connector in connectors {
        counters.register_warmup_target(&connector.name);
        let timeout_duration = connector.warmup_timeout.unwrap_or(app.warmup_timeout);
        let retry_policy = WarmupRetryPolicy::for_connector(app, connector);

        let mut attempts = 0;
        let mut success = false;
        let mut last_error: Option<ChronicleError> = None;
        let overall_start = Instant::now();

        if !connector.warmup {
            mark_dependency_state(readiness, &connector.name, EndpointState::Healthy).await;
            tracing::info!(
                connector = %connector.name,
                "connector warm-up skipped (disabled)"
            );
            continue;
        }

        mark_dependency_state(readiness, &connector.name, EndpointState::WarmingUp).await;

        let mut attempt_window_start = Instant::now();
        while attempts < retry_policy.max_attempts() {
            attempts += 1;

            let warmup_future = warmup_once(connector, factory);
            match timeout(timeout_duration, warmup_future).await {
                Ok(Ok(())) => {
                    success = true;
                    break;
                }
                Ok(Err(err)) => {
                    last_error = Some(err);
                }
                Err(_) => {
                    last_error = Some(crate::err!(format!(
                        "connector `{}` warm-up timed out after {:?}",
                        connector.name, timeout_duration
                    )));
                }
            }

            let elapsed = attempt_window_start.elapsed();
            if let Some(delay) = retry_policy.next_delay(attempts, elapsed) {
                if !delay.is_zero() {
                    sleep(delay).await;
                }
                attempt_window_start = Instant::now();
            } else {
                break;
            }
        }

        let elapsed = overall_start.elapsed();
        let duration_ms = std::cmp::min(elapsed.as_millis(), u128::from(u64::MAX)) as u64;

        if success {
            tracing::info!(
                connector = %connector.name,
                state_from = "pending",
                state_to = "succeeded",
                attempts,
                duration_ms,
                "connector warm-up completed"
            );
            counters.record_warmup_success(&connector.name, attempts, elapsed);
            mark_dependency_state(readiness, &connector.name, EndpointState::Healthy).await;
        } else {
            let error = last_error.unwrap_or_else(|| crate::err!("unknown failure"));
            let reason = error.to_string();
            tracing::error!(
                connector = %connector.name,
                attempts,
                error = %error,
                reason = %reason,
                state_from = "pending",
                state_to = "failed",
                duration_ms,
                "connector warm-up failed"
            );
            counters.record_warmup_failure(&connector.name, attempts, elapsed);
            mark_dependency_state(readiness, &connector.name, EndpointState::Unhealthy).await;
            failures.push(WarmupFailure {
                connector: connector.name.clone(),
                attempts,
                error,
            });
        }
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(WarmupAggregateError { failures })
    }
}

async fn warmup_once(
    connector: &ConnectorConfig,
    factory: &ConnectorFactoryRegistry,
) -> ChronicleResult<()> {
    match connector.kind {
        #[cfg(feature = "db-postgres")]
        ConnectorKind::Postgres => warmup_postgres(&connector.name, factory).await,
        #[cfg(not(feature = "db-postgres"))]
        ConnectorKind::Postgres => Err(crate::err!(
            "postgres connector `{}` requires the `db-postgres` feature",
            connector.name
        )),
        #[cfg(feature = "db-mariadb")]
        ConnectorKind::Mariadb => warmup_mariadb(&connector.name, factory).await,
        #[cfg(not(feature = "db-mariadb"))]
        ConnectorKind::Mariadb => Err(crate::err!(
            "mariadb connector `{}` requires the `db-mariadb` feature",
            connector.name
        )),
        #[cfg(feature = "http-out")]
        ConnectorKind::HttpClient => warmup_http_client(&connector.name, factory).await,
        #[cfg(not(feature = "http-out"))]
        ConnectorKind::HttpClient => Err(crate::err!(
            "http client connector `{}` requires the `http-out` feature",
            connector.name
        )),
        #[cfg(feature = "grpc")]
        ConnectorKind::Grpc => warmup_grpc(&connector.name, factory).await,
        #[cfg(not(feature = "grpc"))]
        ConnectorKind::Grpc => Err(crate::err!(
            "grpc connector `{}` requires the `grpc` feature",
            connector.name
        )),
        ConnectorKind::Kafka => warmup_kafka(&connector.name, factory).await,
        ConnectorKind::Rabbitmq => warmup_rabbitmq(&connector.name, factory).await,
        ConnectorKind::Mqtt => warmup_mqtt(&connector.name, factory).await,
        #[cfg(feature = "db-redis")]
        ConnectorKind::Redis => Ok(()),
        #[cfg(not(feature = "db-redis"))]
        ConnectorKind::Redis => Err(crate::err!(
            "redis connector `{}` requires the `db-redis` feature",
            connector.name
        )),
        #[cfg(feature = "smtp")]
        ConnectorKind::Smtp => warmup_smtp(&connector.name, factory).await,
        #[cfg(not(feature = "smtp"))]
        ConnectorKind::Smtp => Err(crate::err!(
            "smtp connector `{}` requires the `smtp` feature",
            connector.name
        )),
        _ => Ok(()),
    }
}

#[cfg(feature = "db-postgres")]
async fn warmup_postgres(name: &str, factory: &ConnectorFactoryRegistry) -> ChronicleResult<()> {
    let handle = factory.postgres_pool(name)?;
    handle
        .pool()
        .acquire()
        .await
        .with_context(|| format!("postgres warm-up failed for `{name}`"))?;
    Ok(())
}

#[cfg(feature = "db-mariadb")]
async fn warmup_mariadb(name: &str, factory: &ConnectorFactoryRegistry) -> ChronicleResult<()> {
    let handle = factory.mariadb_pool(name)?;
    handle
        .pool()
        .acquire()
        .await
        .with_context(|| format!("mariadb warm-up failed for `{name}`"))?;
    Ok(())
}

#[cfg(feature = "http-out")]
async fn warmup_http_client(name: &str, factory: &ConnectorFactoryRegistry) -> ChronicleResult<()> {
    factory.http_client(name)?.client();
    Ok(())
}

#[cfg(feature = "grpc")]
async fn warmup_grpc(name: &str, factory: &ConnectorFactoryRegistry) -> ChronicleResult<()> {
    let handle = factory.grpc_client(name)?;
    let mut channel = handle.channel();
    channel
        .ready()
        .await
        .with_context(|| format!("grpc warm-up failed for `{name}`"))?;
    Ok(())
}

async fn warmup_kafka(name: &str, factory: &ConnectorFactoryRegistry) -> ChronicleResult<()> {
    #[cfg(feature = "kafka")]
    {
        use std::time::Duration as StdDuration;
        let handle = factory.kafka_producer(name)?;
        handle
            .producer()
            .client()
            .fetch_metadata(None, StdDuration::from_millis(500))
            .with_context(|| format!("kafka warm-up failed for `{name}`"))?;
        Ok(())
    }
    #[cfg(not(feature = "kafka"))]
    {
        let _ = name;
        let _ = factory;
        Err(crate::err!("kafka support not compiled"))
    }
}

async fn warmup_rabbitmq(name: &str, factory: &ConnectorFactoryRegistry) -> ChronicleResult<()> {
    factory.rabbitmq(name)?;
    Ok(())
}

async fn warmup_mqtt(name: &str, factory: &ConnectorFactoryRegistry) -> ChronicleResult<()> {
    factory.mqtt_client(name)?;
    Ok(())
}

#[cfg(feature = "smtp")]
async fn warmup_smtp(name: &str, factory: &ConnectorFactoryRegistry) -> ChronicleResult<()> {
    factory.smtp_mailer(name)?;
    Ok(())
}

async fn mark_dependency_state(
    readiness: Option<&ReadinessHandle>,
    connector: &str,
    state: EndpointState,
) {
    let Some(handle) = readiness else {
        return;
    };
    let endpoint = canonical_endpoint_name(connector);
    let Some(routes) = handle.dependencies.get(&endpoint) else {
        return;
    };

    for route in routes {
        if let Err(err) = handle
            .controller
            .set_dependency_state(route, &endpoint, state)
            .await
        {
            tracing::warn!(
                route = route,
                endpoint = %endpoint,
                connector = connector,
                state = state.as_str(),
                error = ?err,
                "failed to update readiness dependency state"
            );
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct WarmupRetryPolicy {
    max_attempts: u32,
    max_elapsed: Option<Duration>,
    base_backoff: Duration,
    max_backoff: Duration,
    jitter: JitterMode,
}

impl WarmupRetryPolicy {
    fn for_connector(app: &AppConfig, connector: &ConnectorConfig) -> Self {
        let mut policy = Self {
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            max_elapsed: None,
            base_backoff: DEFAULT_BASE_BACKOFF,
            max_backoff: DEFAULT_MAX_BACKOFF,
            jitter: JitterMode::None,
        };

        let merged_budget =
            merge_retry_budgets([app.retry_budget.as_ref(), connector.retry_budget.as_ref()])
                .or_else(|| app.retry_budget.clone());

        if let Some(budget) = merged_budget {
            if let Some(value) = budget.max_attempts {
                policy.max_attempts = value.max(1);
            }
            if let Some(value) = budget.max_elapsed {
                policy.max_elapsed = Some(value);
            }
            if let Some(value) = budget.base_backoff {
                policy.base_backoff = if value.is_zero() {
                    DEFAULT_BASE_BACKOFF
                } else {
                    value
                };
            }
            if let Some(value) = budget.max_backoff {
                policy.max_backoff = if value.is_zero() {
                    DEFAULT_MAX_BACKOFF
                } else {
                    value
                };
            }
            if let Some(mode) = budget.jitter {
                policy.jitter = mode;
            }
        }

        if policy.max_attempts == 0 {
            policy.max_attempts = DEFAULT_MAX_ATTEMPTS;
        }

        if policy.base_backoff.is_zero() {
            policy.base_backoff = DEFAULT_BASE_BACKOFF;
        }

        if policy.max_backoff.is_zero() {
            policy.max_backoff = DEFAULT_MAX_BACKOFF;
        }

        policy
    }

    fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    fn next_delay(&self, attempts: u32, attempt_elapsed: Duration) -> Option<Duration> {
        if attempts >= self.max_attempts() {
            return None;
        }

        let mut delay = self.compute_backoff(attempts);
        delay = match self.jitter {
            JitterMode::None => delay,
            JitterMode::Equal => jitter_between(delay.mul_f64(0.5), delay),
            JitterMode::Full => jitter_between(Duration::from_secs(0), delay),
        };

        if let Some(limit) = self.max_elapsed {
            if attempt_elapsed >= limit {
                return None;
            }
            let remaining = limit - attempt_elapsed;
            if remaining < delay {
                return None;
            }
        }

        Some(delay)
    }

    fn compute_backoff(&self, attempts: u32) -> Duration {
        if self.base_backoff.is_zero() {
            return Duration::from_secs(0);
        }

        let exponent = attempts.saturating_sub(1).min(8);
        let factor = 1u32 << exponent;
        let mut delay = self.base_backoff.mul_f64(factor as f64);
        if delay > self.max_backoff {
            delay = self.max_backoff;
        }
        delay
    }
}
