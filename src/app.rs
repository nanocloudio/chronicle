use crate::app_state::AppState;
use crate::backpressure::BackpressureManager;
use crate::chronicle::engine::ChronicleEngine;
use crate::chronicle::phase::PhaseExecutor;
use crate::config::integration::AppConfig;
use crate::config::{ChronicleConfig, ConnectorFlags, IntegrationConfig};
#[cfg(feature = "kafka")]
use crate::connectors::kafka::KafkaConsumerLoop;
use crate::connectors::{database::Database, management::ManagementServer};
use crate::error::{Context, Result};
use crate::integration::factory::ConnectorFactoryRegistry;
use crate::integration::registry::ConnectorRegistry;
use crate::readiness::graph::canonical_endpoint_name;
use crate::readiness::{DependencyHealth, ReadinessController, WarmupCoordinator};
use crate::transport::{TransportRun, TransportRuntime};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};

#[cfg(feature = "http-in")]
use crate::transport::http_server::HttpTriggerRuntime;
#[cfg(feature = "db-mongodb")]
use crate::transport::mongodb::{MongodbChangeStreamDriver, MongodbTriggerRuntime};
#[cfg(feature = "mqtt")]
use crate::transport::mqtt::{MqttTriggerRuntime, RumqttcMqttSubscriber};
#[cfg(feature = "rabbitmq")]
use crate::transport::rabbitmq::{LapinRabbitmqConsumer, RabbitmqTriggerRuntime};
#[cfg(feature = "db-redis")]
use crate::transport::redis::RedisTriggerRuntime;

#[cfg(feature = "http-in")]
fn build_http_runtime(
    config: &Arc<IntegrationConfig>,
    registry: &Arc<ConnectorRegistry>,
    engine: &Arc<ChronicleEngine>,
    readiness: Option<ReadinessController>,
    dependency_health: Option<DependencyHealth>,
    backpressure: BackpressureManager,
    app_policy: &AppConfig,
) -> Result<HttpTriggerRuntime> {
    HttpTriggerRuntime::build(
        Arc::clone(config),
        Arc::clone(registry),
        Arc::clone(engine),
        readiness,
        dependency_health,
        backpressure,
        app_policy.clone(),
    )
}

pub struct ChronicleApp {
    state: AppState,
    management: Option<ManagementServer>,
    #[cfg(feature = "kafka")]
    kafka: Option<KafkaConsumerLoop>,
    transports: Vec<Box<dyn TransportRuntime>>,
    shutdown: tokio_util::sync::CancellationToken,
}

pub fn should_start_rabbitmq_runtime(flags: &ConnectorFlags, registry: &ConnectorRegistry) -> bool {
    flags.rabbitmq && registry.has_rabbitmq_connectors()
}

pub fn should_start_mqtt_runtime(flags: &ConnectorFlags, registry: &ConnectorRegistry) -> bool {
    flags.mqtt && registry.has_mqtt_connectors()
}

#[cfg(feature = "db-mongodb")]
pub fn should_start_mongodb_runtime(flags: &ConnectorFlags, registry: &ConnectorRegistry) -> bool {
    flags.mongodb && registry.has_mongodb_connectors()
}

#[cfg(not(feature = "db-mongodb"))]
pub fn should_start_mongodb_runtime(_: &ConnectorFlags, _: &ConnectorRegistry) -> bool {
    false
}

#[cfg(feature = "db-redis")]
pub fn should_start_redis_runtime(flags: &ConnectorFlags, registry: &ConnectorRegistry) -> bool {
    flags.redis && registry.has_redis_connectors()
}

#[cfg(not(feature = "db-redis"))]
pub fn should_start_redis_runtime(_: &ConnectorFlags, _: &ConnectorRegistry) -> bool {
    false
}

fn has_kafka_triggers(config: &IntegrationConfig) -> bool {
    config
        .chronicles
        .iter()
        .any(|chronicle| chronicle.trigger.kafka.is_some())
}

impl ChronicleApp {
    pub async fn initialise(config: ChronicleConfig) -> Result<Self> {
        let mut management_server = None;
        let connector_flags = config.connector_flags.clone();

        let mut integration_config: Option<Arc<IntegrationConfig>> = None;
        let mut registry: Option<Arc<ConnectorRegistry>> = None;
        let mut engine: Option<Arc<ChronicleEngine>> = None;
        let mut readiness: Option<ReadinessController> = None;
        let mut dependency_health: Option<DependencyHealth> = None;
        #[allow(unused_mut)]
        let mut transports: Vec<Box<dyn TransportRuntime>> = Vec::new();
        let mut app_policy = AppConfig::default();
        let mut pending_backpressure: Option<BackpressureManager> = None;

        match config.integration_config_path.as_deref() {
            Some(path) if !path.trim().is_empty() => {
                let config_path = std::path::PathBuf::from(path);
                let loaded_config = IntegrationConfig::from_path(&config_path)
                    .with_context(|| format!("failed to load integration config from {}", path))?;

                let base_dir = config_path
                    .parent()
                    .map(std::path::Path::to_path_buf)
                    .unwrap_or_else(|| std::path::PathBuf::from("."));

                if let Some(cfg) = loaded_config.management.as_ref() {
                    management_server = ManagementServer::build(cfg)
                        .context("failed to construct management server")?;
                }

                let registry_built = ConnectorRegistry::build(&loaded_config, &base_dir)
                    .context("failed to construct connector registry")?;

                let loaded = Arc::new(loaded_config);
                app_policy = loaded.app.clone();
                let registry_arc = Arc::new(registry_built);
                let readiness_ctrl = ReadinessController::initialise(&loaded);
                let dependency_index = readiness_ctrl.dependencies_by_connector();
                let breaker_configs = loaded
                    .connectors
                    .iter()
                    .filter_map(|connector| {
                        connector
                            .circuit_breaker
                            .clone()
                            .map(|cfg| (canonical_endpoint_name(&connector.name), cfg))
                    })
                    .collect::<HashMap<_, _>>();
                let health = DependencyHealth::new(
                    readiness_ctrl.clone(),
                    dependency_index,
                    breaker_configs,
                );

                let factory = ConnectorFactoryRegistry::new(Arc::clone(&registry_arc));
                let warmup_runner = WarmupCoordinator::new(&loaded.app, &loaded.connectors)
                    .with_readiness(readiness_ctrl.clone());
                if let Err(err) = warmup_runner.run(&factory).await {
                    let summary = err
                        .failures
                        .iter()
                        .map(|failure| format!("{}: {}", failure.connector, failure.error))
                        .collect::<Vec<_>>()
                        .join("; ");
                    return Err(crate::err!("connector warm-up failed: {summary}"));
                }

                let engine_arc = Arc::new(
                    ChronicleEngine::new(Arc::clone(&loaded), Arc::clone(&registry_arc))
                        .context("failed to construct chronicle engine")?,
                );
                readiness = Some(readiness_ctrl.clone());
                dependency_health = Some(health);

                let backpressure_manager =
                    BackpressureManager::new(&config.backpressure, Some(&app_policy));

                #[cfg(feature = "http-in")]
                {
                    let runtime = build_http_runtime(
                        &loaded,
                        &registry_arc,
                        &engine_arc,
                        Some(readiness_ctrl.clone()),
                        dependency_health.clone(),
                        backpressure_manager.clone(),
                        &app_policy,
                    )
                    .context("failed to construct http trigger runtime")?;
                    transports.push(Box::new(runtime));
                }

                pending_backpressure = Some(backpressure_manager);

                let rabbitmq_enabled =
                    should_start_rabbitmq_runtime(&connector_flags, registry_arc.as_ref());
                if rabbitmq_enabled {
                    #[cfg(feature = "rabbitmq")]
                    {
                        let runtime = RabbitmqTriggerRuntime::build_with(
                            Arc::clone(&loaded),
                            Arc::clone(&registry_arc),
                            Arc::clone(&engine_arc),
                            readiness.clone(),
                            dependency_health.clone(),
                            LapinRabbitmqConsumer::connect,
                        )
                        .await
                        .context("failed to construct rabbitmq trigger runtime")?;

                        if runtime.consumer_count() > 0 {
                            transports.push(Box::new(runtime));
                        }
                    }
                    #[cfg(not(feature = "rabbitmq"))]
                    {
                        tracing::info!(
                            "rabbitmq trigger runtime skipped (feature `rabbitmq` disabled)"
                        );
                    }
                } else if !connector_flags.rabbitmq && registry_arc.has_rabbitmq_connectors() {
                    tracing::info!(
                        "rabbitmq connectors disabled via configuration; trigger runtime skipped"
                    );
                }

                #[cfg(feature = "db-mongodb")]
                {
                    let mongodb_enabled =
                        should_start_mongodb_runtime(&connector_flags, registry_arc.as_ref());
                    if mongodb_enabled {
                        let runtime = MongodbTriggerRuntime::build_with(
                            Arc::clone(&loaded),
                            Arc::clone(&registry_arc),
                            Arc::clone(&engine_arc),
                            dependency_health.clone(),
                            MongodbChangeStreamDriver::connect,
                        )
                        .await
                        .context("failed to construct mongodb trigger runtime")?;

                        if runtime.listener_count() > 0 {
                            transports.push(Box::new(runtime));
                        }
                    } else if !connector_flags.mongodb && registry_arc.has_mongodb_connectors() {
                        tracing::info!(
                            "mongodb connectors disabled via configuration; trigger runtime skipped"
                        );
                    }
                }

                #[cfg(not(feature = "db-mongodb"))]
                {
                    if registry_arc.has_mongodb_connectors() {
                        if !connector_flags.mongodb {
                            tracing::info!(
                                "mongodb connectors disabled via configuration; trigger runtime skipped"
                            );
                        } else {
                            tracing::info!(
                                "mongodb trigger runtime skipped (feature `db-mongodb` disabled)"
                            );
                        }
                    }
                }

                #[cfg(feature = "db-redis")]
                {
                    let redis_enabled =
                        should_start_redis_runtime(&connector_flags, registry_arc.as_ref());
                    if redis_enabled {
                        let runtime = RedisTriggerRuntime::build(
                            Arc::clone(&loaded),
                            Arc::clone(&registry_arc),
                            Arc::clone(&engine_arc),
                            readiness.clone(),
                            dependency_health.clone(),
                        )
                        .await
                        .context("failed to construct redis trigger runtime")?;

                        if runtime.listener_count() > 0 {
                            transports.push(Box::new(runtime));
                        }
                    } else if !connector_flags.redis && registry_arc.has_redis_connectors() {
                        tracing::info!(
                            "redis connectors disabled via configuration; trigger runtime skipped"
                        );
                    }
                }

                #[cfg(not(feature = "db-redis"))]
                {
                    if registry_arc.has_redis_connectors() {
                        if !connector_flags.redis {
                            tracing::info!(
                                "redis connectors disabled via configuration; trigger runtime skipped"
                            );
                        } else {
                            tracing::info!(
                                "redis trigger runtime skipped (feature `db-redis` disabled)"
                            );
                        }
                    }
                }

                let mqtt_enabled =
                    should_start_mqtt_runtime(&connector_flags, registry_arc.as_ref());
                if mqtt_enabled {
                    #[cfg(feature = "mqtt")]
                    {
                        let runtime = MqttTriggerRuntime::build_with(
                            Arc::clone(&loaded),
                            Arc::clone(&registry_arc),
                            Arc::clone(&engine_arc),
                            readiness.clone(),
                            dependency_health.clone(),
                            RumqttcMqttSubscriber::connect,
                        )
                        .await
                        .context("failed to construct mqtt trigger runtime")?;

                        if runtime.subscriber_count() > 0 {
                            transports.push(Box::new(runtime));
                        }
                    }
                    #[cfg(not(feature = "mqtt"))]
                    {
                        tracing::info!("mqtt trigger runtime skipped (feature `mqtt` disabled)");
                    }
                } else if !connector_flags.mqtt && registry_arc.has_mqtt_connectors() {
                    tracing::info!(
                        "mqtt connectors disabled via configuration; trigger runtime skipped"
                    );
                }

                tracing::info!(
                    path,
                    connector_count = loaded.connectors.len(),
                    chronicle_count = loaded.chronicles.len(),
                    "loaded integration config"
                );

                integration_config = Some(Arc::clone(&loaded));
                registry = Some(Arc::clone(&registry_arc));
                engine = Some(engine_arc);
            }
            _ => {
                tracing::info!(
                    "no integration config supplied; service starting without chronicles"
                );
            }
        };

        let kafka_triggers_present = integration_config
            .as_ref()
            .map(|cfg| has_kafka_triggers(cfg.as_ref()))
            .unwrap_or(false);

        let kafka_connectors_present = registry
            .as_ref()
            .map(|reg| reg.has_kafka_connectors())
            .unwrap_or(false);

        let requires_postgres = registry
            .as_ref()
            .map(|reg| reg.has_postgres_connectors())
            .unwrap_or(false);

        let db = if requires_postgres {
            match config.database.as_ref() {
                Some(db_config) => Some(
                    Database::connect(db_config)
                        .await
                        .context("failed to initialise database connection pool")?,
                ),
                None => {
                    tracing::warn!(
                        "postgres connectors defined but no database configuration supplied; skipping connection"
                    );
                    None
                }
            }
        } else {
            if config.database.is_some() {
                tracing::debug!(
                    "database configuration supplied but no postgres connectors referenced; connection skipped"
                );
            }
            None
        };

        let phase_executor = PhaseExecutor::new(db.clone());

        if management_server.is_none() {
            tracing::warn!("no management listener configured; REST API will not be exposed");
        }

        let backpressure = pending_backpressure
            .unwrap_or_else(|| BackpressureManager::new(&config.backpressure, Some(&app_policy)));

        #[cfg(feature = "kafka")]
        let kafka = if kafka_triggers_present && kafka_connectors_present {
            Some(
                KafkaConsumerLoop::new(&config.kafka)
                    .context("failed to construct Kafka consumer loop")?,
            )
        } else {
            if kafka_triggers_present && !kafka_connectors_present {
                tracing::info!("Kafka consumer loop disabled (required Kafka connector missing)");
            } else {
                tracing::info!("Kafka consumer loop disabled (no Kafka triggers configured)");
            }
            None
        };

        #[cfg(not(feature = "kafka"))]
        {
            if kafka_triggers_present {
                if kafka_connectors_present {
                    tracing::info!("Kafka consumer loop skipped (feature `kafka` disabled)");
                } else {
                    tracing::info!(
                        "Kafka triggers configured but Kafka connectors missing; consumer loop unavailable"
                    );
                }
            }
        }

        let shutdown = tokio_util::sync::CancellationToken::new();
        if let Some(health) = dependency_health.clone() {
            health.spawn_maintenance(shutdown.clone());
        }

        Ok(Self {
            state: AppState {
                db,
                phase_executor,
                integration: integration_config,
                connectors: registry,
                chronicle_engine: engine,
                backpressure,
                app_policy,
                readiness,
                dependency_health,
            },
            management: management_server,
            #[cfg(feature = "kafka")]
            kafka,
            transports,
            shutdown,
        })
    }

    pub async fn run(self) -> Result<()> {
        #[cfg(feature = "kafka")]
        let Self {
            state,
            management,
            kafka,
            transports,
            shutdown,
        } = self;

        #[cfg(not(feature = "kafka"))]
        let Self {
            state,
            management,
            transports,
            shutdown,
        } = self;

        let mut management_task = management.map(|server| {
            let management_state = state.clone();
            let management_shutdown = shutdown.clone();
            tokio::spawn(async move { server.serve(management_state, management_shutdown).await })
        });

        #[cfg(feature = "kafka")]
        let mut kafka_task = kafka.map(|kafka_loop| {
            let kafka_state = state.clone();
            let kafka_shutdown = shutdown.clone();
            tokio::spawn(async move { kafka_loop.run(kafka_state, kafka_shutdown).await })
        });

        let mut transport_handles = transports;
        let mut transport_runs: Vec<TransportRun> = Vec::new();
        for handle in transport_handles.iter_mut() {
            handle.prepare().await?;
            handle.start(shutdown.clone()).await?;
            transport_runs.push(handle.run());
        }

        let mut transport_tasks = JoinSet::new();
        for run in transport_runs {
            let kind = run.kind();
            let name = run.name();
            transport_tasks.spawn(async move {
                match run.wait().await {
                    Ok(()) => {
                        tracing::info!(transport = %kind, name = name, "transport runtime stopped");
                        Ok(())
                    }
                    Err(err) => {
                        tracing::error!(
                            transport = %kind,
                            name = name,
                            error = %err,
                            "transport runtime terminated with error"
                        );
                        Err(err)
                    }
                }
            });
        }

        tracing::info!("chronicle service ready; press Ctrl+C to stop");

        #[cfg(feature = "kafka")]
        {
            tokio::select! {
                res = async {
                    management_task
                        .as_mut()
                        .expect("management task guard ensures presence")
                        .await
                }, if management_task.is_some() => {
                    tracing::warn!("management server task terminated unexpectedly");
                    match res {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => return Err(err),
                        Err(join_err) => {
                            return Err(crate::err!(
                                "management server task join error: {join_err}"
                            ))
                        }
                    }
                }
                res = async {
                    kafka_task
                        .as_mut()
                        .expect("kafka task guard ensures presence")
                        .await
                }, if kafka_task.is_some() => {
                    tracing::warn!("Kafka consumer task terminated unexpectedly");
                    match res {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => return Err(err),
                        Err(join_err) => {
                            return Err(crate::err!("kafka consumer join error: {join_err}"))
                        }
                    }
                }
                res = transport_tasks.join_next(), if !transport_tasks.is_empty() => {
                    if let Some(res) = res {
                        match res {
                            Ok(Ok(())) => {}
                            Ok(Err(err)) => return Err(err),
                            Err(join_err) => {
                                return Err(crate::err!(
                                    "transport runtime supervisor join error: {join_err}"
                                ))
                            }
                        }
                    }
                }
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("shutdown signal received");
                }
            }
        }

        #[cfg(not(feature = "kafka"))]
        {
            tokio::select! {
                res = async {
                    management_task
                        .as_mut()
                        .expect("management task guard ensures presence")
                        .await
                }, if management_task.is_some() => {
                    tracing::warn!("management server task terminated unexpectedly");
                    match res {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => return Err(err),
                        Err(join_err) => {
                            return Err(crate::err!(
                                "management server task join error: {join_err}"
                            ))
                        }
                    }
                }
                res = transport_tasks.join_next(), if !transport_tasks.is_empty() => {
                    if let Some(res) = res {
                        match res {
                            Ok(Ok(())) => {}
                            Ok(Err(err)) => return Err(err),
                            Err(join_err) => {
                                return Err(crate::err!(
                                    "transport runtime supervisor join error: {join_err}"
                                ))
                            }
                        }
                    }
                }
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("shutdown signal received");
                }
            }
        }

        if let Some(controller) = state.readiness.clone() {
            controller.enter_draining().await;
        }

        shutdown.cancel();
        let drain_timeout = state.app_policy.drain_timeout;
        let hard_stop = Duration::from_secs(5);

        let graceful_shutdown = async {
            if let Some(task) = management_task.as_mut() {
                if !task.is_finished() {
                    task.abort();
                }
            }

            #[cfg(feature = "kafka")]
            if let Some(task) = kafka_task.as_mut() {
                if !task.is_finished() {
                    task.abort();
                }
            }

            for handle in transport_handles.iter_mut() {
                let result = handle.shutdown().await;
                if let Err(err) = result {
                    tracing::warn!(
                        transport = %handle.kind(),
                        name = handle.name(),
                        error = %err,
                        "failed to shutdown transport gracefully"
                    );
                }
            }

            while let Some(res) = transport_tasks.join_next().await {
                match res {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => return Err(err),
                    Err(join_err) => {
                        tracing::warn!(
                            error = %join_err,
                            "transport monitor task cancelled"
                        );
                    }
                }
            }

            Ok::<(), crate::error::Error>(())
        };

        match timeout(drain_timeout, graceful_shutdown).await {
            Ok(result) => result,
            Err(_) => {
                tracing::error!(
                    timeout_secs = drain_timeout.as_secs_f64(),
                    "graceful shutdown exceeded app.drain_timeout; forcing exit after hard stop"
                );
                if let Some(task) = management_task.as_mut() {
                    task.abort();
                }
                #[cfg(feature = "kafka")]
                if let Some(task) = kafka_task.as_mut() {
                    task.abort();
                }
                transport_tasks.shutdown().await;
                sleep(hard_stop).await;
                Err(crate::err!(
                    "graceful shutdown timed out after {:?}",
                    drain_timeout
                ))
            }
        }
    }
}
