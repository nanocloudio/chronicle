use crate::chronicle::engine::{DependencySource, RouteRuntimeLimits};
use crate::config::integration::{
    AppConfig, ChronicleDefinition, ChroniclePolicy, ConnectorConfig, ConnectorKind,
    DeliveryPolicy, HalfOpenPolicy, IntegrationConfig, RetryBudget, RouteLimits, ScalarValue,
};
use crate::retry::merge_retry_budgets;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{Hash, Hasher};

#[derive(Clone, Debug)]
pub struct RouteGraph {
    pub routes: Vec<RouteNode>,
}

impl RouteGraph {
    pub fn build(config: &IntegrationConfig) -> Self {
        let connector_index = index_connectors(&config.connectors);

        let routes = config
            .chronicles
            .iter()
            .map(|chronicle| RouteNode::from_chronicle(chronicle, &connector_index, &config.app))
            .collect();

        Self { routes }
    }
}

#[derive(Clone, Debug)]
pub struct RouteNode {
    pub name: String,
    pub trigger: ConnectorEndpoint,
    pub dependencies: Vec<DependencyEdge>,
    pub inferred_dependencies: Vec<String>,
    pub policy: RoutePolicyInfo,
}

impl RouteNode {
    fn from_chronicle(
        chronicle: &ChronicleDefinition,
        connectors: &HashMap<String, ConnectorInfo>,
        app: &AppConfig,
    ) -> Self {
        let inferred = infer_dependency_names(chronicle);
        let (dependency_names, dependency_source) = if !chronicle.policy.requires.is_empty() {
            (
                dedupe(chronicle.policy.requires.clone()),
                DependencySource::Explicit,
            )
        } else {
            (inferred.clone(), DependencySource::Inferred)
        };

        let expanded = expand_connector_requires(&dependency_names, connectors);
        let connector_budget_refs = expanded
            .iter()
            .map(|name| {
                connectors
                    .get(name)
                    .and_then(|info| info.retry_budget.as_ref())
            })
            .collect::<Vec<_>>();
        let dependencies = expanded
            .into_iter()
            .map(|name| DependencyEdge {
                endpoint: ConnectorEndpoint::from_name(&name, connectors),
                source: dependency_source,
            })
            .collect();

        let trigger = ConnectorEndpoint::from_name(&chronicle.trigger.connector, connectors);
        let policy = RoutePolicyInfo::from_policy(&chronicle.policy, app, &connector_budget_refs);

        Self {
            name: chronicle.name.clone(),
            trigger,
            dependencies,
            inferred_dependencies: inferred,
            policy,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ConnectorEndpoint {
    pub name: String,
    pub connector: String,
    pub kind: ConnectorKind,
}

impl ConnectorEndpoint {
    fn from_name(name: &str, connectors: &HashMap<String, ConnectorInfo>) -> Self {
        let kind = connectors
            .get(name)
            .map(|info| info.kind.clone())
            .unwrap_or_else(|| ConnectorKind::Unknown(name.to_string()));

        Self {
            name: canonical_endpoint_name(name),
            connector: name.to_string(),
            kind,
        }
    }
}

#[derive(Clone, Debug)]
pub struct DependencyEdge {
    pub endpoint: ConnectorEndpoint,
    pub source: DependencySource,
}

#[derive(Clone, Debug)]
pub struct RoutePolicyInfo {
    pub allow_partial_delivery: bool,
    pub readiness_priority: Option<i32>,
    pub limits: RoutePolicyLimits,
    pub retry_budget: Option<RetryBudget>,
    pub half_open_counts_as_ready: Option<HalfOpenPolicy>,
    pub delivery: Option<DeliveryPolicy>,
    pub fallback: Option<BTreeMap<String, String>>,
}

impl RoutePolicyInfo {
    fn from_policy(
        policy: &ChroniclePolicy,
        app: &AppConfig,
        connector_budgets: &[Option<&RetryBudget>],
    ) -> Self {
        let mut scopes = Vec::with_capacity(2 + connector_budgets.len());
        scopes.push(app.retry_budget.as_ref());
        scopes.extend_from_slice(connector_budgets);
        scopes.push(policy.retry_budget.as_ref());
        let effective_retry_budget =
            merge_retry_budgets(scopes).or_else(|| app.retry_budget.clone());

        Self {
            allow_partial_delivery: policy.allow_partial_delivery,
            readiness_priority: policy.readiness_priority,
            limits: RoutePolicyLimits::from_policy(app, policy),
            retry_budget: effective_retry_budget,
            half_open_counts_as_ready: policy.half_open_counts_as_ready,
            delivery: policy.delivery.clone(),
            fallback: policy.fallback.clone(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct RoutePolicyLimits {
    pub effective: RouteRuntimeLimits,
    pub app: Option<RouteRuntimeLimits>,
    pub policy: Option<RouteRuntimeLimits>,
}

impl RoutePolicyLimits {
    fn from_policy(app: &AppConfig, policy: &ChroniclePolicy) -> Self {
        let app_limits = app.limits.routes.as_ref().map(to_runtime_limits);
        let policy_limits = policy.limits.as_ref().map(to_runtime_limits);
        let effective = merge_limits(app_limits.as_ref(), policy_limits.as_ref());

        Self {
            effective,
            app: app_limits,
            policy: policy_limits,
        }
    }
}

#[derive(Clone, Debug)]
struct ConnectorInfo {
    kind: ConnectorKind,
    requires: Vec<String>,
    retry_budget: Option<RetryBudget>,
}

fn index_connectors(connectors: &[ConnectorConfig]) -> HashMap<String, ConnectorInfo> {
    connectors
        .iter()
        .map(|connector| {
            (
                connector.name.clone(),
                ConnectorInfo {
                    kind: connector.kind.clone(),
                    requires: connector.requires.clone(),
                    retry_budget: connector.retry_budget.clone(),
                },
            )
        })
        .collect()
}

fn infer_dependency_names(chronicle: &ChronicleDefinition) -> Vec<String> {
    let mut connectors = BTreeSet::new();
    for phase in &chronicle.phases {
        if let Some(connector) = phase.option("connector").and_then(ScalarValue::as_str) {
            let trimmed = connector.trim();
            if !trimmed.is_empty() {
                connectors.insert(trimmed.to_string());
            }
        }
    }
    connectors.into_iter().collect()
}

fn expand_connector_requires(
    connectors: &[String],
    catalog: &HashMap<String, ConnectorInfo>,
) -> Vec<String> {
    let mut expanded = BTreeSet::new();
    for name in connectors {
        if expanded.insert(name.clone()) {
            if let Some(info) = catalog.get(name) {
                for extra in &info.requires {
                    let trimmed = extra.trim();
                    if !trimmed.is_empty() {
                        expanded.insert(trimmed.to_string());
                    }
                }
            }
        }
    }
    expanded.into_iter().collect()
}

fn dedupe(values: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut set = BTreeSet::new();
    for value in values {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            set.insert(trimmed.to_string());
        }
    }
    set.into_iter().collect()
}

fn to_runtime_limits(limits: &RouteLimits) -> RouteRuntimeLimits {
    RouteRuntimeLimits {
        max_inflight: limits.max_inflight,
        overflow_policy: limits.overflow_policy,
        max_queue_depth: limits.max_queue_depth,
    }
}

fn merge_limits(
    app: Option<&RouteRuntimeLimits>,
    policy: Option<&RouteRuntimeLimits>,
) -> RouteRuntimeLimits {
    let mut effective = RouteRuntimeLimits::default();

    if let Some(policy_limits) = policy {
        effective.max_inflight = policy_limits.max_inflight;
        effective.overflow_policy = policy_limits.overflow_policy;
        effective.max_queue_depth = policy_limits.max_queue_depth;
    }

    if effective.max_inflight.is_none() {
        if let Some(app_limits) = app {
            effective.max_inflight = app_limits.max_inflight;
        }
    }

    if effective.overflow_policy.is_none() {
        if let Some(app_limits) = app {
            effective.overflow_policy = app_limits.overflow_policy;
        }
    }

    if effective.max_queue_depth.is_none() {
        if let Some(app_limits) = app {
            effective.max_queue_depth = app_limits.max_queue_depth;
        }
    }

    effective
}

pub(crate) fn canonical_endpoint_name(connector: &str) -> String {
    const PREFIX: &str = "endpoint.";
    const MAX_LEN: usize = 36;
    const HASH_LEN: usize = 4;
    const SEPARATOR: &str = "-";

    let trimmed = connector.trim();
    if trimmed.is_empty() {
        return PREFIX.to_string();
    }

    let base = format!("{PREFIX}{trimmed}");
    if base.len() <= MAX_LEN {
        return base;
    }

    let mut hasher = DefaultHasher::new();
    trimmed.hash(&mut hasher);
    let hash = format!("{:04x}", hasher.finish() & 0xffff);

    let reserved = PREFIX.len() + HASH_LEN + SEPARATOR.len();
    let available = MAX_LEN.saturating_sub(reserved);
    let mut truncated = String::new();
    for ch in trimmed.chars() {
        if truncated.len() + ch.len_utf8() > available {
            break;
        }
        truncated.push(ch);
    }

    if truncated.is_empty() {
        return format!("{PREFIX}{hash}");
    }

    format!("{PREFIX}{truncated}{SEPARATOR}{hash}")
}
