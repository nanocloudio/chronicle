//! Execution state: identity, outcomes, and retention.
//!
//! Every chronicle execution is assigned a unique [`ExecutionId`] and produces
//! [`ActionOutcome`] records for each dispatched action. These types form the
//! observability foundation that structured logging, metrics, and the execution
//! store build upon.
//!
//! The [`ExecutionStore`] trait defines the pluggable retention interface.
//! Providers (memory, clustor) implement this trait to control where execution
//! state is persisted and how it is queried.

pub mod memory;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, SystemTime};
use thiserror::Error;

// ---------------------------------------------------------------------------
// Execution identity
// ---------------------------------------------------------------------------

/// Time-ordered unique identifier for a chronicle execution.
///
/// Uses UUIDv7 so that lexicographic ordering matches temporal ordering,
/// which matters for retention eviction and range queries.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ExecutionId(String);

impl ExecutionId {
    pub fn new() -> Self {
        Self(uuid::Uuid::now_v7().to_string())
    }

    pub fn from_string(id: String) -> Self {
        Self(id)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for ExecutionId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ExecutionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

// ---------------------------------------------------------------------------
// Action outcomes
// ---------------------------------------------------------------------------

/// Outcome of a single dispatched action within an execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionOutcome {
    pub action_index: usize,
    pub status: ActionStatus,
    #[serde(with = "duration_ms")]
    pub duration: Duration,
    pub error: Option<String>,
}

/// Status of a dispatched action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionStatus {
    Succeeded,
    Failed,
}

// ---------------------------------------------------------------------------
// Execution status
// ---------------------------------------------------------------------------

/// Aggregate status of a chronicle execution derived from action outcomes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStatus {
    Running,
    Succeeded,
    Failed,
    PartialSuccess,
}

impl ExecutionStatus {
    /// Derive status from a set of action outcomes.
    pub fn from_outcomes(outcomes: &[ActionOutcome], allow_partial: bool) -> Self {
        if outcomes.is_empty() {
            return Self::Running;
        }

        let succeeded = outcomes
            .iter()
            .filter(|o| o.status == ActionStatus::Succeeded)
            .count();
        let total = outcomes.len();

        if succeeded == total {
            Self::Succeeded
        } else if succeeded == 0 {
            Self::Failed
        } else if allow_partial {
            Self::PartialSuccess
        } else {
            Self::Failed
        }
    }
}

impl fmt::Display for ExecutionStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Running => f.write_str("running"),
            Self::Succeeded => f.write_str("succeeded"),
            Self::Failed => f.write_str("failed"),
            Self::PartialSuccess => f.write_str("partial_success"),
        }
    }
}

// ---------------------------------------------------------------------------
// Execution snapshot and summary
// ---------------------------------------------------------------------------

/// Full snapshot of an execution, including slots and action outcomes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionSnapshot {
    pub execution_id: ExecutionId,
    pub chronicle: String,
    pub trace_id: Option<String>,
    pub record_id: Option<String>,
    pub status: ExecutionStatus,
    #[serde(with = "system_time_rfc3339")]
    pub created_at: SystemTime,
    #[serde(
        with = "system_time_rfc3339_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub completed_at: Option<SystemTime>,
    pub slots: Vec<JsonValue>,
    pub outcomes: Vec<ActionOutcome>,
}

/// Compact summary of an execution for list queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionSummary {
    pub execution_id: ExecutionId,
    pub chronicle: String,
    pub status: ExecutionStatus,
    #[serde(with = "system_time_rfc3339")]
    pub created_at: SystemTime,
    pub action_count: usize,
    pub succeeded_count: usize,
    pub failed_count: usize,
}

// ---------------------------------------------------------------------------
// Query filter
// ---------------------------------------------------------------------------

/// Filter criteria for listing executions.
#[derive(Debug, Clone, Default)]
pub struct ExecutionFilter {
    pub chronicle: Option<String>,
    pub status: Option<ExecutionStatus>,
    pub limit: Option<usize>,
}

// ---------------------------------------------------------------------------
// Store trait
// ---------------------------------------------------------------------------

/// Error type for execution store operations.
#[derive(Debug, Error)]
pub enum ExecutionStoreError {
    #[error("execution `{id}` not found")]
    NotFound { id: String },

    #[error("store unavailable: {reason}")]
    Unavailable { reason: String },

    #[error("serialization failed: {reason}")]
    Serialization { reason: String },
}

/// Pluggable retention interface for execution state.
///
/// Providers implement this trait to control where execution state is persisted
/// and how it is queried. The store is observational — failures MUST NOT
/// prevent action dispatch.
pub trait ExecutionStore: Send + Sync + 'static {
    /// Retain an execution snapshot at the start of dispatch.
    fn retain(
        &self,
        snapshot: ExecutionSnapshot,
    ) -> Pin<Box<dyn Future<Output = Result<(), ExecutionStoreError>> + Send + '_>>;

    /// Record action outcomes after dispatch completes.
    fn complete(
        &self,
        id: &ExecutionId,
        outcomes: Vec<ActionOutcome>,
        allow_partial: bool,
    ) -> Pin<Box<dyn Future<Output = Result<(), ExecutionStoreError>> + Send + '_>>;

    /// Query a single execution by ID.
    fn get(
        &self,
        id: &ExecutionId,
    ) -> Pin<Box<dyn Future<Output = Result<Option<ExecutionSnapshot>, ExecutionStoreError>> + Send + '_>>;

    /// List executions matching a filter.
    fn list(
        &self,
        filter: &ExecutionFilter,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ExecutionSummary>, ExecutionStoreError>> + Send + '_>>;
}

// ---------------------------------------------------------------------------
// Serde helpers
// ---------------------------------------------------------------------------

mod duration_ms {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::Duration;

    pub fn serialize<S: Serializer>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
        duration.as_millis().serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
        let ms = u64::deserialize(deserializer)?;
        Ok(Duration::from_millis(ms))
    }
}

mod system_time_rfc3339 {
    use chrono::{DateTime, Utc};
    use serde::{self, Deserialize, Deserializer, Serializer};
    use std::time::SystemTime;

    pub fn serialize<S: Serializer>(time: &SystemTime, serializer: S) -> Result<S::Ok, S::Error> {
        let dt: DateTime<Utc> = (*time).into();
        serializer.serialize_str(&dt.to_rfc3339())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<SystemTime, D::Error> {
        let s = String::deserialize(deserializer)?;
        let dt = DateTime::parse_from_rfc3339(&s).map_err(serde::de::Error::custom)?;
        Ok(dt.with_timezone(&Utc).into())
    }
}

mod system_time_rfc3339_option {
    use chrono::{DateTime, Utc};
    use serde::{self, Deserialize, Deserializer, Serializer};
    use std::time::SystemTime;

    pub fn serialize<S: Serializer>(
        time: &Option<SystemTime>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match time {
            Some(t) => {
                let dt: DateTime<Utc> = (*t).into();
                serializer.serialize_str(&dt.to_rfc3339())
            }
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<SystemTime>, D::Error> {
        let opt = Option::<String>::deserialize(deserializer)?;
        match opt {
            Some(s) => {
                let dt = DateTime::parse_from_rfc3339(&s).map_err(serde::de::Error::custom)?;
                Ok(Some(dt.with_timezone(&Utc).into()))
            }
            None => Ok(None),
        }
    }
}
