//! Execution state: identity, outcomes, and retention.
//!
//! Every chronicle execution is assigned a unique [`ExecutionId`] and produces
//! [`ActionOutcome`] records for each dispatched action. These types form the
//! observability foundation that structured logging, metrics, and the execution
//! store build upon.
//!
//! The [`ExecutionStore`] trait defines the pluggable retention interface.
//! Providers (memory, lattice, clustor) implement this trait to control where
//! execution state is persisted and how it is queried.

pub mod memory;

use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::Duration;

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
