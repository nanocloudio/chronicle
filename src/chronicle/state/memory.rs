//! In-process memory execution store.
//!
//! Retains execution state in a bounded `BTreeMap` with lazy TTL eviction.
//! UUIDv7-based `ExecutionId` ordering ensures that expired entries cluster
//! at the head of the map, making eviction a cheap prefix scan.

use super::{
    ActionOutcome, ActionStatus, ExecutionFilter, ExecutionId, ExecutionSnapshot, ExecutionStatus,
    ExecutionStore, ExecutionStoreError, ExecutionSummary,
};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;

/// In-process execution store backed by a `BTreeMap`.
///
/// When `retention` is zero, all operations are no-ops and no memory is
/// allocated for entries. When non-zero, completed executions are evicted
/// lazily during `retain()` calls.
pub struct MemoryExecutionStore {
    entries: RwLock<BTreeMap<ExecutionId, ExecutionSnapshot>>,
    retention: Duration,
}

impl MemoryExecutionStore {
    pub fn new(retention: Duration) -> Self {
        Self {
            entries: RwLock::new(BTreeMap::new()),
            retention,
        }
    }

    fn is_disabled(&self) -> bool {
        self.retention.is_zero()
    }

    /// Evict entries whose `created_at` is older than `retention` from now.
    async fn evict_expired(&self) {
        let cutoff = SystemTime::now()
            .checked_sub(self.retention)
            .unwrap_or(SystemTime::UNIX_EPOCH);

        let mut guard = self.entries.write().await;
        let expired_keys: Vec<ExecutionId> = guard
            .iter()
            .take_while(|(_, snapshot)| snapshot.created_at < cutoff)
            .map(|(id, _)| id.clone())
            .collect();

        for key in expired_keys {
            guard.remove(&key);
        }
    }
}

impl ExecutionStore for MemoryExecutionStore {
    fn retain(
        &self,
        snapshot: ExecutionSnapshot,
    ) -> Pin<Box<dyn Future<Output = Result<(), ExecutionStoreError>> + Send + '_>> {
        Box::pin(async move {
            if self.is_disabled() {
                return Ok(());
            }
            self.evict_expired().await;
            let mut guard = self.entries.write().await;
            guard.insert(snapshot.execution_id.clone(), snapshot);
            Ok(())
        })
    }

    fn complete(
        &self,
        id: &ExecutionId,
        outcomes: Vec<ActionOutcome>,
        allow_partial: bool,
    ) -> Pin<Box<dyn Future<Output = Result<(), ExecutionStoreError>> + Send + '_>> {
        let id = id.clone();
        Box::pin(async move {
            if self.is_disabled() {
                return Ok(());
            }
            let mut guard = self.entries.write().await;
            let Some(entry) = guard.get_mut(&id) else {
                return Ok(());
            };
            entry.status = ExecutionStatus::from_outcomes(&outcomes, allow_partial);
            entry.completed_at = Some(SystemTime::now());
            entry.outcomes = outcomes;
            Ok(())
        })
    }

    fn get(
        &self,
        id: &ExecutionId,
    ) -> Pin<
        Box<dyn Future<Output = Result<Option<ExecutionSnapshot>, ExecutionStoreError>> + Send + '_>,
    > {
        let id = id.clone();
        Box::pin(async move {
            if self.is_disabled() {
                return Ok(None);
            }
            let guard = self.entries.read().await;
            Ok(guard.get(&id).cloned())
        })
    }

    fn list(
        &self,
        filter: &ExecutionFilter,
    ) -> Pin<
        Box<dyn Future<Output = Result<Vec<ExecutionSummary>, ExecutionStoreError>> + Send + '_>,
    > {
        let filter = filter.clone();
        Box::pin(async move {
            if self.is_disabled() {
                return Ok(Vec::new());
            }
            let guard = self.entries.read().await;
            let limit = filter.limit.unwrap_or(100);

            let results: Vec<ExecutionSummary> = guard
                .values()
                .rev()
                .filter(|snapshot| {
                    if let Some(ref chronicle) = filter.chronicle {
                        if &snapshot.chronicle != chronicle {
                            return false;
                        }
                    }
                    if let Some(status) = filter.status {
                        if snapshot.status != status {
                            return false;
                        }
                    }
                    true
                })
                .take(limit)
                .map(|snapshot| {
                    let succeeded = snapshot
                        .outcomes
                        .iter()
                        .filter(|o| o.status == ActionStatus::Succeeded)
                        .count();
                    let failed = snapshot
                        .outcomes
                        .iter()
                        .filter(|o| o.status == ActionStatus::Failed)
                        .count();
                    ExecutionSummary {
                        execution_id: snapshot.execution_id.clone(),
                        chronicle: snapshot.chronicle.clone(),
                        status: snapshot.status,
                        created_at: snapshot.created_at,
                        action_count: snapshot.outcomes.len(),
                        succeeded_count: succeeded,
                        failed_count: failed,
                    }
                })
                .collect();

            Ok(results)
        })
    }
}
