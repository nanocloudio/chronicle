use chronicle::chronicle::state::memory::MemoryExecutionStore;
use chronicle::chronicle::state::{
    ActionOutcome, ActionStatus, ExecutionFilter, ExecutionId, ExecutionSnapshot, ExecutionStatus,
    ExecutionStore,
};
use std::time::{Duration, SystemTime};

fn sample_snapshot(chronicle: &str) -> ExecutionSnapshot {
    ExecutionSnapshot {
        execution_id: ExecutionId::new(),
        chronicle: chronicle.to_string(),
        trace_id: Some("trace-001".to_string()),
        record_id: Some("rec-001".to_string()),
        status: ExecutionStatus::Running,
        created_at: SystemTime::now(),
        completed_at: None,
        slots: vec![serde_json::json!({"key": "value"})],
        outcomes: Vec::new(),
    }
}

fn sample_outcomes(succeeded: usize, failed: usize) -> Vec<ActionOutcome> {
    let mut outcomes = Vec::new();
    for i in 0..succeeded {
        outcomes.push(ActionOutcome {
            action_index: i,
            status: ActionStatus::Succeeded,
            duration: Duration::from_millis(42),
            error: None,
        });
    }
    for i in 0..failed {
        outcomes.push(ActionOutcome {
            action_index: succeeded + i,
            status: ActionStatus::Failed,
            duration: Duration::from_millis(108),
            error: Some("timeout".to_string()),
        });
    }
    outcomes
}

// ---------------------------------------------------------------------------
// ExecutionId
// ---------------------------------------------------------------------------

#[test]
fn execution_id_new_produces_unique_values() {
    let a = ExecutionId::new();
    let b = ExecutionId::new();
    assert_ne!(a, b);
}

#[test]
fn execution_id_ordering_is_temporal() {
    let a = ExecutionId::new();
    std::thread::sleep(Duration::from_millis(2));
    let b = ExecutionId::new();
    assert!(a < b, "earlier ID should sort before later ID");
}

#[test]
fn execution_id_serialization_roundtrip() {
    let id = ExecutionId::new();
    let json = serde_json::to_string(&id).unwrap();
    let back: ExecutionId = serde_json::from_str(&json).unwrap();
    assert_eq!(id, back);
}

#[test]
fn execution_id_display_matches_str() {
    let id = ExecutionId::new();
    assert_eq!(id.to_string(), id.as_str());
}

#[test]
fn execution_id_from_string() {
    let id = ExecutionId::from_string("custom-id".to_string());
    assert_eq!(id.as_str(), "custom-id");
}

// ---------------------------------------------------------------------------
// ExecutionStatus derivation
// ---------------------------------------------------------------------------

#[test]
fn status_from_empty_outcomes_is_running() {
    assert_eq!(
        ExecutionStatus::from_outcomes(&[], false),
        ExecutionStatus::Running
    );
}

#[test]
fn status_all_succeeded() {
    let outcomes = sample_outcomes(3, 0);
    assert_eq!(
        ExecutionStatus::from_outcomes(&outcomes, false),
        ExecutionStatus::Succeeded
    );
}

#[test]
fn status_all_failed() {
    let outcomes = sample_outcomes(0, 2);
    assert_eq!(
        ExecutionStatus::from_outcomes(&outcomes, false),
        ExecutionStatus::Failed
    );
}

#[test]
fn status_mixed_without_partial_is_failed() {
    let outcomes = sample_outcomes(1, 1);
    assert_eq!(
        ExecutionStatus::from_outcomes(&outcomes, false),
        ExecutionStatus::Failed
    );
}

#[test]
fn status_mixed_with_partial_is_partial_success() {
    let outcomes = sample_outcomes(2, 1);
    assert_eq!(
        ExecutionStatus::from_outcomes(&outcomes, true),
        ExecutionStatus::PartialSuccess
    );
}

#[test]
fn status_display_values() {
    assert_eq!(ExecutionStatus::Running.to_string(), "running");
    assert_eq!(ExecutionStatus::Succeeded.to_string(), "succeeded");
    assert_eq!(ExecutionStatus::Failed.to_string(), "failed");
    assert_eq!(
        ExecutionStatus::PartialSuccess.to_string(),
        "partial_success"
    );
}

// ---------------------------------------------------------------------------
// MemoryExecutionStore — zero retention
// ---------------------------------------------------------------------------

#[tokio::test]
async fn memory_store_zero_retention_retain_is_noop() {
    let store = MemoryExecutionStore::new(Duration::ZERO);
    let snap = sample_snapshot("test");
    let id = snap.execution_id.clone();
    store.retain(snap).await.unwrap();
    let result = store.get(&id).await.unwrap();
    assert!(result.is_none(), "zero-retention store should not retain");
}

#[tokio::test]
async fn memory_store_zero_retention_list_returns_empty() {
    let store = MemoryExecutionStore::new(Duration::ZERO);
    store.retain(sample_snapshot("test")).await.unwrap();
    let results = store.list(&ExecutionFilter::default()).await.unwrap();
    assert!(results.is_empty());
}

// ---------------------------------------------------------------------------
// MemoryExecutionStore — lifecycle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn memory_store_retain_and_get() {
    let store = MemoryExecutionStore::new(Duration::from_secs(300));
    let snap = sample_snapshot("collect_record");
    let id = snap.execution_id.clone();
    store.retain(snap).await.unwrap();

    let retrieved = store.get(&id).await.unwrap().expect("should find retained execution");
    assert_eq!(retrieved.chronicle, "collect_record");
    assert_eq!(retrieved.status, ExecutionStatus::Running);
    assert!(retrieved.outcomes.is_empty());
}

#[tokio::test]
async fn memory_store_complete_updates_status_and_outcomes() {
    let store = MemoryExecutionStore::new(Duration::from_secs(300));
    let snap = sample_snapshot("relay_record");
    let id = snap.execution_id.clone();
    store.retain(snap).await.unwrap();

    let outcomes = sample_outcomes(2, 0);
    store.complete(&id, outcomes, false).await.unwrap();

    let retrieved = store.get(&id).await.unwrap().expect("should find completed execution");
    assert_eq!(retrieved.status, ExecutionStatus::Succeeded);
    assert_eq!(retrieved.outcomes.len(), 2);
    assert!(retrieved.completed_at.is_some());
}

#[tokio::test]
async fn memory_store_complete_partial_delivery() {
    let store = MemoryExecutionStore::new(Duration::from_secs(300));
    let snap = sample_snapshot("test");
    let id = snap.execution_id.clone();
    store.retain(snap).await.unwrap();

    let outcomes = sample_outcomes(1, 1);
    store.complete(&id, outcomes, true).await.unwrap();

    let retrieved = store.get(&id).await.unwrap().unwrap();
    assert_eq!(retrieved.status, ExecutionStatus::PartialSuccess);
}

#[tokio::test]
async fn memory_store_get_unknown_returns_none() {
    let store = MemoryExecutionStore::new(Duration::from_secs(300));
    let id = ExecutionId::from_string("nonexistent".to_string());
    let result = store.get(&id).await.unwrap();
    assert!(result.is_none());
}

// ---------------------------------------------------------------------------
// MemoryExecutionStore — list and filter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn memory_store_list_returns_most_recent_first() {
    let store = MemoryExecutionStore::new(Duration::from_secs(300));

    let s1 = sample_snapshot("alpha");
    let id1 = s1.execution_id.clone();
    store.retain(s1).await.unwrap();

    std::thread::sleep(Duration::from_millis(2));

    let s2 = sample_snapshot("beta");
    let id2 = s2.execution_id.clone();
    store.retain(s2).await.unwrap();

    let results = store.list(&ExecutionFilter::default()).await.unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].execution_id, id2, "most recent should be first");
    assert_eq!(results[1].execution_id, id1);
}

#[tokio::test]
async fn memory_store_list_filters_by_chronicle() {
    let store = MemoryExecutionStore::new(Duration::from_secs(300));
    store.retain(sample_snapshot("collect_record")).await.unwrap();
    store.retain(sample_snapshot("relay_record")).await.unwrap();
    store.retain(sample_snapshot("collect_record")).await.unwrap();

    let filter = ExecutionFilter {
        chronicle: Some("collect_record".to_string()),
        ..Default::default()
    };
    let results = store.list(&filter).await.unwrap();
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|r| r.chronicle == "collect_record"));
}

#[tokio::test]
async fn memory_store_list_filters_by_status() {
    let store = MemoryExecutionStore::new(Duration::from_secs(300));

    let s1 = sample_snapshot("test");
    let id1 = s1.execution_id.clone();
    store.retain(s1).await.unwrap();
    store
        .complete(&id1, sample_outcomes(1, 0), false)
        .await
        .unwrap();

    store.retain(sample_snapshot("test")).await.unwrap(); // stays Running

    let filter = ExecutionFilter {
        status: Some(ExecutionStatus::Succeeded),
        ..Default::default()
    };
    let results = store.list(&filter).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].status, ExecutionStatus::Succeeded);
}

#[tokio::test]
async fn memory_store_list_respects_limit() {
    let store = MemoryExecutionStore::new(Duration::from_secs(300));
    for _ in 0..10 {
        store.retain(sample_snapshot("test")).await.unwrap();
    }

    let filter = ExecutionFilter {
        limit: Some(3),
        ..Default::default()
    };
    let results = store.list(&filter).await.unwrap();
    assert_eq!(results.len(), 3);
}

// ---------------------------------------------------------------------------
// MemoryExecutionStore — eviction
// ---------------------------------------------------------------------------

#[tokio::test]
async fn memory_store_evicts_expired_entries() {
    let store = MemoryExecutionStore::new(Duration::from_millis(50));

    let snap = sample_snapshot("old");
    let old_id = snap.execution_id.clone();
    store.retain(snap).await.unwrap();

    // Wait for entry to expire
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Retain a new entry — triggers eviction
    store.retain(sample_snapshot("new")).await.unwrap();

    let old = store.get(&old_id).await.unwrap();
    assert!(old.is_none(), "expired entry should be evicted");

    let all = store.list(&ExecutionFilter::default()).await.unwrap();
    assert_eq!(all.len(), 1, "only the new entry should remain");
}

// ---------------------------------------------------------------------------
// ActionOutcome serialization
// ---------------------------------------------------------------------------

#[test]
fn action_outcome_serializes_duration_as_ms() {
    let outcome = ActionOutcome {
        action_index: 0,
        status: ActionStatus::Succeeded,
        duration: Duration::from_millis(42),
        error: None,
    };
    let json = serde_json::to_value(&outcome).unwrap();
    assert_eq!(json["duration"], 42);
}

#[test]
fn action_outcome_roundtrip() {
    let outcome = ActionOutcome {
        action_index: 1,
        status: ActionStatus::Failed,
        duration: Duration::from_millis(108),
        error: Some("timeout".to_string()),
    };
    let json = serde_json::to_string(&outcome).unwrap();
    let back: ActionOutcome = serde_json::from_str(&json).unwrap();
    assert_eq!(back.action_index, 1);
    assert_eq!(back.status, ActionStatus::Failed);
    assert_eq!(back.duration, Duration::from_millis(108));
    assert_eq!(back.error.as_deref(), Some("timeout"));
}

// ---------------------------------------------------------------------------
// ExecutionSnapshot serialization
// ---------------------------------------------------------------------------

#[test]
fn execution_snapshot_serializes_timestamps_as_rfc3339() {
    let snap = sample_snapshot("test");
    let json = serde_json::to_value(&snap).unwrap();
    let created = json["created_at"].as_str().unwrap();
    assert!(
        created.contains('T') && (created.contains('Z') || created.contains("+00:00")),
        "created_at should be RFC3339: {created}"
    );
    assert!(json["completed_at"].is_null());
}
