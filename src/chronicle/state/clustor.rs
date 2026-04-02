//! Raft-backed execution store using embedded Clustor.
//!
//! Runs a Raft peer group (Execution Raft Group, ERG) within the Chronicle
//! process. Executions are proposed as Raft log entries and applied to an
//! in-memory state machine identical in structure to
//! [`super::memory::MemoryExecutionStore`].
//!
//! The integration follows the same patterns used by Lattice (KPGs) and
//! Quantum (PRGs): a `ReplicaShared` holds consensus state, a
//! `ReplicaCallbacks` implements [`RaftNodeCallbacks`] for the scaffold,
//! and [`RaftNodeScaffold`] drives election and heartbeat timers.
//!
//! # Propose semantics
//!
//! `propose()` appends to the local log, records a local ACK, and replicates
//! to peers. The commit index advances when quorum ACKs arrive via the
//! durability ledger. The apply hook fires on commit, updating the in-memory
//! state machine.
//!
//! # Read semantics
//!
//! Reads go through [`ConsensusCore::guard(ReadIndex)`]. On the leader after
//! quorum proof, this is linearizable. On followers or when the gate fails,
//! the store returns `Unavailable`.

use super::{
    ActionOutcome, ActionStatus, ExecutionFilter, ExecutionId, ExecutionSnapshot, ExecutionStatus,
    ExecutionStore, ExecutionStoreError, ExecutionSummary,
};
use clustor::net::{
    AsyncRaftNetworkClient, AsyncRaftNetworkServer, AsyncRaftNetworkServerHandle,
    AsyncRaftTransportClientConfig, AsyncRaftTransportClientOptions, AsyncRaftTransportServerConfig,
    TlsIdentity, TlsTrustStore,
};
use clustor::profile::PartitionProfile;
use clustor::replication::consensus::{
    ConsensusCore, ConsensusCoreConfig, DurabilityProof, GateOperation, RaftLogEntry, RaftLogStore,
};
use clustor::replication::raft::runtime_scaffold::{
    PinFuture, RaftNodeCallbacks, RaftNodeHandle, RaftNodeScaffold,
};
use clustor::replication::raft::{
    AppendEntriesProcessor, AppendEntriesRequest, AppendEntriesResponse, ElectionController,
    PartitionQuorumConfig, RaftRouting, ReplicaId, RequestVoteRejectReason, RequestVoteRequest,
    RequestVoteResponse,
};
use clustor::replication::transport::raft::{RaftRpcHandler, RaftRpcServer};
use clustor::security::MtlsIdentityManager;
use clustor::{AckRecord, DurabilityLedger, IoMode};
use parking_lot::Mutex as ParkingMutex;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
// Public configuration
// ---------------------------------------------------------------------------

/// Configuration for starting the Clustor execution store.
pub struct ClustorConfig {
    pub node_id: String,
    pub peer_addrs: Vec<PeerAddr>,
    pub data_dir: PathBuf,
    pub retention: Duration,
    pub raft_bind: SocketAddr,
    pub tls_identity: TlsIdentity,
    pub trust_store: TlsTrustStore,
    pub trust_domain: String,
}

/// Address of a Raft peer.
#[derive(Clone)]
pub struct PeerAddr {
    pub id: String,
    pub host: String,
    pub port: u16,
}

// ---------------------------------------------------------------------------
// Raft log entry types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
enum StoreCommand {
    Retain(ExecutionSnapshot),
    Complete {
        execution_id: ExecutionId,
        outcomes: Vec<ActionOutcome>,
        allow_partial: bool,
    },
}

// ---------------------------------------------------------------------------
// ReplicaShared — consensus state (follows Quantum's pattern)
// ---------------------------------------------------------------------------

struct ReplicaShared {
    id: ReplicaId,
    id_label: String,
    ledger: Mutex<DurabilityLedger>,
    core: Mutex<ConsensusCore>,
    log: Mutex<RaftLogStore>,
    routing: RaftRouting,
    commit_index: AtomicU64,
    term: AtomicU64,
    is_leader: AtomicBool,
    election_deadline: Mutex<Instant>,
    io_mode: IoMode,
    executions: ParkingMutex<BTreeMap<ExecutionId, ExecutionSnapshot>>,
    last_applied: AtomicU64,
    retention: Duration,
}

impl ReplicaShared {
    fn record_local_ack(&self, term: u64, index: u64) {
        let record = AckRecord {
            replica: self.id.clone(),
            term,
            index,
            segment_seq: index,
            io_mode: self.io_mode,
        };
        self.record_ack(record);
    }

    fn record_remote_ack(&self, replica: impl Into<ReplicaId>, term: u64, index: u64) {
        let record = AckRecord {
            replica: replica.into(),
            term,
            index,
            segment_seq: index,
            io_mode: self.io_mode,
        };
        self.record_ack(record);
    }

    fn record_ack(&self, record: AckRecord) {
        let Ok(mut ledger) = self.ledger.lock() else {
            return;
        };
        match ledger.record_ack(record) {
            Ok(update) => {
                let quorum_index = update.quorum_index;
                self.commit_index.store(quorum_index, Ordering::SeqCst);
                if let Ok(mut core) = self.core.lock() {
                    core.mark_proof_published(DurabilityProof::new(
                        update.record.term,
                        quorum_index,
                    ));
                }
                self.apply_up_to(quorum_index);
            }
            Err(err) => {
                tracing::warn!("ERG durability ack rejected: {err}");
            }
        }
    }

    fn apply_up_to(&self, commit_index: u64) {
        let applied = self.last_applied.load(Ordering::SeqCst);
        if commit_index <= applied {
            return;
        }
        let entries = {
            let Ok(log) = self.log.lock() else { return };
            let mut buffer = Vec::new();
            log.copy_entries_in_range(applied + 1, commit_index, &mut buffer);
            buffer
        };
        apply_committed_entries(&entries, &self.executions, &self.last_applied, self.retention);
    }

    fn last_term_index(&self) -> (u64, u64) {
        let Ok(log) = self.log.lock() else {
            return (self.term.load(Ordering::SeqCst), 0);
        };
        let last_index = log.last_index();
        let term = log
            .term_at(last_index)
            .unwrap_or(self.term.load(Ordering::SeqCst));
        (term, last_index)
    }

    fn guard_read(&self) -> Result<(), ExecutionStoreError> {
        let Ok(mut core) = self.core.lock() else {
            return Err(ExecutionStoreError::Unavailable {
                reason: "consensus core lock poisoned".into(),
            });
        };
        core.guard(GateOperation::ReadIndex).map_err(|violation| {
            ExecutionStoreError::Unavailable {
                reason: format!("read gate: {violation:?}"),
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Peer client
// ---------------------------------------------------------------------------

struct PeerClient {
    id: ReplicaId,
    client: AsyncRaftNetworkClient,
}

// ---------------------------------------------------------------------------
// ReplicaCallbacks — drives RaftNodeScaffold
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct ReplicaCallbacks {
    shared: Arc<ReplicaShared>,
    clients: Arc<Vec<PeerClient>>,
}

impl ReplicaCallbacks {
    fn new(shared: Arc<ReplicaShared>, clients: Arc<Vec<PeerClient>>) -> Self {
        let now = Instant::now();
        if let Ok(mut deadline) = shared.election_deadline.lock() {
            *deadline = now + Duration::from_millis(50);
        }
        Self { shared, clients }
    }

    async fn heartbeat(&self) {
        let term = self.shared.term.load(Ordering::SeqCst);
        let (prev_term, prev_index) = self.shared.last_term_index();
        let leader_commit = self.shared.commit_index.load(Ordering::SeqCst);
        let request = AppendEntriesRequest {
            term,
            leader_id: self.shared.id_label.clone(),
            prev_log_index: prev_index,
            prev_log_term: prev_term,
            leader_commit,
            entries: Vec::new(),
            routing: self.shared.routing.clone(),
        };
        for peer in self.clients.iter() {
            if peer.id == self.shared.id {
                continue;
            }
            match peer
                .client
                .append_entries(request.clone(), Instant::now())
                .await
            {
                Ok(AppendEntriesResponse {
                    success: true,
                    match_index,
                    term: peer_term,
                    ..
                }) => {
                    self.shared.record_remote_ack(
                        peer.id.clone(),
                        peer_term.max(term),
                        match_index.max(leader_commit),
                    );
                }
                Ok(_) => {}
                Err(err) => {
                    tracing::debug!(peer = ?peer.id, error = %err, "ERG heartbeat failed");
                }
            }
        }
    }
}

impl RaftNodeCallbacks for ReplicaCallbacks {
    fn on_leader_heartbeat(&self) -> PinFuture<()> {
        let this = self.clone();
        Box::pin(async move {
            this.heartbeat().await;
        })
    }

    fn on_start_election(&self) -> PinFuture<()> {
        let this = self.clone();
        Box::pin(async move {
            this.shared.is_leader.store(true, Ordering::SeqCst);
            this.shared.term.fetch_add(1, Ordering::SeqCst);
            tracing::info!(
                node = %this.shared.id_label,
                term = this.shared.term.load(Ordering::SeqCst),
                "ERG node became leader"
            );
            this.heartbeat().await;
        })
    }

    fn is_leader(&self) -> bool {
        self.shared.is_leader.load(Ordering::SeqCst)
    }

    fn schedule_deadline(&self, now: Instant, timeout: Duration) {
        if let Ok(mut deadline) = self.shared.election_deadline.lock() {
            *deadline = now + timeout;
        }
    }

    fn election_deadline(&self) -> Instant {
        self.shared
            .election_deadline
            .lock()
            .map(|guard| *guard)
            .unwrap_or_else(|_| Instant::now())
    }
}

// ---------------------------------------------------------------------------
// RPC handler
// ---------------------------------------------------------------------------

struct ErgRpcHandler {
    shared: Arc<ReplicaShared>,
}

impl RaftRpcHandler for ErgRpcHandler {
    fn on_request_vote(&mut self, request: RequestVoteRequest) -> RequestVoteResponse {
        let current_term = self.shared.term.load(Ordering::SeqCst);
        if request.term < current_term {
            return RequestVoteResponse {
                term: current_term,
                granted: false,
                reject_reason: Some(RequestVoteRejectReason::TermOutOfDate),
            };
        }
        if request.term > current_term {
            self.shared.term.store(request.term, Ordering::SeqCst);
            self.shared.is_leader.store(false, Ordering::SeqCst);
        }
        let (last_term, last_index) = self.shared.last_term_index();
        let up_to_date = request.last_log_term > last_term
            || (request.last_log_term == last_term && request.last_log_index >= last_index);
        if !up_to_date {
            return RequestVoteResponse {
                term: self.shared.term.load(Ordering::SeqCst),
                granted: false,
                reject_reason: Some(RequestVoteRejectReason::LogBehind),
            };
        }
        self.shared.is_leader.store(false, Ordering::SeqCst);
        if let Ok(mut deadline) = self.shared.election_deadline.lock() {
            *deadline = Instant::now() + Duration::from_millis(300);
        }
        RequestVoteResponse {
            term: self.shared.term.load(Ordering::SeqCst),
            granted: true,
            reject_reason: None,
        }
    }

    fn on_append_entries(&mut self, request: AppendEntriesRequest) -> AppendEntriesResponse {
        let current_term = self.shared.term.load(Ordering::SeqCst);
        if request.term < current_term {
            return AppendEntriesResponse {
                term: current_term,
                success: false,
                match_index: 0,
                conflict_index: Some(request.prev_log_index),
                conflict_term: None,
            };
        }
        if request.term > current_term {
            self.shared.term.store(request.term, Ordering::SeqCst);
        }
        self.shared.is_leader.store(false, Ordering::SeqCst);
        if let Ok(mut deadline) = self.shared.election_deadline.lock() {
            *deadline = Instant::now() + Duration::from_millis(300);
        }

        let Ok(mut log) = self.shared.log.lock() else {
            return AppendEntriesResponse {
                term: self.shared.term.load(Ordering::SeqCst),
                success: false,
                match_index: 0,
                conflict_index: None,
                conflict_term: None,
            };
        };
        let mut processor = AppendEntriesProcessor::new(&mut log);
        let outcome = match processor.apply(&request) {
            Ok(result) => result,
            Err(err) => {
                tracing::error!("ERG append_entries log error: {err}");
                return AppendEntriesResponse {
                    term: self.shared.term.load(Ordering::SeqCst),
                    success: false,
                    match_index: 0,
                    conflict_index: None,
                    conflict_term: None,
                };
            }
        };
        let match_index = outcome.match_index;
        drop(log);

        if outcome.success {
            let commit_index = request.leader_commit.min(match_index);
            if commit_index > 0 {
                self.shared
                    .commit_index
                    .store(commit_index, Ordering::SeqCst);
                if let Ok(mut core) = self.shared.core.lock() {
                    core.mark_proof_published(DurabilityProof::new(request.term, commit_index));
                }
                self.shared.apply_up_to(commit_index);
            }
        }

        outcome.to_response(self.shared.term.load(Ordering::SeqCst))
    }
}

// ---------------------------------------------------------------------------
// State machine apply
// ---------------------------------------------------------------------------

fn apply_committed_entries(
    entries: &[RaftLogEntry],
    executions: &ParkingMutex<BTreeMap<ExecutionId, ExecutionSnapshot>>,
    last_applied: &AtomicU64,
    retention: Duration,
) {
    if entries.is_empty() {
        return;
    }
    let mut applied = last_applied.load(Ordering::SeqCst);
    let mut guard = executions.lock();

    for entry in entries {
        if entry.index <= applied {
            continue;
        }
        match serde_json::from_slice::<StoreCommand>(&entry.payload) {
            Ok(StoreCommand::Retain(snapshot)) => {
                guard.insert(snapshot.execution_id.clone(), snapshot);
            }
            Ok(StoreCommand::Complete {
                execution_id,
                outcomes,
                allow_partial,
            }) => {
                if let Some(snap) = guard.get_mut(&execution_id) {
                    snap.status = ExecutionStatus::from_outcomes(&outcomes, allow_partial);
                    snap.completed_at = Some(SystemTime::now());
                    snap.outcomes = outcomes;
                }
            }
            Err(err) => {
                tracing::error!(
                    index = entry.index,
                    "ERG failed to decode store command: {err}"
                );
            }
        }
        applied = entry.index;
    }

    // Evict expired entries
    if !retention.is_zero() {
        let cutoff = SystemTime::now()
            .checked_sub(retention)
            .unwrap_or(UNIX_EPOCH);
        let expired: Vec<ExecutionId> = guard
            .iter()
            .take_while(|(_, s)| s.created_at < cutoff)
            .map(|(id, _)| id.clone())
            .collect();
        for key in expired {
            guard.remove(&key);
        }
    }

    last_applied.store(applied, Ordering::SeqCst);
}

fn erg_routing() -> RaftRouting {
    RaftRouting::alias("chronicle-erg", 1)
}

// ---------------------------------------------------------------------------
// Public API: ClustorExecutionStore
// ---------------------------------------------------------------------------

/// Raft-backed execution store using embedded Clustor.
pub struct ClustorExecutionStore {
    shared: Arc<ReplicaShared>,
    clients: Arc<Vec<PeerClient>>,
    _timers: Option<RaftNodeHandle>,
    _server: AsyncRaftNetworkServerHandle,
}

impl ClustorExecutionStore {
    /// Bootstrap or join the Execution Raft Group.
    pub async fn start(config: ClustorConfig) -> Result<Self, ExecutionStoreError> {
        // Open WAL
        std::fs::create_dir_all(&config.data_dir).map_err(|e| {
            ExecutionStoreError::Unavailable {
                reason: format!("create data dir: {e}"),
            }
        })?;
        let log = RaftLogStore::open(config.data_dir.join("raft.log")).map_err(|e| {
            ExecutionStoreError::Unavailable {
                reason: format!("open raft log: {e}"),
            }
        })?;

        // Recover state
        let executions = ParkingMutex::new(BTreeMap::new());
        let last_applied = AtomicU64::new(0);
        let existing = log.entries_from(1);
        apply_committed_entries(&existing, &executions, &last_applied, config.retention);
        tracing::info!(
            node = %config.node_id,
            recovered = existing.len(),
            "ERG recovered log entries"
        );

        // Durability ledger
        let quorum_size = config.peer_addrs.len() + 1;
        let mut ledger = DurabilityLedger::new(PartitionQuorumConfig::new(quorum_size));
        let local_id = ReplicaId::new(config.node_id.clone());
        ledger.register_replica(local_id.clone());
        for addr in &config.peer_addrs {
            ledger.register_replica(ReplicaId::new(addr.id.clone()));
        }

        // Consensus core
        let core = ConsensusCore::new(ConsensusCoreConfig::for_profile(PartitionProfile::Latency));

        let shared = Arc::new(ReplicaShared {
            id: local_id,
            id_label: config.node_id.clone(),
            ledger: Mutex::new(ledger),
            core: Mutex::new(core),
            log: Mutex::new(log),
            routing: erg_routing(),
            commit_index: AtomicU64::new(0),
            term: AtomicU64::new(1),
            is_leader: AtomicBool::new(false),
            election_deadline: Mutex::new(Instant::now()),
            io_mode: IoMode::Strict,
            executions,
            last_applied,
            retention: config.retention,
        });

        // Build peer clients
        let mut clients = Vec::new();
        for addr in &config.peer_addrs {
            let mut mtls = MtlsIdentityManager::new(
                config.tls_identity.certificate.clone(),
                config.trust_domain.clone(),
                Duration::from_secs(600),
                Instant::now(),
            );
            mtls.rotate(Instant::now()).ok();

            let client = AsyncRaftNetworkClient::with_options(
                AsyncRaftTransportClientConfig {
                    host: addr.host.clone(),
                    port: addr.port,
                    identity: config.tls_identity.clone(),
                    trust_store: config.trust_store.clone(),
                    mtls: Arc::new(parking_lot::Mutex::new(mtls)),
                },
                AsyncRaftTransportClientOptions::default()
                    .pool_size_per_peer_max(2)
                    .pool_warmup(true)
                    .peer_node_id(addr.id.clone()),
            )
            .map_err(|e| ExecutionStoreError::Unavailable {
                reason: format!("build peer client for {}: {e}", addr.id),
            })?;

            clients.push(PeerClient {
                id: ReplicaId::new(addr.id.clone()),
                client,
            });
        }
        let clients = Arc::new(clients);

        // Scaffold callbacks + timers
        let callbacks = Arc::new(ReplicaCallbacks::new(shared.clone(), clients.clone()));
        let controller = ElectionController::for_partition_profile(
            PartitionProfile::Latency,
            rand::random::<u64>(),
        );
        let heartbeat_interval = controller.heartbeat_interval().max(Duration::from_millis(50));
        let timers = RaftNodeScaffold::new(
            callbacks,
            controller,
            heartbeat_interval,
            config.node_id.clone(),
        )
        .spawn();

        // Raft RPC server
        let mut server_mtls = MtlsIdentityManager::new(
            config.tls_identity.certificate.clone(),
            config.trust_domain.clone(),
            Duration::from_secs(600),
            Instant::now(),
        );
        server_mtls.rotate(Instant::now()).ok();

        let rpc_server = RaftRpcServer::new(
            server_mtls,
            ErgRpcHandler {
                shared: shared.clone(),
            },
            erg_routing(),
        );

        let server = AsyncRaftNetworkServer::spawn(
            AsyncRaftTransportServerConfig {
                bind: config.raft_bind,
                identity: config.tls_identity,
                trust_store: config.trust_store,
            },
            rpc_server,
        )
        .await
        .map_err(|e| ExecutionStoreError::Unavailable {
            reason: format!("start ERG server: {e}"),
        })?;

        tracing::info!(
            node = %config.node_id,
            bind = %config.raft_bind,
            peers = config.peer_addrs.len(),
            "ERG node started"
        );

        Ok(Self {
            shared,
            clients,
            _timers: Some(timers),
            _server: server,
        })
    }

    /// Propose a command to the Raft log. Appends locally, replicates to peers,
    /// and returns once the local ACK is recorded. Quorum commitment happens
    /// asynchronously via heartbeat replication.
    async fn propose(&self, command: &StoreCommand) -> Result<(), ExecutionStoreError> {
        if !self.shared.is_leader.load(Ordering::SeqCst) {
            return Err(ExecutionStoreError::Unavailable {
                reason: "not leader".into(),
            });
        }
        let payload =
            serde_json::to_vec(command).map_err(|e| ExecutionStoreError::Serialization {
                reason: e.to_string(),
            })?;
        let term = self.shared.term.load(Ordering::SeqCst);
        let (prev_term, prev_index) = self.shared.last_term_index();
        let next_index = {
            let Ok(mut log) = self.shared.log.lock() else {
                return Err(ExecutionStoreError::Unavailable {
                    reason: "log lock poisoned".into(),
                });
            };
            let idx = log.last_index().saturating_add(1);
            let entry = RaftLogEntry::new(term, idx, payload);
            log.append(entry)
                .map_err(|e| ExecutionStoreError::Unavailable {
                    reason: format!("log append: {e}"),
                })?;
            idx
        };

        // Record local ACK — this updates commit_index and triggers apply
        self.shared.record_local_ack(term, next_index);

        // Replicate to peers (best-effort, quorum achieved via heartbeat)
        let leader_commit = self.shared.commit_index.load(Ordering::SeqCst);
        let entries = {
            let Ok(log) = self.shared.log.lock() else {
                return Ok(());
            };
            log.entry(next_index)
                .ok()
                .flatten()
                .into_iter()
                .collect::<Vec<_>>()
        };
        let request = AppendEntriesRequest {
            term,
            leader_id: self.shared.id_label.clone(),
            prev_log_index: prev_index,
            prev_log_term: prev_term,
            leader_commit,
            entries,
            routing: self.shared.routing.clone(),
        };
        for peer in self.clients.iter() {
            if peer.id == self.shared.id {
                continue;
            }
            match peer
                .client
                .append_entries(request.clone(), Instant::now())
                .await
            {
                Ok(AppendEntriesResponse {
                    success: true,
                    match_index,
                    term: peer_term,
                    ..
                }) => {
                    self.shared.record_remote_ack(
                        peer.id.clone(),
                        peer_term.max(term),
                        match_index.max(leader_commit),
                    );
                }
                Ok(_) => {}
                Err(err) => {
                    tracing::debug!(peer = ?peer.id, error = %err, "ERG replication failed");
                }
            }
        }

        Ok(())
    }
}

impl ExecutionStore for ClustorExecutionStore {
    fn retain(
        &self,
        snapshot: ExecutionSnapshot,
    ) -> Pin<Box<dyn Future<Output = Result<(), ExecutionStoreError>> + Send + '_>> {
        let command = StoreCommand::Retain(snapshot);
        Box::pin(async move { self.propose(&command).await })
    }

    fn complete(
        &self,
        id: &ExecutionId,
        outcomes: Vec<ActionOutcome>,
        allow_partial: bool,
    ) -> Pin<Box<dyn Future<Output = Result<(), ExecutionStoreError>> + Send + '_>> {
        let command = StoreCommand::Complete {
            execution_id: id.clone(),
            outcomes,
            allow_partial,
        };
        Box::pin(async move { self.propose(&command).await })
    }

    fn get(
        &self,
        id: &ExecutionId,
    ) -> Pin<
        Box<dyn Future<Output = Result<Option<ExecutionSnapshot>, ExecutionStoreError>> + Send + '_>,
    > {
        let id = id.clone();
        Box::pin(async move {
            self.shared.guard_read()?;
            let guard = self.shared.executions.lock();
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
            self.shared.guard_read()?;
            let guard = self.shared.executions.lock();
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
