// The log-index activation barrier — deciding WHEN a new version starts serving,
// and what happens to the one it replaces.
//
// Split out from `activation_core` because it shares nothing with it. Activation
// planning validates an artefact against a node's holdings and needs the whole
// protobuf reader to do it; the barrier is pure log-index bookkeeping over two
// digests. Anything that stages a version needs the barrier — the release
// control plane does, and has no business pulling in artefact validation to get
// it.
//
// Self-contained: no other core required.

/// Log-index activation barrier with version coexistence and rollback.
///
/// The other half of activation: `plan_activation` decides whether a module MAY
/// run; this decides WHEN it starts running, and what happens to the version it
/// replaces.
///
/// The barrier exists because activation is not instantaneous across a
/// replicated log. Reads before the barrier index must still resolve to the old
/// version and reads at or after it to the new one, or two nodes replaying the
/// same log would disagree about which code served a given index — the same
/// determinism requirement the rest of this system is built on.
///
/// Version COEXISTENCE is why `draining` exists rather than the old digest being
/// dropped at commit: in-flight work started under the previous version must
/// still be able to resolve it. `rollback` reverts to that draining version,
/// which is why it is only possible BEFORE `drain` — once the old version is
/// gone there is nothing to go back to, and the honest answer is `false` rather
/// than a silent no-op.
///
/// Digests are fixed 32-byte sha256, so the whole barrier is `Copy` and lives in
/// module state with no allocator.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ActivationBarrier {
    active: Option<[u8; 32]>,
    pending: Option<(u64, [u8; 32])>,
    draining: Option<[u8; 32]>,
}

impl ActivationBarrier {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the currently active version without staging (initial load).
    pub fn set_active(&mut self, digest: [u8; 32]) {
        self.active = Some(digest);
    }

    /// Stage a pending activation to take effect at `barrier_index`.
    pub fn stage(&mut self, barrier_index: u64, digest: [u8; 32]) {
        self.pending = Some((barrier_index, digest));
    }

    /// The digest a read at `log_index` resolves to: the pending version at or
    /// past the barrier, the active one before it.
    pub fn select(&self, log_index: u64) -> Option<[u8; 32]> {
        if let Some((barrier, digest)) = self.pending {
            if log_index >= barrier {
                return Some(digest);
            }
        }
        self.active
    }

    /// Atomically activate the pending version once `applied_index` reaches the
    /// barrier. The prior active version moves to draining — still routable for
    /// work already in flight. Returns whether anything activated.
    pub fn commit(&mut self, applied_index: u64) -> bool {
        if let Some((barrier, digest)) = self.pending {
            if applied_index >= barrier {
                self.draining = self.active;
                self.active = Some(digest);
                self.pending = None;
                return true;
            }
        }
        false
    }

    /// Revert a committed activation while the previous version is still
    /// draining. Returns false when there is nothing to revert to.
    pub fn rollback(&mut self) -> bool {
        if let Some(prev) = self.draining.take() {
            self.active = Some(prev);
            true
        } else {
            false
        }
    }

    /// Drop the draining version — drain complete, rollback no longer possible.
    pub fn drain(&mut self) {
        self.draining = None;
    }

    pub fn active(&self) -> Option<[u8; 32]> {
        self.active
    }

    pub fn draining(&self) -> Option<[u8; 32]> {
        self.draining
    }

    pub fn pending(&self) -> Option<(u64, [u8; 32])> {
        self.pending
    }
}
