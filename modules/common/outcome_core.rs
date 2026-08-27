// Bounded, no_std, no-alloc OUTCOME vocabulary — the single truthful-disposition
// taxonomy every steady-state Chronicle module maps its result to. `include!`d by
// the host crate (tests) and every `.fmod`, so device and host name outcomes
// identically.
//
// This is NOT a human error string and carries no unbounded labels. Each field is
// a closed `#[repr(u8)]` enum so an outcome is four bytes, cheap to store in module
// state and to turn into a statically-interned instrument suffix
// (e.g. `rejected_frame_malformed`). A wrapper maps its core's precise error enum
// to an `Outcome` with an EXHAUSTIVE match, so a new core variant cannot silently
// collapse into `Internal`.

/// What kind of disposition a record reached. Decides retain-vs-dispose.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Class {
    /// Compute, output and state obligations all completed.
    Success = 0,
    /// A deliberate filter/drop. Terminal and counted; never output pressure.
    PolicyDrop = 1,
    /// A complete input unit refused before admission (bad data).
    InputReject = 2,
    /// A transient resource condition; the work is RETAINED and retried.
    ResourceWait = 3,
    /// A dependency (provider/channel peer) failed in a classifiable way.
    DependencyError = 4,
    /// A module/configuration invariant that refuses input until fixed.
    ConfigurationFault = 5,
    /// An internal invariant break — a bug, not an input or a dependency.
    InvariantFault = 6,
}

/// Where in the record lifecycle the outcome was produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Phase {
    Frame = 0,
    Decode = 1,
    Select = 2,
    Evaluate = 3,
    Encode = 4,
    Deliver = 5,
    Checkpoint = 6,
    Barrier = 7,
    Reload = 8,
    Store = 9,
    Activate = 10,
}

/// The closed reason enum. No open-ended text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Reason {
    /// Well-formed framing, ill-formed content.
    Malformed = 0,
    /// Exceeds a declared size/count bound; rejected rather than truncated.
    TooLarge = 1,
    /// A recognised-but-unimplemented opcode/suite/profile.
    Unsupported = 2,
    /// The evaluator's fuel ceiling was reached.
    CostExceeded = 3,
    /// A fixed capacity (lanes, panes, versions, ring) is full.
    Capacity = 4,
    /// A dependency exists but cannot serve right now.
    Unavailable = 5,
    /// A required item is absent (fault or expected, per operation).
    NotFound = 6,
    /// A single-writer/precondition conflict.
    Conflict = 7,
    /// Stored bytes failed an integrity check.
    Corrupt = 8,
    /// Content did not match its content address.
    DigestMismatch = 9,
    /// The channel could not accept the write yet (`EAGAIN`); retain and retry.
    WouldBlock = 10,
    /// A capability/authorisation refusal.
    Permission = 11,
    /// A lower-level I/O error.
    Io = 12,
    /// The active generation moved under a staged operation.
    StaleGeneration = 13,
    /// An unmapped internal condition — should be unreachable in shipping code.
    Internal = 14,
}

/// A complete disposition. `Copy`, four bytes. Bounded context (rule/stage index,
/// generation, provider op) is carried separately by the wrapper where available;
/// it is deliberately not embedded here so `Outcome` stays a metric-friendly key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Outcome {
    pub class: Class,
    pub phase: Phase,
    pub reason: Reason,
}

impl Outcome {
    #[inline]
    pub const fn new(class: Class, phase: Phase, reason: Reason) -> Self {
        Self {
            class,
            phase,
            reason,
        }
    }

    /// A success at a phase (reason is `Internal` as a non-signal placeholder;
    /// callers read `class` for success, not `reason`).
    #[inline]
    pub const fn ok(phase: Phase) -> Self {
        Self::new(Class::Success, phase, Reason::Internal)
    }

    /// Whether the driver should RETAIN the work and retry rather than dispose of
    /// it. Only a transient wait is retryable; every other class is terminal for
    /// this input. `(operation, reason)` refinement (e.g. `NotFound` fatal for a
    /// pinned artefact, expected for an optional read) is decided by the caller
    /// that owns the operation, not globally here.
    #[inline]
    pub const fn is_retryable(&self) -> bool {
        matches!(self.class, Class::ResourceWait)
    }

    /// Whether this outcome should move the module to `Mode::Faulted`.
    #[inline]
    pub const fn is_fault(&self) -> bool {
        matches!(
            self.class,
            Class::ConfigurationFault | Class::InvariantFault
        )
    }
}

/// The current operating mode a module advertises as a gauge. A blocked
/// module retains its work and names the blocking phase; `AwaitingConfig` is NOT
/// `Faulted`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Mode {
    AwaitingConfig = 0,
    Ready = 1,
    OutputBlocked = 2,
    DependencyBlocked = 3,
    StagingReload = 4,
    BarrierWait = 5,
    Draining = 6,
    Faulted = 7,
}

impl Class {
    #[inline]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
    pub const fn name(self) -> &'static str {
        match self {
            Class::Success => "success",
            Class::PolicyDrop => "policy_drop",
            Class::InputReject => "input_reject",
            Class::ResourceWait => "resource_wait",
            Class::DependencyError => "dependency_error",
            Class::ConfigurationFault => "configuration_fault",
            Class::InvariantFault => "invariant_fault",
        }
    }
}

impl Phase {
    #[inline]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
    pub const fn name(self) -> &'static str {
        match self {
            Phase::Frame => "frame",
            Phase::Decode => "decode",
            Phase::Select => "select",
            Phase::Evaluate => "evaluate",
            Phase::Encode => "encode",
            Phase::Deliver => "deliver",
            Phase::Checkpoint => "checkpoint",
            Phase::Barrier => "barrier",
            Phase::Reload => "reload",
            Phase::Store => "store",
            Phase::Activate => "activate",
        }
    }
}

impl Reason {
    #[inline]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
    pub const fn name(self) -> &'static str {
        match self {
            Reason::Malformed => "malformed",
            Reason::TooLarge => "too_large",
            Reason::Unsupported => "unsupported",
            Reason::CostExceeded => "cost_exceeded",
            Reason::Capacity => "capacity",
            Reason::Unavailable => "unavailable",
            Reason::NotFound => "not_found",
            Reason::Conflict => "conflict",
            Reason::Corrupt => "corrupt",
            Reason::DigestMismatch => "digest_mismatch",
            Reason::WouldBlock => "would_block",
            Reason::Permission => "permission",
            Reason::Io => "io",
            Reason::StaleGeneration => "stale_generation",
            Reason::Internal => "internal",
        }
    }
}

impl Mode {
    #[inline]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
    pub const fn name(self) -> &'static str {
        match self {
            Mode::AwaitingConfig => "awaiting_config",
            Mode::Ready => "ready",
            Mode::OutputBlocked => "output_blocked",
            Mode::DependencyBlocked => "dependency_blocked",
            Mode::StagingReload => "staging_reload",
            Mode::BarrierWait => "barrier_wait",
            Mode::Draining => "draining",
            Mode::Faulted => "faulted",
        }
    }
}
