// Bounded, no_std, no-alloc AGGREGATION core. Like `core.rs` and
// `pipeline_core.rs` it carries NO inner attributes and NO test module, so it is
// `include!`d verbatim by both this crate (`lib.rs`) and the on-device Fluxor
// module (`modules/app/aggregation/mod.rs`) — one source of truth, host and
// device.
//
// This is the on-device aggregation engine (spec artefact 5),
// reduced to what a bounded, allocation-free runtime can hold:
//   * event-time TUMBLING and SLIDING windows (pane-aligned: size + step) with a
//     guard watermark (max_event_time - lateness) and on-watermark finalization;
//   * MULTIPLE concurrent open panes per lane (a fixed pane array), so
//     out-of-order events land in the correct window rather than eagerly rolling;
//   * KEYED lanes with bounded cardinality (a fixed lane array; a new key past
//     the ceiling is dropped + audited);
//   * the fixed-size monoid operators COUNT/SUM/MIN/MAX/AVG (all retractable,
//     each a couple of i64s), plus the COLLECTION operators DISTINCT/TOPK/
//     QUANTILE over a bounded sorted multiset (`Coll`). The collection operators
//     are NON-RETRACTABLE: a late correction freezes them and audits the skipped
//     value (`non_retractable_drops`) rather than mutating an answer already
//     reported.
//   * RETRACTABLE CORRECTIONS: a late event within `correction_horizon` of the
//     watermark re-folds into its finalized (still-retained) pane and re-emits;
//     beyond the horizon (or once the pane is reclaimed) it is dropped + audited.
//   * a typed EMIT: the finished state is projected through checked-CEL bytecode
//     over a synthesized `ctx = {key, state, window}` message.
//
// Bounded-state contract (deterministic): each lane holds at most `MAX_PANES`
// live panes (open or finalized-within-horizon); a finalized pane is reclaimed
// once its end falls below `watermark - correction_horizon` and can receive no
// more corrections. A per-lane `finalized_high` remembers the highest finalized
// window start, so a late event for a reclaimed window is dropped, never
// reopened. `windows_for` matches the host pane assignment exactly.

/// Maximum distinct keys (lanes) held at once — the bounded-cardinality ceiling.
pub const MAX_LANES: usize = 16;
/// Maximum operators in one aggregation.
pub const MAX_OPS: usize = 8;
/// Maximum live panes (open + finalized-within-horizon windows) per lane.
pub const MAX_PANES: usize = 8;
/// Maximum windows one event can belong to (sliding-window overlap bound).
pub const MAX_WIN_PER_EVENT: usize = 8;
/// Maximum length of a byte-string partition key.
pub const KEY_CAP: usize = 48;

/// An aggregation operator.
///
/// The first five are fixed-size monoids: their whole accumulator is two `i64`s,
/// so they cost nothing beyond `Acc`. The last three are COLLECTION operators —
/// they are defined over the multiset of selected values, not a running scalar,
/// and so need per-pane storage. On device that storage is bounded (`COLL_CAP`
/// values per pane) and drawn from a fixed pool; see `Coll`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggOp {
    Count,
    Sum,
    Min,
    Max,
    Avg,
    /// Cardinality of the distinct selected values.
    Distinct,
    /// Sum of the `k` largest selected values.
    TopK(u16),
    /// Nearest-rank quantile at `permille`/1000 over the selected values.
    Quantile(u16),
}

impl AggOp {
    /// Whether this operator accumulates a multiset rather than a scalar, and so
    /// needs a `Coll` cell.
    pub fn is_collection(&self) -> bool {
        matches!(self, AggOp::Distinct | AggOp::TopK(_) | AggOp::Quantile(_))
    }
}

/// The checkpoint format version. Exactly one is written and exactly one is
/// accepted — a snapshot that is not this version is refused, not half-understood.
///
/// There is one format, and it is the current one. The byte exists to reject a
/// foreign or corrupt snapshot, NOT to carry old shapes forward: when the format
/// changes, this stays 1 and the old shape is deleted.
pub const CKPT_VER: u8 = 1;

/// How many selected values one collection cell retains.
///
/// This is the device's bounded-state ceiling for Distinct/TopK/Quantile, which
/// are mathematically unbounded. It is deliberately small: the pool is a fixed
/// `MAX_LANES × MAX_PANES` array, so every value here costs 1 KiB of module
/// state. Saturation is COUNTED (`coll_overflows`), never silently absorbed — an
/// operator whose result stopped being exact says so.
pub const COLL_CAP: usize = 16;

/// A bounded, sorted-ascending multiset of `i64` — the single structure behind
/// all three collection operators. Distinct inserts uniquely and reports its
/// length; TopK keeps the `k` largest and reports their sum; Quantile keeps the
/// sample and reports a nearest-rank pick. Kept sorted on insert so every read
/// is a direct index and no operator needs a second pass.
#[derive(Debug, Clone, Copy)]
struct Coll {
    vals: [i64; COLL_CAP],
    len: u8,
}

impl Coll {
    fn empty() -> Self {
        Coll {
            vals: [0; COLL_CAP],
            len: 0,
        }
    }

    /// Index of the first element `>= v` (lower bound) in `vals[..len]`.
    fn lower_bound(&self, v: i64) -> usize {
        let (mut lo, mut hi) = (0usize, self.len as usize);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.vals[mid] < v {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// Insert `v` at `at`, shifting the tail right. Caller guarantees room.
    fn insert_at(&mut self, at: usize, v: i64) {
        let mut i = self.len as usize;
        while i > at {
            self.vals[i] = self.vals[i - 1];
            i -= 1;
        }
        self.vals[at] = v;
        self.len += 1;
    }

    /// Fold one selected value. Returns `false` when the cell was already full
    /// and the value could not be retained exactly (the caller counts it).
    fn fold(&mut self, kind: AggOp, v: i64) -> bool {
        let at = self.lower_bound(v);
        match kind {
            AggOp::Distinct => {
                // Already present: idempotent, and never an overflow.
                if at < self.len as usize && self.vals[at] == v {
                    return true;
                }
                if self.len as usize == COLL_CAP {
                    return false;
                }
                self.insert_at(at, v);
                true
            }
            AggOp::TopK(k) => {
                // Only the k largest matter, so a full cell can still accept a
                // value exactly — by evicting the smallest, which TopK would
                // have discarded anyway. Exact whenever k <= COLL_CAP.
                let cap = if (k as usize) < COLL_CAP {
                    k as usize
                } else {
                    COLL_CAP
                };
                if cap == 0 {
                    return true;
                }
                if self.len as usize == cap {
                    if v <= self.vals[0] {
                        return k as usize <= COLL_CAP;
                    }
                    // Drop the smallest, then insert into the vacated order.
                    let mut i = 0;
                    while i + 1 < self.len as usize {
                        self.vals[i] = self.vals[i + 1];
                        i += 1;
                    }
                    self.len -= 1;
                    let at = self.lower_bound(v);
                    self.insert_at(at, v);
                    return k as usize <= COLL_CAP;
                }
                self.insert_at(at, v);
                k as usize <= COLL_CAP
            }
            AggOp::Quantile(_) => {
                if self.len as usize == COLL_CAP {
                    return false;
                }
                self.insert_at(at, v);
                true
            }
            _ => true,
        }
    }

    fn value(&self, kind: AggOp) -> i64 {
        let n = self.len as usize;
        match kind {
            AggOp::Distinct => n as i64,
            AggOp::TopK(k) => {
                // `vals` is ascending and already truncated to k, so this is the
                // sum of the k largest.
                let mut s: i64 = 0;
                let mut i = 0;
                while i < n {
                    s = s.saturating_add(self.vals[i]);
                    i += 1;
                }
                let _ = k;
                s
            }
            AggOp::Quantile(permille) => {
                if n == 0 {
                    return 0;
                }
                // Nearest-rank: rank = ceil(p/1000 * n), 1-based, clamped to n.
                let nn = n as u64;
                let p = permille as u64;
                let scaled = p.saturating_mul(nn);
                // Nearest-rank: round UP, so p100 lands on the maximum rather
                // than one short of it.
                let mut rank = scaled / 1000;
                if !scaled.is_multiple_of(1000) {
                    rank += 1;
                }
                if rank < 1 {
                    rank = 1;
                }
                if rank > nn {
                    rank = nn;
                }
                self.vals[(rank - 1) as usize]
            }
            _ => 0,
        }
    }
}

/// When a window emits. `OnClose` (the default) fires once, when the event-time
/// watermark closes the window — the low-volume, exactly-final policy. `Continuous`
/// ALSO fires the pane's current partial state after every event folded into it
/// (early firing) — low-latency incremental results, a per-event firing policy;
/// the on-close final still fires. An explicit trigger model, not a hardcoded
/// policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmitTrigger {
    OnClose,
    Continuous,
    /// Fire a pane's running partial every `n` events folded into it since its
    /// last emit (count-based / processing-position firing). `n` is clamped to
    /// at least 1 so it always makes progress.
    OnCount(u32),
    /// Flush ALL live panes every `period` events ingested by the operator — a
    /// deterministic logical processing-time timer (one tick per event, not
    /// wall-clock), bounding emit latency regardless of key distribution.
    /// `period` is clamped to at least 1.
    OnProcessing(u32),
}

impl EmitTrigger {
    /// Decode the def container's trailing trigger bytes. Kind byte: `0`/absent =
    /// OnClose, `1` = Continuous, `2` = OnCount and `3` = OnProcessing, each
    /// followed by `[count:u32 LE]`. Any unknown kind — or a truncated count — is
    /// `OnClose`, so a container written before triggers existed (no trailing
    /// bytes) keeps the original semantics. Panic-free over an arbitrary tail.
    pub fn decode(tail: &[u8]) -> Self {
        let u32_at1 = || {
            tail.get(1..5)
                .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
        };
        match tail.first() {
            Some(1) => EmitTrigger::Continuous,
            Some(2) => match u32_at1() {
                Some(n) => EmitTrigger::OnCount(n.max(1)),
                None => EmitTrigger::OnClose,
            },
            Some(3) => match u32_at1() {
                Some(n) => EmitTrigger::OnProcessing(n.max(1)),
                None => EmitTrigger::OnClose,
            },
            _ => EmitTrigger::OnClose,
        }
    }
}

/// One operator's definition: its kind plus the value-selector bytecode
/// (evaluated over the event; COUNT ignores it).
#[derive(Debug, Clone, Copy)]
pub struct OpSpec<'a> {
    pub kind: AggOp,
    pub selector: &'a [u8],
    pub sel_cost: u64,
}

/// A complete aggregation definition. `key`/`time`/operator selectors are checked
/// CEL over the event; `emit` is checked CEL constructing the output over `ctx`.
pub struct AggSpec<'a> {
    pub window_size: i64,
    /// Pane stride for sliding windows; `0` (or `== window_size`) is tumbling.
    pub window_step: i64,
    pub lateness: i64,
    /// Allowed lateness for retractable corrections after a window finalizes: a
    /// late event whose time is `>= watermark - correction_horizon` re-folds into
    /// the finalized window and re-emits. `0` disables corrections (late = drop).
    pub correction_horizon: i64,
    pub key_code: &'a [u8],
    pub key_cost: u64,
    pub time_code: &'a [u8],
    pub time_cost: u64,
    pub ops: &'a [OpSpec<'a>],
    pub emit_code: &'a [u8],
    pub emit_cost: u64,
    /// Bounded-cardinality ceiling; the effective lane cap is
    /// `min(MAX_LANES, max_lanes)`.
    pub max_lanes: u32,
    /// When windows emit (default `OnClose`). `Continuous` adds early per-event
    /// firing of the touched panes' partial state.
    pub emit_trigger: EmitTrigger,
}

impl AggSpec<'_> {
    /// The pane stride: `window_step` when sliding, else the window size.
    fn step(&self) -> i64 {
        if self.window_step > 0 {
            self.window_step
        } else {
            self.window_size
        }
    }
}

/// Deterministic aggregation failures. Never panics on malformed input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggError {
    Eval(EvalError),
    Emit(PipeError),
    BadFrame,
    /// The `key` or a selector produced a value of the wrong type.
    BadType,
    /// More operators than `MAX_OPS`.
    TooManyOps,
    /// A byte/string key longer than `KEY_CAP`. Rejected rather than truncated:
    /// two distinct keys sharing their first `KEY_CAP` bytes must never alias into
    /// one lane and combine their aggregates.
    KeyTooLong,
}

/// One monoid accumulator cell; interpreted by the operator's kind.
#[derive(Debug, Clone, Copy)]
struct Acc {
    a: i64, // Count/Sum/Min/Max value; Avg sum
    b: i64, // Avg count
}

impl Acc {
    fn init(kind: AggOp) -> Self {
        match kind {
            AggOp::Min => Acc { a: i64::MAX, b: 0 },
            AggOp::Max => Acc { a: i64::MIN, b: 0 },
            // Collection operators keep no scalar; their state is a `Coll`.
            _ => Acc { a: 0, b: 0 },
        }
    }
    fn fold(&mut self, kind: AggOp, v: i64) {
        match kind {
            AggOp::Count => self.a += 1,
            AggOp::Sum => self.a += v,
            AggOp::Min => {
                if v < self.a {
                    self.a = v;
                }
            }
            AggOp::Max => {
                if v > self.a {
                    self.a = v;
                }
            }
            AggOp::Avg => {
                self.a += v;
                self.b += 1;
            }
            // Handled by the pane's `Coll` cell, not here.
            AggOp::Distinct | AggOp::TopK(_) | AggOp::Quantile(_) => {}
        }
    }
    fn value(&self, kind: AggOp) -> i64 {
        match kind {
            // `checked_div` (not `/`) so a freestanding module links no
            // panic-on-overflow path; None (b==0 or MIN/-1) folds to 0.
            AggOp::Avg => self.a.checked_div(self.b).unwrap_or(0),
            _ => self.a,
        }
    }
}

/// One window's accumulators for a lane. `used` distinguishes a live slot;
/// `finalized` marks a window that emitted and is retained only for corrections.
#[derive(Debug, Clone, Copy)]
struct Pane {
    used: bool,
    finalized: bool,
    window_start: i64,
    accs: [Acc; MAX_OPS],
    /// Events folded since this pane last emitted — drives the `OnCount` trigger.
    /// Reset to 0 on every emit; serialized so count-firing survives checkpoints.
    since_emit: u32,
}

impl Pane {
    fn empty() -> Self {
        Pane {
            used: false,
            finalized: false,
            window_start: 0,
            accs: [Acc { a: 0, b: 0 }; MAX_OPS],
            since_emit: 0,
        }
    }
    fn end(&self, spec: &AggSpec) -> i64 {
        self.window_start.saturating_add(spec.window_size)
    }
}

/// One keyed lane: its partition key plus a bounded set of live panes (open
/// windows and finalized-within-horizon windows). `Copy` so the whole state
/// serializes trivially.
#[derive(Debug, Clone, Copy)]
struct Lane {
    active: bool,
    key_is_int: bool,
    key_int: i64,
    key: [u8; KEY_CAP],
    key_len: usize,
    /// Highest window start already finalized (monotonic). A late event for a
    /// window `<= finalized_high` targets a finalized window even after its pane
    /// is reclaimed, so it is never reopened.
    finalized_high: i64,
    panes: [Pane; MAX_PANES],
}

impl Lane {
    fn empty() -> Self {
        Lane {
            active: false,
            key_is_int: false,
            key_int: 0,
            key: [0u8; KEY_CAP],
            key_len: 0,
            finalized_high: i64::MIN,
            panes: [Pane::empty(); MAX_PANES],
        }
    }
    fn key_matches(&self, is_int: bool, int: i64, bytes: &[u8]) -> bool {
        if self.key_is_int != is_int {
            return false;
        }
        if is_int {
            self.key_int == int
        } else {
            self.key_len == bytes.len() && self.key[..self.key_len] == *bytes
        }
    }
    fn find_pane(&self, w: i64) -> Option<usize> {
        let mut pi = 0;
        while pi < MAX_PANES {
            if self.panes[pi].used && self.panes[pi].window_start == w {
                return Some(pi);
            }
            pi += 1;
        }
        None
    }
    /// Claim a free pane slot for window `w`, initializing accumulators. `None`
    /// when all `MAX_PANES` slots are live (bounded-state overflow).
    fn open_pane(&mut self, w: i64, spec: &AggSpec) -> Option<usize> {
        let mut pi = 0;
        while pi < MAX_PANES {
            if !self.panes[pi].used {
                let mut accs = [Acc { a: 0, b: 0 }; MAX_OPS];
                let mut k = 0;
                while k < spec.ops.len() {
                    accs[k] = Acc::init(spec.ops[k].kind);
                    k += 1;
                }
                self.panes[pi] = Pane {
                    used: true,
                    finalized: false,
                    window_start: w,
                    accs,
                    since_emit: 0,
                };
                return Some(pi);
            }
            pi += 1;
        }
        None
    }
}

/// The bounded aggregation engine state. Lives in module state on device.
pub struct AggState {
    lanes: [Lane; MAX_LANES],
    lane_count: usize,
    max_event_time: i64,
    lane_overflows: u32,
    late_drops: u32,
    corrections: u32,
    pane_overflows: u32,
    /// Monotonic logical processing clock: one tick per event ingested. Drives
    /// the `OnProcessing` trigger deterministically (independent of wall-clock,
    /// so it replays identically). Serialized so periodic firing stays aligned
    /// across a checkpoint/restart.
    processing_clock: u64,
    /// Backing store for the collection operators (Distinct/TopK/Quantile).
    ///
    /// Indexed by `lane_idx * MAX_PANES + pane_idx` — DERIVED, never allocated,
    /// so there is no free list to keep consistent and a restored checkpoint
    /// lands every cell back under the same pane it left. Slots are stable
    /// because neither lanes nor panes are ever compacted.
    ///
    /// One collection operator per aggregation (the first one declared); a
    /// second is rejected at ingest rather than silently mis-scored. Keeping the
    /// pool off `Acc` is what stops Distinct/TopK/Quantile from charging their
    /// storage to every Sum-only deployment.
    colls: [Coll; MAX_LANES * MAX_PANES],
    /// Values a full `Coll` could not retain exactly.
    coll_overflows: u32,
    /// Late values skipped by a NON-RETRACTABLE operator during a correction.
    non_retractable_drops: u32,
}

/// Index of the single collection operator in `spec.ops`, if any.
fn coll_op_index(spec: &AggSpec) -> Option<usize> {
    let mut k = 0;
    while k < spec.ops.len() {
        if spec.ops[k].kind.is_collection() {
            return Some(k);
        }
        k += 1;
    }
    None
}

impl Default for AggState {
    fn default() -> Self {
        Self::new()
    }
}

impl AggState {
    pub fn new() -> Self {
        AggState {
            lanes: [Lane::empty(); MAX_LANES],
            lane_count: 0,
            max_event_time: i64::MIN,
            lane_overflows: 0,
            late_drops: 0,
            corrections: 0,
            pane_overflows: 0,
            processing_clock: 0,
            colls: [Coll::empty(); MAX_LANES * MAX_PANES],
            coll_overflows: 0,
            non_retractable_drops: 0,
        }
    }
    /// Selected values a collection operator could not retain exactly because
    /// its bounded cell was full. Non-zero means Distinct/Quantile results are
    /// lower bounds rather than exact — surfaced, not hidden.
    pub fn coll_overflows(&self) -> u32 {
        self.coll_overflows
    }
    /// Late values a collection operator refused during a correction, because
    /// Distinct/TopK/Quantile cannot retract an answer they already reported.
    pub fn non_retractable_drops(&self) -> u32 {
        self.non_retractable_drops
    }
    pub fn lane_overflows(&self) -> u32 {
        self.lane_overflows
    }
    pub fn late_drops(&self) -> u32 {
        self.late_drops
    }
    /// Retractable corrections applied to already-finalized windows.
    pub fn corrections(&self) -> u32 {
        self.corrections
    }
    /// Events dropped because a lane's live-pane set was full (bounded state).
    pub fn pane_overflows(&self) -> u32 {
        self.pane_overflows
    }

    fn find_lane(&self, is_int: bool, int: i64, bytes: &[u8]) -> Option<usize> {
        let mut i = 0;
        while i < self.lane_count {
            if self.lanes[i].key_matches(is_int, int, bytes) {
                return Some(i);
            }
            i += 1;
        }
        None
    }

    /// Serialize the full aggregation state into `out` deterministically — a
    /// CHECKPOINT for replay/recovery (the durable-state capability).
    /// Layout, all little-endian:
    ///   [ver=1][max_event_time:i64][lane_overflows:u32][late_drops:u32]
    ///   [corrections:u32][pane_overflows:u32][coll_overflows:u32]
    ///   [non_retractable_drops:u32][nlanes:u8]
    ///   per active lane: [key_is_int:u8] then (int) [key_int:i64] or (bytes)
    ///     [key_len:u16][key…]; [finalized_high:i64][npanes:u8]
    ///   per used pane: [finalized:u8][window_start:i64] [a:i64][b:i64]×MAX_OPS
    ///     [since_emit:u32][coll_len:u8][coll value:i64 × coll_len]
    ///   [processing_clock:u64]
    /// Every accumulator is written (not just active ops), so snapshot/restore
    /// need no `AggSpec` — and the collection cell rides with its pane for the
    /// same reason, length-prefixed so an empty cell costs one byte. Returns the
    /// byte length, or `None` if `out` is too small. Deterministic:
    /// lanes/panes are emitted in slot order and each cell is already sorted.
    pub fn snapshot(&self, out: &mut [u8]) -> Option<usize> {
        let mut p = 0usize;
        ckpt_put(out, &mut p, &[CKPT_VER])?;
        ckpt_put(out, &mut p, &self.max_event_time.to_le_bytes())?;
        ckpt_put(out, &mut p, &self.lane_overflows.to_le_bytes())?;
        ckpt_put(out, &mut p, &self.late_drops.to_le_bytes())?;
        ckpt_put(out, &mut p, &self.corrections.to_le_bytes())?;
        ckpt_put(out, &mut p, &self.pane_overflows.to_le_bytes())?;
        ckpt_put(out, &mut p, &self.coll_overflows.to_le_bytes())?;
        ckpt_put(out, &mut p, &self.non_retractable_drops.to_le_bytes())?;
        let mut nlanes = 0u8;
        let mut li = 0;
        while li < self.lane_count {
            if self.lanes[li].active {
                nlanes += 1;
            }
            li += 1;
        }
        ckpt_put(out, &mut p, &[nlanes])?;
        let mut li = 0;
        while li < self.lane_count {
            let lane = &self.lanes[li];
            if !lane.active {
                li += 1;
                continue;
            }
            ckpt_put(out, &mut p, &[lane.key_is_int as u8])?;
            if lane.key_is_int {
                ckpt_put(out, &mut p, &lane.key_int.to_le_bytes())?;
            } else {
                ckpt_put(out, &mut p, &(lane.key_len as u16).to_le_bytes())?;
                ckpt_put(out, &mut p, &lane.key[..lane.key_len])?;
            }
            ckpt_put(out, &mut p, &lane.finalized_high.to_le_bytes())?;
            let mut npanes = 0u8;
            let mut pi = 0;
            while pi < MAX_PANES {
                if lane.panes[pi].used {
                    npanes += 1;
                }
                pi += 1;
            }
            ckpt_put(out, &mut p, &[npanes])?;
            let mut pi = 0;
            while pi < MAX_PANES {
                let pane = &lane.panes[pi];
                if pane.used {
                    ckpt_put(out, &mut p, &[pane.finalized as u8])?;
                    ckpt_put(out, &mut p, &pane.window_start.to_le_bytes())?;
                    let mut k = 0;
                    while k < MAX_OPS {
                        ckpt_put(out, &mut p, &pane.accs[k].a.to_le_bytes())?;
                        ckpt_put(out, &mut p, &pane.accs[k].b.to_le_bytes())?;
                        k += 1;
                    }
                    ckpt_put(out, &mut p, &pane.since_emit.to_le_bytes())?;
                    let cell = &self.colls[li * MAX_PANES + pi];
                    ckpt_put(out, &mut p, &[cell.len])?;
                    let mut c = 0;
                    while c < cell.len as usize {
                        ckpt_put(out, &mut p, &cell.vals[c].to_le_bytes())?;
                        c += 1;
                    }
                }
                pi += 1;
            }
            li += 1;
        }
        ckpt_put(out, &mut p, &self.processing_clock.to_le_bytes())?;
        Some(p)
    }

    /// Reconstruct an `AggState` from a [`snapshot`](Self::snapshot). `None` on a
    /// truncated or malformed checkpoint (bad version, over-cap counts) —
    /// fail-closed, never a half-built state. Every count is range-checked
    /// against MAX_LANES / MAX_PANES / KEY_CAP before use.
    pub fn restore(bytes: &[u8]) -> Option<AggState> {
        let mut p = 0usize;
        let ver = ckpt_get(bytes, &mut p, 1)?[0];
        if ver != CKPT_VER {
            return None;
        }
        let mut st = AggState::new();
        st.max_event_time = i64::from_le_bytes(ckpt_get(bytes, &mut p, 8)?.try_into().ok()?);
        st.lane_overflows = u32::from_le_bytes(ckpt_get(bytes, &mut p, 4)?.try_into().ok()?);
        st.late_drops = u32::from_le_bytes(ckpt_get(bytes, &mut p, 4)?.try_into().ok()?);
        st.corrections = u32::from_le_bytes(ckpt_get(bytes, &mut p, 4)?.try_into().ok()?);
        st.pane_overflows = u32::from_le_bytes(ckpt_get(bytes, &mut p, 4)?.try_into().ok()?);
        st.coll_overflows = u32::from_le_bytes(ckpt_get(bytes, &mut p, 4)?.try_into().ok()?);
        st.non_retractable_drops = u32::from_le_bytes(ckpt_get(bytes, &mut p, 4)?.try_into().ok()?);
        let nlanes = ckpt_get(bytes, &mut p, 1)?[0] as usize;
        if nlanes > MAX_LANES {
            return None;
        }
        let mut li = 0;
        while li < nlanes {
            let mut lane = Lane::empty();
            lane.active = true;
            lane.key_is_int = ckpt_get(bytes, &mut p, 1)?[0] != 0;
            if lane.key_is_int {
                lane.key_int = i64::from_le_bytes(ckpt_get(bytes, &mut p, 8)?.try_into().ok()?);
            } else {
                let kl = u16::from_le_bytes(ckpt_get(bytes, &mut p, 2)?.try_into().ok()?) as usize;
                if kl > KEY_CAP {
                    return None;
                }
                lane.key[..kl].copy_from_slice(ckpt_get(bytes, &mut p, kl)?);
                lane.key_len = kl;
            }
            lane.finalized_high = i64::from_le_bytes(ckpt_get(bytes, &mut p, 8)?.try_into().ok()?);
            let npanes = ckpt_get(bytes, &mut p, 1)?[0] as usize;
            if npanes > MAX_PANES {
                return None;
            }
            let mut pi = 0;
            while pi < npanes {
                let mut pane = Pane::empty();
                pane.used = true;
                pane.finalized = ckpt_get(bytes, &mut p, 1)?[0] != 0;
                pane.window_start =
                    i64::from_le_bytes(ckpt_get(bytes, &mut p, 8)?.try_into().ok()?);
                let mut k = 0;
                while k < MAX_OPS {
                    pane.accs[k].a =
                        i64::from_le_bytes(ckpt_get(bytes, &mut p, 8)?.try_into().ok()?);
                    pane.accs[k].b =
                        i64::from_le_bytes(ckpt_get(bytes, &mut p, 8)?.try_into().ok()?);
                    k += 1;
                }
                pane.since_emit = u32::from_le_bytes(ckpt_get(bytes, &mut p, 4)?.try_into().ok()?);
                // The pane's collection cell rides with it, so a restored pane
                // lands its values back under its (possibly compacted) slot.
                let clen = ckpt_get(bytes, &mut p, 1)?[0] as usize;
                if clen > COLL_CAP {
                    return None;
                }
                let mut cell = Coll::empty();
                let mut c = 0;
                while c < clen {
                    cell.vals[c] = i64::from_le_bytes(ckpt_get(bytes, &mut p, 8)?.try_into().ok()?);
                    c += 1;
                }
                cell.len = clen as u8;
                st.colls[li * MAX_PANES + pi] = cell;
                lane.panes[pi] = pane;
                pi += 1;
            }
            st.lanes[li] = lane;
            li += 1;
        }
        st.lane_count = nlanes;
        st.processing_clock = u64::from_le_bytes(ckpt_get(bytes, &mut p, 8)?.try_into().ok()?);
        Some(st)
    }
}

/// Outcome of feeding a replicated-log commit horizon to the [`BarrierGate`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// No checkpoint is awaiting durability, or the horizon has not yet reached
    /// the pending checkpoint's append index.
    Pending,
    /// The pending checkpoint is now durably committed at `(term, index)` — the
    /// commit horizon reached its append index *under the same term* it was
    /// appended at. Safe to treat the replicated state as recoverable.
    Durable { term: u64, index: u64 },
    /// The horizon reached the append index but under a NEWER term than the
    /// checkpoint was appended at: a leader change may have overwritten the entry
    /// before it committed. Fail-closed — the checkpoint must be re-appended.
    Superseded {
        appended_term: u64,
        committed_term: u64,
    },
}

/// Term-fenced log-index activation barrier (the Clustor
/// `state.distributed.raft.v1` binding). A checkpoint whose snapshot was
/// appended to the replicated log at `(append_term, append_index)` is durable
/// once the raft commit horizon `(committed_term, committed_index)` satisfies
///
///   committed_index >= append_index  AND  committed_term == append_term
///
/// The term equality is the fence: raft only commits an entry indirectly once a
/// later-term entry commits above it, so a horizon that crossed the append index
/// under a *different* term gives no proof our specific entry survived a leader
/// change — treated as `Superseded`, never `Durable`. Deterministic, no clocks,
/// bounded (one checkpoint in flight); the horizon is monotone.
#[derive(Debug, Clone, Copy)]
pub struct BarrierGate {
    committed_term: u64,
    committed_index: u64,
    /// `(append_term, append_index)` of the checkpoint awaiting durability.
    pending: Option<(u64, u64)>,
}

impl Default for BarrierGate {
    fn default() -> Self {
        Self::new()
    }
}

impl BarrierGate {
    pub const fn new() -> Self {
        BarrierGate {
            committed_term: 0,
            committed_index: 0,
            pending: None,
        }
    }

    /// Record that a checkpoint's snapshot was appended to the replicated log at
    /// `(term, index)`. At most one checkpoint is tracked at a time; a new append
    /// replaces any still-pending one (the newer snapshot supersedes the older).
    pub fn appended(&mut self, term: u64, index: u64) {
        self.pending = Some((term, index));
    }

    /// Whether a checkpoint is currently awaiting durability.
    pub fn is_pending(&self) -> bool {
        self.pending.is_some()
    }

    pub fn committed_index(&self) -> u64 {
        self.committed_index
    }

    pub fn committed_term(&self) -> u64 {
        self.committed_term
    }

    /// Feed a commit horizon `(term, index)` from `consensus.committed_entries`.
    /// Advances the (monotone) horizon and returns the durability verdict for the
    /// pending checkpoint. On `Durable` the pending slot is cleared; on
    /// `Superseded` it is retained so the caller can re-append and try again.
    pub fn horizon(&mut self, term: u64, index: u64) -> Durability {
        if index > self.committed_index {
            self.committed_index = index;
        }
        if term > self.committed_term {
            self.committed_term = term;
        }
        match self.pending {
            None => Durability::Pending,
            Some((at, ai)) => {
                if self.committed_index < ai {
                    Durability::Pending
                } else if self.committed_term == at {
                    self.pending = None;
                    Durability::Durable {
                        term: at,
                        index: ai,
                    }
                } else {
                    Durability::Superseded {
                        appended_term: at,
                        committed_term: self.committed_term,
                    }
                }
            }
        }
    }
}

/// Bounded LE write into `out` at `*p`; advances `*p`. `None` on overflow.
fn ckpt_put(out: &mut [u8], p: &mut usize, b: &[u8]) -> Option<()> {
    let end = p.checked_add(b.len())?;
    if end > out.len() {
        return None;
    }
    out[*p..end].copy_from_slice(b);
    *p = end;
    Some(())
}

/// Bounded read of `n` bytes from `buf` at `*p`; advances `*p`. `None` if short.
fn ckpt_get<'a>(buf: &'a [u8], p: &mut usize, n: usize) -> Option<&'a [u8]> {
    let end = p.checked_add(n)?;
    let s = buf.get(*p..end)?;
    *p = end;
    Some(s)
}

/// Window starts whose `[start, start+size)` interval contains `t`, written into
/// `out` (bounded to `MAX_WIN_PER_EVENT`). Mirrors the host `windows_for`: for a
/// tumbling window this is a single start; for a sliding window every overlapping
/// pane. `checked_*` so a freestanding module links no panic-on-overflow path.
fn windows_for(spec: &AggSpec, t: i64, out: &mut [i64; MAX_WIN_PER_EVENT]) -> usize {
    let size = spec.window_size;
    let step = spec.step();
    if size <= 0 || step <= 0 {
        return 0;
    }
    let mut n = 0;
    let mut k = t.checked_div_euclid(step).unwrap_or(0);
    // Bound the walk to MAX_WIN_PER_EVENT iterations: the overlapping windows for a
    // point are the contiguous top-most `ceil(size/step)` starts, which activation
    // rejects when it exceeds the cap (see `build_spec`). Capping the loop keeps an
    // extreme size/step ratio from monopolising the cooperative lane even if a spec
    // slipped through.
    let mut iters = 0;
    while k >= 0 && iters < MAX_WIN_PER_EVENT {
        let w = k.wrapping_mul(step);
        if w <= t && w.wrapping_add(size) > t && n < MAX_WIN_PER_EVENT {
            out[n] = w;
            n += 1;
        }
        if w.wrapping_add(size) <= t {
            break;
        }
        k -= 1;
        iters += 1;
    }
    n
}

/// Read a `[max_cost:u32 LE][code_len:u16 LE][code bytes]` program at `off` in a
/// param-driven aggregation container; returns `(max_cost, code, next_off)`.
/// Shared with the on-device module so a config can carry a whole Aggregation.
pub fn read_prog(buf: &[u8], off: usize) -> Option<(u64, &[u8], usize)> {
    if off + 6 > buf.len() {
        return None;
    }
    let cost = u32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]]) as u64;
    let len = u16::from_le_bytes([buf[off + 4], buf[off + 5]]) as usize;
    let start = off + 6;
    if start + len > buf.len() {
        return None;
    }
    Some((cost, &buf[start..start + len], start + len))
}

/// Map a container operator-kind byte to an `AggOp` (0=Count,1=Sum,2=Min,3=Max,
/// 4=Avg,5=Distinct,6=TopK,7=Quantile — the `AggOp` declaration order).
///
/// Kinds 6 and 7 are parameterised, so this reports 0 for their parameter; use
/// [`agg_op_kind_p`] when the container carries one.
pub fn agg_op_kind(b: u8) -> Option<AggOp> {
    agg_op_kind_p(b, 0)
}

/// [`agg_op_kind`] with the operator's canonical parameter: `k` for TopK,
/// permille for Quantile. Ignored by the unparameterised kinds.
///
/// NOTE these bytes are the EXECUTION container's vocabulary and are NOT the
/// `OperatorKind` proto values in `artefact_core` (`OP_SUM = 1`, `OP_COUNT = 2`,
/// … `OP_TOPK = 7`, `OP_DISTINCT = 8`). The two overlap without agreeing — the
/// container's 7 is Quantile where the proto's 7 is TopK — because one encodes
/// artefact IDENTITY and the other encodes a runtime program. Translate
/// deliberately; never pass a value from one into the other.
pub fn agg_op_kind_p(b: u8, param: u16) -> Option<AggOp> {
    match b {
        0 => Some(AggOp::Count),
        1 => Some(AggOp::Sum),
        2 => Some(AggOp::Min),
        3 => Some(AggOp::Max),
        4 => Some(AggOp::Avg),
        5 => Some(AggOp::Distinct),
        6 => Some(AggOp::TopK(param)),
        7 => Some(AggOp::Quantile(param)),
        _ => None,
    }
}

fn agg_as_int(v: Value) -> Result<i64, AggError> {
    match v {
        Value::Int(i) => Ok(i),
        Value::Uint(u) => Ok(u as i64),
        _ => Err(AggError::BadType),
    }
}

/// Ingest one event frame; finalized windows are delivered to `emit` as encoded
/// output frames. Returns after the watermark-driven finalize pass.
pub fn ingest<F: FnMut(&[u8])>(
    state: &mut AggState,
    spec: &AggSpec,
    event_frame: &[u8],
    mut emit: F,
) -> Result<(), AggError> {
    if spec.ops.len() > MAX_OPS {
        return Err(AggError::TooManyOps);
    }

    // Decode the event.
    let mut fields = [Field {
        number: 0,
        value: Value::Null,
    }; MAX_PIPE_FIELDS];
    let nf = decode_frame(event_frame, &mut fields).map_err(|_| AggError::BadFrame)?;
    // One params array, living for the whole ingest, so evaluated values (which
    // borrow the event fields) outlive each `eval` call.
    let ev = [Message {
        fields: &fields[..nf],
    }];

    // Advance the logical processing clock — one tick per ingested event. Drives
    // the deterministic OnProcessing flush after this event's windows are folded.
    state.processing_clock = state.processing_clock.wrapping_add(1);

    // key + event_time.
    let key_val = eval(spec.key_code, &ev, spec.key_cost).map_err(AggError::Eval)?;
    let (key_is_int, key_int, key_bytes): (bool, i64, &[u8]) = match key_val {
        Value::Int(i) => (true, i, &[]),
        Value::Uint(u) => (true, u as i64, &[]),
        Value::Bytes(b) => (false, 0, b),
        Value::Str(s) => (false, 0, s.as_bytes()),
        _ => return Err(AggError::BadType),
    };
    // Reject an over-long key BEFORE any lane is created or mutated — never
    // prefix-truncate it into an alias of another key.
    if !key_is_int && key_bytes.len() > KEY_CAP {
        return Err(AggError::KeyTooLong);
    }
    let t = agg_as_int(eval(spec.time_code, &ev, spec.time_cost).map_err(AggError::Eval)?)?;
    if t > state.max_event_time {
        state.max_event_time = t;
    }
    let watermark = state.max_event_time.saturating_sub(spec.lateness);

    // Resolve (or open) the lane for this key.
    let lane_idx = match state.find_lane(key_is_int, key_int, key_bytes) {
        Some(i) => Some(i),
        None => {
            if state.lane_count < MAX_LANES && (state.lane_count as u32) < spec.max_lanes {
                let i = state.lane_count;
                let lane = &mut state.lanes[i];
                *lane = Lane::empty();
                lane.active = true;
                lane.key_is_int = key_is_int;
                lane.key_int = key_int;
                if !key_is_int {
                    // Length was checked <= KEY_CAP above, so this copies the whole
                    // key — never a truncating prefix.
                    let n = key_bytes.len();
                    lane.key[..n].copy_from_slice(&key_bytes[..n]);
                    lane.key_len = n;
                }
                state.lane_count += 1;
                Some(i)
            } else {
                state.lane_overflows += 1;
                None
            }
        }
    };

    if let Some(i) = lane_idx {
        // Per-operator selected values for this event (Count ignores the value).
        let mut vals = [0i64; MAX_OPS];
        let mut k = 0;
        while k < spec.ops.len() {
            vals[k] = match spec.ops[k].kind {
                AggOp::Count => 0,
                _ => agg_as_int(
                    eval(spec.ops[k].selector, &ev, spec.ops[k].sel_cost)
                        .map_err(AggError::Eval)?,
                )?,
            };
            k += 1;
        }

        // Route the event into every window it belongs to (one for tumbling,
        // several for sliding).
        let mut wins = [0i64; MAX_WIN_PER_EVENT];
        let nw = windows_for(spec, t, &mut wins);
        let mut wi = 0;
        while wi < nw {
            let w = wins[wi];
            wi += 1;
            let finalized = w <= state.lanes[i].finalized_high;
            if finalized {
                // Late data on a finalized window: fold a retractable correction
                // and re-emit if the pane is still retained and within horizon;
                // otherwise drop + audit.
                let within = t >= watermark.saturating_sub(spec.correction_horizon);
                match state.lanes[i].find_pane(w) {
                    Some(pi) if within => {
                        fold_pane(state, spec, i, pi, &vals, true);
                        state.corrections += 1;
                        emit_pane(state, spec, i, pi, &mut emit)?;
                    }
                    _ => state.late_drops += 1,
                }
            } else {
                let pane = match state.lanes[i].find_pane(w) {
                    Some(pi) => Some(pi),
                    None => match state.lanes[i].open_pane(w, spec) {
                        // A freshly claimed slot may be reusing a reclaimed
                        // pane's cell, so reset it with the accumulators.
                        Some(pi) => {
                            state.colls[i * MAX_PANES + pi] = Coll::empty();
                            Some(pi)
                        }
                        None => None,
                    },
                };
                match pane {
                    Some(pi) => {
                        fold_pane(state, spec, i, pi, &vals, false);
                        // Explicit trigger: `Continuous` early-fires the pane's
                        // running partial every event; `OnCount(n)` fires every
                        // `n` events (resetting the pane counter). The on-close
                        // final still fires later via finalize_and_gc.
                        match spec.emit_trigger {
                            EmitTrigger::Continuous => {
                                emit_pane(state, spec, i, pi, &mut emit)?;
                            }
                            EmitTrigger::OnCount(n) => {
                                if state.lanes[i].panes[pi].since_emit >= n {
                                    emit_pane(state, spec, i, pi, &mut emit)?;
                                    state.lanes[i].panes[pi].since_emit = 0;
                                }
                            }
                            // OnProcessing flushes all panes together after the
                            // event, below — nothing per-pane here.
                            EmitTrigger::OnProcessing(_) | EmitTrigger::OnClose => {}
                        }
                    }
                    None => state.pane_overflows += 1,
                }
            }
        }
    }

    // OnProcessing: every `period` ingested events, flush every live pane's
    // running partial (deterministic processing-time timer). Runs after this
    // event is folded so the flush includes it.
    if let EmitTrigger::OnProcessing(period) = spec.emit_trigger {
        // checked_rem avoids the rem-by-zero panic path (period is already
        // clamped >= 1 by decode, but the compiler can't see that here).
        if state.processing_clock.checked_rem(period as u64) == Some(0) {
            flush_live_panes(state, spec, &mut emit)?;
        }
    }

    finalize_and_gc(state, spec, watermark, &mut emit)
}

/// Emit every live (used, not-yet-finalized) pane's running partial, in stable
/// `(lane, pane)` slot order. The deterministic OnProcessing flush; leaves pane
/// state intact (panes still finalize on close as usual).
fn flush_live_panes<F: FnMut(&[u8])>(
    state: &mut AggState,
    spec: &AggSpec,
    emit: &mut F,
) -> Result<(), AggError> {
    let mut li = 0;
    while li < state.lane_count {
        let mut pi = 0;
        while pi < MAX_PANES {
            let p = &state.lanes[li].panes[pi];
            if p.used && !p.finalized {
                emit_pane(state, spec, li, pi, emit)?;
            }
            pi += 1;
        }
        li += 1;
    }
    Ok(())
}

/// Fold this event's per-operator values into a lane's pane.
///
/// `retractable_only` marks a LATE CORRECTION to an already-finalized window.
/// The fixed-size monoids are retractable and merge-safe, so a correction folds
/// into them exactly. The collection operators are NOT: Distinct/TopK/Quantile
/// are defined over a multiset that has already been reported, and folding into
/// them after the fact would silently change a published answer with no way to
/// retract it. So they are frozen and the skipped value is AUDITED — a result
/// that stopped being complete says so.
fn fold_pane(
    state: &mut AggState,
    spec: &AggSpec,
    li: usize,
    pi: usize,
    vals: &[i64; MAX_OPS],
    retractable_only: bool,
) {
    let ck = coll_op_index(spec);
    let mut k = 0;
    while k < spec.ops.len() {
        if Some(k) == ck {
            if retractable_only {
                // Non-retractable: freeze and audit rather than mutate a
                // result that has already been emitted.
                state.non_retractable_drops = state.non_retractable_drops.saturating_add(1);
            } else if !state.colls[li * MAX_PANES + pi].fold(spec.ops[k].kind, vals[k]) {
                state.coll_overflows = state.coll_overflows.saturating_add(1);
            }
        } else {
            state.lanes[li].panes[pi].accs[k].fold(spec.ops[k].kind, vals[k]);
        }
        k += 1;
    }
    let se = &mut state.lanes[li].panes[pi].since_emit;
    *se = se.saturating_add(1);
}

/// Finalize every open pane whose window closed under the watermark — in stable
/// `(window_start, lane)` order — then reclaim finalized panes past the
/// correction horizon so the live-pane set stays bounded.
fn finalize_and_gc<F: FnMut(&[u8])>(
    state: &mut AggState,
    spec: &AggSpec,
    watermark: i64,
    emit: &mut F,
) -> Result<(), AggError> {
    loop {
        // Select the least (window_start, lane) pane still needing finalization.
        let mut best: Option<(i64, usize, usize)> = None;
        let mut li = 0;
        while li < state.lane_count {
            let mut pi = 0;
            while pi < MAX_PANES {
                let p = &state.lanes[li].panes[pi];
                if p.used && !p.finalized && p.end(spec) <= watermark {
                    let cand = (p.window_start, li, pi);
                    best = match best {
                        None => Some(cand),
                        Some(b) if (cand.0, cand.1) < (b.0, b.1) => Some(cand),
                        Some(b) => Some(b),
                    };
                }
                pi += 1;
            }
            li += 1;
        }
        let Some((w, li, pi)) = best else { break };
        state.lanes[li].panes[pi].finalized = true;
        if w > state.lanes[li].finalized_high {
            state.lanes[li].finalized_high = w;
        }
        emit_pane(state, spec, li, pi, emit)?;
    }

    // Reclaim finalized panes that can receive no further corrections.
    let cutoff = watermark.saturating_sub(spec.correction_horizon);
    let mut li = 0;
    while li < state.lane_count {
        let mut pi = 0;
        while pi < MAX_PANES {
            let p = &state.lanes[li].panes[pi];
            if p.used && p.finalized && p.end(spec) <= cutoff {
                state.lanes[li].panes[pi] = Pane::empty();
            }
            pi += 1;
        }
        li += 1;
    }
    Ok(())
}

/// Project a lane pane's window through the emit bytecode and deliver the encoded
/// output frame to `cb`. Used for both on-time finalization and corrections.
fn emit_pane<F: FnMut(&[u8])>(
    state: &AggState,
    spec: &AggSpec,
    lane_idx: usize,
    pane_idx: usize,
    cb: &mut F,
) -> Result<(), AggError> {
    let lane = &state.lanes[lane_idx];
    let pane = &lane.panes[pane_idx];
    let window_start = pane.window_start;

    // state message: op i -> field (i+1) = Int(value).
    let mut state_fields = [Field {
        number: 0,
        value: Value::Null,
    }; MAX_OPS];
    let ck = coll_op_index(spec);
    let mut k = 0;
    while k < spec.ops.len() {
        let kind = spec.ops[k].kind;
        let v = if Some(k) == ck {
            state.colls[lane_idx * MAX_PANES + pane_idx].value(kind)
        } else {
            pane.accs[k].value(kind)
        };
        state_fields[k] = Field {
            number: (k + 1) as u32,
            value: Value::Int(v),
        };
        k += 1;
    }
    let state_msg = Message {
        fields: &state_fields[..spec.ops.len()],
    };

    // window message: start=1, end=2.
    let window_fields = [
        Field {
            number: 1,
            value: Value::Int(window_start),
        },
        Field {
            number: 2,
            value: Value::Int(window_start.saturating_add(spec.window_size)),
        },
    ];
    let window_msg = Message {
        fields: &window_fields,
    };

    // ctx = {1: key, 2: state, 3: window}.
    let key_val = if lane.key_is_int {
        Value::Int(lane.key_int)
    } else {
        Value::Bytes(&lane.key[..lane.key_len])
    };
    let ctx_fields = [
        Field {
            number: 1,
            value: key_val,
        },
        Field {
            number: 2,
            value: Value::Msg(&state_msg),
        },
        Field {
            number: 3,
            value: Value::Msg(&window_msg),
        },
    ];
    let ctx = Message {
        fields: &ctx_fields,
    };

    let params = [ctx];
    let mut builder = Builder::new();
    match eval_full(spec.emit_code, &params, &mut builder, spec.emit_cost) {
        Ok(EvalResult::Constructed) => {
            let mut scratch = [0u8; 256];
            let n = encode_frame(&builder.message(), &mut scratch).map_err(AggError::Emit)?;
            cb(&scratch[..n]);
            Ok(())
        }
        Ok(EvalResult::Scalar(_)) => Err(AggError::Emit(PipeError::NotConstructed)),
        Err(e) => Err(AggError::Eval(e)),
    }
}
