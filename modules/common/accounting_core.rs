// The common accounting taxonomy. One counter set whose disposition methods
// maintain the input/output invariants BY CONSTRUCTION, so "account for every
// received record" is a property of this type rather than
// of each module's hand-written match. `include!`d by every steady-state module
// and by the host harness, so the invariant tests run the exact code the modules
// run.
//
// Invariants, checked by `invariants_hold`:
//   inputs_observed   = inputs_admitted + inputs_rejected
//   inputs_admitted   = inputs_succeeded + inputs_policy_dropped
//                     + inputs_failed + inputs_in_flight
//   outputs_generated = outputs_delivered + outputs_pending + outputs_failed
//
// The core owns the counter semantics and the invariants; the MODULE owns the
// mapping from its StepResult and its retained-output state to these dispositions,
// because only the module knows whether a delivery completed a fresh admission or
// drained a previously-pending output. Every mutator is expressed so that, called
// in a legal order, the three identities above hold after each call.
//
// Mounted after outcome_core/io_core (it names no other core, but keeps their
// company as a pure no_std/alloc-free table).

/// The baseline record-accounting instruments. Monotonic totals are `u64`; the two
/// current-values (`inputs_in_flight`, `outputs_pending`) and `pending_output_bytes`
/// are the only quantities that decrease, held as `u32` because a single step never
/// puts more than a small bounded number of records in flight or bytes on the wire.
#[derive(Default, Clone, Copy)]
#[allow(
    dead_code,
    reason = "each module consumes a subset of the baseline surface"
)]
pub struct Accounting {
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub inputs_observed: u64,
    pub inputs_admitted: u64,
    pub inputs_rejected: u64,
    pub inputs_succeeded: u64,
    pub inputs_policy_dropped: u64,
    pub inputs_failed: u64,
    pub inputs_in_flight: u32,
    pub outputs_generated: u64,
    pub outputs_delivered: u64,
    pub outputs_pending: u32,
    pub outputs_failed: u64,
    pub pending_output_bytes: u32,
    /// work units consumed — VM instructions, stages, or window emissions,
    /// per the module's declared unit. NOT part of the 14-instrument baseline
    /// (its unit differs per phase, `work_<phase>_units`); each module emits it as
    /// its own instrument. Held here so the step cores can account it through the
    /// same `&mut Accounting` they already carry.
    pub work_units: u64,
}

/// The number of baseline instruments, and the width every module reserves at the
/// front of its manifest `[observability] metrics` array before its own metrics.
/// The canonical order is the field order above; `ACCT_KIND` marks each as a
/// monotonic counter (`false`) or a current-value gauge (`true`).
#[allow(
    dead_code,
    reason = "consumed by the emit helper and the manifest-order guard"
)]
pub const ACCT_METRIC_COUNT: usize = 14;

/// Per-instrument kind flag, in canonical order. `true` = gauge (up-down / current
/// value), `false` = monotonic counter. The emit helper reads this so a module
/// never has to know which baseline id is a gauge.
#[allow(dead_code, reason = "consumed by the emit helper")]
pub const ACCT_IS_GAUGE: [bool; ACCT_METRIC_COUNT] = [
    false, // 0  bytes_in
    false, // 1  bytes_out
    false, // 2  inputs_observed
    false, // 3  inputs_admitted
    false, // 4  inputs_rejected
    false, // 5  inputs_succeeded
    false, // 6  inputs_policy_dropped
    false, // 7  inputs_failed
    true,  // 8  inputs_in_flight
    false, // 9  outputs_generated
    false, // 10 outputs_delivered
    true,  // 11 outputs_pending
    false, // 12 outputs_failed
    true,  // 13 pending_output_bytes
];

/// The baseline instrument NAMES in canonical order — the exact strings a module's
/// manifest must list, front-loaded, so the array index equals the emit id. A
/// host test cross-checks each manifest against this list.
#[allow(dead_code, reason = "consumed by the manifest-order guard test")]
pub const ACCT_METRIC_NAMES: [&str; ACCT_METRIC_COUNT] = [
    "bytes_in",
    "bytes_out",
    "inputs_observed",
    "inputs_admitted",
    "inputs_rejected",
    "inputs_succeeded",
    "inputs_policy_dropped",
    "inputs_failed",
    "inputs_in_flight",
    "outputs_generated",
    "outputs_delivered",
    "outputs_pending",
    "outputs_failed",
    "pending_output_bytes",
];

#[allow(
    dead_code,
    reason = "each module consumes a subset of the disposition surface"
)]
impl Accounting {
    /// Read the baseline instrument `id` (canonical order) as a `u64`, for the emit
    /// helper. Gauges are widened; the two `u32` current-values never exceed `u64`.
    #[inline]
    pub fn value(&self, id: usize) -> u64 {
        match id {
            0 => self.bytes_in,
            1 => self.bytes_out,
            2 => self.inputs_observed,
            3 => self.inputs_admitted,
            4 => self.inputs_rejected,
            5 => self.inputs_succeeded,
            6 => self.inputs_policy_dropped,
            7 => self.inputs_failed,
            8 => self.inputs_in_flight as u64,
            9 => self.outputs_generated,
            10 => self.outputs_delivered,
            11 => self.outputs_pending as u64,
            12 => self.outputs_failed,
            13 => self.pending_output_bytes as u64,
            _ => 0,
        }
    }

    /// A complete input refused at admission (untrusted boundary / oversize). It is
    /// observed and rejected and never becomes in-flight.
    #[inline]
    pub fn reject_input(&mut self, bytes: u64) {
        self.bytes_in = self.bytes_in.wrapping_add(bytes);
        self.inputs_observed = self.inputs_observed.wrapping_add(1);
        self.inputs_rejected = self.inputs_rejected.wrapping_add(1);
    }

    /// A complete input accepted for processing; now in flight until it reaches one
    /// terminal disposition (`succeeded` / `policy_dropped` / `failed`).
    #[inline]
    pub fn admit_input(&mut self, bytes: u64) {
        self.bytes_in = self.bytes_in.wrapping_add(bytes);
        self.inputs_observed = self.inputs_observed.wrapping_add(1);
        self.inputs_admitted = self.inputs_admitted.wrapping_add(1);
        self.inputs_in_flight = self.inputs_in_flight.saturating_add(1);
    }

    /// An admitted input completed every required output and state obligation.
    #[inline]
    pub fn input_succeeded(&mut self) {
        self.inputs_succeeded = self.inputs_succeeded.wrapping_add(1);
        self.inputs_in_flight = self.inputs_in_flight.saturating_sub(1);
    }

    /// An admitted input was deliberately filtered (a zero-output policy drop).
    #[inline]
    pub fn input_policy_dropped(&mut self) {
        self.inputs_policy_dropped = self.inputs_policy_dropped.wrapping_add(1);
        self.inputs_in_flight = self.inputs_in_flight.saturating_sub(1);
    }

    /// An admitted input reached a terminal processing failure.
    #[inline]
    pub fn input_failed(&mut self) {
        self.inputs_failed = self.inputs_failed.wrapping_add(1);
        self.inputs_in_flight = self.inputs_in_flight.saturating_sub(1);
    }

    /// A logical output produced and immediately, fully accepted downstream — it
    /// never occupied the retained slot.
    #[inline]
    pub fn output_delivered_now(&mut self, bytes: u32) {
        self.outputs_generated = self.outputs_generated.wrapping_add(1);
        self.outputs_delivered = self.outputs_delivered.wrapping_add(1);
        self.bytes_out = self.bytes_out.wrapping_add(bytes as u64);
    }

    /// A logical output produced and RETAINED for delivery (the downstream ring was
    /// full). Counts against `outputs_generated` now and holds `bytes` pending.
    #[inline]
    pub fn output_staged(&mut self, bytes: u32) {
        self.outputs_generated = self.outputs_generated.wrapping_add(1);
        self.outputs_pending = self.outputs_pending.saturating_add(1);
        self.pending_output_bytes = self.pending_output_bytes.saturating_add(bytes);
    }

    /// A previously-staged output was fully accepted downstream. `bytes` is what
    /// `output_staged` held (the module reads it from its retained cursor).
    #[inline]
    pub fn output_drained(&mut self, bytes: u32) {
        self.outputs_delivered = self.outputs_delivered.wrapping_add(1);
        self.outputs_pending = self.outputs_pending.saturating_sub(1);
        self.pending_output_bytes = self.pending_output_bytes.saturating_sub(bytes);
        self.bytes_out = self.bytes_out.wrapping_add(bytes as u64);
    }

    /// A retained output abandoned after a terminal channel/dependency failure.
    #[inline]
    pub fn output_failed_pending(&mut self, bytes: u32) {
        self.outputs_pending = self.outputs_pending.saturating_sub(1);
        self.pending_output_bytes = self.pending_output_bytes.saturating_sub(bytes);
        self.outputs_failed = self.outputs_failed.wrapping_add(1);
    }

    /// A fresh output abandoned at generation (a terminal channel failure on the
    /// first write, so it never occupied the retained slot).
    #[inline]
    pub fn output_failed_now(&mut self) {
        self.outputs_generated = self.outputs_generated.wrapping_add(1);
        self.outputs_failed = self.outputs_failed.wrapping_add(1);
    }

    /// Account `n` units of work (VM instructions / stages / emissions) this
    /// step consumed. The unit is the module's; only the total is held here.
    #[inline]
    pub fn add_work(&mut self, n: u64) {
        self.work_units = self.work_units.wrapping_add(n);
    }

    /// Fan-out: `count` logical outputs generated and RETAINED together (one event's
    /// window emissions captured into a queue), holding `bytes` total pending. Each
    /// later drains individually via `output_drained`.
    #[inline]
    pub fn outputs_staged_bulk(&mut self, count: u32, bytes: u32) {
        self.outputs_generated = self.outputs_generated.wrapping_add(count as u64);
        self.outputs_pending = self.outputs_pending.saturating_add(count);
        self.pending_output_bytes = self.pending_output_bytes.saturating_add(bytes);
    }

    /// Fan-out: `count` logical outputs that could not be retained (a bounded-queue
    /// saturation drop) — generated and immediately failed, never pending.
    #[inline]
    pub fn outputs_failed_bulk(&mut self, count: u32) {
        self.outputs_generated = self.outputs_generated.wrapping_add(count as u64);
        self.outputs_failed = self.outputs_failed.wrapping_add(count as u64);
    }

    /// The two input identities plus the output identity. Pure predicate for host
    /// tests; the modules never need to call it.
    #[inline]
    pub fn invariants_hold(&self) -> bool {
        self.inputs_observed == self.inputs_admitted + self.inputs_rejected
            && self.inputs_admitted
                == self.inputs_succeeded
                    + self.inputs_policy_dropped
                    + self.inputs_failed
                    + self.inputs_in_flight as u64
            && self.outputs_generated
                == self.outputs_delivered + self.outputs_pending as u64 + self.outputs_failed
    }
}
