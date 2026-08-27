// Shared telemetry-emit helpers for the steady-state modules. `include!`d at each
// module's CRATE ROOT — after the SDK `runtime.rs`,
// so `dev_telemetry_enabled`/`dev_millis`/`dev_self_index`/`dev_micros`/
// `dev_telemetry_metric` and `abi::contracts::telemetry` are in scope. NOT inside
// the domain-core submodule (those helpers live at the crate root).
//
// A module stores its counters/gauges in `ModuleState` and, at the top of every
// `module_step`, calls `tlm_tick`; when it returns `Some((module_index, t))` (at
// most every `TLM_INTERVAL_MS`, and only when a consumer is subscribed) the module
// emits each declared instrument BY INDEX — the id is the metric's position in the
// manifest `[observability] metrics` array (see fluxor tools/observability.rs).
// Emission is otherwise zero-cost: the enabled gate short-circuits inside
// `dev_telemetry_metric`.

/// How often a module publishes its instrument snapshot, in milliseconds. Matches
/// the DNS foundation module's cadence so a collector sees a uniform rate.
const TLM_INTERVAL_MS: u64 = 5000;

/// Throttled telemetry gate. Returns `Some((module_index, t_micros))` when this
/// step should publish its instruments, else `None`. Advances `last_ms` on a
/// publish. Safe to call every step.
///
/// # Safety
/// `sys` must be a live `SyscallTable` (as in `module_step`).
#[allow(dead_code, reason = "emit-side helper; used by instrumented modules")]
#[inline]
unsafe fn tlm_tick(sys: &SyscallTable, last_ms: &mut u64) -> Option<(u16, u64)> {
    if !dev_telemetry_enabled(sys) {
        return None;
    }
    let now = dev_millis(sys);
    if now.wrapping_sub(*last_ms) < TLM_INTERVAL_MS {
        return None;
    }
    *last_ms = now;
    let me = dev_self_index(sys);
    if me < 0 {
        return None;
    }
    Some((me as u16, dev_micros(sys)))
}

/// Emit a monotonic counter instrument `id` = `value`.
///
/// # Safety
/// As `tlm_tick`.
#[allow(dead_code, reason = "emit-side helper; used by instrumented modules")]
#[inline]
unsafe fn tlm_counter(sys: &SyscallTable, midx: u16, t: u64, id: u16, value: u64) {
    dev_telemetry_metric(
        sys,
        -1,
        midx,
        t,
        abi::contracts::telemetry::METRIC_COUNTER,
        id,
        value,
    );
}

/// Emit an up-down / current-value (gauge) instrument `id` = `value` — for
/// `module_mode`, pending counts, and other point-in-time state.
///
/// # Safety
/// As `tlm_tick`.
#[allow(dead_code, reason = "emit-side helper; used by instrumented modules")]
#[inline]
unsafe fn tlm_gauge(sys: &SyscallTable, midx: u16, t: u64, id: u16, value: u64) {
    dev_telemetry_metric(
        sys,
        -1,
        midx,
        t,
        abi::contracts::telemetry::METRIC_UPDOWN,
        id,
        value,
    );
}

/// Emit the whole baseline accounting block for `acct`, ids `base..base+14`
/// in canonical order, each as a counter or a gauge per `ACCT_IS_GAUGE`. A module
/// front-loads these 14 names in its manifest (so `base` is 0) and emits its own
/// instruments after. `acct` is passed by pointer so this stays usable from the
/// unsafe `module_step` without borrowing the whole state.
///
/// # Safety
/// As `tlm_tick`; `acct` must be a live `Accounting`.
#[allow(dead_code, reason = "emit-side helper; used by instrumented modules")]
#[inline]
unsafe fn acct_emit(sys: &SyscallTable, midx: u16, t: u64, base: u16, acct: &Accounting) {
    let mut i = 0usize;
    while i < ACCT_METRIC_COUNT {
        let id = base + i as u16;
        let v = acct.value(i);
        if ACCT_IS_GAUGE[i] {
            tlm_gauge(sys, midx, t, id, v);
        } else {
            tlm_counter(sys, midx, t, id, v);
        }
        i += 1;
    }
}
