//! Decision executor — a Fluxor `.fmod` app module (spec artefact 4, on device).
//!
//! PARAM-DRIVEN: the decision table is NOT baked — it arrives as a `decision`
//! module param (hex of the serialized container `[nrules][when,outcome]…[default]`,
//! see `decision_core.rs`). So one decision binary runs ANY Decision: the compiler
//! emits each predicate/outcome program, a config packs the container, and this
//! module runs a first-hit policy over an input record, emitting the selected
//! outcome as the next record frame.
//!
//! A decision is its OWN node (not a pipeline bytecode stage) because the VM has
//! no branching opcode — a single program constructs one message and cannot select
//! among several. The first-hit driver lives in `decision_core.rs`, `include!`d
//! verbatim from the host harness (tests/harness), so this module and the host tests
//! run identical logic.

#![no_std]
#![allow(
    unused_imports,
    dead_code,
    reason = "the fluxor SDK + shared cores are include!'d wholesale; each module consumes only a subset"
)]

use core::ffi::c_void;

#[allow(
    unused_imports,
    dead_code,
    reason = "shared SDK surface across modules"
)]
#[path = "../../../target/fluxor/fluxor-abi/sdk/abi.rs"]
mod abi;
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");

mod dec {
    use super::abi::SyscallTable;
    include!("../../common/vm_core.rs");
    include!("../../common/pipeline_core.rs");
    include!("../../common/decision_core.rs");
    include!("../../common/hex_core.rs");
    include!("../../common/outcome_core.rs");
    include!("../../common/io_core.rs");
    include!("../../common/accounting_core.rs");
    include!("../../common/syschan_core.rs");
    include!("../../common/decision_step_core.rs");
}
use dec::{
    decision_step, hex_decode, scan_decision_container, Accounting, Mode, Pending, Reason,
    StepResult, SysChan, ACCT_IS_GAUGE, ACCT_METRIC_COUNT,
};

// Telemetry emit helpers — crate root, after the SDK runtime so its primitives are in scope.
include!("../../common/telemetry_core.rs");

const HEX_BUF: usize = 8192;
const CONT_BUF: usize = 4096;

#[repr(C)]
struct ModuleState {
    syscalls: *const SyscallTable,
    in_chan: i32,
    out_chan: i32,
    // 4096, matching `pipeline`'s `REC_BUF`. The two engines sit on the same
    // channels carrying the same records, so a `decision` that could hold
    // less than the `pipeline` feeding it would drop exactly the records the
    // pipeline had just gone to the trouble of carrying.
    in_buf: [u8; 4096],
    out_buf: [u8; 4096],
    /// One retained output frame, drained before any new input is admitted.
    pending: Pending,
    hex: [u8; HEX_BUF],
    hex_len: u16,
    cont: [u8; CONT_BUF],
    cont_len: u16,
    /// The common accounting taxonomy: a delivered non-empty outcome is
    /// `inputs_succeeded`, an empty-outcome filter `inputs_policy_dropped`, an
    /// admission refusal `inputs_rejected`, and any terminal processing failure
    /// `inputs_failed`. The two reason splits below refine `inputs_failed`.
    acct: Accounting,
    /// Frame-decode failures, split from eval errors so a miswired channel
    /// (malformed frames) is distinguishable from a broken program. A refinement of
    /// `inputs_failed`, not an addition to it.
    errors_frame: u32,
    /// Output-frame encode failures (oversized outcome).
    errors_encode: u32,
    /// 1 = configuration fault at init: the node refuses input (declared
    /// metric; the named reason was logged once at error level).
    faulted: u32,
    /// A string param overflowed its buffer during parsing (truncated) — a fault,
    /// since a truncated container could decode to a different policy.
    param_overflow: bool,
    /// Current operating mode (`Mode` as u8), published as `module_mode`.
    mode: u8,
    /// Wall-clock ms of the last telemetry publish (throttle state).
    tlm_last_ms: u64,
    /// Records that matched no rule and took the DEFAULT outcome.
    no_match: u32,
    /// The rule index that produced the most recent outcome (`0xFFFF` = the default,
    /// or none yet) — the `Fired` audit, published as a gauge.
    last_rule: u16,
}

define_params! {
    ModuleState;

    1, decision, str, 0 => |s, d, len| {
        let mut i = 0usize;
        while i < len && (s.hex_len as usize) < HEX_BUF {
            s.hex[s.hex_len as usize] = *d.add(i);
            s.hex_len += 1;
            i += 1;
        }
        if i < len { s.param_overflow = true; }
    };
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<ModuleState>() as u32
}

#[no_mangle]
#[link_section = ".text.module_init"]
pub extern "C" fn module_init(_syscalls: *const c_void) {}

#[no_mangle]
#[link_section = ".text.module_new"]
#[allow(
    clippy::not_unsafe_ptr_arg_deref,
    reason = "the fluxor module ABI entry point: the runtime owns these pointers and \
              their validity is the ABI's contract, and the signature is fixed by that \
              contract rather than chosen here"
)]
pub extern "C" fn module_new(
    in_chan: i32,
    out_chan: i32,
    _ctrl_chan: i32,
    params: *const u8,
    params_len: usize,
    state: *mut u8,
    state_size: usize,
    syscalls: *const c_void,
) -> i32 {
    unsafe {
        if syscalls.is_null() || state.is_null() {
            return -1;
        }
        if state_size < core::mem::size_of::<ModuleState>() {
            return -2;
        }
        let s = &mut *(state as *mut ModuleState);
        let sys = &*(syscalls as *const SyscallTable);
        s.syscalls = sys;
        s.in_chan = in_chan;
        s.out_chan = out_chan;
        s.pending = Pending { off: 0, len: 0 };
        s.hex_len = 0;
        s.cont_len = 0;
        s.acct = Accounting::default();
        s.param_overflow = false;
        s.mode = Mode::AwaitingConfig.as_u8();
        // Backdate so the FIRST telemetry publish fires promptly (dev_millis is
        // uptime, near 0 at startup), not only after one full interval.
        s.tlm_last_ms = dev_millis(sys).wrapping_sub(TLM_INTERVAL_MS);
        s.no_match = 0;
        s.last_rule = 0xFFFF;
        s.errors_frame = 0;
        s.errors_encode = 0;
        s.faulted = 0;

        // FAULT DISCIPLINE: a `decision` param that was PROVIDED but is
        // broken — bad hex, a truncated container, a program this build
        // cannot run — is a configuration fault, named once at error level;
        // the node then refuses input (visible backpressure) rather than
        // sitting inert while records vanish. "No param" stays distinct.
        parse_tlv(s, params, params_len);
        let mut fault: &'static [u8] = b"";
        if s.hex_len > 0 {
            match hex_decode(&s.hex[..s.hex_len as usize], &mut s.cont) {
                Some(n) => s.cont_len = n as u16,
                None => fault = b"[decision] FAULT: decision param is not valid hex",
            }
        }
        if s.cont_len > 0 {
            // Load-time scan of every rule's when/outcome and the default:
            // unknown opcodes, truncation, builtins not in this variant.
            if scan_decision_container(&s.cont[..s.cont_len as usize]).is_err() {
                fault = b"[decision] FAULT: container invalid or needs a builtin not in this build";
            }
        }
        if s.param_overflow {
            fault = b"[decision] FAULT: a param exceeded its buffer and was truncated";
        }
        if !fault.is_empty() {
            s.cont_len = 0;
            s.faulted = 1;
            dev_log(sys, 1, fault.as_ptr(), fault.len());
        } else if s.cont_len == 0 {
            dev_log(sys, 3, b"[decision] no decision param".as_ptr(), 28);
        } else {
            dev_log(sys, 3, b"[decision] init".as_ptr(), 15);
        }
        0
    }
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    unsafe {
        let s = &mut *(state as *mut ModuleState);
        let sys = &*s.syscalls;

        s.mode = if s.faulted != 0 {
            Mode::Faulted.as_u8()
        } else if s.cont_len == 0 {
            Mode::AwaitingConfig.as_u8()
        } else if !s.pending.is_empty() {
            Mode::OutputBlocked.as_u8()
        } else {
            Mode::Ready.as_u8()
        };
        if let Some((midx, t)) = tlm_tick(sys, &mut s.tlm_last_ms) {
            // ids 0..13: the baseline accounting block.
            acct_emit(sys, midx, t, 0, &s.acct);
            // ids 14..: decision's own instruments. module_mode, the init
            // fault flag, the two failure-reason splits, and the `Fired`
            // audit — no_match count and the last rule index (0xFFFF = default).
            let b = ACCT_METRIC_COUNT as u16;
            tlm_gauge(sys, midx, t, b, s.mode as u64);
            tlm_gauge(sys, midx, t, b + 1, s.faulted as u64);
            tlm_counter(sys, midx, t, b + 2, s.errors_frame as u64);
            tlm_counter(sys, midx, t, b + 3, s.errors_encode as u64);
            tlm_counter(sys, midx, t, b + 4, s.no_match as u64);
            tlm_gauge(sys, midx, t, b + 5, s.last_rule as u64);
            // work units — VM instructions across predicates + the chosen outcome.
            tlm_counter(sys, midx, t, b + 6, s.acct.work_units);
        }

        if s.in_chan < 0 || s.cont_len == 0 || s.faulted != 0 {
            return 0;
        }
        let inch = SysChan::new(sys, s.in_chan);
        let outch = SysChan::new(sys, s.out_chan);
        let cont_len = s.cont_len as usize;
        // The whole record lifecycle lives in `decision_step` over the io_core seam;
        // the shell only adapts the ABI and maps the disposition to counters.
        let mut fired: i16 = -2; // unchanged if no decision ran this step
        let r = decision_step(
            &inch,
            &outch,
            &mut s.in_buf,
            &mut s.out_buf,
            &mut s.pending,
            &s.cont[..cont_len],
            &mut fired,
            &mut s.acct,
        );
        // Record the audit: which branch produced the outcome.
        if fired >= 0 {
            s.last_rule = fired as u16;
        } else if fired == -1 {
            s.last_rule = 0xFFFF;
            s.no_match = s.no_match.wrapping_add(1);
        }
        // The step core has already recorded every disposition into `acct`. The
        // wrapper adds only the failure-reason SPLITS that refine
        // `inputs_failed`: a frame-decode failure (a miswired channel) apart from an
        // oversized-outcome encode failure.
        match r {
            StepResult::Failed(Reason::Malformed) => {
                s.errors_frame = s.errors_frame.wrapping_add(1)
            }
            StepResult::Failed(Reason::TooLarge) => {
                s.errors_encode = s.errors_encode.wrapping_add(1)
            }
            _ => {}
        }
        0
    }
}
