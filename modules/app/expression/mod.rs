//! Expression evaluator — Fluxor `.fmod` app module (spec artefact 2).
//!
//! PARAM-DRIVEN: the checked-CEL bytecode is NOT baked into the module — it
//! arrives as a `program` module param (hex-encoded, since a config carries text)
//! with a `max_cost` ceiling. So one evaluator binary serves ANY Expression: the
//! `.uproc`/CEL compiler emits the bytecode, a config sets it as the param, and
//! this module runs it. Two configs with different `program` values produce
//! different results from the identical `.fmod` (see examples/expression/*.yaml).
//!
//! It loads the SAME bounded evaluator source the host harness (tests/harness)
//! uses (via `include!` of `vm_core`), admits a whole typed record frame on `in`
//! through the shared `io_core` lifecycle, decodes it into fields, runs the param
//! bytecode, and stages the scalar result bytes to `out` — retained until fully
//! delivered.

#![no_std]
#![allow(
    unused_imports,
    dead_code,
    reason = "the fluxor SDK is include!'d wholesale and each module consumes only a subset"
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

// The shared cores, mounted together in one submodule so vm_core, the canonical
// typed-frame codec (`pipeline_core`), the record lifecycle (`io_core`), the
// outcome vocabulary and the syscall `Chan` adapter all see one another — the same
// `mod`-wrapped `include!` discipline the decision module uses. Expression
// admits the SAME typed record frame as pipeline/aggregation via `decode_frame`.
mod expr {
    use super::abi::SyscallTable;
    include!("../../common/vm_core.rs");
    include!("../../common/pipeline_core.rs");
    include!("../../common/hex_core.rs");
    include!("../../common/lower_core.rs");
    include!("../../common/outcome_core.rs");
    include!("../../common/io_core.rs");
    include!("../../common/accounting_core.rs");
    include!("../../common/syschan_core.rs");
    include!("../../common/expression_step_core.rs");
}
use expr::{
    expr_step, hex_decode, lower_flat, scan_code, Accounting, Mode, Pending, StepResult, SysChan,
    ACCT_IS_GAUGE, ACCT_METRIC_COUNT,
};

// Telemetry emit helpers — crate root, after the SDK runtime, so the `dev_*`
// telemetry primitives it wraps are in scope.
include!("../../common/telemetry_core.rs");

/// Input and output record buffers, sized to the port max_record so a full typed
/// frame fits and admission is never a partial acceptance.
const REC_BUF: usize = 4096;
const HEX_BUF: usize = 1024;
const CODE_BUF: usize = 512;

#[repr(C)]
struct ModuleState {
    syscalls: *const SyscallTable,
    in_chan: i32,
    out_chan: i32,
    in_buf: [u8; REC_BUF],
    out_buf: [u8; REC_BUF],
    /// One retained output frame, drained before any new input is admitted.
    pending: Pending,

    // Param-driven definition.
    hex: [u8; HEX_BUF],
    hex_len: u16,
    // `true` when the hex holds a flat checked IR (`ir` param) to be lowered at
    // load, rather than pre-lowered bytecode (`program` param).
    is_ir: bool,
    // Scratch for the decoded flat IR before `lower_flat` emits into `code`.
    flat: [u8; CODE_BUF],
    code: [u8; CODE_BUF],
    code_len: u16,
    max_cost: u64,

    /// The common accounting taxonomy — every observed record classified into
    /// exactly one input and output disposition, invariants maintained by the core:
    /// `inputs_succeeded` is a delivered result, `inputs_failed` a terminal
    /// processing failure, and `inputs_rejected` a boundary refused at admission.
    acct: Accounting,
    /// 1 = configuration fault at init: the node refuses input (declared
    /// metric; the named reason was logged once at error level).
    faulted: u32,
    /// Set when a string param exceeded its buffer during parsing. A truncated hex
    /// program could decode to a DIFFERENT valid program, so an overflow faults the
    /// candidate rather than running silently-altered bytecode.
    param_overflow: bool,
    /// Current operating mode (`outcome_core::Mode` as u8), published as the
    /// `module_mode` gauge so an operator can tell ready / output-blocked / faulted
    /// / awaiting-config apart from telemetry alone.
    mode: u8,
    /// Wall-clock ms of the last telemetry publish (throttle state).
    tlm_last_ms: u64,
}

// Module params: the hex bytecode `program` (str, chunk-appended) and its static
// `max_cost` ceiling. `define_params!` generates `parse_tlv`/`dispatch_param`.
define_params! {
    ModuleState;

    1, program, str, 0 => |s, d, len| {
        let mut i = 0usize;
        while i < len && (s.hex_len as usize) < HEX_BUF {
            s.hex[s.hex_len as usize] = *d.add(i);
            s.hex_len += 1;
            i += 1;
        }
        if i < len { s.param_overflow = true; }
    };

    2, max_cost, u32, 8 => |s, d, len| {
        s.max_cost = p_u32(d, len, 0, 8) as u64;
    };

    // A shipped flat checked IR (hex), lowered to bytecode at load. Shares the hex
    // buffer with `program`; the `is_ir` flag selects the load path. No `max_cost`
    // is needed — `lower_flat` re-derives the cost bound from the IR itself.
    // NOTE: `define_params!` generates `set_defaults()`, which invokes EVERY
    // declared param's closure in declaration order regardless of whether the
    // config supplies it. So the `is_ir` flag must be set only when this param
    // actually carries bytes — setting it unconditionally routes a plain
    // `program` bytecode param through `lower_flat` and fails the load.
    3, ir, str, 0 => |s, d, len| {
        if len > 0 { s.is_ir = true; }
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
        s.in_buf = [0u8; REC_BUF];
        s.out_buf = [0u8; REC_BUF];
        s.pending = Pending { off: 0, len: 0 };
        s.hex_len = 0;
        s.is_ir = false;
        s.code_len = 0;
        s.max_cost = 8;
        s.acct = Accounting::default();
        s.faulted = 0;
        s.param_overflow = false;
        s.mode = Mode::AwaitingConfig.as_u8();
        // Backdate so the FIRST telemetry publish fires promptly (dev_millis is
        // uptime, near 0 at startup), not only after one full interval.
        s.tlm_last_ms = dev_millis(sys).wrapping_sub(TLM_INTERVAL_MS);

        // Decode the param-supplied definition into runnable bytecode.
        //
        // FAULT DISCIPLINE: a param that was PROVIDED but is broken — bad
        // hex, failed lowering, an opcode or builtin this build lacks — is a
        // configuration fault, named once at error level, and the node
        // refuses input (visible backpressure) rather than sitting inert
        // while records vanish. "No param at all" stays a distinct message:
        // a node awaiting late config is not a broken one.
        parse_tlv(s, params, params_len);
        let mut fault: &'static [u8] = b"";
        if s.is_ir {
            // `ir` param: decode the hex to the flat IR, then lower it HERE. A
            // successful lowering is the proof this module can run it; the cost
            // bound is re-derived, not trusted.
            match hex_decode(&s.hex[..s.hex_len as usize], &mut s.flat) {
                Some(flen) => match lower_flat(&s.flat[..flen], &mut s.code) {
                    Ok((clen, cost)) => {
                        s.code_len = clen as u16;
                        s.max_cost = cost;
                    }
                    Err(_) => fault = b"[expr] FAULT: ir param failed to lower",
                },
                None => fault = b"[expr] FAULT: ir param is not valid hex",
            }
        } else if s.hex_len > 0 {
            // `program` param: pre-lowered bytecode, hex-decoded straight in.
            match hex_decode(&s.hex[..s.hex_len as usize], &mut s.code) {
                Some(n) => s.code_len = n as u16,
                None => fault = b"[expr] FAULT: program param is not valid hex",
            }
        }
        // Load-time scan: unknown opcodes, truncation, builtins not in this
        // build's variant. Refused here, once — never per-record.
        if s.code_len > 0 && scan_code(&s.code[..s.code_len as usize]).is_err() {
            fault = b"[expr] FAULT: program needs an opcode/builtin not in this build";
        }
        // A param that overflowed its buffer was truncated — fault rather than run
        // silently-altered bytecode. Checked last so it always wins.
        if s.param_overflow {
            fault = b"[expr] FAULT: a param exceeded its buffer and was truncated";
        }
        if !fault.is_empty() {
            s.code_len = 0;
            s.faulted = 1;
            dev_log(sys, 1, fault.as_ptr(), fault.len());
        } else if s.code_len == 0 {
            dev_log(sys, 3, b"[expr] no program param".as_ptr(), 23);
        } else if s.is_ir {
            dev_log(sys, 3, b"[expr] init (ir)".as_ptr(), 16);
        } else {
            dev_log(sys, 3, b"[expr] init".as_ptr(), 11);
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

        // Publish telemetry (throttled; zero-cost when no consumer is subscribed).
        // Emitted BEFORE the early returns so a faulted / awaiting-config / idle
        // module still reports its mode and counters.
        s.mode = if s.faulted != 0 {
            Mode::Faulted.as_u8()
        } else if s.code_len == 0 {
            Mode::AwaitingConfig.as_u8()
        } else if !s.pending.is_empty() {
            Mode::OutputBlocked.as_u8()
        } else {
            Mode::Ready.as_u8()
        };
        if let Some((midx, t)) = tlm_tick(sys, &mut s.tlm_last_ms) {
            // ids 0..13: the baseline accounting block (canonical order).
            acct_emit(sys, midx, t, 0, &s.acct);
            // id 14: module_mode; id 15: the init/reload configuration-fault flag.
            tlm_gauge(sys, midx, t, ACCT_METRIC_COUNT as u16, s.mode as u64);
            tlm_gauge(sys, midx, t, ACCT_METRIC_COUNT as u16 + 1, s.faulted as u64);
            // id 16: work units — VM instructions this module has executed.
            tlm_counter(
                sys,
                midx,
                t,
                ACCT_METRIC_COUNT as u16 + 2,
                s.acct.work_units,
            );
        }

        if s.in_chan < 0 || s.code_len == 0 || s.faulted != 0 {
            return 0;
        }
        let inch = SysChan::new(sys, s.in_chan);
        let outch = SysChan::new(sys, s.out_chan);
        let code_len = s.code_len as usize;
        let max_cost = s.max_cost;
        // The whole record lifecycle lives in `expr_step` over the io_core seam; the
        // shell only adapts the ABI and maps the disposition to counters. `in_buf`,
        // `out_buf`, `pending` and `code` are disjoint fields, borrowed together.
        // The step core records every disposition into `acct` at the one point that
        // knows admit-bytes and drain-vs-fresh; the shell does not re-derive
        // counts from `StepResult` (which cannot distinguish those).
        let _ = expr_step(
            &inch,
            &outch,
            &mut s.in_buf,
            &mut s.out_buf,
            &mut s.pending,
            &s.code[..code_len],
            max_cost,
            &mut s.acct,
        );
        0
    }
}
