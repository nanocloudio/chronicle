//! Expression evaluator — Fluxor `.fmod` app module.
//!
//! PARAM-DRIVEN: the checked-CEL bytecode is NOT baked into the module — it
//! arrives as a `program` module param (a `blob`: `{hex: "…"}` in a graph)
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
    include!("../../common/blob_core.rs");
    include!("../../common/lower_core.rs");
    include!("../../common/outcome_core.rs");
    include!("../../common/io_core.rs");
    include!("../../common/accounting_core.rs");
    include!("../../common/syschan_core.rs");
    include!("../../common/expression_step_core.rs");
}
use expr::{
    expr_step, lower_flat, scan_code, Accounting, BlobIn, Mode, Pending, StepResult, SysChan,
    ACCT_IS_GAUGE, ACCT_METRIC_COUNT,
};

// Telemetry emit helpers — crate root, after the SDK runtime, so the `dev_*`
// telemetry primitives it wraps are in scope.
include!("../../common/telemetry_core.rs");

/// Input and output record buffers, sized to the port `max_record` so a full
/// typed frame fits and admission is never a partial acceptance.
///
/// DERIVED from the state arena the target has, not from a die name: where the
/// whole arena is 64 KiB, two 4 KiB record buffers are an eighth of it, spent on
/// records a graph that size does not carry — such a node is fed by a sensor
/// driver, and a reading is tens of bytes. The manifest declares the matching
/// `max_record` per target, so the advertised ceiling and the buffer behind it are
/// one number.
///
/// Keyed on the target's memory class (`abi::config::TARGET_CLASS`) rather than
/// on `fluxor_abi` because it says the REASON —
/// how much memory there is, not which part — so a target added later inherits it
/// without an edit, and because `abi::config` is in scope in every module while an
/// undeclared `cfg` would trip `unexpected_cfgs` under `-D warnings`.
///
/// The same bound `decision` and `pipeline` carry, and it has to be: the three sit
/// on one channel family carrying the same typed records, so an `expression` that
/// admitted more than the `decision` downstream would pass records that node must
/// then refuse. `limit_register.md` records the coupling and the gate holds the
/// three to one derivation.
const TINY: bool = abi::config::TARGET_CLASS == abi::config::target_class::TINY;
const REC_BUF: usize = if TINY { 512 } else { 4096 };
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

    // Param-driven definition, `blob` params: pre-lowered bytecode (`program`)
    // arriving straight into `code`, or a flat checked IR (`ir`) arriving into
    // `flat` and lowered into `code` at load.
    prog_in: BlobIn,
    ir_in: BlobIn,
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
    /// Current operating mode (`outcome_core::Mode` as u8), published as the
    /// `module_mode` gauge so an operator can tell ready / output-blocked / faulted
    /// / awaiting-config apart from telemetry alone.
    mode: u8,
    /// Wall-clock ms of the last telemetry publish (throttle state).
    tlm_last_ms: u64,
}

// Module params: the bytecode `program` (a `blob`) and its static `max_cost`
// ceiling, or a flat checked IR `ir` (a `blob`) whose cost bound is re-derived.
// `define_params!` generates `parse_tlv`/`dispatch_param`; each blob arrives
// in chunks of at most 255, the first led by its whole length (blob_core).
define_params! {
    ModuleState;

    1, program, blob, 0 => |s, d, len| {
        s.prog_in.take(&mut s.code, core::slice::from_raw_parts(d, len));
    };

    2, max_cost, u32, 8 => |s, d, len| {
        s.max_cost = p_u32(d, len, 0, 8) as u64;
    };

    // A shipped flat checked IR, lowered to bytecode at load. No `max_cost` is
    // needed — `lower_flat` re-derives the cost bound from the IR itself.
    3, ir, blob, 0 => |s, d, len| {
        s.ir_in.take(&mut s.flat, core::slice::from_raw_parts(d, len));
    };
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<ModuleState>() as u32
}

// The same figure as data, so `pack` records this engine's resident footprint
// in its manifest and a graph's state-arena demand is summable at compose time
// rather than discovered when the device fails to load it.
declare_module_state_bytes!(ModuleState);

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
        s.prog_in = BlobIn::new();
        s.ir_in = BlobIn::new();
        s.code_len = 0;
        s.max_cost = 8;
        s.acct = Accounting::default();
        s.faulted = 0;
        s.mode = Mode::AwaitingConfig.as_u8();
        // Backdate so the FIRST telemetry publish fires promptly (dev_millis is
        // uptime, near 0 at startup), not only after one full interval.
        s.tlm_last_ms = dev_millis(sys).wrapping_sub(TLM_INTERVAL_MS);

        // Decode the param-supplied definition into runnable bytecode.
        //
        // FAULT DISCIPLINE: a param that was PROVIDED but is broken — cut
        // short, failed lowering, an opcode or builtin this build lacks — is a
        // configuration fault, named once at error level, and the node
        // refuses input (visible backpressure) rather than sitting inert
        // while records vanish. "No param at all" stays a distinct message:
        // a node awaiting late config is not a broken one.
        parse_tlv(s, params, params_len);
        let mut fault: &'static [u8] = b"";
        if s.ir_in.given() {
            // `ir` param: lower the flat IR HERE. A successful lowering is the
            // proof this module can run it; the cost bound is re-derived, not
            // trusted.
            match s.ir_in.whole() {
                Some(flen) => match lower_flat(&s.flat[..flen], &mut s.code) {
                    Ok((clen, cost)) => {
                        s.code_len = clen as u16;
                        s.max_cost = cost;
                    }
                    Err(_) => fault = b"[expr] FAULT: ir param failed to lower",
                },
                None => fault = b"[expr] FAULT: ir param cut short or past its buffer",
            }
        } else if s.prog_in.given() {
            // `program` param: pre-lowered bytecode, arrived straight in.
            match s.prog_in.whole() {
                Some(n) => s.code_len = n as u16,
                None => fault = b"[expr] FAULT: program param cut short or past its buffer",
            }
        }
        // Load-time scan: unknown opcodes, truncation, builtins not in this
        // build's variant. Refused here, once — never per-record.
        if s.code_len > 0 && scan_code(&s.code[..s.code_len as usize]).is_err() {
            fault = b"[expr] FAULT: program needs an opcode/builtin not in this build";
        }
        if !fault.is_empty() {
            s.code_len = 0;
            s.faulted = 1;
            dev_log(sys, 1, fault.as_ptr(), fault.len());
        } else if s.code_len == 0 {
            dev_log(sys, 3, b"[expr] no program param".as_ptr(), 23);
        } else if s.ir_in.given() {
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
