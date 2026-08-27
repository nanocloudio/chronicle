//! Pipeline executor — Fluxor `.fmod` app module (spec artefact 6, on device).
//!
//! PARAM-DRIVEN: the stage table is NOT baked — it arrives as a `stages` module
//! param (hex-encoded), a serialized `[nstages][cost:u32][len:u16][code]…`
//! container. So one pipeline binary runs ANY Pipeline: the compiler emits each
//! stage's bytecode, a config packs the container, and this module threads a
//! record frame through the stages, serializing each stage's constructed message
//! as the next stage's input.
//!
//! The staged executor + container codec live in `pipeline_core.rs`, `include!`d
//! verbatim from the host harness (tests/harness), so this module and the host tests
//! (`tests/harness/tests/pipeline.rs`) run identical logic.

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

// Evaluator + staged executor + container codec + hex codec — identical source
// to the host harness (tests/harness), all in ONE module so cross-references resolve.
mod pipe {
    use super::abi::SyscallTable;
    include!("../../common/vm_core.rs");
    include!("../../common/pipeline_core.rs");
    include!("../../common/hex_core.rs");
    include!("../../common/ser_core.rs");
    include!("../../common/deser_core.rs");
    include!("../../common/version_core.rs");
    include!("../../common/lower_core.rs");
    include!("../../common/outcome_core.rs");
    include!("../../common/io_core.rs");
    include!("../../common/accounting_core.rs");
    include!("../../common/syschan_core.rs");
    include!("../../common/pipeline_reload_core.rs");
}
use pipe::{
    admit_frame, decode_frame, drain_all, encode_frame, eval_bytes, eval_decode, frame_len,
    hex_decode, lower_stages, parse_version_table, pipeline_reload, run_stages_metered,
    scan_version_table, stage_at, stage_count, version_selector_from_frame, Accounting, Admit,
    Builder, Field, Message, Mode, Pending, Stage, Staged, SysChan, Value, ACCT_IS_GAUGE,
    ACCT_METRIC_COUNT, MAX_PIPE_FIELDS,
};

// Telemetry emit helpers — crate root, after the SDK runtime so its primitives are in scope.
include!("../../common/telemetry_core.rs");

const MAX_STAGES: usize = 8;
const HEX_BUF: usize = 4096;
const PROG_BUF: usize = 2048;
const ENC_BUF: usize = 4096;

/// One record, in bytes: the read buffer, both stage ping-pong buffers and
/// the write buffer.
///
/// 4096, not 512. A `pipeline` carries whatever record its graph carries, and
/// 512 was not sized for a workload — it fit the examples that existed. A
/// record that overruns it is not truncated, it is DROPPED: `channel_read`
/// stops at the buffer, the codec then reads a length the rest of the record
/// was going to satisfy, fails, and the record vanishes with a counter. From
/// the client that is a request that never answers, which is the hardest
/// possible shape to diagnose from the outside.
///
/// A single compact JWS is ~350 bytes, and an HTTP envelope carrying one
/// carries the path and header block beside it. `pipeline` is
/// `hardware_targets = ["bcm2712"]`, so no constrained target pays for this.
const REC_BUF: usize = 4096;
/// Backing buffer for the version table (holds every loaded version's program;
/// mutable for hot reload). Larger than one program so several versions coexist.
const VBIN_BUF: usize = 8192;
/// Control-message scratch for the `ctrl_input` port (hot-reload ops).
const CTRL_BUF: usize = 4096;

#[repr(C)]
struct ModuleState {
    syscalls: *const SyscallTable,
    in_chan: i32,
    out_chan: i32,
    in_buf: [u8; REC_BUF],
    buf_a: [u8; REC_BUF],
    buf_b: [u8; REC_BUF],
    out_buf: [u8; REC_BUF],

    // Param-driven stage table. `hex` holds the `ir_stages` container, lowered
    // into `prog`; `ver_hex` decodes the `versions` param. Both land in `vbin` —
    // the version table the module actually runs, one entry per loaded version
    // (see version_core.rs).
    hex: [u8; HEX_BUF],
    hex_len: u16,
    // Transient scratch: the lowered bytecode-stages container, folded into the
    // one-version table in `vbin`.
    prog: [u8; PROG_BUF],
    // Transient scratch: the decoded IR-stages container before `lower_stages`
    // transcodes it into `prog`.
    ir_scratch: [u8; PROG_BUF],
    ver_hex: [u8; HEX_BUF],
    ver_hex_len: u16,
    vbin: [u8; VBIN_BUF],
    vbin_len: u16,
    // Candidate version table for transactional reload: a control op is applied and
    // scanned HERE, and copied over `vbin` only on success, so a rejected update
    // never touches the active generation.
    vbin_cand: [u8; VBIN_BUF],
    /// One retained output frame, drained before any new input is admitted.
    pending: Pending,
    // Control port: hot-reload ops (add version / flip default / remove).
    ctrl_chan: i32,
    ctrl_buf: [u8; CTRL_BUF],
    reloads: u32,
    rejected: u32,    // hot-reload control messages the table rejected
    unavailable: u32, // records fail-closed because their version was not loaded
    /// 1 = configuration fault at init/reload: the node refuses input
    /// (declared metric; the named reason was logged once at error level).
    faulted: u32,

    // Optional trailing encoder: a byte-serialization program applied to the
    // final record to produce a wire payload (e.g. a RESP request for the
    // tcp_client). When present, the module emits raw bytes instead of a frame.
    enc_hex: [u8; HEX_BUF],
    enc_hex_len: u16,
    enc: [u8; PROG_BUF],
    enc_len: u16,
    enc_out: [u8; ENC_BUF],

    // Optional front-end decoder: a byte-deserialization program that parses a
    // raw protocol reply on the input into a record frame before the stages.
    dec_hex: [u8; HEX_BUF],
    dec_hex_len: u16,
    dec: [u8; PROG_BUF],
    dec_len: u16,
    dec_out: [u8; ENC_BUF],

    /// The common accounting taxonomy: a delivered output (a transformed record OR the fail-closed
    /// VERSION_UNAVAILABLE marker) resolves its input as `inputs_succeeded`; a
    /// terminal processing failure before any deliverable output is `inputs_failed`.
    /// `reloads`/`rejected` count CONTROL messages and stay outside this taxonomy.
    acct: Accounting,
    /// A string param overflowed its buffer during parsing (truncated) — a fault,
    /// since a truncated stage/version table could decode to a different program.
    param_overflow: bool,
    /// Current operating mode (`Mode` as u8), published as `module_mode`.
    mode: u8,
    /// Wall-clock ms of the last telemetry publish (throttle state).
    tlm_last_ms: u64,
}

define_params! {
    ModuleState;

    2, encode, str, 0 => |s, d, len| {
        let mut i = 0usize;
        while i < len && (s.enc_hex_len as usize) < HEX_BUF {
            s.enc_hex[s.enc_hex_len as usize] = *d.add(i);
            s.enc_hex_len += 1;
            i += 1;
        }
        if i < len { s.param_overflow = true; }
    };

    3, decode, str, 0 => |s, d, len| {
        let mut i = 0usize;
        while i < len && (s.dec_hex_len as usize) < HEX_BUF {
            s.dec_hex[s.dec_hex_len as usize] = *d.add(i);
            s.dec_hex_len += 1;
            i += 1;
        }
        if i < len { s.param_overflow = true; }
    };

    4, versions, str, 0 => |s, d, len| {
        let mut i = 0usize;
        while i < len && (s.ver_hex_len as usize) < HEX_BUF {
            s.ver_hex[s.ver_hex_len as usize] = *d.add(i);
            s.ver_hex_len += 1;
            i += 1;
        }
        if i < len { s.param_overflow = true; }
    };

    // A shipped IR-stages container (hex), lowered to a bytecode-stages container
    // at load (each stage's cost re-derived).
    5, ir_stages, str, 0 => |s, d, len| {
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
    ctrl_chan: i32,
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
        s.ctrl_chan = ctrl_chan;
        s.in_buf = [0u8; REC_BUF];
        s.buf_a = [0u8; REC_BUF];
        s.buf_b = [0u8; REC_BUF];
        s.out_buf = [0u8; REC_BUF];
        s.hex_len = 0;
        s.enc_hex_len = 0;
        s.enc_len = 0;
        s.dec_hex_len = 0;
        s.dec_len = 0;
        s.ver_hex_len = 0;
        s.vbin_len = 0;
        s.vbin_cand = [0u8; VBIN_BUF];
        s.pending = Pending { off: 0, len: 0 };
        s.param_overflow = false;
        s.mode = Mode::AwaitingConfig.as_u8();
        // Backdate so the FIRST telemetry publish fires promptly (dev_millis is
        // uptime, near 0 at startup), not only after one full interval.
        s.tlm_last_ms = dev_millis(sys).wrapping_sub(TLM_INTERVAL_MS);
        s.reloads = 0;
        s.rejected = 0;
        s.unavailable = 0;
        s.faulted = 0;

        parse_tlv(s, params, params_len);

        // Build the version table. A `versions` param is a ready-made table; an
        // `ir_stages` param becomes a single default-version table.
        // Lower the shipped `ir_stages` IR container into the bytecode-stages
        // container `s.prog` (each stage's cost re-derived at load).
        // FAULT DISCIPLINE: a param that was PROVIDED but is broken — bad
        // hex, a container that fails to lower, a program this build cannot
        // run — is a configuration fault, named once at error level; the
        // node then refuses input (visible backpressure) rather than sitting
        // inert while records vanish. "No param" stays a distinct message.
        let mut fault: &'static [u8] = b"";
        let prog_n = if s.hex_len == 0 {
            None
        } else {
            match hex_decode(&s.hex[..s.hex_len as usize], &mut s.ir_scratch) {
                None => {
                    fault = b"[pipeline] FAULT: ir_stages param is not valid hex";
                    None
                }
                Some(flen) => match lower_stages(&s.ir_scratch[..flen], &mut s.prog) {
                    Err(_) => {
                        fault = b"[pipeline] FAULT: ir_stages container failed to lower";
                        None
                    }
                    Ok(n) => Some(n),
                },
            }
        };

        if s.ver_hex_len > 0 {
            match hex_decode(&s.ver_hex[..s.ver_hex_len as usize], &mut s.vbin) {
                Some(n) => s.vbin_len = n as u16,
                None => fault = b"[pipeline] FAULT: versions param is not valid hex",
            }
        } else if let Some(n) = prog_n {
            // [1][0][digest:8 = 0][tag_len:0][prog_len:u16][prog]
            let mut w = 0usize;
            s.vbin[0] = 1;
            s.vbin[1] = 0;
            w += 2;
            let mut k = 0;
            while k < 8 {
                s.vbin[w] = 0;
                w += 1;
                k += 1;
            }
            s.vbin[w] = 0; // tag_len (empty tag = default only)
            w += 1;
            let pl = (n as u16).to_le_bytes();
            s.vbin[w] = pl[0];
            s.vbin[w + 1] = pl[1];
            w += 2;
            let mut j = 0;
            while j < n && w < VBIN_BUF {
                s.vbin[w] = s.prog[j];
                w += 1;
                j += 1;
            }
            s.vbin_len = w as u16;
        }

        match hex_decode(&s.enc_hex[..s.enc_hex_len as usize], &mut s.enc) {
            Some(n) => s.enc_len = n as u16,
            None => {
                s.enc_len = 0;
                if s.enc_hex_len > 0 {
                    fault = b"[pipeline] FAULT: encode param is not valid hex";
                }
            }
        }
        match hex_decode(&s.dec_hex[..s.dec_hex_len as usize], &mut s.dec) {
            Some(n) => s.dec_len = n as u16,
            None => {
                s.dec_len = 0;
                if s.dec_hex_len > 0 {
                    fault = b"[pipeline] FAULT: decode param is not valid hex";
                }
            }
        }
        // Load-time scan of every version's stage programs: unknown opcodes,
        // truncation, builtins not in this build's variant. Refused here,
        // once — never per-record. (`encode`/`decode` are ser/rd byte-VM
        // programs with their own opcode space; they carry no CALL and are
        // validated by their own evaluators' fail-closed paths.)
        if s.vbin_len > 0 && scan_version_table(&s.vbin[..s.vbin_len as usize]).is_err() {
            fault = b"[pipeline] FAULT: a stage needs an opcode/builtin not in this build";
        }
        if s.param_overflow {
            fault = b"[pipeline] FAULT: a param exceeded its buffer and was truncated";
        }
        s.acct = Accounting::default();
        if !fault.is_empty() {
            s.vbin_len = 0;
            s.faulted = 1;
            dev_log(sys, 1, fault.as_ptr(), fault.len());
        } else if s.vbin_len == 0 && s.enc_len == 0 && s.dec_len == 0 {
            dev_log(sys, 3, b"[pipeline] no program param".as_ptr(), 27);
        } else {
            dev_log(sys, 3, b"[pipeline] init".as_ptr(), 15);
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
        } else if s.vbin_len == 0 && s.enc_len == 0 && s.dec_len == 0 {
            Mode::AwaitingConfig.as_u8()
        } else if !s.pending.is_empty() {
            Mode::OutputBlocked.as_u8()
        } else {
            Mode::Ready.as_u8()
        };
        if let Some((midx, t)) = tlm_tick(sys, &mut s.tlm_last_ms) {
            // ids 0..13: the baseline accounting block.
            acct_emit(sys, midx, t, 0, &s.acct);
            // ids 14..: pipeline's own instruments. module_mode, the init/reload
            // fault flag, the two reload-CONTROL counters (control-plane counters, not record dispositions),
            // and version-unavailable (a refinement of which succeeded inputs were routed
            // to the fail-closed marker rather than transformed).
            let b = ACCT_METRIC_COUNT as u16;
            tlm_gauge(sys, midx, t, b, s.mode as u64);
            tlm_gauge(sys, midx, t, b + 1, s.faulted as u64);
            tlm_counter(sys, midx, t, b + 2, s.reloads as u64);
            tlm_counter(sys, midx, t, b + 3, s.rejected as u64);
            tlm_counter(sys, midx, t, b + 4, s.unavailable as u64);
            // work units — VM instructions across every stage executed (incl. routes).
            tlm_counter(sys, midx, t, b + 5, s.acct.work_units);
        }

        // 1. Hot reload — TRANSACTIONAL. Apply the control op to the candidate
        //    table, validate it, and copy it over the active table ONLY on success. A
        //    rejected candidate leaves the active generation byte-identical; it can no
        //    longer fault a healthy node.
        if s.ctrl_chan >= 0 {
            let cp = (sys.channel_poll)(s.ctrl_chan, 0x01);
            if cp > 0 && (cp as u32 & 0x01) != 0 {
                let cn = (sys.channel_read)(s.ctrl_chan, s.ctrl_buf.as_mut_ptr(), s.ctrl_buf.len());
                if cn > 0 {
                    let vbin_len = s.vbin_len as usize;
                    let active = core::slice::from_raw_parts(s.vbin.as_ptr(), vbin_len);
                    let msg = core::slice::from_raw_parts(s.ctrl_buf.as_ptr(), cn as usize);
                    match pipeline_reload(active, &mut s.vbin_cand, VBIN_BUF, msg) {
                        Ok(nu) => {
                            let src = core::slice::from_raw_parts(s.vbin_cand.as_ptr(), nu);
                            s.vbin[..nu].copy_from_slice(src);
                            s.vbin_len = nu as u16;
                            s.reloads = s.reloads.wrapping_add(1);
                            dev_log(sys, 3, b"[pipeline] reload".as_ptr(), 17);
                        }
                        // Active table untouched — a rejected reload cannot damage it.
                        Err(_) => s.rejected = s.rejected.wrapping_add(1),
                    }
                }
            }
        }

        let vbin_len = s.vbin_len as usize;
        if s.in_chan < 0 || s.faulted != 0 || (vbin_len == 0 && s.enc_len == 0 && s.dec_len == 0) {
            return 0;
        }
        let outch = SysChan::new(sys, s.out_chan);

        // 2. Deliver any retained output before admitting new input. One record
        //    is in flight at a time, so a blocked write can never lose a mid-batch
        //    frame.
        if !s.pending.is_empty() {
            let plen = s.pending.len as u32;
            match s.pending.drain(&outch, &s.out_buf) {
                // The retained frame is this in-flight record's one defined output;
                // its delivery resolves the record.
                Staged::Delivered => {
                    s.acct.output_drained(plen);
                    s.acct.input_succeeded();
                }
                Staged::Pending => return 0,
                Staged::Failed(_) => {
                    s.pending = Pending { off: 0, len: 0 };
                    s.acct.output_failed_pending(plen);
                    s.acct.input_failed();
                }
            }
            return 0;
        }

        // 3. Admit ONE input unit. mode (a) no decoder: a whole typed frame via peek.
        //    mode (b) decoder: a raw protocol chunk decoded to one frame (the decoder
        //    path carries no partial-read state — a protocol message split across
        //    reads fails the decode, counted).
        let frame_ptr: *const u8;
        let frame_bytes: usize;
        if s.dec_len > 0 {
            let poll = (sys.channel_poll)(s.in_chan, 0x01);
            if poll <= 0 || (poll as u32 & 0x01) == 0 {
                return 0;
            }
            let n = (sys.channel_read)(s.in_chan, s.in_buf.as_mut_ptr(), s.in_buf.len());
            if n <= 0 {
                return 0;
            }
            let read = n as usize;
            // A raw protocol chunk accepted for processing is one admitted record.
            s.acct.admit_input(read as u64);
            let mut b = Builder::new();
            let dec = core::slice::from_raw_parts(s.dec.as_ptr(), s.dec_len as usize);
            let inp = core::slice::from_raw_parts(s.in_buf.as_ptr(), read);
            match eval_decode(dec, inp, &mut b, 100_000) {
                Ok(()) => match encode_frame(&b.message(), &mut s.dec_out) {
                    Ok(rl) => {
                        frame_ptr = s.dec_out.as_ptr();
                        frame_bytes = rl;
                    }
                    Err(_) => {
                        s.acct.input_failed();
                        return 0;
                    }
                },
                Err(_) => {
                    s.acct.input_failed();
                    return 0;
                }
            }
        } else {
            let inch = SysChan::new(sys, s.in_chan);
            match admit_frame(&inch, &mut s.in_buf, frame_len) {
                Admit::Complete(nn) => {
                    s.acct.admit_input(nn as u64);
                    frame_ptr = s.in_buf.as_ptr();
                    frame_bytes = nn;
                }
                Admit::Empty | Admit::NeedMore => return 0,
                Admit::BoundaryLost => {
                    // A frame beyond max_record: a complete-but-untrusted unit,
                    // observed and rejected; nothing was read (no input bytes).
                    let _ = drain_all(&inch, &mut s.in_buf);
                    s.acct.reject_input(0);
                    return 0;
                }
                // A channel fault before a frame is framed is a dependency error,
                // not a received record — it enters no input bucket.
                Admit::ChanError(_) => return 0,
            }
        }
        let frame_in = core::slice::from_raw_parts(frame_ptr, frame_bytes);

        // 4. Resolve the version for this record and thread it through that version's
        //    stages, or emit the fail-closed VERSION_UNAVAILABLE frame.
        // Read the version selector straight from the raw frame — `run_stages`
        // decodes the frame again for stage 0, so a full pre-decode just to find
        // field 255 is duplicate hot-path work.
        let selector = version_selector_from_frame(frame_in);
        let vbin = core::slice::from_raw_parts(s.vbin.as_ptr(), s.vbin_len as usize);
        let prog: Option<&[u8]> = parse_version_table(vbin)
            .and_then(|t| t.resolve(selector).and_then(|i| t.entry(i).map(|e| e.prog)));

        let out_len = match prog {
            Some(prog) => {
                let ns = stage_count(prog);
                if ns > MAX_STAGES {
                    // Reject an over-cap stage container rather than silently skipping
                    // the trailing stages: a valid container that declares more
                    // stages than this build runs is an error, not a truncation.
                    s.acct.input_failed();
                    0
                } else {
                    let mut stages = [Stage {
                        code: &[],
                        max_cost: 0,
                        on_failure: None,
                    }; MAX_STAGES];
                    let mut ok = true;
                    for (i, st) in stages.iter_mut().enumerate().take(ns) {
                        match stage_at(prog, i) {
                            Some(stage) => *st = stage,
                            None => ok = false,
                        }
                    }
                    if !ok {
                        s.acct.input_failed();
                        0
                    } else {
                        {
                            let mut spent = 0u64;
                            let r = run_stages_metered(
                                &stages[..ns],
                                frame_in,
                                &mut s.buf_a,
                                &mut s.buf_b,
                                &mut s.out_buf,
                                &mut spent,
                            );
                            s.acct.add_work(spent);
                            r.unwrap_or_else(|_| {
                                s.acct.input_failed();
                                0
                            })
                        }
                    }
                }
            }
            None => {
                // Fail closed: a record pinned to a version this instance does not hold
                // gets a deterministic {1: "VERSION_UNAVAILABLE"} record (the LB retries
                // another instance) — never the wrong version.
                s.unavailable = s.unavailable.wrapping_add(1);
                write_unavailable_frame(&mut s.out_buf)
            }
        };
        if out_len == 0 {
            return 0;
        }

        // 5. Optional trailing encoder → a wire payload, copied into out_buf so
        //    delivery always retries from one stable buffer. Then stage (retained on a
        //    full ring, delivered on a later step — never dropped, never double-sent).
        let final_len = if s.enc_len > 0 {
            let mut fields = [Field {
                number: 0,
                value: Value::Null,
            }; MAX_PIPE_FIELDS];
            let enc = core::slice::from_raw_parts(s.enc.as_ptr(), s.enc_len as usize);
            let frame = core::slice::from_raw_parts(s.out_buf.as_ptr(), out_len);
            let nf = decode_frame(frame, &mut fields).unwrap_or(0);
            let params = [Message {
                fields: &fields[..nf],
            }];
            match eval_bytes(enc, &params, &mut s.enc_out, 100_000) {
                Ok(m) => {
                    let src = core::slice::from_raw_parts(s.enc_out.as_ptr(), m);
                    s.out_buf[..m].copy_from_slice(src);
                    m
                }
                Err(_) => {
                    s.acct.input_failed();
                    return 0;
                }
            }
        } else {
            out_len
        };

        // The record's one defined output (a transformed frame or the fail-closed
        // marker). Immediate delivery resolves the record now; a retained output
        // leaves it in flight until it drains.
        match s.pending.stage(&outch, &s.out_buf, final_len) {
            Staged::Delivered => {
                s.acct.output_delivered_now(final_len as u32);
                s.acct.input_succeeded();
            }
            Staged::Pending => s.acct.output_staged(final_len as u32),
            Staged::Failed(_) => {
                s.pending = Pending { off: 0, len: 0 };
                s.acct.output_failed_now();
                s.acct.input_failed();
            }
        }
        0
    }
}
/// Build a fail-closed `{1: "VERSION_UNAVAILABLE"}` record frame into `buf` via the
/// shared `encode_frame` (no hand-poked offsets — one frame encoder). Encoders
/// render field 1, so it surfaces as text; raw consumers see a field-1 marker.
fn write_unavailable_frame(buf: &mut [u8]) -> usize {
    let fields = [Field {
        number: 1,
        value: Value::Bytes(b"VERSION_UNAVAILABLE"),
    }];
    encode_frame(&Message { fields: &fields }, buf).unwrap_or(0)
}
