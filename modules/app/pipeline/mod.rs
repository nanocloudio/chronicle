//! Pipeline executor — Fluxor `.fmod` app module (spec artefact 6, on device).
//!
//! PARAM-DRIVEN: the stage table is NOT baked — it arrives as an `ir_stages`
//! module param (hex-encoded), lowered at load into a serialized
//! `[nstages][cost:u32][len:u16][code]…` container. So one pipeline binary runs ANY Pipeline: the compiler emits each
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

// The ordered-ack exchange surface. Egress framing has ONE definition, shared
// with every provider of the surface — a pipeline that publishes speaks the
// same frames as lattice's CDC pump.
#[allow(dead_code, reason = "this module consumes a subset of the surface")]
#[rustfmt::skip]
#[path = "../../../target/fluxor/fluxor-abi/sdk/contracts/exchange.rs"]
mod exchange;
use exchange::{
    Ack, Publish, ACK_WIRE_LEN, MSG_ACK, MSG_PUBLISH, PAYLOAD_MAX, PUBLISH_OVERHEAD,
    REFUSE_OVERSIZE, REFUSE_UNROUTABLE, STATUS_LINK_DOWN, STATUS_LINK_UP, STATUS_OK,
};

/// Publishes that may be unacknowledged at once. The window is what applies
/// BACKPRESSURE: at the limit the pipeline stops admitting records rather than
/// running ahead of a destination that has not confirmed anything.
const MAX_INFLIGHT: u32 = 8;
/// One framed publish at this module's ceiling: the 3-byte envelope, the
/// contract's fixed overhead, and a whole result frame (this module sends no
/// key). What `publish_out` declares as `max_record`.
const PUB_FRAME_MAX: usize = 3 + PUBLISH_OVERHEAD + REC_BUF;
/// One framed ack, envelope included.
const ACK_FRAME_LEN: usize = 3 + ACK_WIRE_LEN;
/// The intake buffer: a whole contract payload, so this module is a sink for
/// any producer of the surface (a CDC row image with its envelope exceeds one
/// typed record). A payload is decoded into a `REC_BUF` record by the
/// `decode` program; without one it must itself be one typed frame.
const INGRESS_BUF: usize = PAYLOAD_MAX;

/// Read and discard `n` bytes from a byte channel, in bounded chunks. Used to
/// step past a frame body that is not wanted once its header is consumed —
/// leaving it would misalign every later read.
unsafe fn chan_skip(sys: &SyscallTable, chan: i32, n: usize) {
    let mut left = n;
    while left > 0 {
        let mut junk = [0u8; 64];
        let want = if left < junk.len() { left } else { junk.len() };
        let r = (sys.channel_read)(chan, junk.as_mut_ptr(), want);
        if r <= 0 {
            return;
        }
        left -= r as usize;
    }
}

/// Flush an ack the ring refused earlier. `true` when nothing is retained.
unsafe fn ack_flush(s: &mut ModuleState, sys: &SyscallTable) -> bool {
    if s.ack_retry_len == 0 {
        return true;
    }
    let n = s.ack_retry_len as usize;
    if (sys.channel_write)(s.ack_out_chan, s.ack_retry.as_ptr(), n) == n as i32 {
        s.ack_retry_len = 0;
    }
    s.ack_retry_len == 0
}

/// Write one ack to `ack_out`, retaining it on a full ring so it is never
/// dropped: the contract answers every publish. Acks are ordered, so nothing
/// is written while an earlier one is retained.
unsafe fn ack_write(s: &mut ModuleState, sys: &SyscallTable, ack: Ack) {
    if !ack_flush(s, sys) {
        return;
    }
    let mut buf = [0u8; ACK_FRAME_LEN];
    buf[0] = MSG_ACK;
    buf[1..3].copy_from_slice(&(ACK_WIRE_LEN as u16).to_le_bytes());
    if ack.encode(&mut buf[3..]).is_none() {
        return;
    }
    if (sys.channel_write)(s.ack_out_chan, buf.as_ptr(), buf.len()) != buf.len() as i32 {
        s.ack_retry = buf;
        s.ack_retry_len = buf.len() as u8;
    }
}

/// Admit one publish from `publish_in` as the record in flight: its payload
/// lands in `in_buf`, its correlation id is held until the record resolves.
/// Returns the payload length, or `None` when nothing was admitted this step.
///
/// A payload beyond the contract ceiling is refused OVERSIZE rather than
/// truncated; a frame that does not parse as a publish is stepped past (there
/// is no corr to answer). Both are counted.
unsafe fn ingress_admit(s: &mut ModuleState, sys: &SyscallTable) -> Option<usize> {
    let ch = s.pub_in_chan;
    let poll = (sys.channel_poll)(ch, 0x01);
    if poll <= 0 || (poll as u32 & 0x01) == 0 {
        return None;
    }
    let mut hdr = [0u8; 3];
    if (sys.channel_read)(ch, hdr.as_mut_ptr(), 3) < 3 {
        return None;
    }
    let len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
    if hdr[0] != MSG_PUBLISH || len < PUBLISH_OVERHEAD {
        chan_skip(sys, ch, len);
        s.sink_refused = s.sink_refused.wrapping_add(1);
        return None;
    }
    let mut ph = [0u8; PUBLISH_OVERHEAD];
    if ((sys.channel_read)(ch, ph.as_mut_ptr(), PUBLISH_OVERHEAD) as usize) < PUBLISH_OVERHEAD {
        return None;
    }
    let corr = u64::from_le_bytes([ph[0], ph[1], ph[2], ph[3], ph[4], ph[5], ph[6], ph[7]]);
    let klen = u16::from_le_bytes([ph[9], ph[10]]) as usize;
    let plen = u16::from_le_bytes([ph[11], ph[12]]) as usize;
    let body = len - PUBLISH_OVERHEAD;
    if corr == 0 || klen + plen != body {
        chan_skip(sys, ch, body);
        s.sink_refused = s.sink_refused.wrapping_add(1);
        return None;
    }
    // The key is the producer's ordering unit; this module has one, so the
    // key is not needed.
    chan_skip(sys, ch, klen);
    if plen > s.in_buf.len() {
        chan_skip(sys, ch, plen);
        s.acct.reject_input(plen as u64);
        s.sink_refused = s.sink_refused.wrapping_add(1);
        if let Some(a) = Ack::reply(corr, REFUSE_OVERSIZE) {
            ack_write(s, sys, a);
        }
        return None;
    }
    if ((sys.channel_read)(ch, s.in_buf.as_mut_ptr(), plen) as usize) < plen {
        return None;
    }
    s.ingress_corr = corr;
    s.ingress_succeeded_at = s.acct.inputs_succeeded;
    s.acct.admit_input(plen as u64);
    Some(plen)
}

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
    in_buf: [u8; INGRESS_BUF],
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

    // ── Egress (`stream.ordered_ack`) ────────────────────────────
    /// out[1]: `publish_out`. `-1` when the graph wired no destination, in
    /// which case results leave on `result_out`.
    publish_chan: i32,
    /// in[1]: `ack_in`.
    ack_chan: i32,
    /// Next correlation id. Never 0 — a publish with corr 0 is malformed, and
    /// 0 on an ack means a link-state signal rather than an answer.
    corr_next: u64,
    /// Publishes issued and not yet answered.
    inflight: u32,
    /// Publishes the destination answered with a typed refusal.
    refused: u32,
    /// Publishes outstanding when a LINK_DOWN arrived. Their fate is unknown
    /// and this module keeps no replay log, so they are counted here rather
    /// than re-sent.
    invalidated: u32,
    /// LINK_DOWN signals seen.
    link_downs: u32,
    /// One framed publish, envelope included.
    pub_buf: [u8; PUB_FRAME_MAX],

    // ── Ingress (`stream.ordered_ack.sink`) ──────────────────────
    /// in[2]: `publish_in`. `-1` when the graph wired no producer, in which
    /// case records arrive on `record_in`.
    pub_in_chan: i32,
    /// out[2]: `ack_out`.
    ack_out_chan: i32,
    /// LINK_UP has been announced on `ack_out`. A producer publishes nothing
    /// before it, so a module that is not ready to take records announces
    /// nothing.
    link_announced: bool,
    /// The publish whose payload is the record in flight (0 = none). Answered
    /// when that record resolves: OK once its output is accepted downstream,
    /// a typed refusal if processing fails.
    ingress_corr: u64,
    /// `inputs_succeeded` when the in-flight publish was admitted; the record's
    /// resolution reads as OK or a refusal against it.
    ingress_succeeded_at: u64,
    /// An ack the ring would not take, retried before anything else so no
    /// publish goes unanswered.
    ack_retry: [u8; ACK_FRAME_LEN],
    ack_retry_len: u8,
    /// Publishes this module accepted, and refused, as a sink.
    sink_acked: u32,
    sink_refused: u32,
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
        s.in_buf = [0u8; INGRESS_BUF];
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
        s.publish_chan = dev_channel_port(sys, 1, 1);
        s.ack_chan = dev_channel_port(sys, 0, 1);
        s.corr_next = 1;
        s.inflight = 0;
        s.refused = 0;
        s.invalidated = 0;
        s.link_downs = 0;
        s.pub_in_chan = dev_channel_port(sys, 0, 2);
        s.ack_out_chan = dev_channel_port(sys, 1, 2);
        s.link_announced = false;
        s.ingress_corr = 0;
        s.ingress_succeeded_at = 0;
        s.ack_retry = [0u8; ACK_FRAME_LEN];
        s.ack_retry_len = 0;
        s.sink_acked = 0;
        s.sink_refused = 0;
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
        } else if !s.pending.is_empty() || s.inflight >= MAX_INFLIGHT {
            // A full in-flight window blocks exactly as a retained output
            // does: backpressure by channel, never by dropping.
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
            // Egress: the in-flight window, and what the destination did with
            // what left it.
            tlm_gauge(sys, midx, t, b + 6, s.inflight as u64);
            tlm_counter(sys, midx, t, b + 7, s.refused as u64);
            tlm_counter(sys, midx, t, b + 8, s.invalidated as u64);
            tlm_counter(sys, midx, t, b + 9, s.link_downs as u64);
            // Ingress: what this module did with what reached it as a sink.
            tlm_counter(sys, midx, t, b + 10, s.sink_acked as u64);
            tlm_counter(sys, midx, t, b + 11, s.sink_refused as u64);
        }

        // 0. Acks from the destination. Drained FIRST because an ack frees
        //    window, and a LINK_DOWN invalidates every publish still in
        //    flight — both change what this step may do next.
        if s.ack_chan >= 0 {
            loop {
                let ap = (sys.channel_poll)(s.ack_chan, 0x01);
                if ap <= 0 || (ap as u32 & 0x01) == 0 {
                    break;
                }
                let mut hdr = [0u8; 3];
                if (sys.channel_read)(s.ack_chan, hdr.as_mut_ptr(), 3) < 3 {
                    break;
                }
                let alen = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
                if alen != ACK_WIRE_LEN {
                    // Not an ack-sized frame: step past its body.
                    chan_skip(sys, s.ack_chan, alen);
                    continue;
                }
                let mut ab = [0u8; ACK_WIRE_LEN];
                if ((sys.channel_read)(s.ack_chan, ab.as_mut_ptr(), alen) as usize) < alen {
                    break;
                }
                if hdr[0] != MSG_ACK {
                    continue;
                }
                let Some(ack) = Ack::decode(&ab) else {
                    continue;
                };
                if ack.is_link_state() {
                    // LINK_DOWN: every unacked publish is now unknowable. The
                    // window is released so the pipeline is not wedged, and the
                    // outstanding publishes are counted as invalidated — this
                    // module keeps no replay log, so it reports them rather
                    // than re-sending. LINK_UP needs nothing: publishing
                    // resumes as records arrive.
                    if ack.status == STATUS_LINK_DOWN {
                        s.link_downs = s.link_downs.wrapping_add(1);
                        s.invalidated = s.invalidated.wrapping_add(s.inflight);
                        s.inflight = 0;
                    }
                    continue;
                }
                s.inflight = s.inflight.saturating_sub(1);
                if ack.status != STATUS_OK {
                    s.refused = s.refused.wrapping_add(1);
                }
            }
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
        if (s.in_chan < 0 && s.pub_in_chan < 0)
            || s.faulted != 0
            || (vbin_len == 0 && s.enc_len == 0 && s.dec_len == 0)
        {
            return 0;
        }
        let outch = SysChan::new(sys, s.out_chan);

        // Ingress housekeeping. A retained ack goes out before anything else
        // (acks are ordered, and every publish is answered); LINK_UP is
        // announced once, only from a module ready to take records; and the
        // publish whose record has resolved is answered — OK if the record
        // succeeded, else UNROUTABLE: this destination could not carry it.
        if s.ack_out_chan >= 0 {
            if !ack_flush(s, sys) {
                return 0;
            }
            if !s.link_announced {
                if let Some(up) = Ack::link(STATUS_LINK_UP) {
                    ack_write(s, sys, up);
                }
                s.link_announced = s.ack_retry_len == 0;
                if !s.link_announced {
                    return 0;
                }
            }
            if s.ingress_corr != 0 && s.acct.inputs_in_flight == 0 {
                let status = if s.acct.inputs_succeeded > s.ingress_succeeded_at {
                    s.sink_acked = s.sink_acked.wrapping_add(1);
                    STATUS_OK
                } else {
                    s.sink_refused = s.sink_refused.wrapping_add(1);
                    REFUSE_UNROUTABLE
                };
                if let Some(a) = Ack::reply(s.ingress_corr, status) {
                    ack_write(s, sys, a);
                }
                s.ingress_corr = 0;
            }
        }

        // 2. Deliver any retained output before admitting new input. One record
        //    is in flight at a time, so a blocked write can never lose a mid-batch
        //    frame.
        if !s.pending.is_empty() {
            let plen = s.pending.len as u32;
            // A retained frame belongs to whichever channel staged it: a
            // publish waits on `publish_out` and lives in `pub_buf`, a plain
            // result on `result_out` in `out_buf`. Draining one from the
            // other's buffer would emit whatever bytes happened to be there.
            let (drain_ch, drain_buf): (SysChan, &[u8]) = if s.publish_chan >= 0 {
                (SysChan::new(sys, s.publish_chan), &s.pub_buf[..])
            } else {
                (SysChan::new(sys, s.out_chan), &s.out_buf[..])
            };
            match s.pending.drain(&drain_ch, drain_buf) {
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

        // A full in-flight window is backpressure: nothing is admitted until the
        // destination answers, so this module never runs ahead of it.
        if s.publish_chan >= 0 && s.inflight >= MAX_INFLIGHT {
            return 0;
        }

        // 3. Admit ONE input unit into `in_buf`: a publish's payload when a
        //    producer is wired to `publish_in`, otherwise from `record_in`.
        //    mode (a) no decoder: a whole typed frame (peeked whole on
        //    `record_in`; checked whole as a payload). mode (b) decoder: a raw
        //    protocol chunk decoded to one frame (no partial-read state — a
        //    message split across reads fails the decode, counted).
        let raw_len: usize;
        if s.pub_in_chan >= 0 {
            match ingress_admit(s, sys) {
                Some(n) => raw_len = n,
                None => return 0,
            }
        } else if s.dec_len > 0 {
            let poll = (sys.channel_poll)(s.in_chan, 0x01);
            if poll <= 0 || (poll as u32 & 0x01) == 0 {
                return 0;
            }
            let n = (sys.channel_read)(s.in_chan, s.in_buf.as_mut_ptr(), REC_BUF);
            if n <= 0 {
                return 0;
            }
            raw_len = n as usize;
            // A raw protocol chunk accepted for processing is one admitted record.
            s.acct.admit_input(raw_len as u64);
        } else {
            let inch = SysChan::new(sys, s.in_chan);
            // `record_in` admits one typed record; the intake buffer's extra
            // room is for publish payloads only.
            match admit_frame(&inch, &mut s.in_buf[..REC_BUF], frame_len) {
                Admit::Complete(nn) => {
                    s.acct.admit_input(nn as u64);
                    raw_len = nn;
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
        let frame_ptr: *const u8;
        let frame_bytes: usize;
        if s.dec_len > 0 {
            let mut b = Builder::new();
            let dec = core::slice::from_raw_parts(s.dec.as_ptr(), s.dec_len as usize);
            let inp = core::slice::from_raw_parts(s.in_buf.as_ptr(), raw_len);
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
            if s.pub_in_chan >= 0 && frame_len(&s.in_buf[..raw_len]) != Some(raw_len) {
                // A payload that is not exactly one typed frame is a terminal
                // failure for this record — refused, never parsed as a prefix.
                s.acct.input_failed();
                return 0;
            }
            frame_ptr = s.in_buf.as_ptr();
            frame_bytes = raw_len;
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
        // A wired destination changes WHERE the result goes and nothing about
        // how it was computed: the record is wrapped in a publish frame and
        // correlated, so the pipeline learns whether it landed.
        if s.publish_chan >= 0 {
            let corr = s.corr_next;
            s.corr_next = s.corr_next.wrapping_add(1);
            if s.corr_next == 0 {
                s.corr_next = 1;
            }
            let publish = Publish {
                corr,
                flags: 0,
                // No key: one destination, one ordering unit. This module
                // takes no key-field param, so every record shares one unit.
                msg_key: &[],
                payload: &s.out_buf[..final_len],
            };
            if let Some(n) = publish.encode(&mut s.pub_buf[3..]) {
                s.pub_buf[0] = MSG_PUBLISH;
                s.pub_buf[1..3].copy_from_slice(&(n as u16).to_le_bytes());
                let pubch = SysChan::new(sys, s.publish_chan);
                match s.pending.stage(&pubch, &s.pub_buf, 3 + n) {
                    Staged::Delivered => {
                        s.inflight = s.inflight.saturating_add(1);
                        s.acct.output_delivered_now(final_len as u32);
                        s.acct.input_succeeded();
                    }
                    Staged::Pending => {
                        s.inflight = s.inflight.saturating_add(1);
                        s.acct.output_staged(final_len as u32);
                    }
                    Staged::Failed(_) => {
                        s.pending = Pending { off: 0, len: 0 };
                        s.acct.output_failed_now();
                        s.acct.input_failed();
                    }
                }
            } else {
                // Larger than the surface admits. Refused here rather than
                // truncated, which is the contract's rule.
                s.acct.output_failed_now();
                s.acct.input_failed();
            }
            return 0;
        }

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
