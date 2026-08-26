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
//! verbatim from the `chronicle-bytecode` crate, so this module and the host tests
//! (`crates/chronicle-bytecode/tests/pipeline*.rs`) run identical logic.

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
// to the `chronicle-bytecode` crate, all in ONE module so cross-references resolve.
mod pipe {
    include!("../../common/vm_core.rs");
    include!("../../common/pipeline_core.rs");
    include!("../../common/hex_core.rs");
    include!("../../common/ser_core.rs");
    include!("../../common/deser_core.rs");
    include!("../../common/version_core.rs");
    include!("../../common/lower_core.rs");
}
use pipe::{
    decode_frame, encode_frame, eval_bytes, eval_decode, frame_len, hex_decode, lower_stages,
    parse_version_table, run_stages, scan_version_table, stage_at, stage_count, vctl,
    version_apply, version_apply_ir, version_selector, Builder, Field, Message, Stage, Value,
    MAX_PIPE_FIELDS,
};

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

    processed: u32,
    errors: u32,
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
    };

    3, decode, str, 0 => |s, d, len| {
        let mut i = 0usize;
        while i < len && (s.dec_hex_len as usize) < HEX_BUF {
            s.dec_hex[s.dec_hex_len as usize] = *d.add(i);
            s.dec_hex_len += 1;
            i += 1;
        }
    };

    4, versions, str, 0 => |s, d, len| {
        let mut i = 0usize;
        while i < len && (s.ver_hex_len as usize) < HEX_BUF {
            s.ver_hex[s.ver_hex_len as usize] = *d.add(i);
            s.ver_hex_len += 1;
            i += 1;
        }
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
        s.processed = 0;
        s.errors = 0;
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

        // Hot reload: drain control ops from the ctrl port and mutate the version
        // table in place (add a version, flip the default, remove a drained one).
        // Non-disruptive — records already in flight keep the version they resolved.
        if s.ctrl_chan >= 0 {
            let cp = (sys.channel_poll)(s.ctrl_chan, 0x01);
            if cp > 0 && (cp as u32 & 0x01) != 0 {
                let cn = (sys.channel_read)(s.ctrl_chan, s.ctrl_buf.as_mut_ptr(), s.ctrl_buf.len());
                if cn > 0 {
                    let msg = core::slice::from_raw_parts(s.ctrl_buf.as_ptr(), cn as usize);
                    // An IR add-version lowers its stages here (the running instance
                    // re-derives the bytecode); every other op is a plain table edit.
                    let applied = if msg.first() == Some(&vctl::ADD_VERSION_IR) {
                        version_apply_ir(&mut s.vbin, s.vbin_len as usize, VBIN_BUF, msg)
                    } else {
                        version_apply(&mut s.vbin, s.vbin_len as usize, VBIN_BUF, msg)
                    };
                    match applied {
                        Ok(nu) => {
                            // The applied table must still be runnable HERE:
                            // an added version whose program needs a builtin
                            // this build lacks faults the node loudly (the
                            // apply mutates in place, so there is no revert —
                            // loud beats limping per record).
                            if scan_version_table(&s.vbin[..nu]).is_err() {
                                s.faulted = 1;
                                let m: &'static [u8] = b"[pipeline] FAULT: reload added a program not runnable in this build";
                                dev_log(sys, 1, m.as_ptr(), m.len());
                            } else {
                                s.vbin_len = nu as u16;
                                s.reloads = s.reloads.wrapping_add(1);
                                dev_log(sys, 3, b"[pipeline] reload".as_ptr(), 17);
                            }
                        }
                        Err(_) => s.rejected = s.rejected.wrapping_add(1),
                    }
                }
            }
        }

        if s.in_chan < 0 || s.faulted != 0 || (s.vbin_len == 0 && s.enc_len == 0 && s.dec_len == 0)
        {
            return 0;
        }
        let poll = (sys.channel_poll)(s.in_chan, 0x01);
        if poll <= 0 || (poll as u32 & 0x01) == 0 {
            return 0;
        }

        let n = (sys.channel_read)(s.in_chan, s.in_buf.as_mut_ptr(), s.in_buf.len());
        if n <= 0 {
            return 0;
        }
        let read = n as usize;

        // Front-end decode: if a decode program is set, the input is a raw
        // protocol reply — parse it into a record frame. Otherwise the input is
        // already a batch of record frames.
        let (src_ptr, src_len) = if s.dec_len > 0 {
            let mut b = Builder::new();
            let dec = core::slice::from_raw_parts(s.dec.as_ptr(), s.dec_len as usize);
            let inp = core::slice::from_raw_parts(s.in_buf.as_ptr(), read);
            match eval_decode(dec, inp, &mut b, 100_000) {
                Ok(()) => match encode_frame(&b.message(), &mut s.dec_out) {
                    Ok(rl) => (s.dec_out.as_ptr(), rl),
                    Err(_) => {
                        s.errors = s.errors.wrapping_add(1);
                        (s.dec_out.as_ptr(), 0)
                    }
                },
                Err(_) => {
                    s.errors = s.errors.wrapping_add(1);
                    (s.dec_out.as_ptr(), 0)
                }
            }
        } else {
            (s.in_buf.as_ptr(), read)
        };

        // Each frame is routed to a version (by its X-Module-Version selector, or
        // the default), threaded through that version's stages, and emitted.
        let mut off = 0usize;
        while off < src_len {
            let avail = core::slice::from_raw_parts(src_ptr.add(off), src_len - off);
            let Some(len) = frame_len(avail) else {
                break;
            };
            if off + len > src_len {
                break;
            }
            let frame_in = core::slice::from_raw_parts(src_ptr.add(off), len);

            // Resolve the version for this record: read its selector (field 255)
            // and look it up in the table. Unknown/unloaded → fail closed.
            let mut sel_fields = [Field {
                number: 0,
                value: Value::Null,
            }; MAX_PIPE_FIELDS];
            let nsf = decode_frame(frame_in, &mut sel_fields).unwrap_or(0);
            let selector = version_selector(&sel_fields[..nsf]);
            let vbin = core::slice::from_raw_parts(s.vbin.as_ptr(), s.vbin_len as usize);
            let prog: Option<&[u8]> = parse_version_table(vbin)
                .and_then(|t| t.resolve(selector).and_then(|i| t.entry(i).map(|e| e.prog)));

            let out_len = match prog {
                Some(prog) => {
                    let mut stages = [Stage {
                        code: &[],
                        max_cost: 0,
                        on_failure: None,
                    }; MAX_STAGES];
                    let np = stage_count(prog).min(MAX_STAGES);
                    let mut ok = true;
                    for (i, st) in stages.iter_mut().enumerate().take(np) {
                        match stage_at(prog, i) {
                            Some(stage) => *st = stage,
                            None => ok = false,
                        }
                    }
                    if !ok {
                        s.errors = s.errors.wrapping_add(1);
                        0
                    } else {
                        run_stages(
                            &stages[..np],
                            frame_in,
                            &mut s.buf_a,
                            &mut s.buf_b,
                            &mut s.out_buf,
                        )
                        .unwrap_or_else(|_| {
                            s.errors = s.errors.wrapping_add(1);
                            0
                        })
                    }
                }
                None => {
                    // Fail closed: a record pinned to a version this instance does
                    // not hold gets a deterministic {1: "VERSION_UNAVAILABLE"} record
                    // (the LB retries another instance) — never the wrong version.
                    s.unavailable = s.unavailable.wrapping_add(1);
                    write_unavailable_frame(&mut s.out_buf)
                }
            };

            if out_len > 0 && s.out_chan >= 0 {
                // If an encoder is configured, serialize the final record into a
                // wire payload; otherwise emit the record frame as-is.
                let (ptr, elen) = if s.enc_len > 0 {
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
                        Ok(m) => (s.enc_out.as_ptr(), m),
                        Err(_) => {
                            s.errors = s.errors.wrapping_add(1);
                            (s.enc_out.as_ptr(), 0)
                        }
                    }
                } else {
                    (s.out_buf.as_ptr(), out_len)
                };
                if elen > 0 {
                    let poll_out = (sys.channel_poll)(s.out_chan, 0x02);
                    if poll_out > 0 && (poll_out as u32 & 0x02) != 0 {
                        (sys.channel_write)(s.out_chan, ptr, elen);
                        s.processed = s.processed.wrapping_add(1);
                    }
                }
            }
            off += len;
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
