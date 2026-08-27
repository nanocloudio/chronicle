//! Aggregation engine — Fluxor `.fmod` app module (spec artefact 5, on device).
//!
//! PARAM-DRIVEN: the aggregation definition is NOT baked — it arrives as a `def`
//! module param (hex-encoded), a serialized container:
//!   [window_size:i64][lateness:i64][max_lanes:u32]
//!   [key prog][time prog][emit prog]           (prog = [cost:u32][len:u16][code])
//!   [nops:u8] then per op [kind:u8][selector prog]
//! So one aggregation binary runs ANY Aggregation: the compiler emits the
//! bytecode, a config packs the container, this module runs the event-time
//! engine over it. The bounded, no-alloc engine + container readers live in
//! `agg_core.rs`, `include!`d verbatim from `modules/common/`.

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

// Evaluator + frame codec + aggregation engine + hex codec — identical source to
// the host harness (tests/harness), all in ONE module so cross-references resolve.
mod agg {
    use super::abi::SyscallTable;
    include!("../../common/vm_core.rs");
    include!("../../common/pipeline_core.rs");
    include!("../../common/agg_core.rs");
    include!("../../common/hex_core.rs");
    // Lower a shipped checked IR-`def` at load (the `ir_def` param), transcoding
    // it to the bytecode `def` `build_spec` consumes. `op::`/`ir::` resolve here
    // because core.rs is include!'d into this same block. Same self-validation
    // as the expression/pipeline modules: "it lowered" == "it can run it".
    include!("../../common/lower_core.rs");
    include!("../../common/outcome_core.rs");
    include!("../../common/io_core.rs");
    include!("../../common/accounting_core.rs");
    include!("../../common/syschan_core.rs");
}
use agg::{
    admit_frame, agg_op_kind, drain_all, frame_len, hex_decode, ingest, lower_def, read_prog,
    Accounting, Admit, AggSpec, AggState, BarrierGate, Durability, EmitTrigger, Mode, OpSpec,
    Pending, Staged, SysChan, ACCT_IS_GAUGE, ACCT_METRIC_COUNT, MAX_OPS,
};

// Telemetry emit helpers — crate root, after the SDK runtime so its primitives are in scope.
include!("../../common/telemetry_core.rs");

const HEX_BUF: usize = 8192;
const CONT_BUF: usize = 4096;
/// Input event buffer, sized to the port max_record so a whole typed event frame
/// from an upstream pipeline/decision module is admitted — never a partial.
const REC_BUF: usize = 4096;
/// Largest single emitted window frame retained for delivery. An aggregation
/// result (a key plus a handful of operator values) is well under this.
const EMIT_FRAME_MAX: usize = 512;
/// One event can emit up to `MAX_LANES * MAX_PANES * 2` = 256 frames (every live
/// pane on an `OnProcessing` trigger, then again on finalization). The emission
/// queue holds a whole event's output so a full output ring (EAGAIN) never forces
/// a drop: emissions are captured here during `ingest`, then drained one frame per
/// step, and the next event is not admitted until the queue empties.
const EMIT_Q_CAP: usize = 256 * (2 + EMIT_FRAME_MAX);
/// The largest snapshot `AggState::snapshot` can produce, so EVERY admitted state
/// is checkpointable. Derived from the capacities: 42 global bytes + 16
/// lanes × (60 lane bytes + 8 panes × 270) ≈ 35,562; rounded up with margin. The
/// checkpoint buffer and the `checkpoint_out` record are both sized to this, so a
/// full snapshot travels as one retained record — delivered atomically, retained
/// on a full ring (no chunk-reassembly protocol, no loss under EAGAIN). Fits a
/// `u16` cursor (`Pending`) since it is under 64 KiB.
const MAX_SNAPSHOT: usize = 40960;
/// Hex of a supplied restore checkpoint: twice the decoded snapshot. `state_hex_len`
/// is `u32` because this exceeds a `u16`.
const SNAP_HEX: usize = 2 * MAX_SNAPSHOT;

#[repr(C)]
struct ModuleState {
    syscalls: *const SyscallTable,
    in_chan: i32,
    out_chan: i32,
    in_buf: [u8; REC_BUF],
    // Emission queue: one event's window emissions, length-prefixed
    // (`[len:u16][frame]`…), captured during `ingest` and drained one frame per
    // step so a full output ring never loses an emission. `emit_head`/`emit_tail`
    // are byte cursors (u32 — the queue exceeds 64 KiB).
    emit_q: [u8; EMIT_Q_CAP],
    emit_head: u32,
    emit_tail: u32,
    /// Emissions dropped because a single event exceeded the queue's proven bound
    /// (a bounded-state overflow, counted like lane/pane overflow — never silent).
    emit_overflow: u32,
    /// The common accounting taxonomy. An admitted EVENT is one input; its
    /// window emissions are its fan-out outputs (staged into the queue, drained one
    /// per step). `emit_overflow` and the saturation counters refine
    /// `outputs_failed`.
    acct: Accounting,
    hex: [u8; HEX_BUF],
    hex_len: u16,
    cont: [u8; CONT_BUF],
    cont_len: u16,
    // `true` when the hex holds a flat checked IR-`def` (`ir_def` param) to be
    // lowered at load, rather than a pre-lowered bytecode `def` (`def` param).
    is_ir: bool,
    // Scratch for the decoded IR-`def` before `lower_def` emits into `cont`.
    ir_scratch: [u8; CONT_BUF],
    // A CHECKPOINT to resume from (`state` param): hex of an AggState::snapshot.
    // Non-empty ⇒ restore the windowed state at load instead of starting fresh —
    // durable deterministic state / replay-recovery.
    state_hex: [u8; SNAP_HEX],
    state_hex_len: u32,
    has_state: bool,
    // A full-snapshot staging buffer, reused for the checkpoint EMIT (snapshot →
    // stage) and, at load, for decoding a supplied restore checkpoint. Sized to the
    // proven maximum snapshot so every admitted state is checkpointable/restorable.
    ckpt_stage: [u8; MAX_SNAPSHOT],
    // One retained checkpoint record, drained across steps so a full ring never
    // drops a checkpoint.
    ckpt_pending: Pending,
    // Checkpoint EMIT: out[1], and emit a snapshot every `ckpt_every` events
    // (0 = disabled). `events_since_ckpt` counts events since the last emit.
    ckpt_chan: i32,
    ckpt_every: u32,
    events_since_ckpt: u32,
    // Distributed activation barrier (Clustor state.distributed.raft.v1). When a
    // `barrier` param gives the (term, index) the restored `state` checkpoint was
    // committed at, the module STAGES that state — it processes no events and
    // emits nothing — until a commit horizon (barrier_in, in[1]) proves
    // the replicated log durable past that index AT THE MATCHING TERM (log-index,
    // term-fenced; BarrierGate). No barrier param ⇒ active immediately (back-compat).
    barrier_hex: [u8; 64],
    barrier_hex_len: u16,
    has_barrier: bool,
    barrier_chan: i32,
    barrier_buf: [u8; 64],
    gate: BarrierGate,
    active: bool,
    // Producer half (raft-propose mode): when proposal_out (out[2]) is wired the
    // module submits its checkpoint snapshot to consensus as a TAGGED proposal,
    // learns the assigned log index via assigned_in (in[2]) and the current term
    // via leader_in (in[3]), records it in the gate, and stages itself until the
    // commit horizon (barrier_in) crosses that index. TWO lifecycles, kept
    // distinct: (1) STARTUP FENCING — the boot generation stages the
    // restored state until it is proven committed, then activates; (2) ONGOING
    // GENERATIONS — after activation the module periodically re-proposes the
    // current state under a FRESH `corr` and re-proposes on a term change, so state
    // is replicated over time, not only fenced once at boot. Commit-acknowledged
    // retry-until-durable and proposals larger than one record are clustor-
    // coordinated and out of a single module's scope.
    raft_mode: bool,
    proposal_chan: i32,
    assigned_chan: i32,
    leader_chan: i32,
    current_term: u64,
    corr: u64,
    proposed: bool,
    /// 1 = a present-but-invalid supplied checkpoint. Distinct from "no checkpoint"
    /// (absent state starts fresh); a malformed/oversized one is a terminal
    /// activation fault — the module refuses to process rather than silently
    /// erasing history and continuing from a fresh state.
    faulted: bool,
    /// A string param overflowed its buffer during parsing (truncated) — a fault,
    /// since a truncated def could decode to a different aggregation.
    param_overflow: bool,
    /// Current operating mode (`Mode` as u8), published as `module_mode`.
    mode: u8,
    /// Wall-clock ms of the last telemetry publish (throttle state).
    tlm_last_ms: u64,
    agg: AggState,
}

define_params! {
    ModuleState;

    1, def, str, 0 => |s, d, len| {
        let mut i = 0usize;
        while i < len && (s.hex_len as usize) < HEX_BUF {
            s.hex[s.hex_len as usize] = *d.add(i);
            s.hex_len += 1;
            i += 1;
        }
        if i < len { s.param_overflow = true; }
    };
    // Higher-level IR-`def` (a shipped checked IR) lowered to bytecode at load.
    // Guard on len>0: set_defaults() fires every closure, so an absent param
    // must not flip `is_ir` and route a bytecode `def` through `lower_def`.
    3, ir_def, str, 0 => |s, d, len| {
        if len > 0 {
            s.is_ir = true;
            let mut i = 0usize;
            while i < len && (s.hex_len as usize) < HEX_BUF {
                s.hex[s.hex_len as usize] = *d.add(i);
                s.hex_len += 1;
                i += 1;
            }
            if i < len { s.param_overflow = true; }
        }
    };
    // A checkpoint (hex of AggState::snapshot) to resume from. Guard on len>0:
    // set_defaults() fires every closure, so an absent param must not flip
    // has_state and try to restore from empty bytes.
    4, state, str, 0 => |s, d, len| {
        if len > 0 {
            s.has_state = true;
            let mut i = 0usize;
            while i < len && (s.state_hex_len as usize) < SNAP_HEX {
                s.state_hex[s.state_hex_len as usize] = *d.add(i);
                s.state_hex_len += 1;
                i += 1;
            }
        }
    };
    // Emit a checkpoint on out[1] every N events (0 = disabled).
    5, checkpoint_every, u32, 0 => |s, d, len| {
        s.ckpt_every = p_u32(d, len, 0, 0);
    };
    // Distributed activation barrier: hex of [term:u64 LE][index:u64 LE] — the
    // replicated-log commit coordinates the loaded `state` checkpoint was made
    // durable at. Guard on len>0 (set_defaults fires every closure). Absent ⇒ no
    // barrier, module active immediately. index 0 ⇒ no barrier too (see new()).
    6, barrier, str, 0 => |s, d, len| {
        if len > 0 {
            s.has_barrier = true;
            let mut i = 0usize;
            while i < len && (s.barrier_hex_len as usize) < 64 {
                s.barrier_hex[s.barrier_hex_len as usize] = *d.add(i);
                s.barrier_hex_len += 1;
                i += 1;
            }
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
        s.emit_head = 0;
        s.emit_tail = 0;
        s.emit_overflow = 0;
        s.acct = Accounting::default();
        s.ckpt_pending = Pending { off: 0, len: 0 };
        s.param_overflow = false;
        s.mode = Mode::AwaitingConfig.as_u8();
        // Backdate so the FIRST telemetry publish fires promptly (dev_millis is
        // uptime, near 0 at startup), not only after one full interval.
        s.tlm_last_ms = dev_millis(sys).wrapping_sub(TLM_INTERVAL_MS);
        s.hex_len = 0;
        s.cont_len = 0;
        s.is_ir = false;
        s.state_hex_len = 0;
        s.has_state = false;
        s.ckpt_chan = dev_channel_port(sys, 1, 1); // out[1]: checkpoint stream
        s.ckpt_every = 0;
        s.events_since_ckpt = 0;
        s.barrier_hex_len = 0;
        s.has_barrier = false;
        s.barrier_chan = dev_channel_port(sys, 0, 1); // input[1]: commit horizon
        s.gate = BarrierGate::new();
        s.active = true; // no barrier ⇒ active; may be staged below
        s.proposal_chan = dev_channel_port(sys, 1, 2); // output[2]: raft proposal
        s.assigned_chan = dev_channel_port(sys, 0, 2); // input[2]: proposal_assigned
        s.leader_chan = dev_channel_port(sys, 0, 3); // input[3]: leader_state
        s.raft_mode = s.proposal_chan >= 0;
        s.current_term = 0;
        s.corr = 1;
        s.proposed = false;
        s.faulted = false;
        core::ptr::write(&mut s.agg, AggState::new());

        parse_tlv(s, params, params_len);
        // A param truncated at its buffer is a terminal fault — a truncated def could
        // decode to a different aggregation.
        if s.param_overflow {
            s.faulted = true;
            let m: &'static [u8] =
                b"[aggregation] FAULT: a param exceeded its buffer and was truncated";
            dev_log(sys, 1, m.as_ptr(), m.len());
        }
        if s.is_ir {
            // `ir_def` param: decode the hex to the flat IR-`def`, then lower it
            // HERE into the bytecode `def`. A program that won't lower fails
            // closed (cont_len=0) at LOAD, not a runtime BadOpcode — and the
            // cost bound is re-derived from the IR, never trusted from a param.
            match hex_decode(&s.hex[..s.hex_len as usize], &mut s.ir_scratch) {
                Some(n) => match lower_def(&s.ir_scratch[..n], &mut s.cont) {
                    Ok(m) => s.cont_len = m as u16,
                    Err(_) => s.cont_len = 0,
                },
                None => s.cont_len = 0,
            }
        } else {
            match hex_decode(&s.hex[..s.hex_len as usize], &mut s.cont) {
                Some(n) => s.cont_len = n as u16,
                None => s.cont_len = 0,
            }
        }
        // Resume from a checkpoint if one was supplied — durable deterministic
        // state. Decode the hex into ir_scratch (reused), restore over
        // the fresh AggState. A malformed/oversized checkpoint fails closed:
        // restore returns None, so we keep the fresh state rather than a
        // half-built one and log it.
        if s.has_state {
            // A checkpoint was SUPPLIED. It restores, or it is a terminal fault —
            // never a silent fall-back to a fresh state, which would erase the very
            // history the checkpoint exists to preserve. Absent state (the
            // `!has_state` case) is what legitimately starts fresh.
            let restored = hex_decode(&s.state_hex[..s.state_hex_len as usize], &mut s.ckpt_stage)
                .and_then(|n| AggState::restore(&s.ckpt_stage[..n]));
            match restored {
                Some(r) => {
                    s.agg = r;
                    dev_log(
                        sys,
                        3,
                        b"[aggregation] resumed from checkpoint".as_ptr(),
                        37,
                    );
                }
                None => {
                    s.faulted = true;
                    let m: &'static [u8] =
                        b"[aggregation] FAULT: supplied checkpoint is invalid; refusing to start with erased state";
                    dev_log(sys, 1, m.as_ptr(), m.len());
                }
            }
        }
        // Distributed activation barrier: if a `barrier` param gives the (term,
        // index) the restored checkpoint was committed at, STAGE the state — hold
        // it inactive until the commit horizon proves it durable past that
        // index at the matching term. A malformed/short barrier fails closed
        // (stays staged, never activates without proof). index 0 = no barrier.
        if s.has_barrier {
            match hex_decode(
                &s.barrier_hex[..s.barrier_hex_len as usize],
                &mut s.barrier_buf,
            ) {
                Some(n) if n >= 16 => {
                    let term = u64::from_le_bytes([
                        s.barrier_buf[0],
                        s.barrier_buf[1],
                        s.barrier_buf[2],
                        s.barrier_buf[3],
                        s.barrier_buf[4],
                        s.barrier_buf[5],
                        s.barrier_buf[6],
                        s.barrier_buf[7],
                    ]);
                    let index = u64::from_le_bytes([
                        s.barrier_buf[8],
                        s.barrier_buf[9],
                        s.barrier_buf[10],
                        s.barrier_buf[11],
                        s.barrier_buf[12],
                        s.barrier_buf[13],
                        s.barrier_buf[14],
                        s.barrier_buf[15],
                    ]);
                    if index > 0 {
                        s.gate.appended(term, index);
                        s.active = false; // staged until the barrier is crossed
                        dev_log(sys, 3, b"[aggregation] staged behind barrier".as_ptr(), 35);
                    }
                }
                _ => {
                    s.active = false; // fail closed: bad barrier ⇒ never activate
                    dev_log(sys, 3, b"[aggregation] bad barrier".as_ptr(), 25);
                }
            }
        }
        // Raft-propose mode (proposal_out wired): stage until OUR checkpoint is
        // proposed, assigned a log index, and committed — the barrier index is
        // learned dynamically from proposal_assigned rather than a static param.
        if s.raft_mode {
            s.active = false;
            dev_log(
                sys,
                3,
                b"[aggregation] staged behind raft commit".as_ptr(),
                39,
            );
        }
        if s.cont_len < 20 {
            dev_log(sys, 3, b"[aggregation] no def param".as_ptr(), 26);
        } else {
            dev_log(sys, 3, b"[aggregation] init".as_ptr(), 18);
        }
        0
    }
}

/// Drain leader_state (in[3]): track the current raft term from MSG_LEADER_HINT
/// `[0x09][len][leader_id:u8][term:u64 LE]`, so a proposal can be stamped with the
/// term it was appended at (the barrier's fence). Bounded / panic-free.
unsafe fn drain_leader(s: &mut ModuleState, sys: &SyscallTable) {
    if s.leader_chan < 0 {
        return;
    }
    let poll = (sys.channel_poll)(s.leader_chan, 0x01);
    if poll <= 0 || (poll as u32 & 0x01) == 0 {
        return;
    }
    let n = (sys.channel_read)(
        s.leader_chan,
        s.barrier_buf.as_mut_ptr(),
        s.barrier_buf.len(),
    );
    if n <= 0 {
        return;
    }
    let buf = &s.barrier_buf[..n as usize];
    // [msg=0x09][len:u16][leader_id:u8][term:u64 LE] — 12 bytes framed.
    if buf.len() >= 12 && buf[0] == 0x09 && buf[3] != 0xFF {
        let term = u64::from_le_bytes([
            buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10], buf[11],
        ]);
        if term > s.current_term {
            s.current_term = term;
        }
    }
}

/// Once a leader term is known, submit the current snapshot to raft_engine as a
/// TAGGED proposal `[0x10][len:u16][corr:u64 LE][snapshot]` (exactly once). The
/// assigned log index comes back on assigned_in; the gate is appended there.
unsafe fn try_propose(s: &mut ModuleState, sys: &SyscallTable) {
    if s.proposed || s.proposal_chan < 0 || s.current_term == 0 {
        return;
    }
    let mut snap = [0u8; 512];
    let sn = match s.agg.snapshot(&mut snap) {
        Some(n) if n <= 512 - 11 => n,
        _ => return,
    };
    let plen = 8 + sn; // corr + body
    let mut pf = [0u8; 512];
    pf[0] = 0x10; // MSG_CLIENT_PROPOSAL
    pf[1] = (plen & 0xff) as u8;
    pf[2] = (plen >> 8) as u8;
    pf[3..11].copy_from_slice(&s.corr.to_le_bytes());
    pf[11..11 + sn].copy_from_slice(&snap[..sn]);
    let total = 3 + plen;
    let poll = (sys.channel_poll)(s.proposal_chan, 0x02);
    if poll > 0 && (poll as u32 & 0x02) != 0 {
        // Mark proposed ONLY on a fully-accepted write; a full ring leaves
        // `proposed` false so the one-shot proposal retries next step rather than
        // being silently lost.
        let w = (sys.channel_write)(s.proposal_chan, pf.as_ptr(), total);
        if w == total as i32 {
            s.proposed = true;
            dev_log(
                sys,
                3,
                b"[aggregation] checkpoint proposed to raft".as_ptr(),
                40,
            );
        }
    }
}

/// Drain assigned_in (in[2]): on MSG_PROPOSAL_ASSIGNED for OUR correlation id,
/// record the assigned (current_term, wal_index) in the barrier gate so the
/// commit horizon on barrier_in can rule it durable. Bounded / panic-free.
unsafe fn drain_assigned(s: &mut ModuleState, sys: &SyscallTable) {
    if s.assigned_chan < 0 || s.gate.is_pending() {
        return;
    }
    let poll = (sys.channel_poll)(s.assigned_chan, 0x01);
    if poll <= 0 || (poll as u32 & 0x01) == 0 {
        return;
    }
    let n = (sys.channel_read)(
        s.assigned_chan,
        s.barrier_buf.as_mut_ptr(),
        s.barrier_buf.len(),
    );
    if n <= 0 {
        return;
    }
    let buf = &s.barrier_buf[..n as usize];
    // [msg=0x14][len:u16][corr:u64][pid:u16][index:u64] — 21 bytes framed.
    if buf.len() >= 21 && buf[0] == 0x14 {
        let corr = u64::from_le_bytes([
            buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
        ]);
        let index = u64::from_le_bytes([
            buf[13], buf[14], buf[15], buf[16], buf[17], buf[18], buf[19], buf[20],
        ]);
        if corr == s.corr {
            s.gate.appended(s.current_term, index);
            dev_log(
                sys,
                3,
                b"[aggregation] checkpoint assigned a log index".as_ptr(),
                45,
            );
        }
    }
}

/// Drain the commit-horizon channel (barrier_in / consensus.committed_entries)
/// and feed each horizon to the barrier gate. When the pending checkpoint
/// crosses the term-fenced log-index barrier, activate the module. Accepts the
/// framed `MSG_COMMITTED_ENTRY` form `[0x24][len:u16][term:8][index:8][body…]`,
/// the framed `MSG_COMMITTED_BATCH` form `[0x23][len:u16][term:8][index:8]`
/// (19 B), or a bare `[term:8][index:8]` (16 B). Panic-free / bounded.
unsafe fn drain_barrier(s: &mut ModuleState, sys: &SyscallTable) {
    if s.active || s.barrier_chan < 0 {
        return;
    }
    let poll = (sys.channel_poll)(s.barrier_chan, 0x01);
    if poll <= 0 || (poll as u32 & 0x01) == 0 {
        return;
    }
    let n = (sys.channel_read)(
        s.barrier_chan,
        s.barrier_buf.as_mut_ptr(),
        s.barrier_buf.len(),
    );
    if n <= 0 {
        return;
    }
    let buf = &s.barrier_buf[..n as usize];
    // MSG_COMMITTED_ENTRY = 0x24 ([term][index][body…]) and
    // MSG_COMMITTED_BATCH = 0x23; both carry [term:u64 LE][index:u64 LE] first.
    let payload = if buf.len() >= 19 && (buf[0] == 0x23 || buf[0] == 0x24) {
        &buf[3..19]
    } else if buf.len() >= 16 {
        &buf[..16]
    } else {
        return;
    };
    let term = u64::from_le_bytes([
        payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
        payload[7],
    ]);
    let index = u64::from_le_bytes([
        payload[8],
        payload[9],
        payload[10],
        payload[11],
        payload[12],
        payload[13],
        payload[14],
        payload[15],
    ]);
    if let Durability::Durable { .. } = s.gate.horizon(term, index) {
        s.active = true;
        dev_log(
            sys,
            3,
            b"[aggregation] barrier crossed: activated".as_ptr(),
            40,
        );
    }
}

/// Build an `AggSpec` borrowing the container, plus the op array (also borrowing
/// it). Returns `None` on a malformed container.
fn build_spec<'a>(
    cont: &'a [u8],
    ops: &'a mut [OpSpec<'a>; MAX_OPS],
) -> Option<(AggSpec<'a>, usize)> {
    if cont.len() < 36 {
        return None;
    }
    let window_size = i64::from_le_bytes([
        cont[0], cont[1], cont[2], cont[3], cont[4], cont[5], cont[6], cont[7],
    ]);
    let lateness = i64::from_le_bytes([
        cont[8], cont[9], cont[10], cont[11], cont[12], cont[13], cont[14], cont[15],
    ]);
    let max_lanes = u32::from_le_bytes([cont[16], cont[17], cont[18], cont[19]]);
    let window_step = i64::from_le_bytes([
        cont[20], cont[21], cont[22], cont[23], cont[24], cont[25], cont[26], cont[27],
    ]);
    let correction_horizon = i64::from_le_bytes([
        cont[28], cont[29], cont[30], cont[31], cont[32], cont[33], cont[34], cont[35],
    ]);
    let (key_cost, key_code, o1) = read_prog(cont, 36)?;
    let (time_cost, time_code, o2) = read_prog(cont, o1)?;
    let (emit_cost, emit_code, o3) = read_prog(cont, o2)?;
    if o3 >= cont.len() {
        return None;
    }
    // Reject a def declaring more operators than MAX_OPS rather than silently
    // truncating to MAX_OPS (which would drop authored operators).
    if cont[o3] as usize > MAX_OPS {
        return None;
    }
    let nops = cont[o3] as usize;
    let mut off = o3 + 1;
    let mut i = 0usize;
    while i < nops {
        if off >= cont.len() {
            return None;
        }
        let kind = agg_op_kind(cont[off])?;
        off += 1;
        let (sel_cost, selector, next) = read_prog(cont, off)?;
        off = next;
        ops[i] = OpSpec {
            kind,
            selector,
            sel_cost,
        };
        i += 1;
    }
    // Optional trailing trigger bytes (backward-compatible: absent = OnClose).
    // Kind byte, plus a u32 count for OnCount.
    let emit_trigger = EmitTrigger::decode(cont.get(off..).unwrap_or(&[]));
    Some((
        AggSpec {
            window_size,
            window_step,
            lateness,
            correction_horizon,
            key_code,
            key_cost,
            time_code,
            time_cost,
            ops: &ops[..nops],
            emit_code,
            emit_cost,
            max_lanes,
            emit_trigger,
        },
        nops,
    ))
}

/// Emit a checkpoint on out[1] when the event interval has elapsed and the
/// channel accepts it. Retried every step (independent of input) so a transient
/// full/not-yet-ready channel never drops the checkpoint. Snapshots into
/// ir_scratch (unused after load).
unsafe fn try_checkpoint(s: &mut ModuleState, sys: &SyscallTable) {
    if s.ckpt_chan < 0 {
        return;
    }
    let ckpt = SysChan::new(sys, s.ckpt_chan);
    // Drain a retained checkpoint first — a full ring never drops it. One
    // whole snapshot travels as a single max_record-sized record.
    if !s.ckpt_pending.is_empty() {
        match s.ckpt_pending.drain(&ckpt, &s.ckpt_stage) {
            Staged::Pending => {}
            _ => s.ckpt_pending = Pending { off: 0, len: 0 },
        }
        return;
    }
    if s.ckpt_every == 0 || s.events_since_ckpt < s.ckpt_every {
        return;
    }
    // Snapshot the FULL state into the max-sized buffer — every admitted state
    // fits — then stage it. `snapshot` returns None only if the state exceeded
    // MAX_SNAPSHOT, which the capacities make impossible; leave the interval to
    // retry if it somehow does.
    let snap = core::slice::from_raw_parts_mut(s.ckpt_stage.as_mut_ptr(), MAX_SNAPSHOT);
    if let Some(sn) = s.agg.snapshot(snap) {
        // The interval resets when the snapshot is TAKEN, so a blocked delivery does
        // not re-snapshot; the retained record drains on later steps.
        s.events_since_ckpt = 0;
        match s.ckpt_pending.stage(&ckpt, &s.ckpt_stage, sn) {
            Staged::Pending => {}
            _ => s.ckpt_pending = Pending { off: 0, len: 0 },
        }
    }
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    unsafe {
        let s = &mut *(state as *mut ModuleState);
        let sys = &*s.syscalls;

        // Publish telemetry (throttled; zero-cost when no consumer). A stateful
        // operator exports its saturation/late/correction and checkpoint/barrier
        // state. Emitted before the early returns so a faulted / barrier-staged
        // module still reports.
        s.mode = if s.faulted {
            Mode::Faulted.as_u8()
        } else if !s.active {
            Mode::BarrierWait.as_u8()
        } else if s.cont_len < 36 {
            Mode::AwaitingConfig.as_u8()
        } else if s.emit_head < s.emit_tail || !s.ckpt_pending.is_empty() {
            Mode::OutputBlocked.as_u8()
        } else {
            Mode::Ready.as_u8()
        };
        if let Some((midx, t)) = tlm_tick(sys, &mut s.tlm_last_ms) {
            // ids 0..13: the baseline accounting block (events as inputs, window
            // emissions as fan-out outputs).
            acct_emit(sys, midx, t, 0, &s.acct);
            // ids 14..: aggregation's own instruments. module_mode, then the
            // saturation / late / correction counters and checkpoint/term health that
            // an edge counter cannot express — each a refinement of the baseline, not
            // a replacement. `emit_overflow` refines `outputs_failed`.
            let b = ACCT_METRIC_COUNT as u16;
            tlm_gauge(sys, midx, t, b, s.mode as u64);
            tlm_counter(sys, midx, t, b + 1, s.emit_overflow as u64);
            tlm_counter(sys, midx, t, b + 2, s.agg.lane_overflows() as u64);
            tlm_counter(sys, midx, t, b + 3, s.agg.late_drops() as u64);
            tlm_counter(sys, midx, t, b + 4, s.agg.corrections() as u64);
            tlm_counter(sys, midx, t, b + 5, s.agg.pane_overflows() as u64);
            tlm_counter(sys, midx, t, b + 6, s.agg.coll_overflows() as u64);
            tlm_counter(sys, midx, t, b + 7, s.agg.non_retractable_drops() as u64);
            tlm_gauge(sys, midx, t, b + 8, s.events_since_ckpt as u64);
            tlm_gauge(sys, midx, t, b + 9, s.current_term);
            // work units — window emissions produced (fold/finalize work).
            tlm_counter(sys, midx, t, b + 10, s.acct.work_units);
        }

        // A present-but-invalid supplied checkpoint is a terminal fault: refuse
        // input rather than run with silently-erased history.
        if s.faulted {
            return 0;
        }

        // Distributed activation barrier: while staged behind an unmet barrier,
        // drain commit horizons and process NO events (upstream back-pressures on
        // record_in). In raft-propose mode also drive the producer loop: learn
        // the term, propose our checkpoint, learn its index, then wait for the
        // commit horizon to cross it. Once crossed the module activates and runs.
        if !s.active {
            if s.raft_mode {
                drain_leader(s, sys);
                try_propose(s, sys);
                drain_assigned(s, sys);
            }
            drain_barrier(s, sys);
            if !s.active {
                return 0;
            }
        }

        // Drain any retained window emissions before admitting the next event. One
        // frame per step, all-or-nothing: a full output ring leaves the frame in the
        // queue (head not advanced) to retry, and the next event is not admitted
        // until the queue empties — so a fan-out event never loses an emission under
        // EAGAIN. Byte cursors are u32 because the queue exceeds 64 KiB.
        if s.emit_head < s.emit_tail {
            let head = s.emit_head as usize;
            let flen = u16::from_le_bytes([s.emit_q[head], s.emit_q[head + 1]]) as usize;
            let poll_out = (sys.channel_poll)(s.out_chan, 0x02);
            if poll_out > 0 && (poll_out as u32 & 0x02) != 0 {
                let w = (sys.channel_write)(s.out_chan, s.emit_q.as_ptr().add(head + 2), flen);
                if w == flen as i32 {
                    s.emit_head += (2 + flen) as u32;
                    s.acct.output_drained(flen as u32);
                    if s.emit_head >= s.emit_tail {
                        s.emit_head = 0;
                        s.emit_tail = 0;
                        // The whole queue drained: this event's every emission is
                        // delivered, so the in-flight event is now resolved.
                        s.acct.input_succeeded();
                    }
                }
            }
            return 0;
        }

        // Checkpoint only when no emissions are pending — a checkpoint must not get
        // ahead of undelivered output. Retried every step until accepted.
        try_checkpoint(s, sys);

        // ONGOING replicated checkpoint (raft-propose mode, post-activation): every
        // `checkpoint_every` events, propose the current state as a NEW generation
        // with a fresh correlation id — so state is periodically replicated, not
        // only fenced once at boot. A term change re-proposes under the new
        // term (drain_leader keeps `current_term` current). Delivery is checked in
        // `try_propose`; the boot gate is not re-driven here — commit-acknowledged
        // retry-until-durable is clustor-coordinated.
        if s.raft_mode && s.ckpt_every > 0 && s.events_since_ckpt >= s.ckpt_every {
            drain_leader(s, sys);
            if s.proposed {
                s.corr = s.corr.wrapping_add(1);
                s.proposed = false;
            }
            try_propose(s, sys);
            if s.proposed {
                s.events_since_ckpt = 0;
            }
        }

        if s.in_chan < 0 || s.cont_len < 36 {
            return 0;
        }
        let cont_len = s.cont_len as usize;

        // Parse the param container into an AggSpec (borrows a raw view of s.cont,
        // so it does not tie up a borrow of `s`).
        let mut ops = [OpSpec {
            kind: agg::AggOp::Count,
            selector: &[],
            sel_cost: 0,
        }; MAX_OPS];
        let cont = core::slice::from_raw_parts(s.cont.as_ptr(), cont_len);
        let Some((spec, _nops)) = build_spec(cont, &mut ops) else {
            return 0;
        };

        // Admit exactly one whole typed event frame (peek-confirmed, never partial).
        let inch = SysChan::new(sys, s.in_chan);
        let read = match admit_frame(&inch, &mut s.in_buf, frame_len) {
            Admit::Complete(nn) => {
                s.acct.admit_input(nn as u64);
                nn
            }
            Admit::Empty | Admit::NeedMore => return 0,
            Admit::BoundaryLost => {
                // An event frame beyond max_record: observed and rejected.
                let _ = drain_all(&inch, &mut s.in_buf);
                s.acct.reject_input(0);
                return 0;
            }
            // A channel fault before an event is framed is a dependency error.
            Admit::ChanError(_) => return 0,
        };
        let frame = core::slice::from_raw_parts(s.in_buf.as_ptr(), read);

        // Ingest the event, CAPTURING every window emission into the queue instead of
        // writing inline. `ingest` commits its state; the captured emissions are then
        // drained over the following steps. Raw pointers into the queue fields so the
        // emit closure does not borrow `s` while `ingest` holds `&mut s.agg`.
        let eq_ptr = s.emit_q.as_mut_ptr();
        let tail_ptr = &mut s.emit_tail as *mut u32;
        let mut overflow = false;
        // Fan-out accounting: how many emissions this event captured (and their total
        // bytes) versus how many the bounded queue could not hold.
        let mut captured: u32 = 0;
        let mut captured_bytes: u32 = 0;
        let mut dropped: u32 = 0;
        let _ = ingest(&mut s.agg, &spec, frame, |out_frame| {
            let flen = out_frame.len();
            let t = *tail_ptr as usize;
            if flen > EMIT_FRAME_MAX || t + 2 + flen > EMIT_Q_CAP {
                // A single event exceeded the queue's proven bound — a bounded-state
                // overflow, counted like lane/pane overflow, never a silent loss.
                overflow = true;
                dropped += 1;
                return;
            }
            *eq_ptr.add(t) = (flen & 0xff) as u8;
            *eq_ptr.add(t + 1) = ((flen >> 8) & 0xff) as u8;
            core::ptr::copy_nonoverlapping(out_frame.as_ptr(), eq_ptr.add(t + 2), flen);
            *tail_ptr = (t + 2 + flen) as u32;
            captured += 1;
            captured_bytes += flen as u32;
        });
        if overflow {
            s.emit_overflow = s.emit_overflow.wrapping_add(1);
        }
        // Account the event's fan-out: captured emissions are generated+pending (they
        // drain over the following steps); dropped emissions are generated+failed. An
        // event that produced NO retained emission has met its output obligation now
        // and is resolved immediately; otherwise it resolves when the queue drains.
        if captured > 0 {
            s.acct.outputs_staged_bulk(captured, captured_bytes);
        }
        if dropped > 0 {
            s.acct.outputs_failed_bulk(dropped);
        }
        if captured == 0 {
            s.acct.input_succeeded();
        }
        // work units: this event's window emissions are its fold/finalize work.
        s.acct.add_work(captured as u64);
        s.events_since_ckpt = s.events_since_ckpt.wrapping_add(1);
        0
    }
}
