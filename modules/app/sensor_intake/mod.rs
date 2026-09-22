//! Sensor intake — `SensorSample` → chronicle typed record frame.
//!
//! The composition boundary between fluxor's measurement surface and
//! chronicle's record format, and it exists because BOTH sides are right to have
//! their own.
//!
//! A `SensorSample` (`abi::contracts::sensor`) is 24 fixed bytes: a reading, its
//! scale, its own timestamp, a sequence and a sensor id. That shape is what a
//! driver can emit from an interrupt with no allocation and no format knowledge,
//! and it is the surface a graph binds by capability (`sensor.sample`) rather
//! than by naming a driver.
//!
//! A chronicle record is the self-describing typed frame every engine here
//! speaks: `[count:u8]` then per field `[number:u8][type:u8][len:u16 LE][payload]`.
//! That shape is what a compiled CEL program addresses by field number, and it
//! is what `pipeline`, `decision` and `aggregation` decode.
//!
//! Neither should learn the other's format. A driver that emitted chronicle
//! frames would be coupled to this engine family; an engine that decoded
//! `SensorSample` would be coupled to one producer's wire shape and would need a
//! second decoder for the next surface. So the translation is a node, and it is
//! the smallest node in this project.
//!
//! Field numbers in the emitted record (fixed, so a CEL program written against
//! one sensor works against every sensor):
//!
//!   1 `value`      i64 — the reading, unscaled
//!   2 `scale`      i64 — decimal exponent; the quantity is `value × 10^scale`
//!   3 `t_micros`   i64 — the reading's OWN time, for event-time windows
//!   4 `seq`        i64 — monotonic per sensor; a gap is a dropped reading
//!   5 `sensor_id`  i64 — which sensor, for keyed lanes
//!   6 `flags`      i64 — `FLAG_SUBSTITUTE` / `FLAG_SATURATED`
//!
//! `scale` is carried rather than applied. Applying it would need either a float
//! — which this VM does not evaluate and this silicon has no unit for — or a
//! multiply that overflows for a large exponent. A rule that wants millidegrees
//! compares against a millidegree literal; one that wants to normalise does it
//! in CEL, where the cost is bounded and visible.

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

// Only the accounting taxonomy is mounted. This module deliberately does NOT
// include `vm_core`/`pipeline_core`: they would bring the bounded CEL evaluator
// and its builtin table with them, and a 24-byte-to-73-byte translation has no
// business linking an expression engine — it would be the largest thing in the
// artefact by an order of magnitude. The record frame is written directly
// instead; the format is four bytes of header per field and is pinned by the
// round-trip test in `tests/`.
mod si {
    use super::abi::SyscallTable;
    include!("../../common/accounting_core.rs");
}
use si::Accounting;

/// Field type tag for a little-endian i64, from the canonical frame format in
/// `pipeline_core`. Mirrored rather than imported for the reason above; the
/// round-trip test decodes with the real codec so the two cannot drift.
const TY_I64: u8 = 1;

/// Write one `[number][type][len:u16 LE][i64 LE]` field. Returns the new offset,
/// or `None` if it would not fit — never a partial field.
fn put_i64(out: &mut [u8], off: usize, number: u8, v: i64) -> Option<usize> {
    let end = off + 4 + 8;
    if end > out.len() {
        return None;
    }
    out[off] = number;
    out[off + 1] = TY_I64;
    out[off + 2..off + 4].copy_from_slice(&8u16.to_le_bytes());
    out[off + 4..end].copy_from_slice(&v.to_le_bytes());
    Some(end)
}

// No telemetry core mounted. This module publishes no instruments of its own —
// its manifest declares that exempt, and its counters are the baseline
// accounting taxonomy, which the scheduler already reports. Mounting the emit
// helpers to use none of them would put an unused surface in the smallest module
// this project ships — a few hundred bytes of state, most of it the two buffers.

/// One `SensorSample` in, one record frame out.
///
/// The output bound is derived from the input: six i64 fields is
/// `1 + 6 × (1 + 1 + 2 + 8)` = 73 bytes, and 96 leaves room for a seventh field
/// without a second constant to remember. Nothing here scales with the arena —
/// a reading is a reading on every die — so unlike the engines this module
/// carries no tier.
const SAMPLE_SIZE: usize = abi::contracts::sensor::SAMPLE_SIZE;
const REC_BUF: usize = 96;

/// Fields emitted per reading; the frame layout above.
const N_FIELDS: usize = 6;
/// One field's frame cost: `[number:u8][type:u8][len:u16][i64]`.
const FIELD_BYTES: usize = 1 + 1 + 2 + 8;
/// The largest frame this module can emit: the count byte plus every field.
const MAX_FRAME: usize = 1 + N_FIELDS * FIELD_BYTES;
/// Asserted against the buffer so a field added to the layout above fails the
/// BUILD rather than failing to encode on a device.
const _: () = assert!(MAX_FRAME <= REC_BUF);

#[repr(C)]
struct ModuleState {
    syscalls: *const SyscallTable,
    in_chan: i32,
    out_chan: i32,
    in_buf: [u8; SAMPLE_SIZE],
    out_buf: [u8; REC_BUF],
    /// Bytes of `out_buf` still owed to the ring. A record is retained until
    /// fully delivered, never re-encoded — a half-written frame downstream
    /// would decode as a different reading.
    pending_off: u16,
    pending_len: u16,
    /// The common accounting taxonomy: a translated reading is
    /// `inputs_succeeded`; a short or undecodable sample is `inputs_rejected`;
    /// an encode failure is `inputs_failed`.
    acct: Accounting,
    /// Samples refused for being shorter than the fixed record — counted rather
    /// than padded, since a truncated fixed-layout record reads plausible values
    /// from the wrong offsets.
    errors_short: u32,
    /// Readings the consumer has not taken yet, so backpressure is visible as a
    /// number rather than inferred from a flat input count.
    stalls: u32,
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<ModuleState>() as u32
}

// The same figure as data, so `pack` records this module's footprint and a
// sensor graph's arena demand is summable at compose time.
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
    _params: *const u8,
    _params_len: usize,
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
        s.in_buf = [0u8; SAMPLE_SIZE];
        s.out_buf = [0u8; REC_BUF];
        s.pending_off = 0;
        s.pending_len = 0;
        s.acct = Accounting::default();
        s.errors_short = 0;
        s.stalls = 0;
        // No params: the translation is total. There is nothing to configure, so
        // there is no awaiting-config mode and no fault mode either.
        0
    }
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    unsafe {
        let s = &mut *(state as *mut ModuleState);
        let sys = &*s.syscalls;

        // Drain a retained record first: a reading already translated is owed to
        // the consumer before another is admitted, so the output order is the
        // measurement order.
        if s.pending_len > 0 {
            let off = s.pending_off as usize;
            let len = s.pending_len as usize;
            let n = (sys.channel_write)(s.out_chan, s.out_buf[off..].as_ptr(), len);
            if n > 0 {
                let n = n as usize;
                if n >= len {
                    s.pending_off = 0;
                    s.pending_len = 0;
                    s.acct.inputs_succeeded = s.acct.inputs_succeeded.wrapping_add(1);
                } else {
                    s.pending_off = (off + n) as u16;
                    s.pending_len = (len - n) as u16;
                }
            } else {
                s.stalls = s.stalls.wrapping_add(1);
            }
            return 0;
        }

        // Admit one whole sample. A fixed-size record is read all-or-nothing:
        // there is no length prefix to recover a partial one from.
        let got = (sys.channel_read)(s.in_chan, s.in_buf.as_mut_ptr(), SAMPLE_SIZE);
        if got <= 0 {
            return 0;
        }
        if (got as usize) < SAMPLE_SIZE {
            s.errors_short = s.errors_short.wrapping_add(1);
            s.acct.inputs_rejected = s.acct.inputs_rejected.wrapping_add(1);
            return 0;
        }

        let Some(sample) = abi::contracts::sensor::decode(&s.in_buf[..SAMPLE_SIZE]) else {
            s.errors_short = s.errors_short.wrapping_add(1);
            s.acct.inputs_rejected = s.acct.inputs_rejected.wrapping_add(1);
            return 0;
        };

        // Translate. Field numbers are the fixed layout in the module doc, so a
        // CEL program written against one sensor reads every sensor.
        s.out_buf[0] = N_FIELDS as u8;
        let mut off = 1usize;
        let mut ok = true;
        for (number, v) in [
            (1u8, sample.value),
            (2, sample.scale as i64),
            (3, sample.t_micros as i64),
            (4, sample.seq as i64),
            (5, sample.sensor_id as i64),
            (6, sample.flags as i64),
        ] {
            match put_i64(&mut s.out_buf, off, number, v) {
                Some(next) => off = next,
                None => {
                    ok = false;
                    break;
                }
            }
        }
        if ok {
            s.pending_off = 0;
            s.pending_len = off as u16;
        } else {
            // Unreachable given the compile-time bound above, counted rather
            // than asserted: a panic in a PIC module has no unwinder.
            s.acct.inputs_failed = s.acct.inputs_failed.wrapping_add(1);
        }
        0
    }
}
