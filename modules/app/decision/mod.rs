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
//! verbatim from the `chronicle-bytecode` crate, so this module and the host tests
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
    include!("../../common/vm_core.rs");
    include!("../../common/pipeline_core.rs");
    include!("../../common/decision_core.rs");
    include!("../../common/hex_core.rs");
}
use dec::{
    decode_frame, encode_frame_scratch, hex_decode, run_decision_scratch, scan_decision_container,
    Builder, Field, Message, Scratch, Value, MAX_PIPE_FIELDS, STAGE_SCRATCH_CAP,
};

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
    hex: [u8; HEX_BUF],
    hex_len: u16,
    cont: [u8; CONT_BUF],
    cont_len: u16,
    fired: u32,
    /// Outcomes that constructed an empty message — the drop convention: the
    /// record was consumed deliberately and routed nowhere.
    dropped: u32,
    errors: u32,
    /// Frame-decode failures, split from eval errors so a miswired channel
    /// (malformed frames) is distinguishable from a broken program.
    errors_frame: u32,
    /// Output-frame encode failures (oversized outcome).
    errors_encode: u32,
    /// 1 = configuration fault at init: the node refuses input (declared
    /// metric; the named reason was logged once at error level).
    faulted: u32,
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
        s.hex_len = 0;
        s.cont_len = 0;
        s.fired = 0;
        s.dropped = 0;
        s.errors = 0;
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

        if s.in_chan < 0 || s.cont_len == 0 || s.faulted != 0 {
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
        let frame = core::slice::from_raw_parts(s.in_buf.as_ptr(), n as usize);

        // Decode the input record frame into a single message parameter.
        let mut fields = [Field {
            number: 0,
            value: Value::Null,
        }; MAX_PIPE_FIELDS];
        let nf = match decode_frame(frame, &mut fields) {
            Ok(nf) => nf,
            Err(_) => {
                s.errors_frame = s.errors_frame.wrapping_add(1);
                return 0;
            }
        };
        let params = [Message {
            fields: &fields[..nf],
        }];

        // First-hit decision -> constructed outcome message.
        let cont = core::slice::from_raw_parts(s.cont.as_ptr(), s.cont_len as usize);
        let mut builder = Builder::new();
        // Arena for the writing builtins (reverse/case/replace/base64) in
        // predicates and outcomes; record-scoped, serialized before reuse.
        let mut sbuf = [0u8; STAGE_SCRATCH_CAP];
        let mut scratch = Scratch::new(&mut sbuf);
        match run_decision_scratch(cont, &params, &mut builder, &mut scratch) {
            Ok(_) => {
                s.fired = s.fired.wrapping_add(1);
                // DROP convention: an outcome that constructs an EMPTY message
                // routes nowhere. A first-hit policy whose default is `M {}`
                // is a filter — the non-matching records vanish here, counted,
                // instead of polluting downstream with hollow records.
                if builder.message().fields.is_empty() {
                    s.dropped = s.dropped.wrapping_add(1);
                    return 0;
                }
                match encode_frame_scratch(&builder.message(), &scratch, &mut s.out_buf) {
                    Ok(m) => {
                        let out_chan = s.out_chan;
                        let poll_out = (sys.channel_poll)(out_chan, 0x02);
                        if poll_out > 0 && (poll_out as u32 & 0x02) != 0 {
                            (sys.channel_write)(out_chan, s.out_buf.as_ptr(), m);
                        }
                    }
                    Err(_) => s.errors_encode = s.errors_encode.wrapping_add(1),
                }
            }
            Err(_) => s.errors = s.errors.wrapping_add(1),
        }
        0
    }
}
