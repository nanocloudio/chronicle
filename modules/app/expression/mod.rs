//! Expression evaluator — Fluxor `.fmod` app module (spec artefact 2).
//!
//! PARAM-DRIVEN: the checked-CEL bytecode is NOT baked into the module — it
//! arrives as a `program` module param (hex-encoded, since a config carries text)
//! with a `max_cost` ceiling. So one evaluator binary serves ANY Expression: the
//! `.uproc`/CEL compiler emits the bytecode, a config sets it as the param, and
//! this module runs it. Two configs with different `program` values produce
//! different results from the identical `.fmod` (see examples/expression/*.yaml).
//!
//! It loads the SAME bounded evaluator source the host crate `chronicle-bytecode`
//! uses (via `include!` of core.rs), reads a length-framed record on `in`,
//! decodes it into fields, runs the param bytecode, and writes the scalar result
//! bytes to `out`.

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

// The bounded evaluator core — identical source to the `chronicle-bytecode` crate.
#[path = "../../common/vm_core.rs"]
mod evalcore;
use evalcore::{builtin_arity, op, scan_code, Field, Message, Value};

// Hex codec — identical source to the crate; turns a param's text back into bytes.
include!("../../common/hex_core.rs");

// Flat checked-IR lowerer — identical source to the crate. Lets this module accept
// the higher-level `ir` param (a shipped checked IR) and lower it to bytecode HERE,
// at load: a successful lowering is the module proving it can run what it was given,
// rather than trusting opaque bytecode. `lower_flat` resolves `op::*` via the
// `use evalcore::op` above.
include!("../../common/lower_core.rs");

const MAX_FIELDS: usize = 8;
const HEX_BUF: usize = 1024;
const CODE_BUF: usize = 512;

#[repr(C)]
struct ModuleState {
    syscalls: *const SyscallTable,
    in_chan: i32,
    out_chan: i32,
    in_buf: [u8; 512],
    out_buf: [u8; 256],

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

    evaluated: u32,
    errors: u32,
    /// 1 = configuration fault at init: the node refuses input (declared
    /// metric; the named reason was logged once at error level).
    faulted: u32,
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
        s.in_buf = [0u8; 512];
        s.out_buf = [0u8; 256];
        s.hex_len = 0;
        s.is_ir = false;
        s.code_len = 0;
        s.max_cost = 8;
        s.evaluated = 0;
        s.errors = 0;
        s.faulted = 0;

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

        if s.in_chan < 0 || s.code_len == 0 || s.faulted != 0 {
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
        let cost = s.max_cost;
        let code_len = s.code_len as usize;

        // Decode fields, evaluate the param bytecode, stage the result bytes.
        let mut out_len: usize = 0;
        {
            let data = &s.in_buf[..read];
            let mut fields = [Field {
                number: 0,
                value: Value::Null,
            }; MAX_FIELDS];
            let nfields = decode(data, &mut fields);
            let params = [Message {
                fields: &fields[..nfields],
            }];
            let code = &s.code[..code_len];
            // Arena for the writing builtins; result-scoped, copied out below.
            let mut sbuf = [0u8; 512];
            let mut scratch = evalcore::Scratch::new(&mut sbuf);
            match evalcore::eval_scratch(code, &params, &mut scratch, cost) {
                Ok(v) => match evalcore::resolve_scratch(v, &scratch) {
                    Value::Bytes(result) => {
                        let len = result.len().min(s.out_buf.len());
                        s.out_buf[..len].copy_from_slice(&result[..len]);
                        out_len = len;
                    }
                    _ => s.errors = s.errors.wrapping_add(1),
                },
                Err(_) => s.errors = s.errors.wrapping_add(1),
            }
        }

        if out_len > 0 && s.out_chan >= 0 {
            let poll_out = (sys.channel_poll)(s.out_chan, 0x02);
            if poll_out > 0 && (poll_out as u32 & 0x02) != 0 {
                (sys.channel_write)(s.out_chan, s.out_buf.as_ptr(), out_len);
                s.evaluated = s.evaluated.wrapping_add(1);
            }
        }
        0
    }
}

/// Decode the length-framed record into borrowed byte-string fields.
/// Framing: `[count:u8]` then `count` × `[field_number:u8][len:u16 LE][bytes]`.
fn decode<'a>(data: &'a [u8], fields: &mut [Field<'a>; MAX_FIELDS]) -> usize {
    if data.is_empty() {
        return 0;
    }
    let count = data[0] as usize;
    let mut off = 1usize;
    let mut fi = 0usize;
    while fi < count && fi < MAX_FIELDS {
        if off + 3 > data.len() {
            break;
        }
        let number = data[off] as u32;
        let len = u16::from_le_bytes([data[off + 1], data[off + 2]]) as usize;
        off += 3;
        if off + len > data.len() {
            break;
        }
        fields[fi] = Field {
            number,
            value: Value::Bytes(&data[off..off + len]),
        };
        off += len;
        fi += 1;
    }
    fi
}
