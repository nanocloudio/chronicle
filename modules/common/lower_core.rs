// Bounded, no_std, no-alloc LOWERER for the flat checked-IR. Like the other
// `*_core.rs` files it carries no inner attributes and no test module, so it is
// `include!`d verbatim by both this crate and the on-device modules — one source
// of truth, host and device.
//
// The checked IR (see chronicle-canonical::cel) is the type-free, fully
// name-resolved form of an expression: all schema access already happened, so
// turning it into bytecode needs no type environment. This file is the piece
// that does that turning, and it is designed to run *at load on the target*: a
// server (or device) handed a flat IR lowers it here, and a successful lowering
// is itself the proof that the target can run it — no separate validation and no
// out-of-band agreement on the opcode set.
//
// Wire form: a POST-ORDER token stream (children before parent), so lowering is a
// single forward pass — each token's operands were already emitted by the tokens
// before it, exactly as the tree-walking `lower_ir` would have. No recursion, no
// stack, no allocation; every read and write is bounds-checked, so a malformed
// stream returns `LowerError`, never panics.

/// Flat checked-IR node tags (the post-order stream alphabet). Disjoint numbering
/// from `op::*` — these are *source* tokens, not VM opcodes.
pub mod ir {
    pub const INT: u8 = 0x01; // i64 LE
    pub const BOOL: u8 = 0x02; // u8 (0/1)
    pub const STR: u8 = 0x03; // len:u16 LE, bytes
    pub const LOADPARAM: u8 = 0x04; // idx:u8
    pub const PATH: u8 = 0x05; // param:u8, nfields:u8, fields[nfields]:u32 LE
    pub const NOT: u8 = 0x06;
    pub const CMP_EQ: u8 = 0x07;
    pub const CMP_NE: u8 = 0x08;
    pub const CMP_LT: u8 = 0x09;
    pub const CMP_LE: u8 = 0x0A;
    pub const CMP_GT: u8 = 0x0B;
    pub const CMP_GE: u8 = 0x0C;
    pub const AND: u8 = 0x0D;
    pub const OR: u8 = 0x0E;
    pub const ADD: u8 = 0x0F;
    pub const SUB: u8 = 0x10;
    pub const MUL: u8 = 0x11;
    pub const SETFIELD: u8 = 0x12; // number:u32 LE
    pub const FINISHMSG: u8 = 0x13;
    pub const RET: u8 = 0x14; // scalar-result terminator
                              // CEL extension builtins (builtins_core.rs) and `cel.bind` locals.
    pub const CALL: u8 = 0x15; // id:u16 LE — args already on the stack
    pub const STORE_LOCAL: u8 = 0x16; // idx:u8
    pub const LOAD_LOCAL: u8 = 0x17; // idx:u8
}

/// Deterministic lowering failures. Never panics on malformed input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LowerError {
    /// The stream ended mid-token.
    Truncated,
    /// An unknown node tag — the target does not recognise this IR (the load-time
    /// analogue of a runtime `BadOpcode`: rejected here, before it can run).
    BadTag(u8),
    /// The output buffer was too small for the lowered bytecode.
    Overflow,
}

/// Lower a flat, post-order checked-IR stream into VM bytecode in `out`. Returns
/// `(bytecode_len, opcode_count)` — the opcode count is the static cost bound the
/// runtime enforces, identical to the compiler's `max_cost`. Pure, allocation-free,
/// and panic-free: this is the exact bytecode the host `lower_ir` produces, which
/// the differential fuzzer proves.
pub fn lower_flat(flat: &[u8], out: &mut [u8]) -> Result<(usize, u64), LowerError> {
    let mut pc: usize = 0;
    let mut end: usize = 0;
    let mut ops: u64 = 0;

    // Read helpers over the source stream (bounds-checked).
    macro_rules! rd_u8 {
        () => {{
            let b = *flat.get(pc).ok_or(LowerError::Truncated)?;
            pc += 1;
            b
        }};
    }
    macro_rules! rd_bytes {
        ($n:expr) => {{
            let n = $n;
            let s = flat.get(pc..pc + n).ok_or(LowerError::Truncated)?;
            pc += n;
            s
        }};
    }
    // Emit one VM opcode (with inline operand bytes) into `out`, counting it.
    macro_rules! emit {
        ($opcode:expr $(, $operand:expr)*) => {{
            if end >= out.len() {
                return Err(LowerError::Overflow);
            }
            out[end] = $opcode;
            end += 1;
            $(
                let bytes = $operand;
                if end + bytes.len() > out.len() {
                    return Err(LowerError::Overflow);
                }
                out[end..end + bytes.len()].copy_from_slice(bytes);
                end += bytes.len();
            )*
            ops += 1;
        }};
    }

    while pc < flat.len() {
        let tag = rd_u8!();
        match tag {
            ir::INT => {
                let v = rd_bytes!(8);
                emit!(op::PUSH_I64, v);
            }
            ir::BOOL => {
                let b = rd_u8!();
                emit!(op::PUSH_BOOL, &[b]);
            }
            ir::STR => {
                let lb = rd_bytes!(2);
                let len = u16::from_le_bytes([lb[0], lb[1]]) as usize;
                let s = rd_bytes!(len);
                emit!(op::PUSH_STR, &(len as u16).to_le_bytes(), s);
            }
            ir::LOADPARAM => {
                let idx = rd_u8!();
                emit!(op::LOAD_PARAM, &[idx]);
            }
            ir::PATH => {
                let param = rd_u8!();
                let nfields = rd_u8!() as usize;
                emit!(op::LOAD_PARAM, &[param]);
                for _ in 0..nfields {
                    let nb = rd_bytes!(4);
                    emit!(op::GET_FIELD, nb);
                }
            }
            ir::NOT => emit!(op::NOT),
            ir::CMP_EQ => emit!(op::CMP_EQ),
            ir::CMP_NE => emit!(op::CMP_NE),
            ir::CMP_LT => emit!(op::CMP_LT),
            ir::CMP_LE => emit!(op::CMP_LE),
            ir::CMP_GT => emit!(op::CMP_GT),
            ir::CMP_GE => emit!(op::CMP_GE),
            ir::AND => emit!(op::AND),
            ir::OR => emit!(op::OR),
            ir::ADD => emit!(op::ADD),
            ir::SUB => emit!(op::SUB),
            ir::MUL => emit!(op::MUL),
            ir::SETFIELD => {
                let nb = rd_bytes!(4);
                emit!(op::SET_FIELD, nb);
            }
            ir::FINISHMSG => emit!(op::FINISH_MSG),
            ir::RET => emit!(op::RET),
            ir::CALL => {
                let idb = rd_bytes!(2);
                let id = u16::from_le_bytes([idb[0], idb[1]]);
                // An id outside the pinned table is rejected HERE, at load —
                // the lowering analogue of BadTag. (An id that is pinned but
                // compiled out of this build still fails closed at runtime
                // with BadBuiltin — feature gating is a runtime property.)
                let arity = builtin_arity(id).ok_or(LowerError::BadTag(ir::CALL))? as u64;
                emit!(op::CALL, &id.to_le_bytes());
                // The VM charges 1 + arity for a CALL; the static bound must
                // cover it or a legal program would trip its own cost ceiling.
                ops += arity;
            }
            ir::STORE_LOCAL => {
                let idx = rd_u8!();
                emit!(op::STORE_LOCAL, &[idx]);
            }
            ir::LOAD_LOCAL => {
                let idx = rd_u8!();
                emit!(op::LOAD_LOCAL, &[idx]);
            }
            other => return Err(LowerError::BadTag(other)),
        }
    }
    Ok((end, ops))
}

/// Transcode an IR-stages container into the bytecode-stages container that
/// `stage_at`/`run_stages` consume, lowering each stage at load.
///
/// Input:  `[nstages:u8]` then per stage `[route:u8][ir_len:u16 LE][flat_ir]`.
/// Output: `[nstages:u8]` then per stage
///         `[route:u8][max_cost:u32 LE][code_len:u16 LE][code]`.
///
/// `route` is the failure-routing policy (`0xff` = none); it passes through
/// untouched because it is policy, not code.
///
/// Each stage's cost bound is re-derived by `lower_flat`, never trusted from the
/// wire. Bounds-checked and panic-free; a stage whose IR does not lower fails the
/// whole transcode (the pipeline then loads nothing rather than a partial table).
pub fn lower_stages(ir_container: &[u8], out: &mut [u8]) -> Result<usize, LowerError> {
    let n = *ir_container.first().ok_or(LowerError::Truncated)? as usize;
    if out.is_empty() {
        return Err(LowerError::Overflow);
    }
    out[0] = n as u8;
    let mut ip = 1usize; // cursor in the IR container
    let mut wp = 1usize; // write cursor in the bytecode container
    for _ in 0..n {
        // The failure route rides through the lowering unchanged: it is policy,
        // not code, so there is nothing to lower.
        let route = *ir_container.get(ip).ok_or(LowerError::Truncated)?;
        ip += 1;
        let lb = ir_container.get(ip..ip + 2).ok_or(LowerError::Truncated)?;
        let ilen = u16::from_le_bytes([lb[0], lb[1]]) as usize;
        ip += 2;
        let flat = ir_container
            .get(ip..ip + ilen)
            .ok_or(LowerError::Truncated)?;
        ip += ilen;
        // Reserve the 7-byte stage header, lower the IR into the bytes after it,
        // then backfill (cost, code_len) once `lower_flat` reports them.
        if wp + 7 > out.len() {
            return Err(LowerError::Overflow);
        }
        let (clen, cost) = {
            let dst = out.get_mut(wp + 7..).ok_or(LowerError::Overflow)?;
            lower_flat(flat, dst)?
        };
        out[wp] = route;
        out[wp + 1..wp + 5].copy_from_slice(&(cost as u32).to_le_bytes());
        out[wp + 5..wp + 7].copy_from_slice(&(clen as u16).to_le_bytes());
        wp += 7 + clen;
    }
    Ok(wp)
}

/// Lower one embedded program: read `[ir_len:u16][flat_ir]` at `ir[ip..]`, lower
/// it via `lower_flat` into `out[wp..]` as the `[cost:u32][len:u16][code]` prog
/// `read_prog` consumes. Returns the advanced `(ip, wp)` cursors.
fn lower_one_prog(
    ir: &[u8],
    ip: usize,
    out: &mut [u8],
    wp: usize,
) -> Result<(usize, usize), LowerError> {
    let lb = ir.get(ip..ip + 2).ok_or(LowerError::Truncated)?;
    let ilen = u16::from_le_bytes([lb[0], lb[1]]) as usize;
    let flat_start = ip + 2;
    let flat = ir
        .get(flat_start..flat_start + ilen)
        .ok_or(LowerError::Truncated)?;
    if wp + 6 > out.len() {
        return Err(LowerError::Overflow);
    }
    let (clen, cost) = {
        let dst = out.get_mut(wp + 6..).ok_or(LowerError::Overflow)?;
        lower_flat(flat, dst)?
    };
    out[wp..wp + 4].copy_from_slice(&(cost as u32).to_le_bytes());
    out[wp + 4..wp + 6].copy_from_slice(&(clen as u16).to_le_bytes());
    Ok((flat_start + ilen, wp + 6 + clen))
}

/// Transcode an aggregation IR-`def` container into the bytecode `def` container
/// `build_spec` consumes. The 36-byte header (window_size, lateness, max_lanes,
/// window_step, correction_horizon) is copied verbatim; each embedded program —
/// key, time, emit, and every op selector — is `[ir_len:u16][flat_ir]` in the IR
/// form and is lowered to `[cost:u32][len:u16][code]`. The `nops` byte and each
/// op `kind` byte pass through. Fail-closed: any program that won't lower fails
/// the whole transcode (a load-time analogue of a runtime BadOpcode).
pub fn lower_def(ir_def: &[u8], out: &mut [u8]) -> Result<usize, LowerError> {
    const HDR: usize = 36;
    let hdr = ir_def.get(..HDR).ok_or(LowerError::Truncated)?;
    if out.len() < HDR {
        return Err(LowerError::Overflow);
    }
    out[..HDR].copy_from_slice(hdr);
    let mut ip = HDR;
    let mut wp = HDR;
    // key, time, emit programs.
    for _ in 0..3 {
        let (nip, nwp) = lower_one_prog(ir_def, ip, out, wp)?;
        ip = nip;
        wp = nwp;
    }
    // Operator count, then per op: kind byte + selector program.
    let nops = *ir_def.get(ip).ok_or(LowerError::Truncated)? as usize;
    ip += 1;
    if wp >= out.len() {
        return Err(LowerError::Overflow);
    }
    out[wp] = nops as u8;
    wp += 1;
    for _ in 0..nops {
        let kind = *ir_def.get(ip).ok_or(LowerError::Truncated)?;
        ip += 1;
        if wp >= out.len() {
            return Err(LowerError::Overflow);
        }
        out[wp] = kind;
        wp += 1;
        let (nip, nwp) = lower_one_prog(ir_def, ip, out, wp)?;
        ip = nip;
        wp = nwp;
    }
    // Optional trailing emit-trigger bytes pass through verbatim: a kind byte and
    // (for OnCount) its u32 count. `build_spec` decodes them the same way from the
    // lowered def. Absent = OnClose.
    let tail = ir_def.get(ip..).unwrap_or(&[]);
    let wend = wp.checked_add(tail.len()).ok_or(LowerError::Overflow)?;
    if wend > out.len() {
        return Err(LowerError::Overflow);
    }
    out[wp..wend].copy_from_slice(tail);
    wp = wend;
    Ok(wp)
}
