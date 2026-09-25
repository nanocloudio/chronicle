// Pure, dependency-free, no_std evaluator core. Contains NO inner attributes and
// NO test module, so it can be `include!`d both by this crate's `lib.rs` and by
// the Fluxor `.fmod` module at `modules/app/expression/mod.rs` — one source of
// truth for the runtime, whether it runs on the host (tests) or on device.
//
// The opcode set covers parameter load, field selection, constants, comparison,
// arithmetic, logical ops, message construction, `CALL` into the pinned CEL
// extension builtin table, and `cel.bind` locals — which is what it takes to
// lower an Expression, a Transformation (message construction) and a Decision
// (predicate plus constructed outcome).
//
// `builtins_core.rs` is included below rather than left to each consumer, so
// every mount of this file gets the builtin table with no second include and no
// way for the two to be mounted at different revisions.

// A sibling include, relative to this file, so it resolves both in
// modules/common/ and in a consumer's flat `chronicle-common` source tree.
include!("builtins_core.rs");

/// A runtime value. Strings/bytes/messages borrow from the input, so the
/// evaluator allocates nothing.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Value<'a> {
    Null,
    Bool(bool),
    Int(i64),
    Uint(u64),
    Str(&'a str),
    Bytes(&'a [u8]),
    /// A (borrowed) protobuf message, addressed by field number.
    Msg(&'a Message<'a>),
    /// A nested message held as its own record frame — frame type 3. A path
    /// selects into it; a frame writes it back as type 3, so it stays a
    /// message across every stage and connector that passes it on.
    Frame(&'a [u8]),
    /// Bytes produced by a builtin into the caller's scratch arena
    /// (`builtins_core::Scratch`), addressed by offset — an index, not a
    /// borrow, because a borrow would freeze the arena against the next
    /// builtin's append. Resolve with [`resolve_scratch`] (or the arena's
    /// `slice`) once evaluation is done and the arena is immutable.
    Scratch {
        off: u32,
        len: u32,
    },
    /// A nested message `FRAME_PACK` built in the scratch arena: a
    /// [`Value::Frame`] addressed as [`Value::Scratch`] is.
    ScratchFrame {
        off: u32,
        len: u32,
    },
}

/// Resolve a `Value::Scratch` against the (now-immutable) arena it was
/// produced into; every other variant passes through. The returned value
/// borrows the arena, so call this at CONSUMPTION time (serialization),
/// after the evaluation that filled the arena has returned.
pub fn resolve_scratch<'a>(v: Value<'a>, scratch: &'a Scratch<'_>) -> Value<'a> {
    match v {
        Value::Scratch { off, len } => Value::Bytes(scratch.slice(off, len)),
        Value::ScratchFrame { off, len } => Value::Frame(scratch.slice(off, len)),
        other => other,
    }
}

/// One decoded protobuf field: its field number and typed value.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Field<'a> {
    pub number: u32,
    pub value: Value<'a>,
}

/// A decoded protobuf message as a flat set of fields addressed by number.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Message<'a> {
    pub fields: &'a [Field<'a>],
}

impl<'a> Message<'a> {
    /// Field access by number. Absent field → `Value::Null` (proto3 presence).
    pub fn get(&self, number: u32) -> Value<'a> {
        let mut i = 0;
        while i < self.fields.len() {
            if self.fields[i].number == number {
                return self.fields[i].value;
            }
            i += 1;
        }
        Value::Null
    }
}

/// Maximum fields a single decoded or constructed message may carry.
///
/// A field table lives on the evaluating step's stack, so the bound is a stack
/// cost and is tiered on pointer width: 128 fields (4 KiB a table) on a 64-bit
/// core, where a module's stack is at least 64 KiB; 32 (768 bytes) on a 32-bit
/// core, where an isolated module's stack is 2 KiB. The frame's one-byte field
/// count holds either. A record with more fields than the table holds is
/// refused, never truncated.
pub const MAX_BUILD_FIELDS: usize = if cfg!(target_pointer_width = "64") {
    128
} else {
    32
};
const _: () = assert!(MAX_BUILD_FIELDS <= u8::MAX as usize);

/// Fixed-capacity accumulator for a constructed message. Its fields borrow from
/// the input (`'a`), so construction allocates nothing. The caller owns the
/// storage and reads it back as a `Message` after `eval_full` returns
/// `EvalResult::Constructed`.
#[derive(Debug, Clone, Copy)]
pub struct Builder<'a> {
    pub fields: [Field<'a>; MAX_BUILD_FIELDS],
    pub len: usize,
}

impl<'a> Builder<'a> {
    pub fn new() -> Self {
        Self {
            fields: [Field {
                number: 0,
                value: Value::Null,
            }; MAX_BUILD_FIELDS],
            len: 0,
        }
    }
    /// The constructed message as a borrowable view.
    pub fn message(&self) -> Message<'_> {
        Message {
            fields: &self.fields[..self.len],
        }
    }
}

impl<'a> Default for Builder<'a> {
    fn default() -> Self {
        Self::new()
    }
}

/// The outcome of an evaluation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EvalResult<'a> {
    /// A scalar/message value left on the stack at `RET`.
    Scalar(Value<'a>),
    /// A message was constructed into the caller's `Builder` (read it back via
    /// `Builder::message`). Emitted by `FINISH_MSG`.
    Constructed,
}

/// Opcodes. Encoding is little-endian; operands are inline after the opcode.
pub mod op {
    pub const RET: u8 = 0x00;
    pub const LOAD_PARAM: u8 = 0x01; // idx:u8 — push param message
    pub const GET_FIELD: u8 = 0x02; // number:u32 — pop msg, push field
    pub const PUSH_I64: u8 = 0x10; // v:i64 — push Int
    pub const PUSH_BOOL: u8 = 0x11; // v:u8 — push Bool
                                    // len:u16 LE, bytes[len] — push a string literal (as borrowed UTF-8 bytes,
                                    // pointing into the code; `Value::Bytes` avoids linking `from_utf8` on device).
    pub const PUSH_STR: u8 = 0x12;
    // Comparison: pop b, a; push Bool(a OP b), or Null when either is absent.
    pub const CMP_EQ: u8 = 0x20;
    pub const CMP_NE: u8 = 0x21;
    pub const CMP_LT: u8 = 0x22;
    pub const CMP_LE: u8 = 0x23;
    pub const CMP_GT: u8 = 0x24;
    pub const CMP_GE: u8 = 0x25;
    // Logical: AND/OR pop b, a; NOT pops a. Operate on Bool or Null (unknown),
    // three-valued as CEL's `&&`/`||`/`!` are over errors.
    pub const AND: u8 = 0x30;
    pub const OR: u8 = 0x31;
    pub const NOT: u8 = 0x32;
    pub const IS_SET: u8 = 0x33; // pop v, push Bool(v is present) — CEL `has()`
    /// n:u8, then n field numbers (u8) — pop n values, push the record FRAME
    /// they make (a nested message, as it crosses a frame boundary). Written
    /// into the scratch arena; absent values are left out, as in any frame.
    pub const FRAME_PACK: u8 = 0x42;
    // Integer arithmetic: pop b, a; push Int(a OP b). A result outside i64 is
    // `Overflow`, never wrapped (CEL's int64 overflow error).
    pub const ADD: u8 = 0x50;
    pub const SUB: u8 = 0x51;
    pub const MUL: u8 = 0x52;
    /// Integer division, truncating toward zero. A zero divisor is a
    /// `DivByZero` eval error — the decision fails closed, as every other
    /// fault does; a policy that divides by a field must guard it.
    pub const DIV: u8 = 0x56;
    /// Integer remainder, sign of the dividend (CEL's `%`). A zero divisor is a
    /// `DivByZero` eval error, as for `DIV`.
    pub const REM: u8 = 0x57;
    // Message construction.
    pub const SET_FIELD: u8 = 0x40; // number:u32 — pop value into builder
    pub const FINISH_MSG: u8 = 0x41; // result is the built message
                                     // Extension builtins (builtins_core.rs). Pops the builtin's arity off the
                                     // stack (receiver deepest), pushes the result. An id absent from the build
                                     // fails closed with BadBuiltin.
    pub const CALL: u8 = 0x53; // id:u16 LE
                               // `cel.bind` locals (feature "bindings"): a bounded local-slot file.
                               // STORE pops into slot idx; LOAD pushes a copy. Builds without the
                               // feature reject both opcodes as BadOpcode — fail closed, never skip.
    pub const STORE_LOCAL: u8 = 0x54; // idx:u8
    pub const LOAD_LOCAL: u8 = 0x55; // idx:u8
}

/// Local slots available to `cel.bind` (feature "bindings").
pub const MAX_LOCALS: usize = 8;

/// Deterministic evaluation failures. Never panics on malformed input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvalError {
    Truncated,
    BadOpcode(u8),
    StackOverflow,
    StackUnderflow,
    BadParam(u8),
    NotAMessage,
    CostExceeded,
    BadResultArity,
    TypeError,
    BuildOverflow,
    /// A `CALL` named a builtin this build does not carry (unknown id, or its
    /// extension feature is compiled out).
    BadBuiltin(u16),
    /// `DIV` or `REM` with a zero integer divisor.
    DivByZero,
    /// Integer arithmetic whose result lies outside i64 — refused, never
    /// wrapped into a plausible wrong number.
    Overflow,
    /// A scratch-producing builtin overflowed the caller's arena (or the
    /// caller provided none — `eval_full` without an arena).
    ScratchOverflow,
    /// STORE_LOCAL/LOAD_LOCAL outside `0..MAX_LOCALS`, or LOAD of a slot
    /// never stored (reading garbage is not an option).
    BadLocal(u8),
}

const STACK_CAP: usize = 32;

/// Scalar-only convenience wrapper (used by the Expression module). Programs
/// that construct a message must call `eval_full` with a `Builder`.
pub fn eval<'a>(
    code: &'a [u8],
    params: &'a [Message<'a>],
    max_cost: u64,
) -> Result<Value<'a>, EvalError> {
    let mut scratch = Scratch::new(&mut []);
    eval_scratch(code, params, &mut scratch, max_cost)
}

/// [`eval`] with a scratch arena, for scalar programs that may end in a
/// writing builtin. The result may be `Value::Scratch`; resolve it against
/// `scratch` (still alive, now immutable) via [`resolve_scratch`].
pub fn eval_scratch<'a>(
    code: &'a [u8],
    params: &'a [Message<'a>],
    scratch: &mut Scratch<'_>,
    max_cost: u64,
) -> Result<Value<'a>, EvalError> {
    let mut spent = 0u64;
    eval_scratch_metered(code, params, scratch, max_cost, &mut spent)
}

/// [`eval_scratch`] that also reports the VM instructions spent (work units).
/// `spent` is written whether the program succeeds or fails — the work a failed
/// program did before faulting is still work the step consumed.
pub fn eval_scratch_metered<'a>(
    code: &'a [u8],
    params: &'a [Message<'a>],
    scratch: &mut Scratch<'_>,
    max_cost: u64,
    spent: &mut u64,
) -> Result<Value<'a>, EvalError> {
    match eval_in(code, params, None, scratch, max_cost, spent)? {
        EvalResult::Scalar(v) => Ok(v),
        EvalResult::Constructed => Err(EvalError::BadResultArity),
    }
}

/// Execute `code` against `params`, spending at most `max_cost` steps. A
/// constructed message (via `SET_FIELD`/`FINISH_MSG`) is written into `builder`.
///
/// No scratch arena: every op that needs to WRITE (reverse, case mapping,
/// replace, base64, string `+`, nested message construction) fails closed
/// with `ScratchOverflow`. Subslice
/// and scalar builtins still work. Callers that enable the writing builtins
/// use [`eval_full_scratch`] and resolve `Value::Scratch` results against
/// the arena when serializing.
pub fn eval_full<'a>(
    code: &'a [u8],
    params: &'a [Message<'a>],
    builder: &mut Builder<'a>,
    max_cost: u64,
) -> Result<EvalResult<'a>, EvalError> {
    let mut scratch = Scratch::new(&mut []);
    eval_full_scratch(code, params, builder, &mut scratch, max_cost)
}

/// [`eval_full`] with a caller-owned scratch arena for the writing builtins.
/// Results (the scalar, or builder fields) may be `Value::Scratch` — offsets
/// into `scratch` — which the caller resolves via [`resolve_scratch`] at
/// serialization time, while the arena is still alive and now immutable.
pub fn eval_full_scratch<'a>(
    code: &'a [u8],
    params: &'a [Message<'a>],
    builder: &mut Builder<'a>,
    scratch: &mut Scratch<'_>,
    max_cost: u64,
) -> Result<EvalResult<'a>, EvalError> {
    let mut spent = 0u64;
    eval_full_scratch_metered(code, params, builder, scratch, max_cost, &mut spent)
}

/// [`eval_full_scratch`] that reports VM instructions spent into `spent`.
/// The counter is live on every return path, so a caller reads the true work a
/// record cost even when the program faulted or exceeded its budget.
pub fn eval_full_scratch_metered<'a>(
    code: &'a [u8],
    params: &'a [Message<'a>],
    builder: &mut Builder<'a>,
    scratch: &mut Scratch<'_>,
    max_cost: u64,
    spent: &mut u64,
) -> Result<EvalResult<'a>, EvalError> {
    eval_in(code, params, Some(builder), scratch, max_cost, spent)
}

/// The evaluator. A scalar evaluation passes no builder, so it carries no
/// field table on its stack; a program that constructs is then refused at its
/// first `SET_FIELD` (`BadResultArity`), as it would be at `FINISH_MSG`.
fn eval_in<'a>(
    code: &'a [u8],
    params: &'a [Message<'a>],
    mut builder: Option<&mut Builder<'a>>,
    scratch: &mut Scratch<'_>,
    max_cost: u64,
    spent: &mut u64,
) -> Result<EvalResult<'a>, EvalError> {
    let mut stack: [Value<'a>; STACK_CAP] = [Value::Null; STACK_CAP];
    let mut sp: usize = 0;
    let mut pc: usize = 0;
    *spent = 0;
    #[cfg(feature = "bindings")]
    let mut locals: [Option<Value<'a>>; MAX_LOCALS] = [None; MAX_LOCALS];

    macro_rules! push {
        ($v:expr) => {{
            if sp >= STACK_CAP {
                return Err(EvalError::StackOverflow);
            }
            stack[sp] = $v;
            sp += 1;
        }};
    }
    macro_rules! pop {
        () => {{
            if sp == 0 {
                return Err(EvalError::StackUnderflow);
            }
            sp -= 1;
            stack[sp]
        }};
    }

    loop {
        if pc >= code.len() {
            return Err(EvalError::Truncated);
        }
        *spent += 1;
        if *spent > max_cost {
            return Err(EvalError::CostExceeded);
        }
        let opcode = code[pc];
        pc += 1;

        // Shared value-load and arithmetic ops (one implementation for all VMs).
        if let Some(res) = load_op(opcode, code, &mut pc, params, &mut stack, &mut sp) {
            res?;
            continue;
        }
        if let Some(res) = arith_op(opcode, code, &mut pc, &mut stack, &mut sp) {
            res?;
            continue;
        }

        match opcode {
            op::RET => {
                if sp != 1 {
                    return Err(EvalError::BadResultArity);
                }
                return Ok(EvalResult::Scalar(stack[0]));
            }
            op::FINISH_MSG => return Ok(EvalResult::Constructed),
            op::PUSH_BOOL => {
                let v = *code.get(pc).ok_or(EvalError::Truncated)?;
                pc += 1;
                push!(Value::Bool(v != 0));
            }
            op::CMP_EQ | op::CMP_NE | op::CMP_LT | op::CMP_LE | op::CMP_GT | op::CMP_GE => {
                let b = pop!();
                let a = pop!();
                // A comparison with an ABSENT operand is UNKNOWN, not false and
                // not an error yet: `&&`/`||` may still absorb it (CEL's
                // commutative error semantics), and a predicate left unknown
                // fails its decision closed. A present value of the wrong type
                // stays a TypeError.
                if matches!(a, Value::Null) || matches!(b, Value::Null) {
                    push!(Value::Null);
                    continue;
                }
                let ord = compare(a, b, scratch).ok_or(EvalError::TypeError)?;
                let r = match opcode {
                    op::CMP_EQ => ord == Ordering::Equal,
                    op::CMP_NE => ord != Ordering::Equal,
                    op::CMP_LT => ord == Ordering::Less,
                    op::CMP_LE => ord != Ordering::Greater,
                    op::CMP_GT => ord == Ordering::Greater,
                    _ => ord != Ordering::Less, // CMP_GE
                };
                push!(Value::Bool(r));
            }
            op::AND | op::OR => {
                // Three-valued, as CEL specifies `&&`/`||` over errors: a
                // decisive operand wins whatever the other is (`false && ?` is
                // false, `true || ?` is true, in either order); otherwise an
                // unknown operand leaves the result unknown.
                let b = as_tri(pop!())?;
                let a = as_tri(pop!())?;
                let r = if opcode == op::AND {
                    match (a, b) {
                        (Some(false), _) | (_, Some(false)) => Some(false),
                        (Some(true), Some(true)) => Some(true),
                        _ => None,
                    }
                } else {
                    match (a, b) {
                        (Some(true), _) | (_, Some(true)) => Some(true),
                        (Some(false), Some(false)) => Some(false),
                        _ => None,
                    }
                };
                push!(match r {
                    Some(v) => Value::Bool(v),
                    None => Value::Null,
                });
            }
            op::NOT => {
                let a = as_tri(pop!())?;
                push!(match a {
                    Some(v) => Value::Bool(!v),
                    None => Value::Null,
                });
            }
            op::IS_SET => {
                // `has(x)`: presence, never an error — the one way a predicate
                // asks about absence instead of being undone by it.
                let a = pop!();
                push!(Value::Bool(!matches!(a, Value::Null)));
            }
            op::SET_FIELD => {
                let number = read_u32(code, pc)?;
                pc += 4;
                let value = pop!();
                let Some(builder) = builder.as_deref_mut() else {
                    return Err(EvalError::BadResultArity);
                };
                if builder.len >= MAX_BUILD_FIELDS {
                    return Err(EvalError::BuildOverflow);
                }
                builder.fields[builder.len] = Field { number, value };
                builder.len += 1;
            }
            op::CALL => {
                let id = {
                    let b = code.get(pc..pc + 2).ok_or(EvalError::Truncated)?;
                    u16::from_le_bytes([b[0], b[1]])
                };
                pc += 2;
                let arity = builtin_arity(id).ok_or(EvalError::BadBuiltin(id))?;
                // Each call costs its arity on top of the step, so a builtin
                // is never cheaper than the loads it consumed.
                *spent += arity as u64;
                if sp < arity {
                    return Err(EvalError::StackUnderflow);
                }
                sp -= arity;
                let mut args: [Value<'a>; 3] = [Value::Null; 3];
                let mut i = 0;
                let mut absent = false;
                while i < arity {
                    args[i] = stack[sp + i];
                    absent |= matches!(args[i], Value::Null);
                    i += 1;
                }
                // Absence propagates through a builtin as through an operator:
                // `contains(absent)` is UNKNOWN, which `has(x) && …` absorbs.
                if absent {
                    push!(Value::Null);
                } else {
                    push!(call_builtin(id, &args[..arity], scratch)?);
                }
            }
            op::FRAME_PACK => {
                let n = *code.get(pc).ok_or(EvalError::Truncated)? as usize;
                let nums = code.get(pc + 1..pc + 1 + n).ok_or(EvalError::Truncated)?;
                pc += 1 + n;
                // Charged like a call: one unit per value it consumes.
                *spent += n as u64;
                if sp < n {
                    return Err(EvalError::StackUnderflow);
                }
                sp -= n;
                let vals = stack.get(sp..sp + n).ok_or(EvalError::StackUnderflow)?;
                let v = frame_pack(nums, vals, scratch)?;
                push!(v);
            }
            #[cfg(feature = "bindings")]
            op::STORE_LOCAL => {
                let idx = *code.get(pc).ok_or(EvalError::Truncated)?;
                pc += 1;
                if idx as usize >= MAX_LOCALS {
                    return Err(EvalError::BadLocal(idx));
                }
                locals[idx as usize] = Some(pop!());
            }
            #[cfg(feature = "bindings")]
            op::LOAD_LOCAL => {
                let idx = *code.get(pc).ok_or(EvalError::Truncated)?;
                pc += 1;
                let v = *locals
                    .get(idx as usize)
                    .ok_or(EvalError::BadLocal(idx))?
                    .as_ref()
                    .ok_or(EvalError::BadLocal(idx))?;
                push!(v);
            }
            other => return Err(EvalError::BadOpcode(other)),
        }
    }
}

/// Total ordering across numeric variants; `None` for incomparable types.
#[derive(PartialEq, Eq, Clone, Copy)]
enum Ordering {
    Less,
    Equal,
    Greater,
}

fn compare(a: Value<'_>, b: Value<'_>, scratch: &Scratch<'_>) -> Option<Ordering> {
    // Integer comparison across Int/Uint via i128 widening (CEL's
    // heterogeneous numeric order).
    if let (Some(x), Some(y)) = (as_i128(a), as_i128(b)) {
        return Some(ord_i128(x, y));
    }
    // Strings, byte literals and scratch-backed builtin results compare by
    // their bytes (a string literal is `Value::Bytes` — see `PUSH_STR`;
    // scratch offsets resolve against the arena's frozen prefix).
    fn as_b<'v>(v: Value<'v>, scratch: &'v Scratch<'_>) -> Option<&'v [u8]> {
        match v {
            Value::Str(x) => Some(x.as_bytes()),
            Value::Bytes(x) | Value::Frame(x) => Some(x),
            Value::Scratch { off, len } | Value::ScratchFrame { off, len } => {
                Some(scratch.slice(off, len))
            }
            _ => None,
        }
    }
    let as_b = |v| as_b(v, scratch);
    if let (Some(x), Some(y)) = (as_b(a), as_b(b)) {
        return Some(ord_bytes(x, y));
    }
    match (a, b) {
        (Value::Bool(x), Value::Bool(y)) => Some(ord_i128(x as i128, y as i128)),
        _ => None,
    }
}

fn ord_i128(x: i128, y: i128) -> Ordering {
    if x < y {
        Ordering::Less
    } else if x > y {
        Ordering::Greater
    } else {
        Ordering::Equal
    }
}

fn ord_bytes(x: &[u8], y: &[u8]) -> Ordering {
    let n = if x.len() < y.len() { x.len() } else { y.len() };
    let mut i = 0;
    while i < n {
        if x[i] < y[i] {
            return Ordering::Less;
        }
        if x[i] > y[i] {
            return Ordering::Greater;
        }
        i += 1;
    }
    ord_i128(x.len() as i128, y.len() as i128)
}

fn as_i128(v: Value<'_>) -> Option<i128> {
    match v {
        Value::Int(i) => Some(i as i128),
        Value::Uint(u) => Some(u as i128),
        _ => None,
    }
}

// ---- Shared opcode dispatch (one implementation for the three VMs) ----
//
// `eval_full` (this file), `eval_bytes` (ser_core), and `eval_decode` (deser_core)
// are three stack machines that all honour the same immediate/arithmetic ops, and
// the first two also share the value-load ops. These helpers hold that logic once;
// each VM tries them before its own opcodes. `None` means "not my opcode — the VM
// handles it"; `Some(result)` is the handled outcome. Each advances `pc` and the
// stack exactly as a VM's own arm would, so which VM honours an op is invisible to
// a program.

/// PUSH_I64, ADD, SUB, MUL, DIV, REM — honoured by all three VMs.
#[inline]
fn arith_op<'a>(
    opcode: u8,
    code: &'a [u8],
    pc: &mut usize,
    stack: &mut [Value<'a>],
    sp: &mut usize,
) -> Option<Result<(), EvalError>> {
    macro_rules! push {
        ($v:expr) => {{
            if *sp >= stack.len() {
                return Some(Err(EvalError::StackOverflow));
            }
            stack[*sp] = $v;
            *sp += 1;
        }};
    }
    macro_rules! pop {
        () => {{
            if *sp == 0 {
                return Some(Err(EvalError::StackUnderflow));
            }
            *sp -= 1;
            stack[*sp]
        }};
    }
    match opcode {
        op::PUSH_I64 => {
            let v = match read_i64(code, *pc) {
                Ok(v) => v,
                Err(e) => return Some(Err(e)),
            };
            *pc += 8;
            push!(Value::Int(v));
        }
        op::ADD | op::SUB | op::MUL | op::DIV | op::REM => {
            let bv = pop!();
            let av = pop!();
            // Absence propagates: arithmetic on an absent operand is UNKNOWN,
            // left for `&&`/`||`/`has()` to resolve or a decision to refuse.
            if matches!(av, Value::Null) || matches!(bv, Value::Null) {
                push!(Value::Null);
                return Some(Ok(()));
            }
            let b = match as_arith_int(bv) {
                Ok(v) => v,
                Err(e) => return Some(Err(e)),
            };
            let a = match as_arith_int(av) {
                Ok(v) => v,
                Err(e) => return Some(Err(e)),
            };
            if (opcode == op::DIV || opcode == op::REM) && b == 0 {
                return Some(Err(EvalError::DivByZero));
            }
            // Checked, so no panic path is linked: a zero divisor is refused
            // above, and `MIN / -1` is the one quotient past i64::MAX. `REM`
            // cannot leave i64 (`MIN % -1` is 0).
            let r = match opcode {
                op::ADD => a.checked_add(b),
                op::SUB => a.checked_sub(b),
                op::DIV => a.checked_div(b),
                op::REM if b == -1 => Some(0),
                op::REM => a.checked_rem(b),
                _ => a.checked_mul(b), // MUL
            };
            match r {
                Some(r) => push!(Value::Int(r)),
                None => return Some(Err(EvalError::Overflow)),
            }
        }
        _ => return None,
    }
    Some(Ok(()))
}

/// LOAD_PARAM, GET_FIELD, PUSH_STR — the value-load ops honoured by the evaluator
/// and the byte-serializer (the deserializer has no params and does not use this).
#[inline]
fn load_op<'a>(
    opcode: u8,
    code: &'a [u8],
    pc: &mut usize,
    params: &'a [Message<'a>],
    stack: &mut [Value<'a>],
    sp: &mut usize,
) -> Option<Result<(), EvalError>> {
    macro_rules! push {
        ($v:expr) => {{
            if *sp >= stack.len() {
                return Some(Err(EvalError::StackOverflow));
            }
            stack[*sp] = $v;
            *sp += 1;
        }};
    }
    macro_rules! pop {
        () => {{
            if *sp == 0 {
                return Some(Err(EvalError::StackUnderflow));
            }
            *sp -= 1;
            stack[*sp]
        }};
    }
    match opcode {
        op::LOAD_PARAM => {
            let idx = match code.get(*pc) {
                Some(&i) => i,
                None => return Some(Err(EvalError::Truncated)),
            };
            *pc += 1;
            let msg = match params.get(idx as usize) {
                Some(m) => m,
                None => return Some(Err(EvalError::BadParam(idx))),
            };
            push!(Value::Msg(msg));
        }
        op::GET_FIELD => {
            let number = match read_u32(code, *pc) {
                Ok(n) => n,
                Err(e) => return Some(Err(e)),
            };
            *pc += 4;
            match pop!() {
                Value::Msg(m) => push!(m.get(number)),
                // A nested message crosses a frame boundary as its own FRAME,
                // typed as one (3), so selecting into one reads the frame. A
                // byte string is not a message, whatever its bytes look like.
                Value::Frame(f) => match frame_get(f, number) {
                    Ok(v) => push!(v),
                    Err(e) => return Some(Err(e)),
                },
                // A member of an ABSENT nested message is absent too — the
                // path propagates absence as every operator does, so
                // `r.ctx.x` on a record with no `ctx` is unknown, not an error.
                Value::Null => push!(Value::Null),
                _ => return Some(Err(EvalError::NotAMessage)),
            }
        }
        op::PUSH_STR => {
            let b = match code.get(*pc..*pc + 2) {
                Some(b) => b,
                None => return Some(Err(EvalError::Truncated)),
            };
            let len = u16::from_le_bytes([b[0], b[1]]) as usize;
            *pc += 2;
            let bytes = match code.get(*pc..*pc + len) {
                Some(b) => b,
                None => return Some(Err(EvalError::Truncated)),
            };
            *pc += len;
            push!(Value::Bytes(bytes));
        }
        _ => return None,
    }
    Some(Ok(()))
}

/// Load-time program scan: walk `code` opcode-by-opcode and reject anything
/// this BUILD cannot run — an unknown opcode, a truncated operand, a `CALL`
/// to a builtin that is unpinned or compiled out, or a `cel.bind` local op
/// on a bindings-less build. Engines run this on every program at init and
/// reload so a broken or over-featured program is refused ONCE, loudly, at
/// load — never per-record, never silently (the inert-node failure class).
///
/// Same fixed operand widths the evaluator uses; scanning is O(len) and
/// allocation-free.
pub fn scan_code(code: &[u8]) -> Result<(), EvalError> {
    let mut pc = 0usize;
    while pc < code.len() {
        let opcode = code[pc];
        pc += 1;
        let operand = match opcode {
            op::RET
            | op::FINISH_MSG
            | op::CMP_EQ
            | op::CMP_NE
            | op::CMP_LT
            | op::CMP_LE
            | op::CMP_GT
            | op::CMP_GE
            | op::AND
            | op::OR
            | op::NOT
            | op::IS_SET
            | op::ADD
            | op::SUB
            | op::MUL
            | op::DIV
            | op::REM => 0,
            op::LOAD_PARAM | op::PUSH_BOOL => 1,
            op::FRAME_PACK => 1 + *code.get(pc).ok_or(EvalError::Truncated)? as usize,
            op::GET_FIELD | op::SET_FIELD => 4,
            op::PUSH_I64 => 8,
            op::PUSH_STR => {
                let b = code.get(pc..pc + 2).ok_or(EvalError::Truncated)?;
                2 + u16::from_le_bytes([b[0], b[1]]) as usize
            }
            op::CALL => {
                let b = code.get(pc..pc + 2).ok_or(EvalError::Truncated)?;
                let id = u16::from_le_bytes([b[0], b[1]]);
                if !builtin_available(id) {
                    return Err(EvalError::BadBuiltin(id));
                }
                2
            }
            op::STORE_LOCAL | op::LOAD_LOCAL => {
                if !BINDINGS_AVAILABLE {
                    return Err(EvalError::BadOpcode(opcode));
                }
                let idx = *code.get(pc).ok_or(EvalError::Truncated)?;
                if idx as usize >= MAX_LOCALS {
                    return Err(EvalError::BadLocal(idx));
                }
                1
            }
            other => return Err(EvalError::BadOpcode(other)),
        };
        if pc + operand > code.len() {
            return Err(EvalError::Truncated);
        }
        pc += operand;
    }
    Ok(())
}

/// An integer operand as an `i64` VALUE: a `Uint` above `i64::MAX` has none,
/// so it is `Overflow` rather than a wrapped negative number.
fn as_arith_int(v: Value<'_>) -> Result<i64, EvalError> {
    match v {
        Value::Int(i) => Ok(i),
        Value::Uint(u) => i64::try_from(u).map_err(|_| EvalError::Overflow),
        _ => Err(EvalError::TypeError),
    }
}

/// Field `number` of a record frame (`[count][number][type][len u16][payload]…`),
/// typed as `decode_frame` types it; absent is `Null`, and an empty frame is a
/// message with no fields. A frame that does not frame is `NotAMessage` — the
/// value selected into was not a nested message after all.
fn frame_get(f: &[u8], number: u32) -> Result<Value<'_>, EvalError> {
    if f.is_empty() {
        return Ok(Value::Null);
    }
    let count = *f.first().ok_or(EvalError::NotAMessage)? as usize;
    let mut p = 1usize;
    let mut i = 0usize;
    while i < count {
        let h = f.get(p..p + 4).ok_or(EvalError::NotAMessage)?;
        let (num, ty) = (h.first().copied(), h.get(1).copied());
        let len = match (h.get(2), h.get(3)) {
            (Some(&a), Some(&b)) => u16::from_le_bytes([a, b]) as usize,
            _ => return Err(EvalError::NotAMessage),
        };
        let body = f.get(p + 4..p + 4 + len).ok_or(EvalError::NotAMessage)?;
        if num.map(u32::from) == Some(number) {
            return match ty {
                Some(1) => match <[u8; 8]>::try_from(body) {
                    Ok(b) => Ok(Value::Int(i64::from_le_bytes(b))),
                    Err(_) => Err(EvalError::NotAMessage),
                },
                Some(0) => Ok(Value::Bytes(body)),
                Some(3) => Ok(Value::Frame(body)),
                _ => Err(EvalError::NotAMessage),
            };
        }
        p += 4 + len;
        i += 1;
    }
    if p != f.len() {
        return Err(EvalError::NotAMessage);
    }
    Ok(Value::Null)
}

/// Copy `src` into `buf` at `at` — bounds-checked, no panic path (a PIC
/// module links none).
fn frame_put(buf: &mut [u8], at: usize, src: &[u8]) -> Result<(), EvalError> {
    let dst = buf
        .get_mut(at..at + src.len())
        .ok_or(EvalError::ScratchOverflow)?;
    for (d, s) in dst.iter_mut().zip(src.iter()) {
        *d = *s;
    }
    Ok(())
}

/// Encode `vals` under `nums` as a record frame in the scratch arena — the
/// wire form a nested message takes, returned as a [`Value::ScratchFrame`].
/// Absent values are left out; a nested message packs as type 3; a value with
/// no frame representation (a decoded message) is a `TypeError`, and a `Uint`
/// above `i64::MAX` (the frame's integer is an i64) is `Overflow`.
fn frame_pack<'a>(
    nums: &[u8],
    vals: &[Value<'a>],
    scratch: &mut Scratch<'_>,
) -> Result<Value<'a>, EvalError> {
    // Size first, so the frame is reserved once and written in place.
    let mut size = 1usize;
    let mut count = 0usize;
    for v in vals {
        size += 4 + match *v {
            Value::Null => continue,
            Value::Int(_) | Value::Uint(_) | Value::Bool(_) => 8,
            Value::Bytes(b) | Value::Frame(b) => b.len(),
            Value::Str(s) => s.len(),
            Value::Scratch { len, .. } | Value::ScratchFrame { len, .. } => len as usize,
            _ => return Err(EvalError::TypeError),
        };
        count += 1;
    }
    if count > u8::MAX as usize {
        return Err(EvalError::BuildOverflow);
    }
    let off = scratch.reserve(size)?;
    frame_put(scratch.buf, off, &[count as u8])?;
    let mut w = off + 1;
    for (k, v) in vals.iter().enumerate() {
        let num = *nums.get(k).ok_or(EvalError::Truncated)?;
        let (ty, len) = match *v {
            Value::Null => continue,
            Value::Int(_) | Value::Uint(_) | Value::Bool(_) => (1u8, 8usize),
            Value::Bytes(b) => (0, b.len()),
            Value::Frame(b) => (3, b.len()),
            Value::Str(s) => (0, s.len()),
            Value::Scratch { len, .. } => (0, len as usize),
            Value::ScratchFrame { len, .. } => (3, len as usize),
            _ => return Err(EvalError::TypeError),
        };
        if len > u16::MAX as usize {
            return Err(EvalError::BuildOverflow);
        }
        let l = (len as u16).to_le_bytes();
        frame_put(scratch.buf, w, &[num, ty, l[0], l[1]])?;
        w += 4;
        match *v {
            Value::Int(i) => frame_put(scratch.buf, w, &i.to_le_bytes())?,
            Value::Uint(u) => {
                let i = i64::try_from(u).map_err(|_| EvalError::Overflow)?;
                frame_put(scratch.buf, w, &i.to_le_bytes())?
            }
            Value::Bool(b) => frame_put(scratch.buf, w, &(b as i64).to_le_bytes())?,
            Value::Bytes(b) | Value::Frame(b) => frame_put(scratch.buf, w, b)?,
            Value::Str(s) => frame_put(scratch.buf, w, s.as_bytes())?,
            // An earlier builtin's result lives in this arena, below `off`:
            // copied byte by byte, since source and destination share it.
            Value::Scratch { off: so, .. } | Value::ScratchFrame { off: so, .. } => {
                for i in 0..len {
                    let b = *scratch
                        .buf
                        .get(so as usize + i)
                        .ok_or(EvalError::ScratchOverflow)?;
                    *scratch
                        .buf
                        .get_mut(w + i)
                        .ok_or(EvalError::ScratchOverflow)? = b;
                }
            }
            _ => {}
        }
        w += len;
    }
    Ok(Value::ScratchFrame {
        off: off as u32,
        len: size as u32,
    })
}

/// A logic operand: `Some(bool)`, or `None` for UNKNOWN (an absent value, or a
/// comparison that involved one). Anything else is a TypeError.
fn as_tri(v: Value<'_>) -> Result<Option<bool>, EvalError> {
    match v {
        Value::Bool(b) => Ok(Some(b)),
        Value::Null => Ok(None),
        _ => Err(EvalError::TypeError),
    }
}

fn read_u32(code: &[u8], at: usize) -> Result<u32, EvalError> {
    let b = code.get(at..at + 4).ok_or(EvalError::Truncated)?;
    Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
}

fn read_i64(code: &[u8], at: usize) -> Result<i64, EvalError> {
    let b = code.get(at..at + 8).ok_or(EvalError::Truncated)?;
    Ok(i64::from_le_bytes([
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
    ]))
}
