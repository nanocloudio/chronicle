// Bounded, no_std, no-alloc DECISION driver — a first-hit policy over compiled
// predicate + outcome bytecode. `include!`d by the `decision` and `pipeline`
// modules, the `chronicle` CLI and the test harness, so all of them run the same
// walk.
//
// The VM has no branching opcode: a single bytecode program constructs exactly
// one message and cannot SELECT among several. A decision — "first rule whose
// predicate holds constructs its outcome, else the default" — is therefore not
// one VM program; it is a driver that orchestrates the existing evaluator over
// several sub-programs. The driver runs either in the `decision` node or inline
// as a `STAGE_KIND_DECISION` pipeline stage.
//
// Serialized container (what a config ships, hex-encoded):
//   [nrules:u8]
//   per rule: <when prog> <outcome prog>
//   <default prog>
// where each prog is `[cost:u32 LE][len:u16 LE][code bytes]` — the same encoding
// pipeline stages use. `when` must evaluate to Bool; each outcome (and the
// default) must construct a message.

/// A branch of a decision: one of its rules, or the default. A run that
/// succeeds returns the branch that fired; one that fails names the branch it
/// could not decide ([`Undecided`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Branch {
    /// Rule `index` (0-based, in declared order).
    Rule(u8),
    /// The default outcome, reached because no rule matched.
    Default,
}

/// Deterministic decision failures. Never panics on malformed input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecisionError {
    /// The container ended mid-header or mid-program.
    Truncated,
    /// A `when` predicate produced a non-boolean value.
    NotBool,
    /// An outcome program returned a scalar instead of constructing a message.
    BadArity,
    /// The evaluator faulted (bad opcode, cost ceiling, type error, …).
    Eval(EvalError),
    /// A `when` predicate read an absent field and nothing absorbed it
    /// (`false && …`, `true || …`, or `has()`): its outcome is UNKNOWN. The
    /// decision fails closed — no rule and no default fires, because the rule
    /// that could not be evaluated may have been a refusal.
    Absent,
}

/// A run that decided nothing: the branch it stopped at, and why.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Undecided {
    pub at: Branch,
    pub error: DecisionError,
}

/// Say why a run failed, in a few words, through `put`. One call per arm: a
/// match yielding the strings would lower to a table of absolute pointers,
/// which a PIC module cannot hold.
pub fn describe_decision_error(e: DecisionError, put: &mut impl FnMut(&[u8])) {
    match e {
        DecisionError::Truncated => put(b"truncated container"),
        DecisionError::NotBool => put(b"a when predicate was not boolean"),
        DecisionError::BadArity => put(b"an outcome did not construct a message"),
        DecisionError::Absent => put(b"a predicate read an absent field (unknown)"),
        DecisionError::Eval(ev) => match ev {
            EvalError::Truncated => put(b"truncated program"),
            EvalError::BadOpcode(_) => put(b"bad opcode"),
            EvalError::StackOverflow => put(b"stack overflow"),
            EvalError::StackUnderflow => put(b"stack underflow"),
            EvalError::BadParam(_) => put(b"bad parameter index"),
            EvalError::NotAMessage => put(b"selected into a non-message"),
            EvalError::CostExceeded => put(b"cost ceiling exceeded"),
            EvalError::BadResultArity => put(b"bad result arity"),
            EvalError::TypeError => put(b"type error"),
            EvalError::BuildOverflow => put(b"too many constructed fields"),
            EvalError::BadBuiltin(_) => put(b"builtin not in this build"),
            EvalError::DivByZero => put(b"division by zero"),
            EvalError::Overflow => put(b"integer overflow"),
            EvalError::ScratchOverflow => put(b"scratch arena overflow"),
            EvalError::BadLocal(_) => put(b"bad cel.bind local slot"),
        },
    }
}

/// Read one `[cost:u32 LE][len:u16 LE][code]` program at `container[*off..]`,
/// advancing `*off` past it. Returns `(max_cost, code)`. Bounds-checked.
fn dec_read_prog<'a>(
    container: &'a [u8],
    off: &mut usize,
) -> Result<(u64, &'a [u8]), DecisionError> {
    let hdr = container
        .get(*off..*off + 6)
        .ok_or(DecisionError::Truncated)?;
    let cost = u32::from_le_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as u64;
    let len = u16::from_le_bytes([hdr[4], hdr[5]]) as usize;
    let start = *off + 6;
    let code = container
        .get(start..start + len)
        .ok_or(DecisionError::Truncated)?;
    *off = start + len;
    Ok((cost, code))
}

/// Load-time validation of a decision container: every rule's `when` and
/// `outcome`, plus the default, must parse and pass [`scan_code`]. Same
/// refuse-at-load contract as `scan_stage_container`.
pub fn scan_decision_container(container: &[u8]) -> Result<(), DecisionError> {
    let nrules = *container.first().ok_or(DecisionError::Truncated)? as usize;
    let mut off = 1usize;
    let mut i = 0usize;
    while i < nrules {
        let (_, when_code) = dec_read_prog(container, &mut off)?;
        let (_, out_code) = dec_read_prog(container, &mut off)?;
        scan_code(when_code).map_err(DecisionError::Eval)?;
        scan_code(out_code).map_err(DecisionError::Eval)?;
        i += 1;
    }
    let (_, def_code) = dec_read_prog(container, &mut off)?;
    scan_code(def_code).map_err(DecisionError::Eval)?;
    Ok(())
}

/// Run a serialized first-hit decision against `params`, constructing the selected
/// outcome into `builder`. Returns which branch fired. Deterministic: the first
/// rule (in order) whose `when` is `true` wins; otherwise the default. Pure,
/// allocation-free, and panic-free — a malformed container or a bad program is a
/// structured error, never a panic.
pub fn run_decision<'a>(
    container: &'a [u8],
    params: &'a [Message<'a>],
    builder: &mut Builder<'a>,
) -> Result<Branch, DecisionError> {
    let mut scratch = Scratch::new(&mut []);
    run_decision_scratch(container, params, builder, &mut scratch)
}

/// [`run_decision`] with a scratch arena so predicates and outcomes may use
/// the writing builtins. Outcome fields may be `Value::Scratch` — serialize
/// them against the SAME arena (`encode_frame_scratch`).
pub fn run_decision_scratch<'a>(
    container: &'a [u8],
    params: &'a [Message<'a>],
    builder: &mut Builder<'a>,
    scratch: &mut Scratch<'_>,
) -> Result<Branch, DecisionError> {
    let mut spent = 0u64;
    run_decision_metered(container, params, builder, scratch, &mut spent).map_err(|u| u.error)
}

/// [`run_decision_scratch`] that also reports the work it did, and on failure
/// the branch it stopped at. `spent` is the VM instructions across every
/// predicate evaluated and the constructed outcome, charged whether the run
/// matches, falls to the default, or fails.
pub fn run_decision_metered<'a>(
    container: &'a [u8],
    params: &'a [Message<'a>],
    builder: &mut Builder<'a>,
    scratch: &mut Scratch<'_>,
    spent: &mut u64,
) -> Result<Branch, Undecided> {
    *spent = 0;
    let mut at = Branch::Rule(0);
    walk(container, params, builder, scratch, spent, &mut at)
        .map_err(|error| Undecided { at, error })
}

/// The first-hit walk. `at` tracks the branch being decided, so a failure can
/// say where it happened.
fn walk<'a>(
    container: &'a [u8],
    params: &'a [Message<'a>],
    builder: &mut Builder<'a>,
    scratch: &mut Scratch<'_>,
    spent: &mut u64,
    at: &mut Branch,
) -> Result<Branch, DecisionError> {
    let nrules = *container.first().ok_or(DecisionError::Truncated)?;
    let mut off = 1usize;
    let mut i = 0u8;
    while i < nrules {
        *at = Branch::Rule(i);
        let (when_cost, when_code) = dec_read_prog(container, &mut off)?;
        let (out_cost, out_code) = dec_read_prog(container, &mut off)?;
        let mut w = 0u64;
        let r = eval_scratch_metered(when_code, params, scratch, when_cost, &mut w);
        // Charged before any outcome is inspected: a predicate that faults
        // (cost ceiling, type error) spent its instructions all the same.
        *spent += w;
        let matched = match r.map_err(DecisionError::Eval)? {
            Value::Bool(b) => b,
            Value::Null => return Err(DecisionError::Absent),
            _ => return Err(DecisionError::NotBool),
        };
        if matched {
            construct(out_code, params, builder, scratch, out_cost, spent)?;
            return Ok(Branch::Rule(i));
        }
        i += 1;
    }
    *at = Branch::Default;
    let (def_cost, def_code) = dec_read_prog(container, &mut off)?;
    construct(def_code, params, builder, scratch, def_cost, spent)?;
    Ok(Branch::Default)
}

/// Evaluate a message-constructing program into `builder`, resetting it first,
/// and add its VM instructions to `spent`.
fn construct<'a>(
    code: &'a [u8],
    params: &'a [Message<'a>],
    builder: &mut Builder<'a>,
    scratch: &mut Scratch<'_>,
    max_cost: u64,
    spent: &mut u64,
) -> Result<(), DecisionError> {
    builder.len = 0;
    let mut w = 0u64;
    let r = eval_full_scratch_metered(code, params, builder, scratch, max_cost, &mut w)
        .map_err(DecisionError::Eval);
    *spent += w;
    match r? {
        EvalResult::Constructed => Ok(()),
        EvalResult::Scalar(_) => Err(DecisionError::BadArity),
    }
}
