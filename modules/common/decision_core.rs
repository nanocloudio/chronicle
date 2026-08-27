// Bounded, no_std, no-alloc DECISION driver (spec artefact 4) — a first-hit
// policy over compiled predicate + outcome bytecode. `include!`d by the host
// crate (tests) and the `decision` .fmod, so device and host run identical logic.
//
// The VM has no branching opcode: a single bytecode program constructs exactly
// one message and cannot SELECT among several. A decision — "first rule whose
// predicate holds constructs its outcome, else the default" — is therefore not a
// pipeline bytecode stage; it is its own driver that orchestrates the existing
// evaluator over several sub-programs. (Same reason aggregation, which needs
// state, is its own module: the node boundary is where the VM's straight-line
// model runs out.)
//
// Serialized container (what a config ships, hex-encoded):
//   [nrules:u8]
//   per rule: <when prog> <outcome prog>
//   <default prog>
// where each prog is `[cost:u32 LE][len:u16 LE][code bytes]` — the same encoding
// pipeline stages use. `when` must evaluate to Bool; each outcome (and the
// default) must construct a message.

/// Which branch produced the outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fired {
    /// Rule `index` (0-based, in declared order) matched.
    Rule(u8),
    /// No rule matched; the default outcome was constructed.
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
) -> Result<Fired, DecisionError> {
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
) -> Result<Fired, DecisionError> {
    let nrules = *container.first().ok_or(DecisionError::Truncated)? as usize;
    let mut off = 1usize;
    let mut i = 0usize;
    while i < nrules {
        let (when_cost, when_code) = dec_read_prog(container, &mut off)?;
        let (out_cost, out_code) = dec_read_prog(container, &mut off)?;
        let matched = match eval_scratch(when_code, params, scratch, when_cost)
            .map_err(DecisionError::Eval)?
        {
            Value::Bool(b) => b,
            _ => return Err(DecisionError::NotBool),
        };
        if matched {
            construct(out_code, params, builder, scratch, out_cost)?;
            return Ok(Fired::Rule(i as u8));
        }
        i += 1;
    }
    // No rule matched — construct the default outcome.
    let (def_cost, def_code) = dec_read_prog(container, &mut off)?;
    construct(def_code, params, builder, scratch, def_cost)?;
    Ok(Fired::Default)
}

/// [`run_decision_scratch`] that also reports the VM instructions spent across
/// every predicate evaluated and the constructed outcome (work units).
/// `spent` accumulates whether the policy matches a rule or falls to the default.
pub fn run_decision_scratch_metered<'a>(
    container: &'a [u8],
    params: &'a [Message<'a>],
    builder: &mut Builder<'a>,
    scratch: &mut Scratch<'_>,
    spent: &mut u64,
) -> Result<Fired, DecisionError> {
    *spent = 0;
    let nrules = *container.first().ok_or(DecisionError::Truncated)? as usize;
    let mut off = 1usize;
    let mut i = 0usize;
    while i < nrules {
        let (when_cost, when_code) = dec_read_prog(container, &mut off)?;
        let (out_cost, out_code) = dec_read_prog(container, &mut off)?;
        let mut w = 0u64;
        let matched = match eval_scratch_metered(when_code, params, scratch, when_cost, &mut w)
            .map_err(DecisionError::Eval)?
        {
            Value::Bool(b) => b,
            _ => {
                *spent += w;
                return Err(DecisionError::NotBool);
            }
        };
        *spent += w;
        if matched {
            construct_metered(out_code, params, builder, scratch, out_cost, spent)?;
            return Ok(Fired::Rule(i as u8));
        }
        i += 1;
    }
    let (def_cost, def_code) = dec_read_prog(container, &mut off)?;
    construct_metered(def_code, params, builder, scratch, def_cost, spent)?;
    Ok(Fired::Default)
}

/// [`construct`] that adds the outcome program's VM instructions to `spent`.
fn construct_metered<'a>(
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

/// Evaluate a message-constructing program into `builder`, resetting it first.
fn construct<'a>(
    code: &'a [u8],
    params: &'a [Message<'a>],
    builder: &mut Builder<'a>,
    scratch: &mut Scratch<'_>,
    max_cost: u64,
) -> Result<(), DecisionError> {
    builder.len = 0;
    match eval_full_scratch(code, params, builder, scratch, max_cost)
        .map_err(DecisionError::Eval)?
    {
        EvalResult::Constructed => Ok(()),
        EvalResult::Scalar(_) => Err(DecisionError::BadArity),
    }
}
