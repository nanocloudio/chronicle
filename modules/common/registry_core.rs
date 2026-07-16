// The dynamic (server-side) compile path, on device — type-checking source
// handed over at RUNTIME rather than compiled from a document at build time.
//
// The build-time flow gets its type environment from a `.uproc` document. This
// is the other entry point: a node holds a SCHEMA and type-checks source given
// to it, emitting the same shippable checked IR the modules lower at load. A
// source string checked here becomes a self-validating artefact there — the
// target proves it can run it by lowering it, with no opaque bytecode and no
// out-of-band agreement on the opcode set.
//
// The "registry" is the schema TEXT (`Name{field:ty@N,...};…;ENUM=n`) that
// `celc_core` already compiles against and `uproc_lower_core` already builds. A
// second in-memory catalog would be a second source of truth for the same
// environment, so there isn't one — registering a schema means holding that
// text, and this layer is the orchestration around it: bind the parameter, run
// the compiler, check the result type, pack the container.
//
// The type checks here are the point, not incidental. A pipeline stage that does
// not construct a message, or a decision predicate that is not Bool, would pack
// into a perfectly well-formed container and fail somewhere inside the VM at
// runtime. Rejecting them at compile time is what makes "it compiled" mean
// "it can run".
//
// Requires `celc_core` (the compiler), `lower_core` (IR -> bytecode for the
// decision container), `pack_core` (the containers) and `uproc_lower_core`
// (the parameter binding text).

/// Why a dynamic compile was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegError {
    /// The source did not type-check.
    Compile(CelcErr),
    /// Lowering the checked IR to bytecode failed.
    Lower(LowerError),
    /// Packing the result into its container failed.
    Pack(PackError),
    /// A stage or outcome did not construct a message.
    NotAMessage,
    /// A decision predicate did not type-check to Bool.
    NotBool,
    /// A scratch or output buffer was too small.
    Capacity,
    /// More stages or rules than the container's one-byte count allows.
    TooManyItems,
}

/// One dynamically-compiled stage: its source, its single parameter name, and
/// that parameter's message type (the previous stage's output).
#[derive(Clone, Copy)]
pub struct StageSource<'a> {
    pub source: &'a [u8],
    pub param_name: &'a [u8],
    pub input_type: &'a [u8],
}

/// One decision rule: a `when` predicate and the outcome it selects.
#[derive(Clone, Copy)]
pub struct RuleSource<'a> {
    pub when: &'a [u8],
    pub outcome: &'a [u8],
}

/// Build the `name:Type` parameter binding the compiler wants.
///
/// `uproc_params_text` does this for the build-time path, but it binds SPANS
/// into a document buffer; the dynamic path has free-standing slices. The type
/// name still goes through `schema_type_name`, so the one translation that
/// matters (`string` -> `str`) has a single owner. Skipping it would not fail
/// loudly: an unknown type name reads as a MESSAGE name, so `string` would
/// silently become "a message called string".
fn bind(param_name: &[u8], input_type: &[u8], out: &mut [u8]) -> Result<usize, RegError> {
    let mut p = put_bytes(out, 0, param_name)?;
    p = put_bytes(out, p, b":")?;
    put_bytes(out, p, schema_type_name(input_type))
}

fn put_bytes(out: &mut [u8], p: usize, s: &[u8]) -> Result<usize, RegError> {
    if p + s.len() > out.len() {
        return Err(RegError::Capacity);
    }
    out[p..p + s.len()].copy_from_slice(s);
    Ok(p + s.len())
}

/// Type-check `source` against `schema` and write the shippable flat checked IR.
///
/// All schema access happens here; lowering the result on the target needs none.
/// Returns the IR length and whether the result constructs a message.
pub fn compile_ir(
    schema: &[u8],
    source: &[u8],
    param_name: &[u8],
    input_type: &[u8],
    out: &mut [u8],
) -> Result<(usize, bool), RegError> {
    let mut params = [0u8; 128];
    let pn = bind(param_name, input_type, &mut params)?;
    let (n, ty) = celc_compile_ty(schema, &params[..pn], source, out).map_err(RegError::Compile)?;
    let is_msg = matches!(ty, CTy::Msg(..));
    if is_msg {
        return Ok((n, true));
    }
    // The host `compile`/`encode_ir` convention: a non-message result carries an
    // explicit RET terminator. Omitting it would leave the lowerer reading past
    // the program.
    if n >= out.len() {
        return Err(RegError::Capacity);
    }
    out[n] = ir::RET;
    Ok((n + 1, false))
}

/// [`compile_ir`], hex-encoded for a module's `ir` param.
pub fn compile_ir_param(
    schema: &[u8],
    source: &[u8],
    param_name: &[u8],
    input_type: &[u8],
    out: &mut [u8],
    scratch: &mut [u8],
) -> Result<usize, RegError> {
    let (n, _) = compile_ir(schema, source, param_name, input_type, scratch)?;
    hex_encode(scratch.get(..n).ok_or(RegError::Capacity)?, out).ok_or(RegError::Capacity)
}

/// The most stages or rules a dynamic compile will accept — the containers carry
/// a one-byte count, and the per-item spans are held on the stack.
pub const MAX_ITEMS: usize = 32;

/// Compile a sequence of stage sources into the `ir_stages` container the
/// pipeline module lowers at load.
///
/// Each stage MUST construct a message: the pipeline executor hands one stage's
/// output frame to the next, so a scalar-returning stage has nothing to pass on.
/// The caller wires stage N's output type to stage N+1's `input_type`.
///
/// `scratch` holds the compiled IR for every stage end to end.
pub fn compile_pipeline_ir(
    schema: &[u8],
    stages: &[StageSource],
    out: &mut [u8],
    scratch: &mut [u8],
) -> Result<usize, RegError> {
    if stages.len() > MAX_ITEMS {
        return Err(RegError::TooManyItems);
    }
    let mut spans = [(0usize, 0usize); MAX_ITEMS];
    let mut used = 0usize;
    for (i, st) in stages.iter().enumerate() {
        let (n, is_msg) = compile_ir(
            schema,
            st.source,
            st.param_name,
            st.input_type,
            scratch.get_mut(used..).ok_or(RegError::Capacity)?,
        )?;
        if !is_msg {
            return Err(RegError::NotAMessage);
        }
        spans[i] = (used, n);
        used += n;
    }
    let mut refs = [&[] as &[u8]; MAX_ITEMS];
    for i in 0..stages.len() {
        let (off, len) = spans[i];
        refs[i] = scratch.get(off..off + len).ok_or(RegError::Capacity)?;
    }
    pack_ir_stages(out, &refs[..stages.len()], &[]).map_err(RegError::Pack)
}

/// [`compile_pipeline_ir`], hex-encoded for the module's `ir_stages` param.
pub fn compile_pipeline_ir_param(
    schema: &[u8],
    stages: &[StageSource],
    out: &mut [u8],
    container: &mut [u8],
    scratch: &mut [u8],
) -> Result<usize, RegError> {
    let n = compile_pipeline_ir(schema, stages, container, scratch)?;
    hex_encode(container.get(..n).ok_or(RegError::Capacity)?, out).ok_or(RegError::Capacity)
}

/// Compile one source to BYTECODE, returning `(offset, len, cost, is_message)`.
///
/// The decision module runs bytecode rather than lowering IR at load, so each
/// sub-program is lowered here. The cost bound comes from the lowerer, not from
/// the caller — it is re-derived, never trusted.
fn compile_bytecode(
    schema: &[u8],
    source: &[u8],
    param_name: &[u8],
    input_type: &[u8],
    scratch: &mut [u8],
    used: &mut usize,
) -> Result<(usize, usize, u64, bool), RegError> {
    // The IR is built at the far end of the free space and lowered back into the
    // near end, so only the bytecode is retained.
    //
    // Every index here is checked rather than sliced directly: this core is
    // `include!`d into `.fmod` modules built with no panic machinery, so a
    // bounds panic is not a runtime abort, it is an UNDEFINED SYMBOL at link
    // time. Arithmetic is checked for the same reason — `scratch.len() - used`
    // would underflow rather than report a full buffer.
    let free = scratch.len().checked_sub(*used).ok_or(RegError::Capacity)?;
    if free < 64 {
        return Err(RegError::Capacity);
    }
    let split = *used + free / 2;
    if split > scratch.len() {
        return Err(RegError::Capacity);
    }
    let (lo, hi) = scratch.split_at_mut(split);

    let (irn, is_msg) = compile_ir(schema, source, param_name, input_type, hi)?;
    let ir = hi.get(..irn).ok_or(RegError::Capacity)?;
    let dst = lo.get_mut(*used..).ok_or(RegError::Capacity)?;
    let (bn, cost) = lower_flat(ir, dst).map_err(RegError::Lower)?;
    let off = *used;
    *used += bn;
    Ok((off, bn, cost, is_msg))
}

/// Compile a Decision — ordered `(when, outcome)` sources plus a default — into
/// the container the decision module runs.
///
/// Each `when` must type-check to Bool and each outcome must construct a
/// message. A non-Bool predicate would be truthy-tested by the VM against a
/// value that has no truth, and a scalar outcome would emit no frame at all.
pub fn compile_decision(
    schema: &[u8],
    param_name: &[u8],
    input_type: &[u8],
    rules: &[RuleSource],
    default: &[u8],
    out: &mut [u8],
    scratch: &mut [u8],
) -> Result<usize, RegError> {
    if rules.len() > MAX_ITEMS {
        return Err(RegError::TooManyItems);
    }
    let mut used = 0usize;
    let mut wspans = [(0usize, 0usize, 0u64); MAX_ITEMS];
    let mut ospans = [(0usize, 0usize, 0u64); MAX_ITEMS];

    for (i, r) in rules.iter().enumerate() {
        let (woff, wlen, wcost, w_is_msg) =
            compile_bytecode(schema, r.when, param_name, input_type, scratch, &mut used)?;
        if w_is_msg {
            return Err(RegError::NotBool);
        }
        wspans[i] = (woff, wlen, wcost);

        let (ooff, olen, ocost, o_is_msg) = compile_bytecode(
            schema, r.outcome, param_name, input_type, scratch, &mut used,
        )?;
        if !o_is_msg {
            return Err(RegError::NotAMessage);
        }
        ospans[i] = (ooff, olen, ocost);
    }
    let (doff, dlen, dcost, d_is_msg) =
        compile_bytecode(schema, default, param_name, input_type, scratch, &mut used)?;
    if !d_is_msg {
        return Err(RegError::NotAMessage);
    }

    let mut packed = [(Prog { cost: 0, code: &[] }, Prog { cost: 0, code: &[] }); MAX_ITEMS];
    for i in 0..rules.len() {
        let (wo, wl, wc) = wspans[i];
        let (oo, ol, oc) = ospans[i];
        packed[i] = (
            Prog {
                cost: wc,
                code: scratch.get(wo..wo + wl).ok_or(RegError::Capacity)?,
            },
            Prog {
                cost: oc,
                code: scratch.get(oo..oo + ol).ok_or(RegError::Capacity)?,
            },
        );
    }
    let def = Prog {
        cost: dcost,
        code: scratch.get(doff..doff + dlen).ok_or(RegError::Capacity)?,
    };
    pack_decision(out, &packed[..rules.len()], &def).map_err(RegError::Pack)
}

/// [`compile_decision`], hex-encoded for a module's `decision` param.
#[allow(
    clippy::too_many_arguments,
    reason = "no allocator: every intermediate buffer is the caller's, so the schema/binding/rules and the three staging buffers are all explicit"
)]
pub fn compile_decision_param(
    schema: &[u8],
    param_name: &[u8],
    input_type: &[u8],
    rules: &[RuleSource],
    default: &[u8],
    out: &mut [u8],
    container: &mut [u8],
    scratch: &mut [u8],
) -> Result<usize, RegError> {
    let n = compile_decision(
        schema, param_name, input_type, rules, default, container, scratch,
    )?;
    hex_encode(container.get(..n).ok_or(RegError::Capacity)?, out).ok_or(RegError::Capacity)
}
