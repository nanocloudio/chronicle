// Canonical artefact encoding — ONE implementation, host and device.
//
// An artefact's identity is the sha256 of its canonical protobuf encoding. This
// encodes artefacts directly with `pb_core`, so the same source runs in the
// authoring crate and inside a `.fmod` — there is no second encoder to drift
// against, only one that `chronicle-canonical/tests/pb_differential.rs` pins to
// prost.
//
// FIELD ORDER IS SIGNIFICANT. prost writes fields in ascending tag order and
// omits proto3 defaults; every writer below follows the schema's tag order
// exactly, and passes empty/zero values through `pb_core`'s `*_field` helpers,
// which skip them. Reordering a line here changes an artefact's digest.
//
// Requires `pb_core.rs` (the wire layer) to be included alongside.

/// `ArtefactKind` (common.proto). Only the kinds this core seals are named.
pub const KIND_SCHEMA: i32 = 1;
pub const KIND_EXPRESSION: i32 = 2;
pub const KIND_TRANSFORMATION: i32 = 3;
pub const KIND_DECISION: i32 = 4;
pub const KIND_AGGREGATION: i32 = 5;
pub const KIND_PIPELINE: i32 = 6;
pub const KIND_MODULE: i32 = 7;

/// `ProvenanceClass` — how the artefact was produced.
pub const PROVENANCE_LOCAL_BUILD: i32 = 1;
pub const PROVENANCE_PUBLISHED: i32 = 2;

/// `Cardinality::ONE` — one input, one output; every chronicle Transformation.
pub const CARDINALITY_ONE: i32 = 1;
/// `HitPolicy::FIRST` — first matching rule wins, the only policy chronicle
/// compiles (see `decision_core`'s first-hit driver).
pub const HIT_POLICY_FIRST: i32 = 1;

/// `NumericProfile::INTEGER` / `TextProfile::UNICODE_NFC` — the deterministic
/// environment every chronicle expression is checked and run under.
pub const NUMERIC_INTEGER: i32 = 1;
pub const TEXT_UNICODE_NFC: i32 = 1;

/// `QualifiedName { package = 1, symbol = 2 }` as a nested field.
pub fn pb_qualified_name(
    w: &mut Pb,
    field: u32,
    package: &[u8],
    symbol: &[u8],
) -> Result<(), PbError> {
    let m = w.open(field)?;
    w.bytes_field(1, package)?;
    w.bytes_field(2, symbol)?;
    w.close(m)
}

/// `Digest { algorithm = 1, value = 2 }` as a nested field.
pub fn pb_digest(w: &mut Pb, field: u32, algorithm: &[u8], value: &[u8]) -> Result<(), PbError> {
    let m = w.open(field)?;
    w.bytes_field(1, algorithm)?;
    w.bytes_field(2, value)?;
    w.close(m)
}

/// `TypeRef { message_name = 1, schema = 2 }` as a nested field.
pub fn pb_type_ref(w: &mut Pb, field: u32, message_name: &[u8]) -> Result<(), PbError> {
    let m = w.open(field)?;
    w.bytes_field(1, message_name)?;
    w.close(m)
}

/// The metadata an artefact header carries, minus the content digest (which is
/// cleared while computing identity and written only when sealing).
pub struct HeaderSpec<'a> {
    pub package: &'a [u8],
    pub symbol: &'a [u8],
    pub kind: i32,
    /// The single required `CapabilityRequirement { capability = 1 }`; empty
    /// emits none.
    ///
    /// Deliberately ONE slice, not `&[&[u8]]`: a PIC module does not relocate
    /// the inner pointers of a nested reference table, so iterating one
    /// dereferences unrelocated addresses and faults at runtime (it compiles
    /// and links cleanly, which is what makes the trap expensive). Every
    /// artefact chronicle seals declares exactly one capability; if that ever
    /// changes, pass a count plus fixed-size storage rather than a slice table.
    pub capability: &'a [u8],
}

/// `ArtefactHeader { name = 1, kind = 2, …, capabilities = 5, …,
/// content_digest = 7 }`. `digest` is `None` while computing identity — the
/// rule that makes a digest recomputable from the artefact itself.
pub fn pb_header(
    w: &mut Pb,
    field: u32,
    spec: &HeaderSpec,
    digest: Option<&[u8]>,
) -> Result<(), PbError> {
    let m = w.open(field)?;
    pb_qualified_name(w, 1, spec.package, spec.symbol)?;
    w.i32_field(2, spec.kind)?;
    if !spec.capability.is_empty() {
        let c = w.open(5)?;
        w.bytes_field(1, spec.capability)?;
        w.close(c)?;
    }
    if let Some(d) = digest {
        pb_digest(w, 7, b"sha256", d)?;
    }
    w.close(m)
}

/// Everything an `Expression` artefact needs beyond its header.
pub struct ExpressionSpec<'a> {
    pub param_name: &'a [u8],
    pub param_message: &'a [u8],
    pub result_type: &'a [u8],
    pub source: &'a [u8],
    pub bytecode: &'a [u8],
    pub max_cost: u64,
}

/// `Expression { header = 1, parameters = 2, result = 3, compiled = 4,
/// environment = 5, max_cost = 6 }`.
///
/// Mirrors the host `build_expression`: one parameter, the strict-CEL
/// capability, and the deterministic integer/NFC environment.
pub fn encode_expression(
    out: &mut [u8],
    header: &HeaderSpec,
    spec: &ExpressionSpec,
    digest: Option<&[u8]>,
) -> Result<usize, PbError> {
    let mut w = Pb::new(out);
    pb_header(&mut w, 1, header, digest)?;

    // repeated Parameter parameters = 2 { name = 1; TypeRef type = 2; }
    let p = w.open(2)?;
    w.bytes_field(1, spec.param_name)?;
    pb_type_ref(&mut w, 2, spec.param_message)?;
    w.close(p)?;

    pb_type_ref(&mut w, 3, spec.result_type)?;

    // CompiledExpression compiled = 4 { source = 1; bytecode = 2; max_cost = 3; }
    let c = w.open(4)?;
    w.bytes_field(1, spec.source)?;
    w.bytes_field(2, spec.bytecode)?;
    w.u64_field(3, spec.max_cost)?;
    w.close(c)?;

    // CelEnvironment environment = 5 { …, numeric = 3; text = 4; }
    let e = w.open(5)?;
    w.i32_field(3, NUMERIC_INTEGER)?;
    w.i32_field(4, TEXT_UNICODE_NFC)?;
    w.close(e)?;

    w.u64_field(6, spec.max_cost)?;
    Ok(w.len())
}

/// Seal an Expression: encode with the digest cleared, hash that, then re-encode
/// with the digest present. The two-pass shape IS the identity rule — the digest
/// covers the artefact with its own digest field absent, so any holder can
/// recompute it byte-for-byte.
///
/// `scratch` holds the digest-free encoding; `out` receives the sealed artefact.
/// Returns `(sealed_len, digest)`.
pub fn seal_expression(
    out: &mut [u8],
    scratch: &mut [u8],
    header: &HeaderSpec,
    spec: &ExpressionSpec,
) -> Result<(usize, [u8; 32]), PbError> {
    let n = encode_expression(scratch, header, spec, None)?;
    let digest = sha256(&scratch[..n]);
    let sealed = encode_expression(out, header, spec, Some(&digest))?;
    Ok((sealed, digest))
}

/// `CompiledExpression { source = 1, bytecode = 2, max_cost = 3 }` as a nested
/// field — the semantic payload shared by Expression, Transformation and every
/// Decision rule.
pub fn pb_compiled(
    w: &mut Pb,
    field: u32,
    source: &[u8],
    bytecode: &[u8],
    max_cost: u64,
) -> Result<(), PbError> {
    let m = w.open(field)?;
    w.bytes_field(1, source)?;
    w.bytes_field(2, bytecode)?;
    w.u64_field(3, max_cost)?;
    w.close(m)
}

/// A Transformation's payload: a construction whose result is a message.
pub struct TransformationSpec<'a> {
    pub input_type: &'a [u8],
    pub output_type: &'a [u8],
    pub source: &'a [u8],
    pub bytecode: &'a [u8],
    pub max_cost: u64,
}

/// `Transformation { header = 1, input = 2, output = 3, cardinality = 4,
/// compiled = 5 (oneof construction), max_cost = 8 }`.
pub fn encode_transformation(
    out: &mut [u8],
    header: &HeaderSpec,
    spec: &TransformationSpec,
    digest: Option<&[u8]>,
) -> Result<usize, PbError> {
    let mut w = Pb::new(out);
    pb_header(&mut w, 1, header, digest)?;
    pb_type_ref(&mut w, 2, spec.input_type)?;
    pb_type_ref(&mut w, 3, spec.output_type)?;
    w.i32_field(4, CARDINALITY_ONE)?;
    // `construction` is a oneof; the compiled arm is field 5.
    pb_compiled(&mut w, 5, spec.source, spec.bytecode, spec.max_cost)?;
    w.u64_field(8, spec.max_cost)?;
    Ok(w.len())
}

/// Seal a Transformation — same two-pass identity rule as [`seal_expression`].
pub fn seal_transformation(
    out: &mut [u8],
    scratch: &mut [u8],
    header: &HeaderSpec,
    spec: &TransformationSpec,
) -> Result<(usize, [u8; 32]), PbError> {
    let n = encode_transformation(scratch, header, spec, None)?;
    let digest = sha256(&scratch[..n]);
    let sealed = encode_transformation(out, header, spec, Some(&digest))?;
    Ok((sealed, digest))
}

/// One compiled Decision rule: a Bool `when` and a message-constructing outcome.
/// Flat fields rather than nested slices — see [`HeaderSpec::capability`] on why
/// a PIC module must not walk a slice-of-slices.
#[derive(Clone, Copy)]
pub struct RuleSpec<'a> {
    pub name: &'a [u8],
    pub priority: i32,
    pub when_source: &'a [u8],
    pub when_code: &'a [u8],
    pub when_cost: u64,
    pub outcome_source: &'a [u8],
    pub outcome_code: &'a [u8],
    pub outcome_cost: u64,
}

/// `Rule { name = 1, priority = 2, when = 3, outcome = 4 }`, where
/// `Outcome { return_value = 1 }`.
pub fn pb_rule(w: &mut Pb, field: u32, r: &RuleSpec) -> Result<(), PbError> {
    let m = w.open(field)?;
    w.bytes_field(1, r.name)?;
    w.i32_field(2, r.priority)?;
    pb_compiled(w, 3, r.when_source, r.when_code, r.when_cost)?;
    let o = w.open(4)?;
    pb_compiled(w, 1, r.outcome_source, r.outcome_code, r.outcome_cost)?;
    w.close(o)?;
    w.close(m)
}

/// A Decision's payload. `rules` is a caller-provided slice of OWNED specs
/// (not a slice of references), which a PIC module can walk safely.
pub struct DecisionSpec<'a> {
    pub input_type: &'a [u8],
    pub output_type: &'a [u8],
    pub default_source: &'a [u8],
    pub default_code: &'a [u8],
    pub default_cost: u64,
    pub explain: bool,
}

/// `Decision { header = 1, input = 2, output = 3, hit_policy = 4, rules = 5,
/// default = 6, explain = 7 }` with the `first` hit policy.
pub fn encode_decision(
    out: &mut [u8],
    header: &HeaderSpec,
    spec: &DecisionSpec,
    rules: &[RuleSpec],
    digest: Option<&[u8]>,
) -> Result<usize, PbError> {
    let mut w = Pb::new(out);
    pb_header(&mut w, 1, header, digest)?;
    pb_type_ref(&mut w, 2, spec.input_type)?;
    pb_type_ref(&mut w, 3, spec.output_type)?;
    w.i32_field(4, HIT_POLICY_FIRST)?;
    for r in rules {
        pb_rule(&mut w, 5, r)?;
    }
    let d = w.open(6)?;
    pb_compiled(
        &mut w,
        1,
        spec.default_source,
        spec.default_code,
        spec.default_cost,
    )?;
    w.close(d)?;
    w.bool_field(7, spec.explain)?;
    Ok(w.len())
}

/// Seal a Decision — same two-pass identity rule as [`seal_expression`].
pub fn seal_decision(
    out: &mut [u8],
    scratch: &mut [u8],
    header: &HeaderSpec,
    spec: &DecisionSpec,
    rules: &[RuleSpec],
) -> Result<(usize, [u8; 32]), PbError> {
    let n = encode_decision(scratch, header, spec, rules, None)?;
    let digest = sha256(&scratch[..n]);
    let sealed = encode_decision(out, header, spec, rules, Some(&digest))?;
    Ok((sealed, digest))
}

// ---- Module: the deployment unit ------------------------------------------
//
// A Module is what distribution actually moves: it names the artefacts it
// contains by digest, the capabilities it needs, its entry points and its
// provenance, and carries DETACHED signatures. Sealing one on device is what
// lets a node publish work it authored itself.

/// `ArtefactRef { name = 1, kind = 2, pinned = 3 }` — a content-pinned
/// reference to another artefact.
pub fn pb_artefact_ref(
    w: &mut Pb,
    field: u32,
    package: &[u8],
    symbol: &[u8],
    kind: i32,
    digest: &[u8],
) -> Result<(), PbError> {
    let m = w.open(field)?;
    pb_qualified_name(w, 1, package, symbol)?;
    w.i32_field(2, kind)?;
    if !digest.is_empty() {
        pb_digest(w, 3, b"sha256", digest)?;
    }
    w.close(m)
}

/// One artefact a Module contains: which repeated field it belongs to is
/// decided by its `kind`, exactly as the host `build_module` classifies refs.
#[derive(Clone, Copy)]
pub struct ModuleRef<'a> {
    pub package: &'a [u8],
    pub symbol: &'a [u8],
    pub kind: i32,
    pub digest: &'a [u8],
}

/// The Module field number a contained artefact of `kind` is listed under.
/// Schemas=2, expressions=3, transformations=4, decisions=5, aggregations=6,
/// pipelines=7 — the classification the host builder performs.
pub fn module_field_for_kind(kind: i32) -> Option<u32> {
    match kind {
        KIND_SCHEMA => Some(2),
        KIND_EXPRESSION => Some(3),
        KIND_TRANSFORMATION => Some(4),
        KIND_DECISION => Some(5),
        KIND_AGGREGATION => Some(6),
        KIND_PIPELINE => Some(7),
        _ => None,
    }
}

/// A detached `Signature { algorithm = 1, signature = 2, signer = 3 }`.
/// A resource a module REQUIRES the deployment to bind. `required` is the
/// difference between "activation fails without this" and "nice to have".
#[derive(Clone, Copy)]
pub struct BindingSpec<'a> {
    pub package: &'a [u8],
    pub symbol: &'a [u8],
    pub required: bool,
}

/// An activatable surface: a name bound to one of the module's pipelines.
#[derive(Clone, Copy)]
pub struct EntrySpec<'a> {
    pub name: &'a [u8],
    pub package: &'a [u8],
    pub symbol: &'a [u8],
    pub digest: &'a [u8],
}

/// Detached because the digest it signs excludes signatures — signing must not
/// perturb identity.
pub struct SignatureSpec<'a> {
    pub algorithm: &'a [u8],
    pub signature: &'a [u8],
    pub signer: &'a [u8],
}

/// A Module's non-repeated payload.
pub struct ModuleSpec<'a> {
    pub source_revision: &'a [u8],
    pub build_toolchain: &'a [u8],
    pub provenance_class: i32,
}

/// `Module { header = 1, <contained refs> = 2..7, capabilities = 11,
/// entry_points = 12, provenance = 13, signatures = 14 }`.
///
/// Refs MUST arrive grouped by kind in ascending field order (schemas first,
/// then expressions, …): prost writes repeated fields contiguously in tag
/// order, so interleaving kinds here would produce a different — and therefore
/// differently-identified — encoding.
#[allow(
    clippy::too_many_arguments,
    reason = "a Module aggregates six independent declaration sets; grouping them would only move the list"
)]
pub fn encode_module(
    out: &mut [u8],
    header: &HeaderSpec,
    refs: &[ModuleRef],
    bindings: &[BindingSpec],
    entries: &[EntrySpec],
    spec: &ModuleSpec,
    signatures: &[SignatureSpec],
    digest: Option<&[u8]>,
) -> Result<usize, PbError> {
    let mut w = Pb::new(out);
    pb_header(&mut w, 1, header, digest)?;

    // Contained artefacts, grouped by their destination field so the wire
    // order matches prost's field-by-field emission.
    let mut field = 2u32;
    while field <= 7 {
        for r in refs {
            if module_field_for_kind(r.kind) == Some(field) {
                pb_artefact_ref(&mut w, field, r.package, r.symbol, r.kind, r.digest)?;
            }
        }
        field += 1;
    }

    // ResourceBindingRequirement = 9 { contract = QualifiedName(1), required = 2 }
    for b in bindings {
        let m = w.open(9)?;
        pb_qualified_name(&mut w, 1, b.package, b.symbol)?;
        w.bool_field(2, b.required)?;
        w.close(m)?;
    }

    // EntryPoint = 12 { name = 1, pipeline = ArtefactRef(2) }
    for e in entries {
        let m = w.open(12)?;
        w.bytes_field(1, e.name)?;
        pb_artefact_ref(&mut w, 2, e.package, e.symbol, KIND_PIPELINE, e.digest)?;
        w.close(m)?;
    }

    // Provenance = 13 { source_revision = 1, build_toolchain = 2, class = 3 }
    let p = w.open(13)?;
    w.bytes_field(1, spec.source_revision)?;
    w.bytes_field(2, spec.build_toolchain)?;
    w.i32_field(3, spec.provenance_class)?;
    w.close(p)?;

    // Detached signatures = 14. Excluded from the digest, so they are written
    // only on the sealed pass.
    for sig in signatures {
        let m = w.open(14)?;
        w.bytes_field(1, sig.algorithm)?;
        w.bytes_field(2, sig.signature)?;
        w.bytes_field(3, sig.signer)?;
        w.close(m)?;
    }
    Ok(w.len())
}

/// Seal a Module. Unlike the other artefacts the digest excludes BOTH the
/// header's digest field AND every signature — a signature signs the digest, so
/// including it would make identity unrecomputable.
#[allow(
    clippy::too_many_arguments,
    reason = "mirrors encode_module's declaration sets"
)]
pub fn seal_module(
    out: &mut [u8],
    scratch: &mut [u8],
    header: &HeaderSpec,
    refs: &[ModuleRef],
    bindings: &[BindingSpec],
    entries: &[EntrySpec],
    spec: &ModuleSpec,
    signatures: &[SignatureSpec],
) -> Result<(usize, [u8; 32]), PbError> {
    let n = encode_module(scratch, header, refs, bindings, entries, spec, &[], None)?;
    let digest = sha256(&scratch[..n]);
    let sealed = encode_module(
        out,
        header,
        refs,
        bindings,
        entries,
        spec,
        signatures,
        Some(&digest),
    )?;
    Ok((sealed, digest))
}

// ---- Aggregation ----------------------------------------------------------

/// `OperatorKind` (aggregation.proto). Spelled out in full rather than guessed:
/// SUM is 1 and COUNT is 2, which is the reverse of the intuitive order and
/// silently moves an Aggregation's digest if assumed the other way round.
pub const OP_SUM: i32 = 1;
pub const OP_COUNT: i32 = 2;
pub const OP_AVG: i32 = 3;
pub const OP_MIN: i32 = 4;
pub const OP_MAX: i32 = 5;
pub const OP_QUANTILE: i32 = 6;
pub const OP_TOPK: i32 = 7;
pub const OP_DISTINCT: i32 = 8;

/// One monoid operator over the event stream. `selector` is the value it folds;
/// COUNT has none, which is why the slice is empty for it.
#[derive(Clone, Copy)]
pub struct OperatorSpec<'a> {
    pub name: &'a [u8],
    pub kind: i32,
    pub selector_source: &'a [u8],
    pub selector_code: &'a [u8],
    pub selector_cost: u64,
}

/// An Aggregation's payload. Tumbling windows only — the shape chronicle's
/// device kernel implements.
pub struct AggregationSpec<'a> {
    pub input_type: &'a [u8],
    pub state_type: &'a [u8],
    pub output_type: &'a [u8],
    pub key_source: &'a [u8],
    pub key_code: &'a [u8],
    pub key_cost: u64,
    pub time_source: &'a [u8],
    pub time_code: &'a [u8],
    pub time_cost: u64,
    pub window_size_ms: u64,
    pub lateness_ms: u64,
    pub guard_ms: u64,
    pub emit_source: &'a [u8],
    pub emit_code: &'a [u8],
    pub emit_cost: u64,
    pub max_lanes: u32,
    pub warn_lanes: u32,
}

/// `Aggregation { header = 1, input = 2, state = 3, output = 4, key = 5,
/// event_time = 6, window = 7, watermark = 8, operators = 9, emit = 10,
/// cardinality = 11 }`. `Window` is a oneof; the tumbling arm is field 2.
pub fn encode_aggregation(
    out: &mut [u8],
    header: &HeaderSpec,
    spec: &AggregationSpec,
    operators: &[OperatorSpec],
    digest: Option<&[u8]>,
) -> Result<usize, PbError> {
    let mut w = Pb::new(out);
    pb_header(&mut w, 1, header, digest)?;
    pb_type_ref(&mut w, 2, spec.input_type)?;
    pb_type_ref(&mut w, 3, spec.state_type)?;
    pb_type_ref(&mut w, 4, spec.output_type)?;
    pb_compiled(&mut w, 5, spec.key_source, spec.key_code, spec.key_cost)?;
    pb_compiled(&mut w, 6, spec.time_source, spec.time_code, spec.time_cost)?;

    // Window window = 7 { oneof kind { … tumbling = 2 … }, trigger = 10,
    //                     allowed_lateness_ms = 11 }
    let win = w.open(7)?;
    let tum = w.open(2)?;
    w.u64_field(1, spec.window_size_ms)?;
    w.close(tum)?;
    // Trigger { on_watermark = 1, fire_on_close = 10, emit_retractions = 11 } —
    // the fixed policy chronicle compiles: fire on watermark, always close a
    // final pane, emit corrections for retractable operators.
    let trg = w.open(10)?;
    w.bool_field(1, true)?;
    w.bool_field(10, true)?;
    w.bool_field(11, true)?;
    w.close(trg)?;
    w.u64_field(11, spec.lateness_ms)?;
    w.close(win)?;

    // Watermark watermark = 8 { lateness_allowance_ms = 1, guard_ms = 2 }
    let wm = w.open(8)?;
    w.u64_field(1, spec.lateness_ms)?;
    w.u64_field(2, spec.guard_ms)?;
    w.close(wm)?;

    // repeated Operator operators = 9 { name = 1, kind = 2, selector = 3 }
    for op in operators {
        let m = w.open(9)?;
        w.bytes_field(1, op.name)?;
        w.i32_field(2, op.kind)?;
        if !op.selector_code.is_empty() {
            pb_compiled(
                &mut w,
                3,
                op.selector_source,
                op.selector_code,
                op.selector_cost,
            )?;
        }
        w.close(m)?;
    }

    pb_compiled(&mut w, 10, spec.emit_source, spec.emit_code, spec.emit_cost)?;

    // BoundedCardinality cardinality = 11 { max = 1, warn = 2 }
    let c = w.open(11)?;
    w.u64_field(1, spec.max_lanes as u64)?;
    w.u64_field(2, spec.warn_lanes as u64)?;
    w.close(c)?;
    Ok(w.len())
}

/// Seal an Aggregation — same two-pass identity rule as [`seal_expression`].
pub fn seal_aggregation(
    out: &mut [u8],
    scratch: &mut [u8],
    header: &HeaderSpec,
    spec: &AggregationSpec,
    operators: &[OperatorSpec],
) -> Result<(usize, [u8; 32]), PbError> {
    let n = encode_aggregation(scratch, header, spec, operators, None)?;
    let digest = sha256(&scratch[..n]);
    let sealed = encode_aggregation(out, header, spec, operators, Some(&digest))?;
    Ok((sealed, digest))
}

// ---- Pipeline -------------------------------------------------------------

/// One pipeline stage: either a `Call` on an artefact or an `Effect` on a
/// resource. `operation` empty means Call; non-empty means Effect — the two
/// arms of the `action` oneof (fields 2 and 3).
#[derive(Clone, Copy)]
pub struct StageSpec<'a> {
    pub name: &'a [u8],
    pub target_package: &'a [u8],
    pub target_symbol: &'a [u8],
    pub target_kind: i32,
    /// Non-empty selects the Effect arm and names the resource operation.
    pub operation: &'a [u8],
    /// Single `Binding { name = 1, value_ref = 2 }`, both set to this value.
    pub argument: &'a [u8],
}

/// `Pipeline { header = 1, inputs = 2, outputs = 3, sources = 4, stages = 5,
/// commit_after = 6, return_stage = 7 }`, with one input and one output port.
pub struct PipelineSpec<'a> {
    pub input_port: &'a [u8],
    pub input_type: &'a [u8],
    pub output_type: &'a [u8],
    pub commit_after: &'a [u8],
    pub return_stage: &'a [u8],
}

pub fn encode_pipeline(
    out: &mut [u8],
    header: &HeaderSpec,
    spec: &PipelineSpec,
    stages: &[StageSpec],
    digest: Option<&[u8]>,
) -> Result<usize, PbError> {
    let mut w = Pb::new(out);
    pb_header(&mut w, 1, header, digest)?;

    // repeated Port inputs = 2 / outputs = 3 { name = 1, TypeRef type = 2 }
    let i = w.open(2)?;
    w.bytes_field(1, spec.input_port)?;
    pb_type_ref(&mut w, 2, spec.input_type)?;
    w.close(i)?;
    let o = w.open(3)?;
    w.bytes_field(1, b"result")?;
    pb_type_ref(&mut w, 2, spec.output_type)?;
    w.close(o)?;

    // repeated Stage stages = 5 { name = 1, action oneof {Call=2, Effect=3},
    //                             arguments = 4 }
    for st in stages {
        let m = w.open(5)?;
        w.bytes_field(1, st.name)?;
        if st.operation.is_empty() {
            let call = w.open(2)?;
            pb_artefact_ref(
                &mut w,
                1,
                st.target_package,
                st.target_symbol,
                st.target_kind,
                &[],
            )?;
            w.close(call)?;
        } else {
            let eff = w.open(3)?;
            pb_artefact_ref(
                &mut w,
                1,
                st.target_package,
                st.target_symbol,
                st.target_kind,
                &[],
            )?;
            w.bytes_field(2, st.operation)?;
            w.close(eff)?;
        }
        let b = w.open(4)?;
        w.bytes_field(1, st.argument)?;
        w.bytes_field(2, st.argument)?;
        w.close(b)?;
        w.close(m)?;
    }

    w.bytes_field(6, spec.commit_after)?;
    w.bytes_field(7, spec.return_stage)?;
    Ok(w.len())
}

/// Seal a Pipeline — same two-pass identity rule as [`seal_expression`].
pub fn seal_pipeline(
    out: &mut [u8],
    scratch: &mut [u8],
    header: &HeaderSpec,
    spec: &PipelineSpec,
    stages: &[StageSpec],
) -> Result<(usize, [u8; 32]), PbError> {
    let n = encode_pipeline(scratch, header, spec, stages, None)?;
    let digest = sha256(&scratch[..n]);
    let sealed = encode_pipeline(out, header, spec, stages, Some(&digest))?;
    Ok((sealed, digest))
}

// ---- Schema ---------------------------------------------------------------

/// `Schema { header = 1, closure = 2, descriptor_digest = 3, constraints = 4 }`.
///
/// The closure is an arbitrary `FileDescriptorSet`, so it is taken as
/// ALREADY-ENCODED BYTES rather than a typed structure: a device receives a
/// descriptor set (from storage, a port, or an authoring tool), it does not
/// construct one, and re-implementing descriptor.proto here would be a large
/// open-ended surface for no gain.
///
/// TWO DIFFERENT BYTE STRINGS, deliberately. The host embeds the closure
/// VERBATIM as it was given, but computes `descriptor_digest` over a SORTED
/// copy (files by name, messages within each file by name). So:
///
///   * `closure` — exactly the bytes to embed; the artefact's own identity
///     therefore DOES depend on file order;
///   * `descriptor_digest` — over the sorted encoding, which is what makes the
///     *descriptor* identity stable across ordering and surrounding metadata.
///
/// This core cannot sort (it does not parse the blob), so a caller that wants
/// the host's digest must supply both, sorted where the host sorts.
pub fn encode_schema(
    out: &mut [u8],
    header: &HeaderSpec,
    closure: &[u8],
    descriptor_digest: &[u8],
    digest: Option<&[u8]>,
) -> Result<usize, PbError> {
    let mut w = Pb::new(out);
    pb_header(&mut w, 1, header, digest)?;
    w.bytes_field(2, closure)?;
    if !descriptor_digest.is_empty() {
        pb_digest(&mut w, 3, b"sha256", descriptor_digest)?;
    }
    Ok(w.len())
}

/// The descriptor digest for an ALREADY-SORTED closure — sha256 over the bytes
/// as given. See [`encode_schema`] on why sorting is the caller's job.
///
/// Named for the precondition, not the result. `compat_core` has a
/// `schema_descriptor_digest` that DOES the sorting first; if both were called
/// that, a file including both cores would silently bind whichever came first in
/// include order and hash an unsorted closure — producing a confident wrong
/// identity rather than an error.
pub fn sorted_closure_digest(closure: &[u8]) -> [u8; 32] {
    sha256(closure)
}

/// Seal a Schema — same two-pass identity rule as [`seal_expression`].
pub fn seal_schema(
    out: &mut [u8],
    scratch: &mut [u8],
    header: &HeaderSpec,
    closure: &[u8],
    descriptor_digest: &[u8],
) -> Result<(usize, [u8; 32]), PbError> {
    let n = encode_schema(scratch, header, closure, descriptor_digest, None)?;
    let digest = sha256(&scratch[..n]);
    let sealed = encode_schema(out, header, closure, descriptor_digest, Some(&digest))?;
    Ok((sealed, digest))
}
