// Turning a parsed `.uproc` document into the inputs the compiler and sealer
// need — the step between `uproc_core` (structure) and `celc_core` +
// `artefact_core` (bytecode + identity).
//
// `uproc_core` gives declarations as spans. `celc_core` wants its type
// environment as TEXT: `Name{field:ty@N,...};Other{...};ENUM=n`, with the
// parameter binding as `name:Type`. This builds those strings from the document,
// so a device can compile an artefact body without a host ever having assembled
// the environment for it.
//
// The one translation that happens here is the TYPE VOCABULARY. The DSL writes
// `string`; the compiler's schema text writes `str`. Everything else is spelled
// the same, and a message type is its qualified name in both. Getting this wrong
// would not fail loudly — an unknown type name reads as a message name, so
// `string` would silently become "a message called string" and the error would
// surface later as a type mismatch inside an expression body. So the mapping is
// explicit and total, and unknown names are passed through as message names
// deliberately rather than by accident.
//
// Requires `uproc_core`.

/// Why lowering a document to compiler inputs failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LowerDocError {
    /// An output buffer was too small for the assembled text.
    TooLarge,
}

/// Append `s` to `out` at `p`, returning the new length.
fn put(out: &mut [u8], p: usize, s: &[u8]) -> Result<usize, LowerDocError> {
    if p + s.len() > out.len() {
        return Err(LowerDocError::TooLarge);
    }
    out[p..p + s.len()].copy_from_slice(s);
    Ok(p + s.len())
}

fn put_u32(out: &mut [u8], p: usize, mut v: u32) -> Result<usize, LowerDocError> {
    let mut tmp = [0u8; 10];
    let mut n = 0;
    if v == 0 {
        tmp[0] = b'0';
        n = 1;
    } else {
        while v > 0 {
            tmp[n] = b'0' + (v % 10) as u8;
            v /= 10;
            n += 1;
        }
        tmp[..n].reverse();
    }
    put(out, p, &tmp[..n])
}

fn put_i64(out: &mut [u8], p: usize, v: i64) -> Result<usize, LowerDocError> {
    let mut p = p;
    let mag = if v < 0 {
        p = put(out, p, b"-")?;
        v.unsigned_abs()
    } else {
        v as u64
    };
    let mut tmp = [0u8; 20];
    let mut n = 0;
    let mut m = mag;
    if m == 0 {
        tmp[0] = b'0';
        n = 1;
    } else {
        while m > 0 {
            tmp[n] = b'0' + (m % 10) as u8;
            m /= 10;
            n += 1;
        }
        tmp[..n].reverse();
    }
    put(out, p, &tmp[..n])
}

/// Translate a DSL type name to the compiler's schema vocabulary.
///
/// The scalars are spelled identically except `string`, which the schema text
/// writes as `str`. Anything else is a MESSAGE name and passes through — which
/// is why the scalar list is exhaustive rather than a default case.
pub fn schema_type_name(dsl: &[u8]) -> &[u8] {
    match dsl {
        b"string" => b"str",
        b"int" | b"uint" | b"double" | b"bool" | b"bytes" | b"str" => dsl,
        other => other, // a qualified message name
    }
}

/// Build the compiler's schema text for `doc` into `out`, returning its length.
///
/// Emits every declared message as `Name{field:ty@number,...};` then every enum
/// constant as `NAME=value;` — each entry TERMINATED by `;`, including the last,
/// matching the host byte for byte. Declaration order is preserved, so the same
/// document always yields the same text, and therefore the same bytecode and the
/// same digests.
pub fn uproc_schema_text(
    src: &[u8],
    doc: &Doc,
    arena: &UprocArena,
    out: &mut [u8],
) -> Result<usize, LowerDocError> {
    let mut p = 0usize;
    for mi in 0..doc.n_messages {
        let m = arena.messages[mi];
        p = put(out, p, m.name.of(src))?;
        p = put(out, p, b"{")?;
        for k in 0..m.n_fields as usize {
            let f = arena.fields[m.first_field as usize + k];
            if k > 0 {
                p = put(out, p, b",")?;
            }
            p = put(out, p, f.name.of(src))?;
            p = put(out, p, b":")?;
            p = put(out, p, schema_type_name(f.ty.of(src)))?;
            p = put(out, p, b"@")?;
            p = put_u32(out, p, f.number)?;
        }
        // Each entry is TERMINATED by `;`, not separated by it — including the
        // last. Matching the host exactly matters more than looking tidy: the
        // schema text is the input every digest is ultimately a function of.
        p = put(out, p, b"};")?;
    }
    for ei in 0..doc.n_enums {
        let e = arena.enums[ei];
        p = put(out, p, e.name.of(src))?;
        p = put(out, p, b"=")?;
        p = put_i64(out, p, e.value)?;
        p = put(out, p, b";")?;
    }
    Ok(p)
}

/// Build the compiler's parameter binding `name:Type` into `out`.
pub fn uproc_params_text(
    src: &[u8],
    param_name: Span,
    param_type: Span,
    out: &mut [u8],
) -> Result<usize, LowerDocError> {
    let mut p = put(out, 0, param_name.of(src))?;
    p = put(out, p, b":")?;
    put(out, p, schema_type_name(param_type.of(src)))
}

/// The message name a parameter type refers to, or empty when it is a scalar.
///
/// A sealed Expression records the parameter's MESSAGE type separately from its
/// name; a scalar parameter has none.
pub fn param_message_name(param_type: &[u8]) -> &[u8] {
    match param_type {
        b"int" | b"uint" | b"double" | b"bool" | b"bytes" | b"string" | b"str" => b"",
        other => other,
    }
}

/// The fully-qualified name of a declaration: `package.symbol`, or the bare
/// symbol when the module has no package.
pub fn qualify(package: &[u8], symbol: &[u8], out: &mut [u8]) -> Result<usize, LowerDocError> {
    if package.is_empty() {
        return put(out, 0, symbol);
    }
    let mut p = put(out, 0, package)?;
    p = put(out, p, b".")?;
    put(out, p, symbol)
}

/// Split a module's qualified name into `(package, symbol)` at the last dot.
pub fn split_qname(qualified: &[u8]) -> (&[u8], &[u8]) {
    let mut i = qualified.len();
    while i > 0 {
        i -= 1;
        if qualified[i] == b'.' {
            return (&qualified[..i], &qualified[i + 1..]);
        }
    }
    (&[], qualified)
}

/// Append an aggregation's synthesized EMIT CONTEXT to a schema text.
///
/// The `emit` expression does not read the input event — it reads a `ctx`
/// message the engine builds from the finished window: the partition key, one
/// int field per operator (numbered in operator order, matching the engine's
/// state layout), and the window bounds. Those three messages do not appear in
/// the document, so they are synthesized here, named after the aggregation so
/// two aggregations in one module cannot collide.
///
/// `base_len` is the length of the schema text already in `out`; the context is
/// appended after it, in the same order the host adds it.
pub fn uproc_agg_emit_schema(
    src: &[u8],
    agg: &AggregationDecl,
    arena: &UprocArena,
    package: &[u8],
    out: &mut [u8],
    base_len: usize,
) -> Result<usize, LowerDocError> {
    let name = agg.name.of(src);
    // `<package>.<agg>.State{...};`
    let mut p = base_len;
    let emit_fq = |p: usize, out: &mut [u8], suffix: &[u8]| -> Result<usize, LowerDocError> {
        let mut q = p;
        if !package.is_empty() {
            q = put(out, q, package)?;
            q = put(out, q, b".")?;
        }
        q = put(out, q, name)?;
        put(out, q, suffix)
    };

    p = emit_fq(p, out, b".State{")?;
    for k in 0..agg.n_ops as usize {
        let o = arena.operators[agg.first_op as usize + k];
        if k > 0 {
            p = put(out, p, b",")?;
        }
        p = put(out, p, o.name.of(src))?;
        p = put(out, p, b":int@")?;
        p = put_u32(out, p, (k + 1) as u32)?;
    }
    p = put(out, p, b"};")?;

    p = emit_fq(p, out, b".Window{start:int@1,end:int@2};")?;

    p = emit_fq(p, out, b".EmitCtx{key:str@1,state:")?;
    p = emit_fq(p, out, b".State@2,window:")?;
    p = emit_fq(p, out, b".Window@3};")?;
    Ok(p)
}

/// The parameter binding for an aggregation's `emit`: `ctx:<package>.<agg>.EmitCtx`.
pub fn uproc_agg_emit_params(
    src: &[u8],
    agg: &AggregationDecl,
    package: &[u8],
    out: &mut [u8],
) -> Result<usize, LowerDocError> {
    let mut p = put(out, 0, b"ctx:")?;
    if !package.is_empty() {
        p = put(out, p, package)?;
        p = put(out, p, b".")?;
    }
    p = put(out, p, agg.name.of(src))?;
    put(out, p, b".EmitCtx")
}
