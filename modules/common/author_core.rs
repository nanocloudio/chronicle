// Authoring core: the chronicle_cli domain orchestration
// for `chronicle author`, kept out of the `.fmod` wrapper as a flat, host-
// testable core. The command wrapper in mod.rs is ABI adaptation only — decode
// hex, build the parse arena over module state, format the reply — while the
// compile→seal→reference logic lives here over plain buffers.
//
// Mounted inside chronicle_cli's `mod tc {}` AFTER celc_core/lower_core/uproc_core/
// artefact_core, so `celc_compile_auto`, `lower_flat`, `uproc_parse`, the `seal_*`
// builders and `split_qname` are all in scope. The shared CLI output helpers
// (`append`/`append_u32`) and the `compile_one`/`compile_failed` pair live here too
// and are re-exported to the crate root, so every `cmd_*` resolves them.

pub fn parse_i64(b: &[u8]) -> Option<i64> {
    let (neg, digits) = match b.first() {
        Some(b'-') => (true, &b[1..]),
        _ => (false, b),
    };
    if digits.is_empty() {
        return None;
    }
    let mut v: i64 = 0;
    for c in digits {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v.checked_mul(10)?.checked_add((c - b'0') as i64)?;
    }
    Some(if neg { -v } else { v })
}

pub fn split_argv(rec: &[u8], out: &mut [(usize, usize); MAX_ARGV]) -> usize {
    let mut n = 0;
    let mut start = 0;
    let mut i = 0;
    while i <= rec.len() && n < out.len() {
        if i == rec.len() || rec[i] == 0 {
            if i > start {
                out[n] = (start, i);
                n += 1;
            }
            start = i + 1;
        }
        i += 1;
    }
    n
}

pub fn celc_err_name(e: CelcErr) -> &'static [u8] {
    use CelcErr as E;
    match e {
        E::Empty => b"empty source",
        E::Parse(_) => b"parse error",
        E::BadInteger => b"integer out of range",
        E::Trailing => b"trailing input",
        E::UnknownName(..) => b"unknown name",
        E::UnknownParam(..) => b"unknown parameter",
        E::UnknownField { .. } => b"unknown field",
        E::UnknownMessageType { .. } => b"unknown message type",
        E::NotAMessage { .. } => b"selected into a non-message",
        E::NotBool => b"operand is not bool",
        E::NotInteger => b"operand is not integer",
        E::NestedConstruction => b"nested construction",
        E::Depth => b"expression too deep",
        E::Capacity => b"input too large",
        E::BadSchema(_) => b"malformed schema",
        E::UnknownFunction(..) => b"unknown function",
        E::BadCallArgs(..) => b"bad call arguments",
        E::LocalDepth => b"cel.bind nesting too deep",
    }
}

pub fn put_prog(cont: &mut [u8], w: &mut usize, code: &[u8], cost: u64) -> bool {
    if *w + 6 + code.len() > cont.len() {
        return false;
    }
    cont[*w..*w + 4].copy_from_slice(&(cost as u32).to_le_bytes());
    cont[*w + 4..*w + 6].copy_from_slice(&(code.len() as u16).to_le_bytes());
    cont[*w + 6..*w + 6 + code.len()].copy_from_slice(code);
    *w += 6 + code.len();
    true
}

pub fn put_ir_prog(cont: &mut [u8], w: &mut usize, ir: &[u8]) -> bool {
    if *w + 2 + ir.len() > cont.len() {
        return false;
    }
    cont[*w..*w + 2].copy_from_slice(&(ir.len() as u16).to_le_bytes());
    cont[*w + 2..*w + 2 + ir.len()].copy_from_slice(ir);
    *w += 2 + ir.len();
    true
}

pub fn kind_from_name(n: &[u8]) -> Option<i32> {
    match n {
        b"schema" => Some(KIND_SCHEMA),
        b"expression" => Some(KIND_EXPRESSION),
        b"transformation" => Some(KIND_TRANSFORMATION),
        b"decision" => Some(KIND_DECISION),
        b"aggregation" => Some(KIND_AGGREGATION),
        b"pipeline" => Some(KIND_PIPELINE),
        _ => None,
    }
}

pub fn split_qualified(n: &[u8]) -> (&[u8], &[u8]) {
    let mut i = n.len();
    while i > 0 {
        i -= 1;
        if n[i] == b'.' {
            return (&n[..i], &n[i + 1..]);
        }
    }
    (&[], n)
}

pub fn split_csv<'a>(arg: &'a [u8], out: &mut [&'a [u8]]) -> usize {
    if arg == b"-" || arg.is_empty() {
        return 0;
    }
    let mut n = 0;
    let mut start = 0;
    let mut i = 0;
    while i <= arg.len() && n < out.len() {
        if i == arg.len() || arg[i] == b',' {
            if i > start {
                out[n] = &arg[start..i];
                n += 1;
            }
            start = i + 1;
        }
        i += 1;
    }
    n
}

pub fn split_digests(arg: &[u8], out: &mut [[u8; 32]]) -> Option<usize> {
    let mut items = [b"".as_slice(); MAX_SET];
    let n = split_csv(arg, &mut items);
    for k in 0..n {
        if items[k].len() != 64 || hex_decode(items[k], &mut out[k]) != Some(32) {
            return None;
        }
    }
    Some(n)
}

pub fn append_slot_reason(out: &mut [u8], at: usize, e: SlotError) -> usize {
    match e {
        SlotError::TooShort => append(out, at, b"shorter than the slot header"),
        SlotError::BadMagic => append(out, at, b"not a slot image"),
        SlotError::BadVersion => append(out, at, b"unknown slot format version"),
        SlotError::BadExtent => append(out, at, b"a blob extent falls outside the image"),
        SlotError::TooLarge => append(out, at, b"larger than the slot"),
        SlotError::ShaMismatch => append(out, at, b"payload does not match its recorded sha256"),
        SlotError::AbiMismatch => append(out, at, b"built for a different fluxor ABI surface"),
    }
}

pub fn version_digest(program: &[u8]) -> [u8; VERSION_DIGEST_LEN] {
    let h = sha256(program);
    let mut d = [0u8; VERSION_DIGEST_LEN];
    d.copy_from_slice(&h[..VERSION_DIGEST_LEN]);
    d
}

pub fn append_release_reason(out: &mut [u8], at: usize, e: ReleaseError) -> usize {
    match e {
        ReleaseError::TooManyVersions => append(out, at, b"more versions than the table holds"),
        ReleaseError::TagTooLong => append(out, at, b"a tag is too long"),
        ReleaseError::ProgramTooLarge => append(out, at, b"a program is too large"),
        ReleaseError::DuplicateTag => append(out, at, b"two versions share a tag"),
        ReleaseError::UnknownDefaultTag => append(out, at, b"the default tag names no version"),
        ReleaseError::BadDigestLen => append(out, at, b"a digest is the wrong width"),
        ReleaseError::TooLarge => append(out, at, b"the output does not fit"),
    }
}

pub const MAX_ARGV: usize = 24;
pub const MAX_SET: usize = 8;

pub const BIN_BUF: usize = 2048;

pub fn print_digest(out: &mut [u8], d: &[u8; 32]) -> (usize, i32) {
    let mut dhex = [0u8; 64];
    let Some(dl) = hex_encode(d, &mut dhex) else {
        return (append(out, 0, b"error: encode\n"), 1);
    };
    let mut p = append(out, 0, &dhex[..dl]);
    p = append(out, p, b"\n");
    (p, 0)
}

pub fn emit_hex_buf(cont: &[u8], w: usize, out: &mut [u8]) -> (usize, i32) {
    let mut hexed = [0u8; 2 * BIN_BUF];
    let Some(hl) = hex_encode(&cont[..w], &mut hexed) else {
        return (append(out, 0, b"error: container too large to print\n"), 1);
    };
    let mut p = append(out, 0, &hexed[..hl]);
    p = append(out, p, b"\n");
    (p, 0)
}

pub fn lower_arg_buf(hex: &[u8], ir: &mut [u8], code: &mut [u8]) -> Option<(usize, u64)> {
    let ilen = hex_decode(hex, ir)?;
    let mut irc = [0u8; BIN_BUF];
    irc[..ilen].copy_from_slice(&ir[..ilen]);
    lower_flat(&irc[..ilen], code).ok()
}

pub fn compile_to_code_buf(
    schema: &[u8],
    params: &[u8],
    src: &[u8],
    code: &mut [u8],
    out: &mut [u8],
) -> Result<(usize, u64), usize> {
    let mut ir = [0u8; 512];
    let ilen = match celc_compile_auto(schema, params, src, &mut ir) {
        Ok(n) => n,
        Err(e) => {
            let mut p = append(out, 0, b"error: compile failed: ");
            p = append(out, p, celc_err_name(e));
            p = append(out, p, b"\n");
            return Err(p);
        }
    };
    match lower_flat(&ir[..ilen], code) {
        Ok(v) => Ok(v),
        Err(_) => Err(append(out, 0, b"error: IR failed to lower\n")),
    }
}

pub fn emit_sealed_buf(cont: &[u8], n: usize, digest: &[u8; 32], out: &mut [u8]) -> (usize, i32) {
    let (mut p, rc) = print_digest(out, digest);
    if rc != 0 {
        return (p, rc);
    }
    p = append_hex(out, p, &cont[..n]);
    p = append(out, p, b"\n");
    (p, 0)
}

/// Copy `src` into `dst` at `at`, bounded by `dst`'s capacity; returns the new
/// write cursor. The CLI's one output-appending primitive.
pub fn append(dst: &mut [u8], at: usize, src: &[u8]) -> usize {
    let n = src.len().min(dst.len().saturating_sub(at));
    dst[at..at + n].copy_from_slice(&src[..n]);
    at + n
}

/// Hex-encode `src` straight into `dst` a byte at a time — no second full-size
/// buffer, so the source can be as large as the output has room for.
pub fn append_hex(dst: &mut [u8], at: usize, src: &[u8]) -> usize {
    let mut p = at;
    for b in src {
        let mut pair = [0u8; 2];
        if hex_encode(core::slice::from_ref(b), &mut pair).is_none() {
            break;
        }
        p = append(dst, p, &pair);
    }
    p
}

/// Append `n` as decimal text.
pub fn append_u32(dst: &mut [u8], at: usize, mut n: u32) -> usize {
    if at >= dst.len() {
        return at;
    }
    if n == 0 {
        dst[at] = b'0';
        return at + 1;
    }
    let mut tmp = [0u8; 10];
    let mut i = 0;
    while n > 0 && i < tmp.len() {
        tmp[i] = b'0' + (n % 10) as u8;
        n /= 10;
        i += 1;
    }
    let mut p = at;
    while i > 0 && p < dst.len() {
        i -= 1;
        dst[p] = tmp[i];
        p += 1;
    }
    p
}

/// Compile one CEL body against a schema+params into flat IR, then lower it to
/// bytecode. Returns `(bytecode_len, cost_bound)`.
pub fn compile_one(
    schema: &[u8],
    params: &[u8],
    body: &[u8],
    ir: &mut [u8],
    code: &mut [u8],
) -> Result<(usize, u64), ()> {
    let ilen = celc_compile_auto(schema, params, body, ir).map_err(|_| ())?;
    lower_flat(&ir[..ilen], code).map_err(|_| ())
}

/// The stable "compile failed for <name>" reply line.
pub fn compile_failed(out: &mut [u8], name: &[u8]) -> usize {
    let mut p = append(out, 0, b"error: compile failed for ");
    p = append(out, p, name);
    append(out, p, b"\n")
}

/// `ArtefactKind::Resource` — the kind an `effect` stage targets.
pub const KIND_RESOURCE: i32 = 8;
pub fn agg_kind_to_op(kind: u8) -> i32 {
    match kind {
        AGG_SUM => OP_SUM,
        AGG_COUNT => OP_COUNT,
        AGG_AVG => OP_AVG,
        AGG_MIN => OP_MIN,
        AGG_MAX => OP_MAX,
        AGG_DISTINCT => OP_DISTINCT,
        AGG_TOPK => OP_TOPK,
        _ => OP_QUANTILE,
    }
}
pub fn declared_kind(arena: &UprocArena, doc: &Doc, src: &[u8], symbol: &[u8]) -> i32 {
    for i in 0..doc.n_transformations {
        if arena.transformations[i].name.of(src) == symbol {
            return KIND_TRANSFORMATION;
        }
    }
    for i in 0..doc.n_decisions {
        if arena.decisions[i].name.of(src) == symbol {
            return KIND_DECISION;
        }
    }
    KIND_EXPRESSION
}
pub fn append_uproc_reason(out: &mut [u8], at: usize, k: UprocErrorKind) -> usize {
    match k {
        UprocErrorKind::ExpectedModule => append(out, at, b"expected a `module` header"),
        UprocErrorKind::ExpectedIdent => append(out, at, b"expected an identifier"),
        UprocErrorKind::ExpectedByte(_) => append(out, at, b"unexpected byte"),
        UprocErrorKind::ExpectedType => append(out, at, b"expected a type"),
        UprocErrorKind::ExpectedInt => append(out, at, b"expected an integer"),
        UprocErrorKind::ExpectedString => append(out, at, b"expected a string"),
        UprocErrorKind::ExpectedArrow => append(out, at, b"expected `->`"),
        UprocErrorKind::UnknownDeclaration => append(out, at, b"unknown declaration"),
        UprocErrorKind::UnknownOperator => append(out, at, b"unknown operator kind"),
        UprocErrorKind::UnterminatedBody => append(out, at, b"unterminated body"),
        UprocErrorKind::TrailingInput => {
            append(out, at, b"content after the module's closing brace")
        }
        UprocErrorKind::MissingClause => append(out, at, b"a required clause is missing"),
        UprocErrorKind::TooMany => append(out, at, b"too many declarations for this node"),
    }
}

/// Author every artefact a `.uproc` declares: parse it, emit the schema, compile
/// and seal each expression / transformation / decision / pipeline / aggregation,
/// then seal a Module referencing them all. Pure compute over caller buffers — no
/// ABI, no syscalls — so the host harness drives it directly.
#[allow(
    clippy::too_many_arguments,
    reason = "the CLI State buffers, passed explicitly so the core stays a flat function"
)]
pub fn author_document(
    src: &[u8],
    arena: &mut UprocArena,
    st_prog: &mut [u8],
    st_code: &mut [u8],
    st_cont: &mut [u8],
    st_scratch: &mut [u8],
    st_out: &mut [u8],
) -> (usize, i32) {
    let doc = match uproc_parse(src, &mut *arena) {
        Ok(d) => d,
        Err(e) => {
            let (line, col) = uproc_line_col(src, e.offset);
            let mut p = append(&mut *st_out, 0, b"error: ");
            p = append_uproc_reason(&mut *st_out, p, e.kind);
            p = append(&mut *st_out, p, b" at line ");
            p = append_u32(&mut *st_out, p, line as u32);
            p = append(&mut *st_out, p, b" column ");
            p = append_u32(&mut *st_out, p, col as u32);
            p = append(&mut *st_out, p, b"\n");
            return (p, 1);
        }
    };

    let Ok(slen) = uproc_schema_text(src, &doc, arena, &mut *st_prog) else {
        return (append(&mut *st_out, 0, b"error: schema too large\n"), 1);
    };
    let (pkg, _sym) = split_qname(doc.module.of(src));

    // Every sealed artefact becomes a Module ref, so the document produces a
    // deployment unit rather than a loose pile of digests.
    const MAX_ART: usize = 24;
    let mut rdigest = [[0u8; 32]; MAX_ART];
    let mut rsym = [b"".as_slice(); MAX_ART];
    let mut rkind = [0i32; MAX_ART];
    let mut nrefs = 0usize;

    let mut out_p = 0usize;
    let emit = |out: &mut [u8], at: usize, name: &[u8], digest: &[u8; 32]| -> usize {
        let mut dhex = [0u8; 64];
        let mut p = append(out, at, name);
        p = append(out, p, b" ");
        if let Some(dl) = hex_encode(digest, &mut dhex) {
            p = append(out, p, &dhex[..dl]);
        }
        append(out, p, b"\n")
    };

    // ---- Expressions and Transformations: one compiled body each ----------
    for i in 0..doc.n_expressions + doc.n_transformations {
        let is_expr = i < doc.n_expressions;
        let d = if is_expr {
            arena.expressions[i]
        } else {
            arena.transformations[i - doc.n_expressions]
        };
        let mut params = [0u8; 256];
        let Ok(plen) = uproc_params_text(src, d.param_name, d.param_type, &mut params) else {
            return (append(&mut *st_out, 0, b"error: params too large\n"), 1);
        };
        let mut code = [0u8; 512];
        let (clen, cost) = match compile_one(
            &st_prog[..slen],
            &params[..plen],
            d.body.of(src),
            &mut *st_code,
            &mut code,
        ) {
            Ok(v) => v,
            Err(()) => return (compile_failed(&mut *st_out, d.name.of(src)), 1),
        };
        // Each artefact kind declares its own capability; they are part of the
        // sealed header and therefore part of the identity.
        let header = HeaderSpec {
            package: pkg,
            symbol: d.name.of(src),
            kind: if is_expr {
                KIND_EXPRESSION
            } else {
                KIND_TRANSFORMATION
            },
            capability: if is_expr {
                b"expression.cel.strict.v1".as_slice()
            } else {
                b"transformation.cel.v1".as_slice()
            },
        };
        let sealed = if is_expr {
            seal_expression(
                &mut *st_cont,
                &mut *st_scratch,
                &header,
                &ExpressionSpec {
                    param_name: d.param_name.of(src),
                    param_message: param_message_name(d.param_type.of(src)),
                    result_type: d.result_type.of(src),
                    source: d.body.of(src),
                    bytecode: &code[..clen],
                    max_cost: cost,
                },
            )
        } else {
            seal_transformation(
                &mut *st_cont,
                &mut *st_scratch,
                &header,
                &TransformationSpec {
                    input_type: d.param_type.of(src),
                    output_type: d.result_type.of(src),
                    source: d.body.of(src),
                    bytecode: &code[..clen],
                    max_cost: cost,
                },
            )
        };
        let Ok((_, digest)) = sealed else {
            return (append(&mut *st_out, 0, b"error: seal failed\n"), 1);
        };
        if nrefs < MAX_ART {
            rdigest[nrefs] = digest;
            rsym[nrefs] = d.name.of(src);
            rkind[nrefs] = if is_expr {
                KIND_EXPRESSION
            } else {
                KIND_TRANSFORMATION
            };
            nrefs += 1;
        }
        out_p = emit(&mut *st_out, out_p, d.name.of(src), &digest);
    }

    // ---- Decisions: a compiled when + outcome per rule, plus a default ----
    for i in 0..doc.n_decisions {
        let d = arena.decisions[i];
        let mut params = [0u8; 256];
        let Ok(plen) = uproc_params_text(src, d.param_name, d.input_type, &mut params) else {
            return (append(&mut *st_out, 0, b"error: params too large\n"), 1);
        };
        const MAX_RULE: usize = 8;
        if d.n_rules as usize > MAX_RULE {
            return (
                append(&mut *st_out, 0, b"error: too many rules for this node\n"),
                1,
            );
        }
        let mut wcode = [[0u8; 256]; MAX_RULE];
        let mut ocode = [[0u8; 256]; MAX_RULE];
        let mut wlen = [0usize; MAX_RULE];
        let mut olen = [0usize; MAX_RULE];
        let mut wcost = [0u64; MAX_RULE];
        let mut ocost = [0u64; MAX_RULE];
        for k in 0..d.n_rules as usize {
            let r = arena.rules[d.first_rule as usize + k];
            match compile_one(
                &st_prog[..slen],
                &params[..plen],
                r.when.of(src),
                &mut *st_code,
                &mut wcode[k],
            ) {
                Ok((l, c)) => {
                    wlen[k] = l;
                    wcost[k] = c;
                }
                Err(()) => return (compile_failed(&mut *st_out, d.name.of(src)), 1),
            }
            match compile_one(
                &st_prog[..slen],
                &params[..plen],
                r.outcome.of(src),
                &mut *st_code,
                &mut ocode[k],
            ) {
                Ok((l, c)) => {
                    olen[k] = l;
                    ocost[k] = c;
                }
                Err(()) => return (compile_failed(&mut *st_out, d.name.of(src)), 1),
            }
        }
        let mut dcode = [0u8; 256];
        let (dlen, dcost) = match compile_one(
            &st_prog[..slen],
            &params[..plen],
            d.default.of(src),
            &mut *st_code,
            &mut dcode,
        ) {
            Ok(v) => v,
            Err(()) => return (compile_failed(&mut *st_out, d.name.of(src)), 1),
        };
        let mut rules = [RuleSpec {
            name: b"",
            priority: 0,
            when_source: b"",
            when_code: b"",
            when_cost: 0,
            outcome_source: b"",
            outcome_code: b"",
            outcome_cost: 0,
        }; MAX_RULE];
        // Rules are named `rule_<i>` and priced `count - i` — earlier rules bind
        // tighter, which the `first` hit policy honours by order anyway. Both are
        // sealed into the artefact, so both are part of its identity.
        let mut rname = [[0u8; 16]; MAX_RULE];
        let mut rnlen = [0usize; MAX_RULE];
        for k in 0..d.n_rules as usize {
            let mut q = append(&mut rname[k], 0, b"rule_");
            q = append_u32(&mut rname[k], q, k as u32);
            rnlen[k] = q;
        }
        let count = d.n_rules as i32;
        for k in 0..d.n_rules as usize {
            let r = arena.rules[d.first_rule as usize + k];
            rules[k] = RuleSpec {
                name: &rname[k][..rnlen[k]],
                priority: count - k as i32,
                when_source: r.when.of(src),
                when_code: &wcode[k][..wlen[k]],
                when_cost: wcost[k],
                outcome_source: r.outcome.of(src),
                outcome_code: &ocode[k][..olen[k]],
                outcome_cost: ocost[k],
            };
        }
        let header = HeaderSpec {
            package: pkg,
            symbol: d.name.of(src),
            kind: KIND_DECISION,
            capability: b"decision.hit-policy.first.v1",
        };
        let Ok((_, digest)) = seal_decision(
            &mut *st_cont,
            &mut *st_scratch,
            &header,
            &DecisionSpec {
                input_type: d.input_type.of(src),
                output_type: d.output_type.of(src),
                default_source: d.default.of(src),
                default_code: &dcode[..dlen],
                default_cost: dcost,
                // The host always seals decisions explainable; it is part of the
                // artefact and therefore of its identity.
                explain: true,
            },
            &rules[..d.n_rules as usize],
        ) else {
            return (append(&mut *st_out, 0, b"error: seal failed\n"), 1);
        };
        if nrefs < MAX_ART {
            rdigest[nrefs] = digest;
            rsym[nrefs] = d.name.of(src);
            rkind[nrefs] = KIND_DECISION;
            nrefs += 1;
        }
        out_p = emit(&mut *st_out, out_p, d.name.of(src), &digest);
    }

    // ---- Pipelines: structure only; stages name artefacts, they do not
    //      embed logic, so nothing here is compiled --------------------------
    for i in 0..doc.n_pipelines {
        let pl = arena.pipelines[i];
        const MAX_ST: usize = 8;
        if pl.n_stages as usize > MAX_ST {
            return (
                append(&mut *st_out, 0, b"error: too many stages for this node\n"),
                1,
            );
        }
        let mut stages = [StageSpec {
            name: b"",
            target_package: b"",
            target_symbol: b"",
            target_kind: KIND_EXPRESSION,
            operation: b"",
            argument: b"",
        }; MAX_ST];
        for (k, st) in arena
            .stages
            .iter()
            .skip(pl.first_stage as usize)
            .take(pl.n_stages as usize)
            .enumerate()
        {
            let st = *st;
            let target = st.target.of(src);
            let (tp, ts) = split_qname(target);
            stages[k] = StageSpec {
                name: st.name.of(src),
                target_package: if tp.is_empty() { pkg } else { tp },
                target_symbol: ts,
                // An EFFECT targets a Resource; only a `call` targets an
                // artefact whose kind the document declares.
                target_kind: if st.kind == STAGE_EFFECT {
                    KIND_RESOURCE
                } else {
                    declared_kind(&*arena, &doc, src, ts)
                },
                operation: st.operation.of(src),
                argument: if st.arg0.is_empty() {
                    if st.n_args > 0 {
                        arena.args[st.first_arg as usize].of(src)
                    } else {
                        b""
                    }
                } else {
                    st.arg0.of(src)
                },
            };
        }
        let header = HeaderSpec {
            package: pkg,
            symbol: pl.name.of(src),
            kind: KIND_PIPELINE,
            capability: b"pipeline.effects.v1",
        };
        let Ok((_, digest)) = seal_pipeline(
            &mut *st_cont,
            &mut *st_scratch,
            &header,
            &PipelineSpec {
                input_port: pl.port_name.of(src),
                input_type: pl.input_type.of(src),
                output_type: pl.output_type.of(src),
                commit_after: pl.commit_after.of(src),
                return_stage: pl.return_stage.of(src),
            },
            &stages[..pl.n_stages as usize],
        ) else {
            return (append(&mut *st_out, 0, b"error: seal failed\n"), 1);
        };
        if nrefs < MAX_ART {
            rdigest[nrefs] = digest;
            rsym[nrefs] = pl.name.of(src);
            rkind[nrefs] = KIND_PIPELINE;
            nrefs += 1;
        }
        out_p = emit(&mut *st_out, out_p, pl.name.of(src), &digest);
    }

    // ---- Aggregations: key/event_time/selectors over the input, and `emit`
    //      over a SYNTHESIZED context the engine builds from the finished
    //      window (it does not read the input event) ------------------------
    for i in 0..doc.n_aggregations {
        let ag = arena.aggregations[i];
        const MAX_OP: usize = 8;
        if ag.n_ops as usize > MAX_OP {
            return (
                append(
                    &mut *st_out,
                    0,
                    b"error: too many operators for this node\n",
                ),
                1,
            );
        }
        let mut params = [0u8; 256];
        let Ok(plen) = uproc_params_text(src, ag.param_name, ag.input_type, &mut params) else {
            return (append(&mut *st_out, 0, b"error: params too large\n"), 1);
        };

        let mut kcode = [0u8; 256];
        let (klen, kcost) = match compile_one(
            &st_prog[..slen],
            &params[..plen],
            ag.key.of(src),
            &mut *st_code,
            &mut kcode,
        ) {
            Ok(v) => v,
            Err(()) => return (compile_failed(&mut *st_out, ag.name.of(src)), 1),
        };
        let mut tcode = [0u8; 256];
        let (tlen, tcost) = match compile_one(
            &st_prog[..slen],
            &params[..plen],
            ag.event_time.of(src),
            &mut *st_code,
            &mut tcode,
        ) {
            Ok(v) => v,
            Err(()) => return (compile_failed(&mut *st_out, ag.name.of(src)), 1),
        };

        let mut scode = [[0u8; 256]; MAX_OP];
        let mut slen_op = [0usize; MAX_OP];
        let mut scost = [0u64; MAX_OP];
        for k in 0..ag.n_ops as usize {
            let o = arena.operators[ag.first_op as usize + k];
            // Count selects nothing — it counts events, so it has no selector.
            if o.selector.is_empty() {
                continue;
            }
            match compile_one(
                &st_prog[..slen],
                &params[..plen],
                o.selector.of(src),
                &mut *st_code,
                &mut scode[k],
            ) {
                Ok((l, c)) => {
                    slen_op[k] = l;
                    scost[k] = c;
                }
                Err(()) => return (compile_failed(&mut *st_out, ag.name.of(src)), 1),
            }
        }

        // `emit` reads `ctx`, whose type is synthesized from the operator set.
        let Ok(elen_schema) = uproc_agg_emit_schema(src, &ag, arena, pkg, &mut *st_prog, slen)
        else {
            return (
                append(&mut *st_out, 0, b"error: emit schema too large\n"),
                1,
            );
        };
        let mut eparams = [0u8; 256];
        let Ok(eplen) = uproc_agg_emit_params(src, &ag, pkg, &mut eparams) else {
            return (append(&mut *st_out, 0, b"error: params too large\n"), 1);
        };
        let mut ecode = [0u8; 512];
        let (eclen, ecost) = match compile_one(
            &st_prog[..elen_schema],
            &eparams[..eplen],
            ag.emit.of(src),
            &mut *st_code,
            &mut ecode,
        ) {
            Ok(v) => v,
            Err(()) => return (compile_failed(&mut *st_out, ag.name.of(src)), 1),
        };

        let mut ops = [OperatorSpec {
            name: b"",
            kind: OP_COUNT,
            selector_source: b"",
            selector_code: b"",
            selector_cost: 0,
        }; MAX_OP];
        for k in 0..ag.n_ops as usize {
            let o = arena.operators[ag.first_op as usize + k];
            ops[k] = OperatorSpec {
                name: o.name.of(src),
                kind: agg_kind_to_op(o.kind),
                selector_source: o.selector.of(src),
                selector_code: &scode[k][..slen_op[k]],
                selector_cost: scost[k],
            };
        }

        // The state type is the synthesized `<pkg>.<agg>.State`.
        let mut state_ty = [0u8; 128];
        let mut sp = 0usize;
        if !pkg.is_empty() {
            sp = append(&mut state_ty, sp, pkg);
            sp = append(&mut state_ty, sp, b".");
        }
        sp = append(&mut state_ty, sp, ag.name.of(src));
        sp = append(&mut state_ty, sp, b".State");

        let header = HeaderSpec {
            package: pkg,
            symbol: ag.name.of(src),
            kind: KIND_AGGREGATION,
            capability: b"aggregation.event-time.sliding.v1",
        };
        let Ok((_, digest)) = seal_aggregation(
            &mut *st_cont,
            &mut *st_scratch,
            &header,
            &AggregationSpec {
                input_type: ag.input_type.of(src),
                state_type: &state_ty[..sp],
                output_type: ag.output_type.of(src),
                key_source: ag.key.of(src),
                key_code: &kcode[..klen],
                key_cost: kcost,
                time_source: ag.event_time.of(src),
                time_code: &tcode[..tlen],
                time_cost: tcost,
                window_size_ms: ag.window_size_ms as u64,
                lateness_ms: ag.lateness_ms as u64,
                guard_ms: ag.guard_ms as u64,
                emit_source: ag.emit.of(src),
                emit_code: &ecode[..eclen],
                emit_cost: ecost,
                max_lanes: ag.max_lanes,
                // The warn threshold is derived, not authored: three quarters of
                // the ceiling. It is sealed, so it is part of the identity.
                warn_lanes: ag.max_lanes * 3 / 4,
            },
            &ops[..ag.n_ops as usize],
        ) else {
            return (append(&mut *st_out, 0, b"error: seal failed\n"), 1);
        };
        if nrefs < MAX_ART {
            rdigest[nrefs] = digest;
            rsym[nrefs] = ag.name.of(src);
            rkind[nrefs] = KIND_AGGREGATION;
            nrefs += 1;
        }
        out_p = emit(&mut *st_out, out_p, ag.name.of(src), &digest);
    }

    // ---- The Module itself: the deployment unit the artefacts belong to ----
    {
        let mut refs = [ModuleRef {
            package: &[],
            symbol: &[],
            kind: 0,
            digest: &[],
        }; MAX_ART];
        for k in 0..nrefs {
            refs[k] = ModuleRef {
                package: pkg,
                symbol: rsym[k],
                kind: rkind[k],
                digest: &rdigest[k],
            };
        }
        // Resource declarations become binding REQUIREMENTS the deployment must
        // satisfy; `entry` declarations become the module's activatable surfaces.
        const MAX_BIND: usize = 8;
        let mut binds = [BindingSpec {
            package: &[],
            symbol: &[],
            required: false,
        }; MAX_BIND];
        let nb = (doc.n_resources).min(MAX_BIND);
        for (k, r) in arena.resources.iter().take(nb).enumerate() {
            let r = *r;
            binds[k] = BindingSpec {
                package: pkg,
                symbol: r.name.of(src),
                required: r.required,
            };
        }
        let mut ents = [EntrySpec {
            name: &[],
            package: &[],
            symbol: &[],
            digest: &[],
        }; MAX_BIND];
        let mut ne = 0usize;
        for k in 0..doc.n_entries.min(MAX_BIND) {
            let e = arena.entries[k];
            // An entry names a pipeline; its digest is the one just sealed.
            let target = e.pipeline.of(src);
            for r in 0..nrefs {
                if rkind[r] == KIND_PIPELINE && rsym[r] == target {
                    ents[ne] = EntrySpec {
                        name: e.name.of(src),
                        package: pkg,
                        symbol: target,
                        digest: &rdigest[r],
                    };
                    ne += 1;
                    break;
                }
            }
        }
        let header = HeaderSpec {
            package: pkg,
            symbol: _sym,
            kind: KIND_MODULE,
            capability: b"",
        };
        let spec = ModuleSpec {
            source_revision: doc.provenance_revision.of(src),
            build_toolchain: if doc.provenance_toolchain.is_empty() {
                b"chronicle-authoring".as_slice()
            } else {
                doc.provenance_toolchain.of(src)
            },
            provenance_class: PROVENANCE_LOCAL_BUILD,
        };
        let Ok((_, mdigest)) = seal_module(
            &mut *st_cont,
            &mut *st_scratch,
            &header,
            &refs[..nrefs],
            &binds[..nb],
            &ents[..ne],
            &spec,
            &[],
        ) else {
            return (append(&mut *st_out, 0, b"error: module seal failed\n"), 1);
        };
        out_p = emit(&mut *st_out, out_p, b"MODULE", &mdigest);
    }

    if out_p == 0 {
        out_p = append(&mut *st_out, out_p, b"no artefacts declared\n");
    }
    (out_p, 0)
}

/// Build a deployment GRAPH from a `.uproc`: parse it, resolve the named pipeline
/// to its stages, and emit the graph plan. Flat over caller buffers so the host
/// harness drives it directly.
#[allow(
    clippy::too_many_arguments,
    reason = "the CLI State buffers, passed explicitly so the core stays a flat function"
)]
pub fn graph_document(
    src: &[u8],
    arena: &mut UprocArena,
    st_prog: &mut [u8],
    st_code: &mut [u8],
    st_cont: &mut [u8],
    st_scratch: &mut [u8],
    st_out: &mut [u8],
    pipeline: &[u8],
    target: &[u8],
) -> (usize, i32) {
    let doc = match uproc_parse(src, &mut *arena) {
        Ok(d) => d,
        Err(e) => {
            let (line, col) = uproc_line_col(src, e.offset);
            let mut p = append(&mut *st_out, 0, b"error: ");
            p = append_uproc_reason(&mut *st_out, p, e.kind);
            p = append(&mut *st_out, p, b" at line ");
            p = append_u32(&mut *st_out, p, line as u32);
            p = append(&mut *st_out, p, b" column ");
            p = append_u32(&mut *st_out, p, col as u32);
            p = append(&mut *st_out, p, b"\n");
            return (p, 1);
        }
    };

    let Ok(slen) = uproc_schema_text(src, &doc, arena, &mut *st_prog) else {
        return (append(&mut *st_out, 0, b"error: schema too large\n"), 1);
    };
    let schema = &st_prog[..slen];

    // Find the named pipeline.
    let mut pipe = None;
    for i in 0..doc.n_pipelines {
        if arena.pipelines[i].name.of(src) == pipeline {
            pipe = Some(arena.pipelines[i]);
            break;
        }
    }
    let Some(pipe) = pipe else {
        let mut p = append(&mut *st_out, 0, b"error: no pipeline named '");
        p = append(&mut *st_out, p, pipeline);
        p = append(&mut *st_out, p, b"'\n");
        return (p, 1);
    };

    // Compile every stage into the plan. Stage IR accumulates end to end in
    // `cont`; `plan[]` holds slices into it.
    const MAX_PLAN_STAGES: usize = 16;
    let mut spans = [(0usize, 0usize); MAX_PLAN_STAGES];
    let mut kinds = [0u8; MAX_PLAN_STAGES]; // 0 compute, 1 decision
    let mut used = 0usize;
    let mut n_stages = 0usize;

    for k in 0..pipe.n_stages as usize {
        if n_stages >= MAX_PLAN_STAGES {
            return (append(&mut *st_out, 0, b"error: too many stages\n"), 1);
        }
        let st = arena.stages[pipe.first_stage as usize + k];
        if st.kind == STAGE_EFFECT {
            let mut p = append(&mut *st_out, 0, b"error: stage '");
            p = append(&mut *st_out, p, st.name.of(src));
            p = append(
                &mut *st_out,
                p,
                b"' is an effect; a connector binding is not in the document\n",
            );
            return (p, 1);
        }
        let target_name = st.target.of(src);

        // A Call names either a transformation (a compute stage) or a decision
        // (its own node). Transformations are searched first because that is the
        // common case; a name in both would be a document the parser rejects.
        let mut done = false;
        for i in 0..doc.n_transformations {
            let f = arena.transformations[i];
            if f.name.of(src) != target_name {
                continue;
            }
            let (irn, is_msg) = match compile_ir(
                schema,
                f.body.of(src),
                f.param_name.of(src),
                f.param_type.of(src),
                &mut st_cont[used..],
            ) {
                Ok(v) => v,
                Err(_) => {
                    let mut p = append(&mut *st_out, 0, b"error: stage '");
                    p = append(&mut *st_out, p, target_name);
                    p = append(&mut *st_out, p, b"' did not compile\n");
                    return (p, 1);
                }
            };
            if !is_msg {
                let mut p = append(&mut *st_out, 0, b"error: stage '");
                p = append(&mut *st_out, p, target_name);
                p = append(&mut *st_out, p, b"' must construct a message\n");
                return (p, 1);
            }
            spans[n_stages] = (used, irn);
            kinds[n_stages] = 0;
            used += irn;
            n_stages += 1;
            done = true;
            break;
        }
        if done {
            continue;
        }

        for i in 0..doc.n_decisions {
            let d = arena.decisions[i];
            if d.name.of(src) != target_name {
                continue;
            }
            let mut rules = [RuleSource {
                when: b"",
                outcome: b"",
            }; MAX_ITEMS];
            let nr = d.n_rules as usize;
            if nr > MAX_ITEMS {
                return (append(&mut *st_out, 0, b"error: too many rules\n"), 1);
            }
            for (r, rd) in arena
                .rules
                .iter()
                .skip(d.first_rule as usize)
                .take(nr)
                .enumerate()
            {
                rules[r] = RuleSource {
                    when: rd.when.of(src),
                    outcome: rd.outcome.of(src),
                };
            }
            let dn = match compile_decision(
                schema,
                d.param_name.of(src),
                d.input_type.of(src),
                &rules[..nr],
                d.default.of(src),
                &mut st_cont[used..],
                &mut *st_code,
            ) {
                Ok(v) => v,
                Err(_) => {
                    let mut p = append(&mut *st_out, 0, b"error: decision '");
                    p = append(&mut *st_out, p, target_name);
                    p = append(&mut *st_out, p, b"' did not compile\n");
                    return (p, 1);
                }
            };
            spans[n_stages] = (used, dn);
            kinds[n_stages] = 1;
            used += dn;
            n_stages += 1;
            done = true;
            break;
        }
        if !done {
            let mut p = append(&mut *st_out, 0, b"error: unknown artefact '");
            p = append(&mut *st_out, p, target_name);
            p = append(&mut *st_out, p, b"'\n");
            return (p, 1);
        }
    }

    let mut plan = [PlanStage::Compute { stage_ir: b"" }; MAX_PLAN_STAGES];
    for i in 0..n_stages {
        let (off, len) = spans[i];
        let bytes = &st_cont[off..off + len];
        plan[i] = if kinds[i] == 0 {
            PlanStage::Compute { stage_ir: bytes }
        } else {
            PlanStage::Decision { container: bytes }
        };
    }

    let profile = if target == b"linux" {
        TargetProfile::host()
    } else {
        TargetProfile::embedded(target)
    };
    match lower_pipeline_with(
        &plan[..n_stages],
        &profile,
        1000,
        &mut *st_out,
        &mut *st_scratch,
    ) {
        Ok(p) => (p, 0),
        Err(_) => (append(&mut *st_out, 0, b"error: graph too large\n"), 1),
    }
}

/// Compile and seal an AGGREGATION artefact from CLI argv (key / event-time /
/// selector programs, windows, operators). Flat over caller buffers.
#[allow(
    clippy::too_many_arguments,
    reason = "the CLI State buffers, passed explicitly so the core stays a flat function"
)]
pub fn agg_from_argv(
    arec: &[u8],
    argv: &[(usize, usize)],
    argc: usize,
    st_cont: &mut [u8],
    st_out: &mut [u8],
) -> (usize, i32) {
    if argc < 9 {
        return (
            append(
                &mut *st_out,
                0,
                b"error: agg needs <window> <lateness> <lanes> <step> <horizon> \
                  <key_ir> <time_ir> <emit_ir> [<kind>:<sel_ir>]...\n",
            ),
            1,
        );
    }
    let mut nums = [0i64; 5];
    for (k, slot) in nums.iter_mut().enumerate() {
        let (a, b) = argv[1 + k];
        match parse_i64(&arec[a..b]) {
            Some(v) => *slot = v,
            None => {
                return (
                    append(&mut *st_out, 0, b"error: window scalars must be integers\n"),
                    1,
                )
            }
        }
    }
    let mut w = 0usize;
    // 36-byte header: window, lateness, lanes(u32), step, horizon.
    st_cont[0..8].copy_from_slice(&nums[0].to_le_bytes());
    st_cont[8..16].copy_from_slice(&nums[1].to_le_bytes());
    st_cont[16..20].copy_from_slice(&(nums[2] as u32).to_le_bytes());
    st_cont[20..28].copy_from_slice(&nums[3].to_le_bytes());
    st_cont[28..36].copy_from_slice(&nums[4].to_le_bytes());
    w += 36;

    // key / time / emit, each a flat checked IR.
    for k in 0..3 {
        let (a, b) = argv[6 + k];
        let mut irc = [0u8; BIN_BUF];
        let Some(ilen) = hex_decode(&arec[a..b], &mut irc) else {
            return (
                append(&mut *st_out, 0, b"error: a program is not valid IR hex\n"),
                1,
            );
        };
        if !put_ir_prog(&mut *st_cont, &mut w, &irc[..ilen]) {
            return (append(&mut *st_out, 0, b"error: container too large\n"), 1);
        }
    }

    // Operators: `<kind>:<selector_ir_hex>`, selector optionally empty.
    let nops = argc - 9;
    if nops > 255 {
        return (append(&mut *st_out, 0, b"error: too many operators\n"), 1);
    }
    if w >= st_cont.len() {
        return (append(&mut *st_out, 0, b"error: container too large\n"), 1);
    }
    st_cont[w] = nops as u8;
    w += 1;
    for k in 0..nops {
        let (a, b) = argv[9 + k];
        let arg = &arec[a..b];
        let Some(colon) = arg.iter().position(|c| *c == b':') else {
            return (
                append(
                    &mut *st_out,
                    0,
                    b"error: an operator must be <kind>:<sel_ir>\n",
                ),
                1,
            );
        };
        let Some(kind) = parse_i64(&arg[..colon]).filter(|k| (0..=255).contains(k)) else {
            return (
                append(&mut *st_out, 0, b"error: operator kind must be 0..255\n"),
                1,
            );
        };
        let mut irc = [0u8; BIN_BUF];
        let sel = &arg[colon + 1..];
        let ilen = if sel.is_empty() {
            0
        } else {
            match hex_decode(sel, &mut irc) {
                Some(n) => n,
                None => {
                    return (
                        append(&mut *st_out, 0, b"error: a selector is not valid IR hex\n"),
                        1,
                    )
                }
            }
        };
        if w >= st_cont.len() {
            return (append(&mut *st_out, 0, b"error: container too large\n"), 1);
        }
        st_cont[w] = kind as u8;
        w += 1;
        if !put_ir_prog(&mut *st_cont, &mut w, &irc[..ilen]) {
            return (append(&mut *st_out, 0, b"error: container too large\n"), 1);
        }
    }
    emit_hex_buf(st_cont, w, &mut *st_out)
}

/// Assemble and seal a RELEASE artefact from CLI argv. Flat over caller buffers.
#[allow(
    clippy::too_many_arguments,
    reason = "the CLI State buffers, passed explicitly so the core stays a flat function"
)]
pub fn release_from_argv(
    arec: &[u8],
    argv: &[(usize, usize)],
    argc: usize,
    st_prog: &mut [u8],
    st_cont: &mut [u8],
    st_scratch: &mut [u8],
    st_out: &mut [u8],
) -> (usize, i32) {
    if argc < 3 {
        return (
            append(
                &mut *st_out,
                0,
                b"error: release needs <default_tag> <tag>:<prog_hex>...\n",
            ),
            1,
        );
    }
    let (a, b) = argv[1];
    let default_tag = &arec[a..b];

    let n = argc - 2;
    if n > MAX_VERSIONS {
        return (append(&mut *st_out, 0, b"error: too many versions\n"), 1);
    }
    // Programs decode end to end into `cont`; the specs borrow from it.
    let mut spans = [(0usize, 0usize); MAX_VERSIONS];
    let mut tags = [b"".as_slice(); MAX_VERSIONS];
    let mut used = 0usize;
    for (i, (x, y)) in argv[2..argc].iter().enumerate() {
        let arg = &arec[*x..*y];
        // `<tag>:<hex>` — split at the FIRST colon, since hex has none.
        let Some(c) = arg.iter().position(|&ch| ch == b':') else {
            return (
                append(&mut *st_out, 0, b"error: expected <tag>:<prog_hex>\n"),
                1,
            );
        };
        tags[i] = &arg[..c];
        let Some(pn) = hex_decode(&arg[c + 1..], &mut st_cont[used..]) else {
            return (
                append(&mut *st_out, 0, b"error: program is not valid hex\n"),
                1,
            );
        };
        spans[i] = (used, pn);
        used += pn;
    }

    let mut specs = [VersionSpec {
        tag: b"",
        program: b"",
        digest: [0u8; VERSION_DIGEST_LEN],
    }; MAX_VERSIONS];
    for i in 0..n {
        let (off, len) = spans[i];
        let program = &st_cont[off..off + len];
        specs[i] = VersionSpec {
            tag: tags[i],
            program,
            digest: version_digest(program),
        };
    }

    // Validate BEFORE building: an unknown default tag would otherwise fall back
    // to index 0 and silently serve whichever version was listed first.
    let refs: [VersionRef; MAX_VERSIONS] = core::array::from_fn(|i| VersionRef {
        tag: specs[i].tag,
        program: specs[i].program,
        digest: &specs[i].digest,
    });
    let manifest = ManifestRef {
        versions: &refs[..n],
        default_tag,
    };
    if let Err(e) = manifest.validate() {
        let mut p = append(&mut *st_out, 0, b"error: ");
        p = append_release_reason(&mut *st_out, p, e);
        p = append(&mut *st_out, p, b"\n");
        return (p, 1);
    }

    let Some(bn) = build_versions_param(&specs[..n], default_tag, &mut *st_scratch) else {
        return (
            append(&mut *st_out, 0, b"error: versions param too large\n"),
            1,
        );
    };
    let Some(hn) = hex_encode(&st_scratch[..bn], &mut *st_prog) else {
        return (append(&mut *st_out, 0, b"error: output too large\n"), 1);
    };
    let mut p = append(&mut *st_out, 0, &st_prog[..hn]);
    p = append(&mut *st_out, p, b"\n");
    (p, 0)
}
