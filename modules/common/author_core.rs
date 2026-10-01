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

/// Split a NUL-separated argv record into `(start, end)` spans over `rec`.
///
/// `cli_in` NUL-JOINS the arguments (no terminator), so every piece is an
/// argument — an empty one included. Dropping empty pieces would shift every
/// later argument into the wrong position. An empty record is no arguments.
///
/// Returns the number of arguments PRESENT, not the number stored: arguments
/// past `MAX_ARGV` are counted and not written, so a caller comparing the
/// result against `MAX_ARGV` refuses an over-long argv rather than acting on a
/// silently truncated one. Same discipline as `split_on`.
pub fn split_argv(rec: &[u8], out: &mut [(usize, usize); MAX_ARGV]) -> usize {
    if rec.is_empty() {
        return 0;
    }
    let mut n = 0;
    let mut start = 0;
    let mut i = 0;
    while i <= rec.len() {
        if i == rec.len() || rec[i] == 0 {
            if n < out.len() {
                out[n] = (start, i);
            }
            n += 1;
            start = i + 1;
        }
        i += 1;
    }
    n
}

/// Append a `CelcErr`'s stable name. Each arm appends its own literal: a
/// `match` RETURNING literals can lower to a `.rodata` table of fat pointers,
/// which a PIC build does not relocate.
pub fn append_celc_err(out: &mut [u8], at: usize, e: CelcErr) -> usize {
    use CelcErr as E;
    match e {
        E::Empty => append(out, at, b"empty source"),
        E::Parse(_) => append(out, at, b"parse error"),
        E::BadInteger => append(out, at, b"integer out of range"),
        E::Trailing => append(out, at, b"trailing input"),
        E::UnknownName(..) => append(out, at, b"unknown name"),
        E::UnknownParam(..) => append(out, at, b"unknown parameter"),
        E::UnknownField { .. } => append(out, at, b"unknown field"),
        E::UnknownMessageType { .. } => append(out, at, b"unknown message type"),
        E::NotAMessage { .. } => append(out, at, b"selected into a non-message"),
        E::NotBool => append(out, at, b"operand is not bool"),
        E::NotInteger => append(out, at, b"operand is not integer"),
        E::NestedConstruction => append(out, at, b"nested construction"),
        E::Depth => append(out, at, b"expression too deep"),
        E::Capacity => append(out, at, b"input too large"),
        E::BadSchema(_) => append(out, at, b"malformed schema"),
        E::UnknownFunction(..) => append(out, at, b"unknown function"),
        E::BadCallArgs(..) => append(out, at, b"bad call arguments"),
        E::LocalDepth => append(out, at, b"cel.bind nesting too deep"),
        E::ReservedField(_) => append(
            out,
            at,
            b"reserved field number (240..=255) used other than as its fixed type",
        ),
    }
}

pub fn put_prog(cont: &mut [u8], w: &mut usize, code: &[u8], cost: u64) -> bool {
    // The length prefix is two bytes and `read_prog` frames the next program
    // off it, so a longer program is refused here — the same refusal
    // `pk_prog` makes — rather than wrapped into a length that silently
    // mis-frames everything after it. The cost narrowing beside it needs no
    // check: `lower_core` proves the bound at compile time.
    if code.len() > u16::MAX as usize || *w + 6 + code.len() > cont.len() {
        return false;
    }
    cont[*w..*w + 4].copy_from_slice(&(cost as u32).to_le_bytes());
    cont[*w + 4..*w + 6].copy_from_slice(&(code.len() as u16).to_le_bytes());
    cont[*w + 6..*w + 6 + code.len()].copy_from_slice(code);
    *w += 6 + code.len();
    true
}

pub fn put_ir_prog(cont: &mut [u8], w: &mut usize, ir: &[u8]) -> bool {
    // Same two-byte length prefix as `put_prog`: a longer program is refused
    // rather than wrapped into a length that mis-frames the rest.
    if ir.len() > u16::MAX as usize || *w + 2 + ir.len() > cont.len() {
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

/// Split a comma-separated argument into slices over `arg`.
///
/// Returns the number of items PRESENT, not the number stored: items past
/// `out`'s capacity are counted and not written, so a caller comparing the
/// result against `out.len()` refuses an over-long list rather than acting on a
/// silently truncated one. Same discipline as `split_on`.
pub fn split_csv<'a>(arg: &'a [u8], out: &mut [&'a [u8]]) -> usize {
    if arg == b"-" || arg.is_empty() {
        return 0;
    }
    let mut n = 0;
    let mut start = 0;
    let mut i = 0;
    while i <= arg.len() {
        if i == arg.len() || arg[i] == b',' {
            if i > start {
                if n < out.len() {
                    out[n] = &arg[start..i];
                }
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
    if n > items.len() || n > out.len() {
        return None; // more digests than the set holds — refuse, never truncate
    }
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

/// The compile work buffers one `.uproc` artefact is assembled in: the
/// container, the lowered code, and the digest-free encoding the two-pass
/// seal needs. All three live in module STATE (`chronicle_cli::State`), never
/// on the PIC stack, so the bound costs state rather than a frame.
///
/// Sized for the largest artefact kind, which is a decision: a packed
/// container grows with its arms, and the shipping examples measure 74-138
/// bytes per arm depending on how many fields the outcome constructs. A full
/// `MAX_RULE` table is therefore ~4.5 KB at the richer end, so 16 KiB leaves
/// the arm count as the bound an author meets rather than the buffer.
pub const BIN_BUF: usize = 16384;

/// The authoring buffer must be able to assemble anything a runtime engine will
/// accept. Stated as an assertion because the two numbers live in different
/// files for good reasons — this one is the tool's work buffer, the other is the
/// wire bound every engine sizes to — and the relation between them is the only
/// thing that makes either claim about the other true.
///
/// Crossing it in one direction would mean a container an engine admits but the
/// CLI cannot produce; in the other, an engine sizing for artefacts no author can
/// emit. Neither is detectable from either file alone.
///
/// Held against the FULL tier, which is the one a full-arena engine sizes to:
/// equality there means such an engine loads anything assembled here. The floor
/// tier is deliberately below this — an RP2040-class target admits less than the
/// CLI can produce, and that gap is a property of the target rather than a
/// mistake, so it is not what this assert guards.
const _: () = assert!(BIN_BUF >= MAX_CONTAINER_BIN_FULL);

/// Rule arms in ONE decision.
///
/// A decision is how a document expresses a state machine, and a state
/// machine's arm count follows its states rather than its author's restraint:
/// a lifecycle with a handful of states, each with an entry and an exit
/// condition, reaches the twenties without padding. 32 is that shape with
/// headroom, and the arms live in `RuleCode` in module state, so the bound
/// costs state and not a PIC frame.
pub const MAX_RULE: usize = 32;

/// Stages in ONE pipeline declaration. `author` (which seals the Pipeline)
/// and `graph` (which lowers it) share this bound, so a pipeline one accepts
/// the other does too.
pub const MAX_PIPELINE_STAGES: usize = 16;

/// `error:` for a pipeline declaring more than [`MAX_PIPELINE_STAGES`].
fn too_many_stages(out: &mut [u8]) -> (usize, i32) {
    let mut p = append(out, 0, b"error: a pipeline declares at most ");
    p = append_u32(out, p, MAX_PIPELINE_STAGES as u32);
    (append(out, p, b" stages\n"), 1)
}

/// `error:` for a run of compute and map stages longer than one pipeline node
/// runs: the run lowers to one node, which runs at most `MAX_NODE_STAGES`.
fn run_too_long(out: &mut [u8]) -> (usize, i32) {
    let mut p = append(out, 0, b"error: a pipeline node runs at most ");
    p = append_u32(out, p, MAX_NODE_STAGES as u32);
    (append(out, p, b" consecutive compute or map stages\n"), 1)
}

/// `error:` for a stage calling a plain expression. An expression yields a
/// value, not a record, so a stage calls a transformation, map or decision,
/// or names a resource.
fn stage_calls_expression(out: &mut [u8], target: &[u8]) -> (usize, i32) {
    let mut p = append(out, 0, b"error: stage calls expression '");
    p = append(out, p, target);
    (
        append(
            out,
            p,
            b"'; a stage calls a transformation, map or decision, or names a resource\n",
        ),
        1,
    )
}

/// Bytecode one arm's `when` or outcome program may compile to.
///
/// 512 bytes because the long arm is not the predicate but the outcome: a
/// pass-through arm in a chained decision constructs every field of its
/// record, and a predicate testing a discriminant against several values
/// compiles longer than one testing it against a single value.
pub const RULE_CODE: usize = 512;

/// The per-rule compile buffers, in module state (`2 * MAX_RULE * RULE_CODE`
/// = 32 KiB) rather than on the stack, which is why raising the bound costs
/// state and not a PIC frame.
pub struct RuleCode {
    pub wcode: [[u8; RULE_CODE]; MAX_RULE],
    pub ocode: [[u8; RULE_CODE]; MAX_RULE],
}

impl RuleCode {
    pub const fn new() -> Self {
        Self {
            wcode: [[0u8; RULE_CODE]; MAX_RULE],
            ocode: [[0u8; RULE_CODE]; MAX_RULE],
        }
    }
}

impl Default for RuleCode {
    fn default() -> Self {
        Self::new()
    }
}

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
    // Encoded straight into `out`, which is module state. Going through a
    // `2 * BIN_BUF` stack buffer would tie a PIC frame to the artefact bound,
    // doubling the frame every time the artefact grew.
    let Some(hl) = hex_encode(&cont[..w], out) else {
        return (append(out, 0, b"error: container too large to print\n"), 1);
    };
    let p = append(out, hl, b"\n");
    (p, 0)
}

pub fn lower_arg_buf(hex: &[u8], ir: &mut [u8], code: &mut [u8]) -> Option<(usize, u64)> {
    let ilen = hex_decode(hex, ir)?;
    lower_flat(&ir[..ilen], code).ok()
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
            p = append_celc_err(out, p, e);
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
/// The kind a LOCAL declaration named `symbol` was declared as, or `None`
/// when the document declares nothing by that name. A map's predicate is
/// sealed as an Expression; the iteration is the calling Stage's MapSpec.
pub fn declared_kind(arena: &UprocArena, doc: &Doc, src: &[u8], symbol: &[u8]) -> Option<i32> {
    for i in 0..doc.n_expressions {
        if arena.expressions[i].name.of(src) == symbol {
            return Some(KIND_EXPRESSION);
        }
    }
    for i in 0..doc.n_transformations {
        if arena.transformations[i].name.of(src) == symbol {
            return Some(KIND_TRANSFORMATION);
        }
    }
    for i in 0..doc.n_decisions {
        if arena.decisions[i].name.of(src) == symbol {
            return Some(KIND_DECISION);
        }
    }
    if find_map(arena, doc, src, symbol).is_some() {
        return Some(KIND_EXPRESSION);
    }
    None
}

/// The local symbol a stage target names: the bare name, or `<pkg>.<name>`
/// for this document's own package. `None` for a name in another package,
/// which a document cannot resolve (and so cannot type or lower).
pub fn local_target<'a>(target: &'a [u8], pkg: &[u8]) -> Option<&'a [u8]> {
    let (tp, ts) = split_qname(target);
    if tp.is_empty() || tp == pkg {
        Some(ts)
    } else {
        None
    }
}

/// The map declaration named `symbol`, if the document declares one.
pub fn find_map(arena: &UprocArena, doc: &Doc, src: &[u8], symbol: &[u8]) -> Option<MapDecl> {
    (0..doc.n_maps)
        .map(|i| arena.maps[i])
        .find(|m| m.name.of(src) == symbol)
}

/// The parameters a map's predicate is compiled against: its element and the
/// record — `e:E,r:T`, in the order the map stage passes them.
pub fn map_params_text(src: &[u8], d: &MapDecl, out: &mut [u8]) -> Result<usize, ()> {
    let mut p = 0usize;
    let put = |b: &[u8], out: &mut [u8], p: &mut usize| -> Result<(), ()> {
        let dst = out.get_mut(*p..*p + b.len()).ok_or(())?;
        for (x, y) in dst.iter_mut().zip(b.iter()) {
            *x = *y;
        }
        *p += b.len();
        Ok(())
    };
    put(d.elem_name.of(src), out, &mut p)?;
    put(b":", out, &mut p)?;
    put(schema_type_name(d.elem_type.of(src)), out, &mut p)?;
    put(b",", out, &mut p)?;
    put(d.param_name.of(src), out, &mut p)?;
    put(b":", out, &mut p)?;
    put(schema_type_name(d.param_type.of(src)), out, &mut p)?;
    Ok(p)
}

/// Check a map declaration against the schema and return its container
/// header `[over][max][out_true][out_false][out_unknown]` (see
/// `pipeline_core::MAP_HEADER`). `author` and `graph` both call this, so the
/// two accept and refuse exactly the same maps.
///
/// Refused rather than lowered to something else:
/// * `over`/`count` naming a parameter other than the map's own;
/// * a `max` outside `1..=255` (the header holds it in one byte);
/// * a result type other than the parameter type (the stage emits the record
///   it read, with its counts filled in);
/// * an `over` field that is not a message, or whose message is not the
///   declared element type — the predicate would read one message's fields
///   by another's numbers;
/// * a count field that is not an int, or two counts naming the same field;
/// * any field number above 255 (each header slot is one byte).
pub fn map_validate(schema: &[u8], src: &[u8], d: &MapDecl) -> Result<[u8; MAP_HEADER], ()> {
    let pn = d.param_name.of(src);
    if d.over_param.of(src) != pn
        || d.count_params.iter().any(|c| c.of(src) != pn)
        || d.max < 1
        || d.max > u8::MAX as i64
        || d.result_type.of(src) != d.param_type.of(src)
    {
        return Err(());
    }
    let body = find_message(schema, schema_type_name(d.param_type.of(src))).ok_or(())?;
    let field = |name: &[u8]| -> Result<(u8, CTy), ()> {
        let (n, ty) = find_field(schema, body, name).map_err(|_| ())?.ok_or(())?;
        if n > u8::MAX as u32 {
            return Err(());
        }
        Ok((n as u8, ty))
    };
    let (over, over_ty) = field(d.over.of(src))?;
    let CTy::Msg(false, ms, me) = over_ty else {
        return Err(());
    };
    if schema.get(ms as usize..me as usize) != Some(schema_type_name(d.elem_type.of(src))) {
        return Err(());
    }
    let mut hdr = [over, d.max as u8, 0, 0, 0];
    for k in 0..3 {
        let (n, ty) = field(d.counts[k].of(src))?;
        if ty != CTy::Int {
            return Err(());
        }
        hdr[2 + k] = n;
    }
    if hdr[2] == hdr[3] || hdr[2] == hdr[4] || hdr[3] == hdr[4] {
        return Err(());
    }
    Ok(hdr)
}

/// Build a map stage's container (`pipeline_core::MAP_HEADER` then the
/// lowered predicate) from its declaration, after [`map_validate`].
pub fn map_container(
    schema: &[u8],
    src: &[u8],
    d: &MapDecl,
    st_ir: &mut [u8],
    out: &mut [u8],
) -> Result<usize, ()> {
    let hdr = map_validate(schema, src, d)?;
    let mut params = [0u8; 256];
    let pl = map_params_text(src, d, &mut params)?;
    let code = out.get_mut(MAP_HEADER + 6..).ok_or(())?;
    let (clen, cost) = compile_one(schema, &params[..pl], d.when.of(src), st_ir, code)?;
    if clen > u16::MAX as usize {
        return Err(());
    }
    let head = out.get_mut(..MAP_HEADER + 6).ok_or(())?;
    for (x, y) in head.iter_mut().zip(hdr.iter()) {
        *x = *y;
    }
    // The cost narrowing is proven at compile time in `lower_core`.
    let c = (cost as u32).to_le_bytes();
    let l = (clen as u16).to_le_bytes();
    for (x, y) in head[MAP_HEADER..].iter_mut().zip(c.iter().chain(l.iter())) {
        *x = *y;
    }
    Ok(MAP_HEADER + 6 + clen)
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
        UprocErrorKind::DuplicateClause => {
            append(out, at, b"a clause appears twice in one declaration")
        }
        UprocErrorKind::DuplicateName => append(out, at, b"two declarations share a name"),
        UprocErrorKind::OutOfRange => append(out, at, b"integer out of range for its field"),
        UprocErrorKind::UnsupportedType => {
            append(out, at, b"unsupported type (no double/float scalar)")
        }
        UprocErrorKind::TooManyCollections => append(
            out,
            at,
            b"at most one distinct, topk or quantile operator per aggregation",
        ),
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
    st_rules: &mut RuleCode,
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
                    more_params: &[],
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
        if nrefs >= MAX_ART {
            return (append(&mut *st_out, 0, b"error: too many artefacts\n"), 1);
        }
        rdigest[nrefs] = digest;
        rsym[nrefs] = d.name.of(src);
        rkind[nrefs] = if is_expr {
            KIND_EXPRESSION
        } else {
            KIND_TRANSFORMATION
        };
        nrefs += 1;
        out_p = emit(&mut *st_out, out_p, d.name.of(src), &digest);
    }

    // ---- Maps: the predicate is an Expression over (element, record); the
    //      iteration is the calling Stage's MapSpec ----------------------
    for i in 0..doc.n_maps {
        let d = arena.maps[i];
        // The same check `graph` applies, so the two commands accept and
        // refuse the same documents.
        if map_validate(&st_prog[..slen], src, &d).is_err() {
            let mut p = append(&mut *st_out, 0, b"error: map '");
            p = append(&mut *st_out, p, d.name.of(src));
            return (
                append(&mut *st_out, p, b"' is invalid against the schema\n"),
                1,
            );
        }
        let mut params = [0u8; 256];
        let Ok(plen) = map_params_text(src, &d, &mut params) else {
            return (append(&mut *st_out, 0, b"error: params too large\n"), 1);
        };
        let mut code = [0u8; 512];
        let (clen, cost) = match compile_one(
            &st_prog[..slen],
            &params[..plen],
            d.when.of(src),
            &mut *st_code,
            &mut code,
        ) {
            Ok(v) => v,
            Err(()) => return (compile_failed(&mut *st_out, d.name.of(src)), 1),
        };
        let header = HeaderSpec {
            package: pkg,
            symbol: d.name.of(src),
            kind: KIND_EXPRESSION,
            capability: b"expression.cel.strict.v1",
        };
        let record_param = [(
            d.param_name.of(src),
            param_message_name(d.param_type.of(src)),
        )];
        let Ok((_, digest)) = seal_expression(
            &mut *st_cont,
            &mut *st_scratch,
            &header,
            &ExpressionSpec {
                param_name: d.elem_name.of(src),
                param_message: param_message_name(d.elem_type.of(src)),
                more_params: &record_param,
                result_type: b"bool",
                source: d.when.of(src),
                bytecode: &code[..clen],
                max_cost: cost,
            },
        ) else {
            return (append(&mut *st_out, 0, b"error: seal failed\n"), 1);
        };
        if nrefs >= MAX_ART {
            return (append(&mut *st_out, 0, b"error: too many artefacts\n"), 1);
        }
        rdigest[nrefs] = digest;
        rsym[nrefs] = d.name.of(src);
        rkind[nrefs] = KIND_EXPRESSION;
        nrefs += 1;
        out_p = emit(&mut *st_out, out_p, d.name.of(src), &digest);
    }

    // ---- Decisions: a compiled when + outcome per rule, plus a default ----
    for i in 0..doc.n_decisions {
        let d = arena.decisions[i];
        let mut params = [0u8; 256];
        let Ok(plen) = uproc_params_text(src, d.param_name, d.input_type, &mut params) else {
            return (append(&mut *st_out, 0, b"error: params too large\n"), 1);
        };
        if d.n_rules as usize > MAX_RULE {
            return (
                append(&mut *st_out, 0, b"error: too many rules for this node\n"),
                1,
            );
        }
        // Caller-owned, in module state — the same rule the document buffers
        // follow. `MAX_RULE * RULE_CODE` per program is far past what a PIC
        // frame can hold, so a stack array here would cap the arm count at
        // the frame rather than at the declared bound.
        let RuleCode { wcode, ocode } = st_rules;
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
                // Decisions are always sealed explainable; the flag is part of the
                // artefact and therefore of its identity.
                explain: true,
            },
            &rules[..d.n_rules as usize],
        ) else {
            return (append(&mut *st_out, 0, b"error: seal failed\n"), 1);
        };
        if nrefs >= MAX_ART {
            return (append(&mut *st_out, 0, b"error: too many artefacts\n"), 1);
        }
        rdigest[nrefs] = digest;
        rsym[nrefs] = d.name.of(src);
        rkind[nrefs] = KIND_DECISION;
        nrefs += 1;
        out_p = emit(&mut *st_out, out_p, d.name.of(src), &digest);
    }

    // ---- Pipelines: structure only; stages name artefacts, they do not
    //      embed logic, so nothing here is compiled --------------------------
    for i in 0..doc.n_pipelines {
        let pl = arena.pipelines[i];
        if pl.n_stages as usize > MAX_PIPELINE_STAGES {
            return too_many_stages(&mut *st_out);
        }
        // Consecutive compute and map stages so far: they lower to one node.
        let mut run = 0usize;
        let mut stages = [StageSpec {
            name: b"",
            target_package: b"",
            target_symbol: b"",
            target_kind: KIND_EXPRESSION,
            operation: b"",
            argument: b"",
            map: None,
        }; MAX_PIPELINE_STAGES];
        for (k, st) in arena
            .stages
            .iter()
            .skip(pl.first_stage as usize)
            .take(pl.n_stages as usize)
            .enumerate()
        {
            let st = *st;
            let target = st.target.of(src);
            // An EFFECT targets a Resource; a `call` targets an artefact this
            // document declares, which is the only way its kind is known.
            let (target_symbol, target_kind, map) = if st.kind == STAGE_EFFECT {
                (split_qname(target).1, KIND_RESOURCE, None)
            } else {
                let resolved = local_target(target, pkg)
                    .and_then(|ts| declared_kind(&*arena, &doc, src, ts).map(|k| (ts, k)));
                let Some((ts, kind)) = resolved else {
                    let mut p = append(&mut *st_out, 0, b"error: unknown artefact '");
                    p = append(&mut *st_out, p, target);
                    return (append(&mut *st_out, p, b"'\n"), 1);
                };
                // A call on a `map` carries the declared iteration; the call
                // itself targets the map's sealed predicate expression.
                let map = find_map(&*arena, &doc, src, ts).map(|m| MapStageSpec {
                    over: m.over.of(src),
                    max: m.max as u64,
                    count_true: m.counts[0].of(src),
                    count_false: m.counts[1].of(src),
                    count_unknown: m.counts[2].of(src),
                });
                (ts, kind, map)
            };
            if target_kind == KIND_EXPRESSION && map.is_none() {
                return stage_calls_expression(&mut *st_out, target);
            }
            // A decision or an effect ends the run; the graph gives each its
            // own node.
            run = if st.kind == STAGE_EFFECT || target_kind == KIND_DECISION {
                0
            } else {
                run + 1
            };
            if run > MAX_NODE_STAGES {
                return run_too_long(&mut *st_out);
            }
            // A Stage carries ONE binding; a call naming more arguments would
            // be sealed as a different call.
            if st.n_args > 1 {
                return (
                    append(
                        &mut *st_out,
                        0,
                        b"error: a call takes exactly one argument\n",
                    ),
                    1,
                );
            }
            stages[k] = StageSpec {
                name: st.name.of(src),
                target_package: pkg,
                target_symbol,
                target_kind,
                operation: st.operation.of(src),
                map,
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
        if nrefs >= MAX_ART {
            return (append(&mut *st_out, 0, b"error: too many artefacts\n"), 1);
        }
        rdigest[nrefs] = digest;
        rsym[nrefs] = pl.name.of(src);
        rkind[nrefs] = KIND_PIPELINE;
        nrefs += 1;
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
            parameter: 0,
        }; MAX_OP];
        for k in 0..ag.n_ops as usize {
            let o = arena.operators[ag.first_op as usize + k];
            ops[k] = OperatorSpec {
                name: o.name.of(src),
                kind: agg_kind_to_op(o.kind),
                selector_source: o.selector.of(src),
                selector_code: &scode[k][..slen_op[k]],
                selector_cost: scost[k],
                parameter: o.param as u64,
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
                // Non-negative: the parser refuses anything else.
                window_size_ms: ag.window_size_ms as u64,
                window_step_ms: if ag.window_kind == WINDOW_SLIDING {
                    ag.window_step_ms as u64
                } else {
                    0
                },
                lateness_ms: ag.lateness_ms as u64,
                guard_ms: ag.guard_ms as u64,
                emit_source: ag.emit.of(src),
                emit_code: &ecode[..eclen],
                emit_cost: ecost,
                max_lanes: ag.max_lanes,
                // The warn threshold is derived, not authored: three quarters of
                // the ceiling, computed without overflowing it. It is sealed, so
                // it is part of the identity.
                warn_lanes: ag.max_lanes - ag.max_lanes / 4,
            },
            &ops[..ag.n_ops as usize],
        ) else {
            return (append(&mut *st_out, 0, b"error: seal failed\n"), 1);
        };
        if nrefs >= MAX_ART {
            return (append(&mut *st_out, 0, b"error: too many artefacts\n"), 1);
        }
        rdigest[nrefs] = digest;
        rsym[nrefs] = ag.name.of(src);
        rkind[nrefs] = KIND_AGGREGATION;
        nrefs += 1;
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
        if doc.n_resources as usize > MAX_BIND {
            return (append(&mut *st_out, 0, b"error: too many resources\n"), 1);
        }
        let nb = doc.n_resources as usize;
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
        if doc.n_entries as usize > MAX_BIND {
            return (append(&mut *st_out, 0, b"error: too many entries\n"), 1);
        }
        for k in 0..doc.n_entries as usize {
            let e = arena.entries[k];
            // An entry names a pipeline; its digest is the one just sealed. An
            // entry naming no pipeline is refused, never left out.
            let target = e.pipeline.of(src);
            let Some(r) = (0..nrefs).find(|&r| rkind[r] == KIND_PIPELINE && rsym[r] == target)
            else {
                let mut p = append(&mut *st_out, 0, b"error: entry '");
                p = append(&mut *st_out, p, e.name.of(src));
                p = append(&mut *st_out, p, b"' names no pipeline '");
                p = append(&mut *st_out, p, target);
                return (append(&mut *st_out, p, b"'\n"), 1);
            };
            ents[ne] = EntrySpec {
                name: e.name.of(src),
                package: pkg,
                symbol: target,
                digest: &rdigest[r],
            };
            ne += 1;
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
                b"chronicle".as_slice()
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
    bindings: &[ResourceBinding],
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
    // `cont`; `plan[]` holds slices into it. Stage kinds are the executor's
    // (`STAGE_KIND_*`), plus `EFFECT` for a connector stage.
    const EFFECT: u8 = 0xff;
    let mut spans = [(0usize, 0usize); MAX_PIPELINE_STAGES];
    let mut kinds = [STAGE_KIND_COMPUTE; MAX_PIPELINE_STAGES];
    // Connectors resolved from the deployment's bindings, parallel to `kinds`.
    let mut effects = [None::<Connector>; MAX_PIPELINE_STAGES];
    let mut used = 0usize;
    let mut n_stages = 0usize;
    let (pkg, _) = split_qname(doc.module.of(src));

    if pipe.n_stages as usize > MAX_PIPELINE_STAGES {
        return too_many_stages(&mut *st_out);
    }
    for k in 0..pipe.n_stages as usize {
        let st = arena.stages[pipe.first_stage as usize + k];
        let target_name = if st.kind == STAGE_EFFECT {
            st.target.of(src)
        } else {
            // The same resolution `author` applies: a local name, bare or
            // qualified with this document's package.
            match local_target(st.target.of(src), pkg) {
                Some(ts) => ts,
                None => {
                    let mut p = append(&mut *st_out, 0, b"error: unknown artefact '");
                    p = append(&mut *st_out, p, st.target.of(src));
                    p = append(&mut *st_out, p, b"'\n");
                    return (p, 1);
                }
            }
        };
        if st.kind != STAGE_EFFECT && st.n_args > 1 {
            return (
                append(
                    &mut *st_out,
                    0,
                    b"error: a call takes exactly one argument\n",
                ),
                1,
            );
        }

        // An EFFECT names a `resource`; the deployment says what serves it.
        // The document deliberately does not, so an unbound resource is a
        // deployment error and says so.
        if st.kind == STAGE_EFFECT {
            let Some(connector) = resolve_binding(bindings, target_name) else {
                let mut p = append(&mut *st_out, 0, b"error: resource '");
                p = append(&mut *st_out, p, target_name);
                p = append(
                    &mut *st_out,
                    p,
                    b"' has no binding; supply one for each `resource` the \
                      pipeline uses\n",
                );
                return (p, 1);
            };
            effects[n_stages] = Some(*connector);
            kinds[n_stages] = EFFECT;
            spans[n_stages] = (used, 0);
            n_stages += 1;
            continue;
        }

        // A Call names a transformation (a compute stage), a map (a stage of
        // the compute run) or a decision (its own node). The parser refuses two
        // declarations sharing a name (`DuplicateName`), so at most one matches.
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
            kinds[n_stages] = STAGE_KIND_COMPUTE;
            used += irn;
            n_stages += 1;
            done = true;
            break;
        }
        if done {
            continue;
        }

        // A map is a stage of the compute run, not a node of its own: it does
        // not branch. Its container is built now, resolved against the schema.
        if let Some(m) = find_map(&*arena, &doc, src, target_name) {
            match map_container(schema, src, &m, &mut *st_code, &mut st_cont[used..]) {
                Ok(n) => {
                    spans[n_stages] = (used, n);
                    kinds[n_stages] = STAGE_KIND_MAP;
                    used += n;
                    n_stages += 1;
                    continue;
                }
                Err(()) => {
                    let mut p = append(&mut *st_out, 0, b"error: map '");
                    p = append(&mut *st_out, p, target_name);
                    p = append(&mut *st_out, p, b"' did not compile\n");
                    return (p, 1);
                }
            }
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
            kinds[n_stages] = STAGE_KIND_DECISION;
            used += dn;
            n_stages += 1;
            done = true;
            break;
        }
        if !done {
            if declared_kind(&*arena, &doc, src, target_name) == Some(KIND_EXPRESSION) {
                return stage_calls_expression(&mut *st_out, target_name);
            }
            let mut p = append(&mut *st_out, 0, b"error: unknown artefact '");
            p = append(&mut *st_out, p, target_name);
            p = append(&mut *st_out, p, b"'\n");
            return (p, 1);
        }
    }

    let mut plan = [PlanStage::Compute { stage_ir: b"" }; MAX_PIPELINE_STAGES];
    for i in 0..n_stages {
        let (off, len) = spans[i];
        let bytes = &st_cont[off..off + len];
        plan[i] = match kinds[i] {
            STAGE_KIND_COMPUTE => PlanStage::Compute { stage_ir: bytes },
            STAGE_KIND_DECISION => PlanStage::Decision { container: bytes },
            STAGE_KIND_MAP => PlanStage::Map { container: bytes },
            // Resolved above from the deployment's bindings; an effect stage
            // without one cannot reach here, and is refused rather than
            // silently lowered as compute if it ever does.
            _ => match effects[i] {
                Some(c) => PlanStage::Effect(c),
                None => {
                    return (
                        append(
                            &mut *st_out,
                            0,
                            b"error: effect stage has no resolved binding\n",
                        ),
                        1,
                    )
                }
            },
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
        // Appends inside each arm: a match yielding `&'static [u8]` compiles
        // to a pointer table, which does not relocate in a PIC module.
        Err(GraphError::RunTooLong) => run_too_long(&mut *st_out),
        Err(GraphError::TooLarge) => (append(&mut *st_out, 0, b"error: graph too large\n"), 1),
        Err(GraphError::EffectUnbound) => (
            append(&mut *st_out, 0, b"error: an effect stage has no binding\n"),
            1,
        ),
        Err(GraphError::EffectNotChainable) => (
            append(
                &mut *st_out,
                0,
                b"error: a stage follows an effect that answers with no record\n",
            ),
            1,
        ),
    }
}

/// Assemble the aggregation IR-`def` container from CLI argv: the window
/// scalars, the key / event-time / emit programs and each operator with its
/// selector. Flat over caller buffers.
#[allow(
    clippy::too_many_arguments,
    reason = "the CLI State buffers, passed explicitly so the core stays a flat function"
)]
pub fn agg_from_argv(
    arec: &[u8],
    argv: &[(usize, usize)],
    argc: usize,
    st_ir: &mut [u8],
    st_cont: &mut [u8],
    st_out: &mut [u8],
) -> (usize, i32) {
    if argc < 9 {
        return (
            append(
                &mut *st_out,
                0,
                b"error: agg needs <window> <lateness> <lanes> <step> <horizon> \
                  <key_ir> <time_ir> <emit_ir> [<kind>[/<param>]:<sel_ir>]...\n",
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
    // Lanes travel as a u32: a value that does not fit is refused, never
    // wrapped into a different ceiling.
    if !(0..=u32::MAX as i64).contains(&nums[2]) {
        return (
            append(&mut *st_out, 0, b"error: lanes must be 0..4294967295\n"),
            1,
        );
    }
    if st_cont.len() < 36 {
        return (append(&mut *st_out, 0, b"error: container too large\n"), 1);
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
    // Each program decodes into `st_ir` (module state) before it is framed.
    for k in 0..3 {
        let (a, b) = argv[6 + k];
        let Some(ilen) = hex_decode(&arec[a..b], &mut *st_ir) else {
            return (
                append(&mut *st_out, 0, b"error: a program is not valid IR hex\n"),
                1,
            );
        };
        if !put_ir_prog(&mut *st_cont, &mut w, &st_ir[..ilen]) {
            return (append(&mut *st_out, 0, b"error: container too large\n"), 1);
        }
    }

    // Operators: `<kind>[/<param>]:<selector_ir_hex>`, selector optionally
    // empty. TopK (6) and Quantile (7) take their parameter; no other kind does.
    let nops = argc - 9;
    if nops > 255 {
        return (append(&mut *st_out, 0, b"error: too many operators\n"), 1);
    }
    if w >= st_cont.len() {
        return (append(&mut *st_out, 0, b"error: container too large\n"), 1);
    }
    st_cont[w] = nops as u8;
    w += 1;
    // A pane holds one collection cell, so the engine refuses a second
    // Distinct (5), TopK (6) or Quantile (7) at load; refused here first.
    let mut has_collection = false;
    for k in 0..nops {
        let (a, b) = argv[9 + k];
        let arg = &arec[a..b];
        let Some(colon) = arg.iter().position(|c| *c == b':') else {
            return (
                append(
                    &mut *st_out,
                    0,
                    b"error: an operator must be <kind>[/<param>]:<sel_ir>\n",
                ),
                1,
            );
        };
        let head = &arg[..colon];
        let (kind_text, param_text) = match head.iter().position(|c| *c == b'/') {
            Some(slash) => (&head[..slash], Some(&head[slash + 1..])),
            None => (head, None),
        };
        let Some(kind) = parse_i64(kind_text).filter(|k| (0..=255).contains(k)) else {
            return (
                append(&mut *st_out, 0, b"error: operator kind must be 0..255\n"),
                1,
            );
        };
        if (5..=7).contains(&kind) {
            if has_collection {
                return (
                    append(
                        &mut *st_out,
                        0,
                        b"error: at most one distinct (5), topk (6) or quantile (7) operator\n",
                    ),
                    1,
                );
            }
            has_collection = true;
        }
        let param = match param_text {
            None => 0,
            Some(t) => match parse_i64(t).filter(|v| (0..=u16::MAX as i64).contains(v)) {
                Some(v) => v as u16,
                None => {
                    return (
                        append(
                            &mut *st_out,
                            0,
                            b"error: operator parameter must be 0..65535\n",
                        ),
                        1,
                    )
                }
            },
        };
        let sel = &arg[colon + 1..];
        let ilen = if sel.is_empty() {
            0
        } else {
            match hex_decode(sel, &mut *st_ir) {
                Some(n) => n,
                None => {
                    return (
                        append(&mut *st_out, 0, b"error: a selector is not valid IR hex\n"),
                        1,
                    )
                }
            }
        };
        w = match pack_op_head(&mut *st_cont, w, kind as u8, param) {
            Ok(p) => p,
            Err(PackError::BadParameter) => {
                return (
                    append(
                        &mut *st_out,
                        0,
                        b"error: topk (6) takes k 1..65535, quantile (7) permille 0..1000; \
                          no other kind takes a parameter\n",
                    ),
                    1,
                )
            }
            Err(_) => return (append(&mut *st_out, 0, b"error: container too large\n"), 1),
        };
        if !put_ir_prog(&mut *st_cont, &mut w, &st_ir[..ilen]) {
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

// ── Deployment bindings ───────────────────────────────────────────────────
//
// A `.uproc` names the resources a pipeline needs and deliberately says
// nothing about what serves them. This parses the other half — the
// deployment's answer — from one argument, so a node can lower a document
// with effects without a build host.
//
// Grammar, one binding per `|`:
//
//   <resource>,<kind>,<provider>,<version>,<in_port>,<out_port>,<r|n>,<params>
//
// where `<params>` is `k=v` pairs joined by `;` (so neither `,` nor `;` nor
// `=` can appear inside a value), and a value led by `#` is NUMERIC — emitted
// unquoted, because a provider's `u32` decoder rejects a quoted number. `r`/`n`
// says whether the provider answers with data a next stage could read: `r` for
// a `stream.ordered_ack.exchange` or request/reply provider, `n` for a sink.
//
//   orders_store,pg,pg_client,0.1.0,request_in,reply_out,r,authority=127.0.0.1:5432;user=app;cid_len=#4
//   feed,kafka,kafka_sink,0.1.0,publish_in,ack_out,n,authority=10.0.0.1:9092;topic=orders
//
// Every field is the PROVIDER's, not chronicle's: the param names, the port
// names and the module name all arrive from the deployment, which is what
// lets a new destination be a config change rather than a code change.

/// Bindings a single `chronicle graph` invocation may carry.
pub const MAX_BINDINGS: usize = 8;
/// Params one binding may carry.
pub const MAX_BINDING_PARAMS: usize = 12;
/// One connector param as the binding carries it: `(name, value, quoted)`.
pub type BindingParam<'a> = (&'a [u8], &'a [u8], bool);
/// Caller-owned storage the parsed bindings' params point into.
pub type BindingParamStore<'a> = [[BindingParam<'a>; MAX_BINDING_PARAMS]; MAX_BINDINGS];

/// Split `src` on `sep`, writing up to `out.len()` pieces. Returns the number
/// of pieces in `src`, which exceeds `out.len()` when some were not stored —
/// a caller refuses that rather than acting on a prefix.
fn split_on<'a>(src: &'a [u8], sep: u8, out: &mut [&'a [u8]]) -> usize {
    let (mut n, mut start, mut i) = (0usize, 0usize, 0usize);
    while i <= src.len() {
        if i == src.len() || src[i] == sep {
            if n < out.len() {
                out[n] = &src[start..i];
            }
            n += 1;
            start = i + 1;
        }
        i += 1;
    }
    n
}

/// Parse the deployment's bindings. Returns the count, or `None` on a
/// malformed field, a binding with the wrong field count, or more bindings or
/// params than fit — a binding that cannot be read is refused rather than
/// half-applied, because a partly-bound graph is worse than an unbuilt one.
///
/// Two passes, deliberately: the params are filled first, and only then are
/// the bindings built pointing INTO them. One pass cannot borrow the buffer
/// it is still writing to.
pub fn parse_bindings<'a>(
    src: &'a [u8],
    params: &'a mut BindingParamStore<'a>,
    out: &mut [ResourceBinding<'a>; MAX_BINDINGS],
) -> Option<usize> {
    if src.is_empty() {
        return Some(0);
    }
    let mut specs = [b"".as_slice(); MAX_BINDINGS];
    let n = split_on(src, b'|', &mut specs);
    if n > MAX_BINDINGS {
        return None;
    }

    // Fields per binding, held until the params are in place.
    let mut fields = [[b"".as_slice(); 8]; MAX_BINDINGS];
    let mut replies = [false; MAX_BINDINGS];
    let mut lens = [0usize; MAX_BINDINGS];

    for (b, spec) in specs.iter().take(n).enumerate() {
        let mut f = [b"".as_slice(); 8];
        if split_on(spec, b',', &mut f) != 8 {
            return None;
        }
        replies[b] = match f[6] {
            b"r" => true,
            b"n" => false,
            _ => return None,
        };
        fields[b] = f;

        let mut kvs = [b"".as_slice(); MAX_BINDING_PARAMS];
        let np = split_on(f[7], b';', &mut kvs);
        if np > MAX_BINDING_PARAMS {
            return None;
        }
        let mut pn = 0usize;
        for kv in kvs.iter().take(np) {
            if kv.is_empty() {
                continue;
            }
            let mut halves = [b"".as_slice(); 2];
            if split_on(kv, b'=', &mut halves) != 2 {
                return None;
            }
            let (key, mut val) = (halves[0], halves[1]);
            // `#` marks a numeric value: emitted unquoted.
            let quoted = val.is_empty() || val[0] != b'#';
            if !quoted {
                val = &val[1..];
            }
            params[b][pn] = (key, val, quoted);
            pn += 1;
        }
        lens[b] = pn;
    }

    // The fill is done; take the buffer immutably for the bindings to borrow.
    let params: &'a BindingParamStore<'a> = params;
    for b in 0..n {
        out[b] = ResourceBinding {
            resource: fields[b][0],
            connector: Connector {
                kind: fields[b][1],
                provider: fields[b][2],
                version: fields[b][3],
                in_port: fields[b][4],
                out_port: fields[b][5],
                replies: replies[b],
                params: &params[b][..lens[b]],
            },
        };
    }
    Some(n)
}
