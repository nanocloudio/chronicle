// Bounded, no_std, no-alloc PIPELINE threading core. Like `core.rs` it carries
// NO inner attributes and NO test module, so it is `include!`d verbatim by both
// this crate (`lib.rs`) and the on-device Fluxor module
// (`modules/app/pipeline/mod.rs`) — one source of truth for staged execution,
// host and device.
//
// A Pipeline runs an ordered list of `Stage`s. Each stage is one artefact's
// bytecode (a Transformation-shaped construction) run on the bounded evaluator;
// its constructed output message is serialized to a typed record frame and fed
// as the input of the next stage — exactly the serialize-at-the-boundary
// semantics a real multi-module pipeline has (messages cross channels as bytes).
// Threading through frames also sidesteps borrow lifetimes: each stage's input
// borrows only its own decode buffer, never a previous stage's `Builder`.
//
// Typed record frame (self-describing, so integer fields survive a round trip):
//   [count:u8] then count × [number:u8][type:u8][len:u16 LE][payload]
//   type 0 = byte string (payload = raw bytes)
//   type 1 = i64        (payload = 8 bytes little-endian)
//   type 3 = message    (payload = the nested message's own frame)

/// Maximum fields a pipeline record frame may carry: the builder's bound, so
/// any record a stage can construct, the next stage can decode. A frame
/// claiming more is refused with [`PipeError::TooManyFields`].
pub const MAX_PIPE_FIELDS: usize = MAX_BUILD_FIELDS;

/// The largest compiled artefact container an engine will hold, per arena tier.
///
/// These live here, in the core every engine and the authoring CLI `include!`s,
/// because they are the numbers both halves must agree on and neither owns. An
/// engine that sized its own param buffer independently would be guessing at what
/// the authoring path can produce, and a guess in the generous direction buys a
/// buffer no artefact can fill — unreachable by construction, since the container
/// is assembled in `author_core::BIN_BUF` and cannot leave it larger than that.
///
/// TIERED, because one number cannot serve both ends of a 4000× range in arena.
/// On a target with arena to spare the bound IS `author_core::BIN_BUF`: the
/// engine loads anything the authoring path can assemble, which is the strongest
/// form of the agreement and the one that needs no estimate to be right. On an
/// RP2040-class target the arena decides instead, and the honest consequence is
/// that a rich table is not deployable there — a fact about that target, not a
/// budget every other target should be held to.
///
/// Arms that carry a full outcome cost about 272 bytes each (a 29-arm lifecycle
/// table measures 7894 bytes), so the tiny tier holds fewer than 23 of them.
/// `author_core::BIN_BUF` asserts it is at least the full tier, so the two
/// cannot cross.
pub const MAX_CONTAINER_BIN_FULL: usize = 16384;

/// The same bound on a 64 KiB-arena target (RP2040 class).
pub const MAX_CONTAINER_BIN: usize = 6144;

const TY_BYTES: u8 = 0;
const TY_I64: u8 = 1;
/// A nested message: its payload is the message's own frame.
const TY_MSG: u8 = 3;

/// One pipeline stage: an artefact's bytecode and its static cost ceiling.
#[derive(Debug, Clone, Copy)]
pub struct Stage<'a> {
    pub code: &'a [u8],
    pub max_cost: u64,
    /// Failure routing: the stage index to continue from when this stage FAILS,
    /// or `None` to abort the pipeline.
    ///
    /// This is the one piece of the spec's stage policy that is meaningful in a
    /// pure-compute executor. Retries and compensation are not: a deterministic
    /// stage re-run on the same input fails identically, and there is no effect
    /// to undo. On device an effect is a separate connector `.fmod` wired into
    /// the graph, so retry and compensation are graph-level concerns — see the
    /// note on `run_stages`.
    pub on_failure: Option<u8>,
    /// Which executor runs this stage's `code`.
    ///
    /// NOT carried in the stage container: the wire format is
    /// `[count][route][cost][len][code]`, so a recorded graph and an emitted
    /// `ir_stages` hex mean the same thing whatever kinds accompany them. A
    /// caller that runs mixed kinds supplies them alongside the container
    /// (see the pipeline module's `stage_kinds` param); `stage_at` reports
    /// `STAGE_KIND_COMPUTE`, so a container read on its own is all compute.
    pub kind: u8,
}

/// The stage's `code` is transformation bytecode for the expression VM.
pub const STAGE_KIND_COMPUTE: u8 = 0;
/// The stage's `code` is a DECISION container — first-hit routing over the
/// record. Structurally the same step as a compute stage (decode the frame,
/// evaluate, encode the result), so it threads through the same executor
/// rather than occupying a node of its own.
pub const STAGE_KIND_DECISION: u8 = 1;
/// The stage's `code` is a MAP container: a predicate applied to every
/// element of a repeated field, bounded by a declared maximum, the verdicts
/// counted into the record. The iteration is the driver's and declarative;
/// the per-element logic is ordinary expression bytecode (bytecode_policy.md,
/// "constrained artefact kinds").
pub const STAGE_KIND_MAP: u8 = 2;

/// Stages one pipeline node runs: the stage table it builds per record holds
/// this many. The graph lowering refuses a run of compute and map stages
/// longer than this, and `author` refuses the pipeline that would need one,
/// so no node is handed more stages than it runs.
pub const MAX_NODE_STAGES: usize = 8;

/// A map container's fixed header: `[over][max][out_true][out_false][out_unknown]`,
/// then the predicate program `[cost:u32 LE][len:u16 LE][code]`.
///
/// * `over` — the repeated field whose elements (each a nested message) are
///   mapped; more than `max` of them refuses the stage (`TooMany`). A
///   container whose `max` and three counts cannot fit this target's field
///   table is refused at load.
/// * the predicate runs with params `[element, record]` and answers true,
///   false or unknown. Whatever an element is judged against travels in the
///   element itself — a producer that pairs, pairs before the stage.
/// * the counts land at `out_true`/`out_false`/`out_unknown` (replacing any
///   the record carried); "every element holds" is `false == 0 && unknown == 0`.
pub const MAP_HEADER: usize = 5;

/// How one stage is evaluated.
///
/// A trait rather than a `match` inside this core, because the decision
/// executor lives in `decision_core` and this file is mounted by modules that
/// have no reason to carry it (`aggregation`, `expression`, `chronicle_cli`).
/// Generic dispatch monomorphises, so a PIC build gets a direct call and no
/// pointer table — which a static dispatch table could not survive.
pub trait StageEval {
    fn eval(
        &self,
        stage: &Stage,
        src: &[u8],
        dst: &mut [u8],
        spent: &mut u64,
    ) -> Result<usize, PipeError>;
}

/// The default: every stage is transformation bytecode.
pub struct ComputeOnly;

impl StageEval for ComputeOnly {
    fn eval(
        &self,
        stage: &Stage,
        src: &[u8],
        dst: &mut [u8],
        spent: &mut u64,
    ) -> Result<usize, PipeError> {
        if stage.kind == STAGE_KIND_MAP {
            return run_map_stage(stage.code, src, dst, spent);
        }
        run_stage_metered(stage, src, dst, spent)
    }
}

/// The route byte meaning "no failure route" — abort instead of routing.
pub const ROUTE_NONE: u8 = 0xff;

/// Deterministic pipeline failures. Never panics on malformed input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipeError {
    /// A stage's bytecode failed to evaluate.
    StageEval(EvalError),
    /// A stage did not construct a message (its program must end in FINISH_MSG).
    NotConstructed,
    /// Failure routing did not terminate within the step budget — a route cycle.
    RouteLoop,
    /// A record frame was truncated or malformed.
    BadFrame,
    /// The output buffer was too small, or a field type is not serializable.
    Encode,
    /// A map stage met more elements than its declared maximum — refused
    /// whole, never evaluated in part. Routable: the bound is declared by the
    /// author, so exceeding it is a condition a failure route can answer.
    TooMany,
    /// A frame claims more fields than this target's field table holds
    /// (`MAX_PIPE_FIELDS`). Well formed, but not representable here.
    TooManyFields,
}

impl PipeError {
    /// Whether the record was refused for exceeding a bound — more fields
    /// than the table holds, read or built, or more elements than a map stage
    /// declares — as opposed to failing to evaluate or being malformed.
    /// Engines count these apart.
    pub fn over_bound(self) -> bool {
        matches!(
            self,
            PipeError::TooMany
                | PipeError::TooManyFields
                | PipeError::StageEval(EvalError::BuildOverflow)
        )
    }
}

/// Decode a typed record frame into borrowed fields. Returns the field count.
/// Shared with the aggregation core (`agg_core.rs`).
pub fn decode_frame<'a>(
    data: &'a [u8],
    fields: &mut [Field<'a>; MAX_PIPE_FIELDS],
) -> Result<usize, PipeError> {
    if data.is_empty() {
        return Ok(0);
    }
    let count = data[0] as usize;
    if count > MAX_PIPE_FIELDS {
        return Err(PipeError::TooManyFields);
    }
    let mut off = 1usize;
    let mut fi = 0usize;
    while fi < count {
        if off + 4 > data.len() {
            return Err(PipeError::BadFrame);
        }
        let number = data[off] as u32;
        let ty = data[off + 1];
        let len = u16::from_le_bytes([data[off + 2], data[off + 3]]) as usize;
        off += 4;
        if off + len > data.len() {
            return Err(PipeError::BadFrame);
        }
        let payload = &data[off..off + len];
        let value = match ty {
            TY_BYTES => Value::Bytes(payload),
            TY_MSG => Value::Frame(payload),
            TY_I64 => {
                if len != 8 {
                    return Err(PipeError::BadFrame);
                }
                Value::Int(i64::from_le_bytes([
                    payload[0], payload[1], payload[2], payload[3], payload[4], payload[5],
                    payload[6], payload[7],
                ]))
            }
            _ => return Err(PipeError::BadFrame),
        };
        fields[fi] = Field { number, value };
        off += len;
        fi += 1;
    }
    Ok(fi)
}

/// A serialized stage table for param-driven pipelines: `[nstages:u8]` then, per
/// stage, `[route:u8][max_cost:u32 LE][code_len:u16 LE][code bytes]` (`route` is
/// the failure route, `ROUTE_NONE` for none). This is what a config carries
/// (hex-encoded) so one pipeline `.fmod` runs any Pipeline.
///
/// Number of stages in `container`, or 0 if empty/truncated.
pub fn stage_count(container: &[u8]) -> usize {
    if container.is_empty() {
        0
    } else {
        container[0] as usize
    }
}

/// The `index`-th stage of `container` as `(max_cost, code)`, or `None` if the
/// container is malformed or `index` is out of range.
pub fn stage_at(container: &[u8], index: usize) -> Option<Stage<'_>> {
    let n = stage_count(container);
    if index >= n {
        return None;
    }
    let mut off = 1usize;
    let mut i = 0usize;
    loop {
        if off + 7 > container.len() {
            return None;
        }
        let route = container[off];
        let cost = u32::from_le_bytes([
            container[off + 1],
            container[off + 2],
            container[off + 3],
            container[off + 4],
        ]) as u64;
        let len = u16::from_le_bytes([container[off + 5], container[off + 6]]) as usize;
        off += 7;
        if off + len > container.len() {
            return None;
        }
        if i == index {
            return Some(Stage {
                code: &container[off..off + len],
                max_cost: cost,
                kind: STAGE_KIND_COMPUTE,
                on_failure: if route == ROUTE_NONE {
                    None
                } else {
                    Some(route)
                },
            });
        }
        off += len;
        i += 1;
    }
}

/// Load-time validation of a bytecode-stages container: every stage must
/// parse AND pass [`scan_code`] — no unknown opcodes, no truncation, no
/// builtin this build does not carry. Engines call this at init/reload so a
/// broken container is refused once, loudly, instead of failing per record.
pub fn scan_stage_container(container: &[u8]) -> Result<(), EvalError> {
    let n = stage_count(container);
    let mut i = 0;
    while i < n {
        let st = stage_at(container, i).ok_or(EvalError::Truncated)?;
        scan_code(st.code)?;
        i += 1;
    }
    Ok(())
}

/// Byte length of the first typed record frame in `data`, or `None` if it is
/// truncated. Lets a reader split a batch of concatenated frames.
pub fn frame_len(data: &[u8]) -> Option<usize> {
    if data.is_empty() {
        return None;
    }
    let count = data[0] as usize;
    let mut off = 1usize;
    for _ in 0..count {
        if off + 4 > data.len() {
            return None;
        }
        let len = u16::from_le_bytes([data[off + 2], data[off + 3]]) as usize;
        off += 4 + len;
    }
    if off > data.len() {
        None
    } else {
        Some(off)
    }
}

/// The reserved field a record's CARRIED CONTEXT rides in: a nested message
/// (its own frame, as bytes) that comes back unchanged on the record made from
/// an effect's answer, so a request built before an effect and the answer
/// handled after it share no state but the record. A requester keeps it beside
/// the open exchange, held to the contract's `KEY_MAX`; it never reaches the
/// provider. Engine meanings take `240..=255` (celc's `RESERVED_FIELD_MIN`);
/// data fields are `1..=239`.
pub const CARRY_FIELD: u32 = 254;

/// The reserved field a record made from an exchange answer carries its
/// status in: the HTTP status code the provider answered with — what the
/// provider said, apart from anything the body says. Only a record made from
/// an answer carries it.
pub const EXCHANGE_STATUS_FIELD: u32 = 253;

/// Serialize a constructed message into a typed record frame. Returns its length.
/// Shared with the aggregation core (`agg_core.rs`).
pub fn encode_frame(msg: &Message, out: &mut [u8]) -> Result<usize, PipeError> {
    encode_frame_scratch(msg, &Scratch::new(&mut []), out)
}

/// [`encode_frame`] for messages whose fields may be `Value::Scratch` —
/// offsets into the arena the constructing evaluation wrote. The arena must
/// be the SAME one that evaluation used; a foreign arena would serialize
/// someone else's bytes, which is why the scratch-less wrapper above exists
/// only for paths that provably run no writing builtin (rd-decoded frames,
/// status frames).
pub fn encode_frame_scratch(
    msg: &Message,
    scratch: &Scratch<'_>,
    out: &mut [u8],
) -> Result<usize, PipeError> {
    // An ABSENT field (Null — a stage copied a field its input never carried)
    // is left out of the frame rather than written as zero-length bytes: the
    // next stage must see it absent, and fail as absence fails, not read ""
    // and decide on it. The count is of the fields actually written.
    let n = msg
        .fields
        .iter()
        .filter(|f| !matches!(f.value, Value::Null))
        .count();
    if n > u8::MAX as usize || out.is_empty() {
        return Err(PipeError::Encode);
    }
    out[0] = n as u8;
    let mut off = 1usize;

    let put = |bytes: &[u8], out: &mut [u8], off: &mut usize| -> Result<(), PipeError> {
        if *off + bytes.len() > out.len() {
            return Err(PipeError::Encode);
        }
        out[*off..*off + bytes.len()].copy_from_slice(bytes);
        *off += bytes.len();
        Ok(())
    };

    for f in msg.fields {
        // The frame stores the field number as a u8, so a wider number would
        // truncate silently (field 256 -> 0). Reject rather than corrupt.
        if f.number > u8::MAX as u32 {
            return Err(PipeError::Encode);
        }
        if matches!(f.value, Value::Null) {
            continue;
        }
        // An arena-backed result serializes as what it holds.
        let value = resolve_scratch(f.value, scratch);
        let f = &Field {
            number: f.number,
            value,
        };
        let (ty, payload): (u8, [u8; 8]) = match f.value {
            Value::Int(i) => (TY_I64, i.to_le_bytes()),
            // The frame's integer is an i64: an unsigned value past it is
            // refused, never written as the negative number it would wrap to.
            Value::Uint(u) => match i64::try_from(u) {
                Ok(i) => (TY_I64, i.to_le_bytes()),
                Err(_) => return Err(PipeError::StageEval(EvalError::Overflow)),
            },
            Value::Bool(b) => (TY_I64, (b as i64).to_le_bytes()),
            Value::Frame(_) => (TY_MSG, [0u8; 8]),
            _ => (TY_BYTES, [0u8; 8]),
        };
        // header: number:u8, type:u8, len:u16 LE
        let (len, bytes): (usize, &[u8]) = match f.value {
            Value::Bytes(b) | Value::Frame(b) => (b.len(), b),
            Value::Str(s) => (s.len(), s.as_bytes()),
            // A Msg has no typed frame representation; reject rather than emit
            // a zero-filled byte string mislabelled as data.
            Value::Msg(_) => return Err(PipeError::Encode),
            _ => (8, &payload[..]),
        };
        if len > u16::MAX as usize {
            return Err(PipeError::Encode);
        }
        put(&[f.number as u8, ty], out, &mut off)?;
        put(&(len as u16).to_le_bytes(), out, &mut off)?;
        put(&bytes[..len], out, &mut off)?;
    }
    Ok(off)
}

/// Scratch-arena bytes available to one stage's writing builtins (reverse,
/// case mapping, replace, base64). Stage-local and reset per stage — a
/// stage's outputs are SERIALIZED into the frame before the next stage runs,
/// so nothing outlives the stage. Overflow fails the stage closed
/// (`ScratchOverflow`), like every other bound here.
pub const STAGE_SCRATCH_CAP: usize = 512;

/// Run one stage: decode `src`, evaluate, and serialize the constructed message
/// into `dst`, adding the stage program's VM instructions to `spent`. Returns the
/// encoded length. Public so a caller supplying its own [`StageEval`] can delegate the
/// compute case rather than reimplementing it.
///
/// Never inlined: a stage runner's field tables belong on the stack only
/// while that runner runs, not in the frame of whichever caller dispatches
/// across kinds.
#[inline(never)]
pub fn run_stage_metered(
    stage: &Stage,
    src: &[u8],
    dst: &mut [u8],
    spent: &mut u64,
) -> Result<usize, PipeError> {
    let mut fields = [Field {
        number: 0,
        value: Value::Null,
    }; MAX_PIPE_FIELDS];
    let nf = decode_frame(src, &mut fields)?;
    let params = [Message {
        fields: &fields[..nf],
    }];
    let mut builder = Builder::new();
    let mut sbuf = [0u8; STAGE_SCRATCH_CAP];
    let mut scratch = Scratch::new(&mut sbuf);
    let mut w = 0u64;
    let r = eval_full_scratch_metered(
        stage.code,
        &params,
        &mut builder,
        &mut scratch,
        stage.max_cost,
        &mut w,
    );
    *spent += w;
    match r {
        Ok(EvalResult::Constructed) => encode_frame_scratch(&builder.message(), &scratch, dst),
        Ok(EvalResult::Scalar(_)) => Err(PipeError::NotConstructed),
        Err(e) => Err(PipeError::StageEval(e)),
    }
}

/// Check a map container's shape at load: header, one program the VM
/// accepts, nothing after it.
pub fn scan_map_container(c: &[u8]) -> Result<(), PipeError> {
    let hdr = c.get(..MAP_HEADER).ok_or(PipeError::BadFrame)?;
    if hdr[0] == 0 {
        return Err(PipeError::BadFrame);
    }
    if hdr[1] as usize + 3 > MAX_PIPE_FIELDS {
        return Err(PipeError::TooManyFields);
    }
    let lb = c
        .get(MAP_HEADER + 4..MAP_HEADER + 6)
        .ok_or(PipeError::BadFrame)?;
    let len = u16::from_le_bytes([lb[0], lb[1]]) as usize;
    let code = c
        .get(MAP_HEADER + 6..MAP_HEADER + 6 + len)
        .ok_or(PipeError::BadFrame)?;
    if MAP_HEADER + 6 + len != c.len() {
        return Err(PipeError::BadFrame);
    }
    scan_code(code).map_err(PipeError::StageEval)
}

/// Run one MAP stage over the record `src`, writing the record with its
/// counts into `dst`. See [`STAGE_KIND_MAP`] and [`MAP_HEADER`]. Never
/// inlined, for the reason [`run_stage_metered`] gives.
#[inline(never)]
pub fn run_map_stage(
    code: &[u8],
    src: &[u8],
    dst: &mut [u8],
    spent: &mut u64,
) -> Result<usize, PipeError> {
    let h = code.get(..MAP_HEADER).ok_or(PipeError::BadFrame)?;
    let (over, max) = (h[0], h[1]);
    let (o_true, o_false, o_unknown) = (h[2], h[3], h[4]);
    let cb = code
        .get(MAP_HEADER..MAP_HEADER + 6)
        .ok_or(PipeError::BadFrame)?;
    let cost = u32::from_le_bytes([cb[0], cb[1], cb[2], cb[3]]) as u64;
    let plen = u16::from_le_bytes([cb[4], cb[5]]) as usize;
    let prog = code
        .get(MAP_HEADER + 6..MAP_HEADER + 6 + plen)
        .ok_or(PipeError::BadFrame)?;

    let mut rf = [Field {
        number: 0,
        value: Value::Null,
    }; MAX_PIPE_FIELDS];
    let nr = decode_frame(src, &mut rf)?;
    let rec = &rf[..nr];
    let elements = rec.iter().filter(|f| f.number == over as u32).count();
    if elements > max as usize {
        return Err(PipeError::TooMany);
    }
    let (mut n_true, mut n_false, mut n_unknown) = (0i64, 0i64, 0i64);
    // One field table for the whole stage, reused per element. A table is
    // `MAX_PIPE_FIELDS` fields and this runs on the step's stack, so a table
    // per element is a cost worth not paying.
    let mut efs = [Field {
        number: 0,
        value: Value::Null,
    }; MAX_PIPE_FIELDS];
    for ef in rec.iter().filter(|f| f.number == over as u32) {
        let Value::Frame(eb) = ef.value else {
            return Err(PipeError::BadFrame);
        };
        let ne = decode_frame(eb, &mut efs)?;
        let params = [Message { fields: &efs[..ne] }, Message { fields: rec }];
        let mut w = 0u64;
        let mut sbuf = [0u8; STAGE_SCRATCH_CAP];
        let mut scratch = Scratch::new(&mut sbuf);
        let v = eval_scratch_metered(prog, &params, &mut scratch, cost, &mut w);
        *spent += w;
        match v.map_err(PipeError::StageEval)? {
            Value::Bool(true) => n_true += 1,
            Value::Bool(false) => n_false += 1,
            Value::Null => n_unknown += 1,
            _ => return Err(PipeError::StageEval(EvalError::TypeError)),
        }
    }

    // The record, less any counts it carried, plus the three counts — built in
    // the element table, which the loop no longer needs.
    let out = &mut efs;
    let mut n = 0usize;
    for f in rec {
        let num = f.number;
        if num == o_true as u32 || num == o_false as u32 || num == o_unknown as u32 {
            continue;
        }
        let slot = out.get_mut(n).ok_or(PipeError::Encode)?;
        *slot = *f;
        n += 1;
    }
    for (num, c) in [(o_true, n_true), (o_false, n_false), (o_unknown, n_unknown)] {
        let slot = out.get_mut(n).ok_or(PipeError::Encode)?;
        *slot = Field {
            number: num as u32,
            value: Value::Int(c),
        };
        n += 1;
    }
    encode_frame(&Message { fields: &out[..n] }, dst)
}

/// Execute a pipeline: thread `input` (a typed record frame) through every stage
/// in order, serializing each stage's output as the next stage's input, and
/// write the final frame into `out`. `buf_a`/`buf_b` are caller-provided scratch
/// buffers (ping-ponged between stages) so the executor allocates nothing — on
/// device they live in module state. Returns the final frame length.
///
/// Stages are also subject to per-stage FAILURE ROUTING, which is deliberately
/// narrower on device than on the host. Of the spec's four policy knobs:
///
/// * **failure routing** — implemented here. A stage that fails evaluation, or
///   a map stage over its declared maximum, continues from its `on_failure`
///   stage instead of aborting, which is
///   deterministic and needs nothing outside this executor.
/// * **retries** — NOT implemented, because they would be a lie. A stage here is
///   pure compute over its input; re-running one that failed yields the same
///   failure. Retries are only meaningful for an effect that can fail
///   transiently.
/// * **compensation** — NOT implemented, for the same reason: there is no effect
///   to undo.
/// * **timeouts** — not enforceable in a synchronous executor.
///
/// That is not a gap so much as a placement. On device an effect IS a separate
/// connector `.fmod` wired into the graph, not an action inside this VM — so
/// retry and compensation belong to the graph and to the connector that owns the
/// external interaction, where a real timeout and a real undo exist. Putting
/// them here would give a deployment the appearance of durability with none of
/// the mechanism.
///
/// Routing is bounded: routes may form a cycle, so the walk carries a step
/// budget and returns `PipeError::RouteLoop` rather than spinning.
pub fn run_stages(
    stages: &[Stage],
    input: &[u8],
    buf_a: &mut [u8],
    buf_b: &mut [u8],
    out: &mut [u8],
) -> Result<usize, PipeError> {
    let mut spent = 0u64;
    run_stages_metered(stages, input, buf_a, buf_b, out, &mut spent)
}

/// [`run_stages`] that reports the total VM instructions spent across every stage
/// executed, including re-executed routes (work units).
pub fn run_stages_metered(
    stages: &[Stage],
    input: &[u8],
    buf_a: &mut [u8],
    buf_b: &mut [u8],
    out: &mut [u8],
    spent: &mut u64,
) -> Result<usize, PipeError> {
    run_stages_with(&ComputeOnly, stages, input, buf_a, buf_b, out, spent)
}

/// [`run_stages_metered`] with the per-stage evaluator supplied.
///
/// This is the one executor: failure routing, the bounded route walk and the
/// buffer ping-pong are identical whatever a stage runs. A caller that mixes
/// compute and decision stages threads them through HERE rather than through
/// a channel, so a chain that routes mid-way is one graph node.
pub fn run_stages_with<E: StageEval>(
    ev: &E,
    stages: &[Stage],
    input: &[u8],
    buf_a: &mut [u8],
    buf_b: &mut [u8],
    out: &mut [u8],
    spent: &mut u64,
) -> Result<usize, PipeError> {
    *spent = 0;
    if stages.is_empty() {
        if input.len() > out.len() {
            return Err(PipeError::Encode); // don't silently clip a pass-through
        }
        out[..input.len()].copy_from_slice(input);
        return Ok(input.len());
    }
    // Index-driven rather than a plain iteration, because a FAILURE ROUTE can
    // move execution to another stage. `steps` bounds the walk: routes may form
    // a cycle, and a pipeline that never terminates is not an option on device.
    let budget = stages.len().saturating_mul(4).saturating_add(8);
    let mut cur_len = 0usize;
    let mut latest_is_a = false;
    let mut i = 0usize;
    let mut steps = 0usize;
    let mut started = false;
    while i < stages.len() {
        steps += 1;
        if steps > budget {
            return Err(PipeError::RouteLoop);
        }
        let stage = &stages[i];
        // The input a stage reads is the previous stage's output — or, for the
        // first stage executed, the pipeline input. A routed-to stage reads the
        // same bytes the FAILED stage read: the failure produced no output, so
        // there is nothing newer to hand it.
        let res = if !started {
            ev.eval(stage, input, buf_a, spent)
        } else if latest_is_a {
            ev.eval(stage, &buf_a[..cur_len], buf_b, spent)
        } else {
            ev.eval(stage, &buf_b[..cur_len], buf_a, spent)
        };
        match res {
            Ok(n) => {
                cur_len = n;
                if !started {
                    latest_is_a = true;
                    started = true;
                } else {
                    latest_is_a = !latest_is_a;
                }
                i += 1;
            }
            Err(e) => {
                // Only a failure the author could anticipate is routable: an
                // evaluation failure, or a map stage exceeding the maximum it
                // declared. A structural fault (a truncated frame, an
                // undersized buffer, a record too wide for this target) is a
                // defect in the deployment, and the stage routed to would read
                // the same bytes, so it propagates whatever the policy says.
                let routable = matches!(
                    e,
                    PipeError::StageEval(_) | PipeError::NotConstructed | PipeError::TooMany
                );
                match stage.on_failure {
                    Some(target) if routable && (target as usize) < stages.len() => {
                        i = target as usize;
                    }
                    _ => return Err(e),
                }
            }
        }
    }
    let final_frame: &[u8] = if latest_is_a {
        &buf_a[..cur_len]
    } else {
        &buf_b[..cur_len]
    };
    if cur_len > out.len() {
        return Err(PipeError::Encode); // don't silently clip the final frame
    }
    // Copied element by element: the two branches above give `final_frame`
    // a length the compiler cannot fold into one value, and a slice copy it
    // cannot prove equal carries a panic path a module image links none of.
    for (dst, src) in out.iter_mut().zip(final_frame) {
        *dst = *src;
    }
    Ok(cur_len)
}
