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
// Typed record frame (self-describing, so integer fields survive a round trip —
// the scalar frame of the Expression module was bytes-only):
//   [count:u8] then count × [number:u8][type:u8][len:u16 LE][payload]
//   type 0 = byte string (payload = raw bytes)
//   type 1 = i64        (payload = 8 bytes little-endian)

/// Maximum fields a pipeline record frame may carry (matches the builder bound).
pub const MAX_PIPE_FIELDS: usize = MAX_BUILD_FIELDS;

const TY_BYTES: u8 = 0;
const TY_I64: u8 = 1;

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
    let mut off = 1usize;
    let mut fi = 0usize;
    while fi < count {
        if fi >= MAX_PIPE_FIELDS {
            return Err(PipeError::BadFrame);
        }
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
/// stage, `[max_cost:u32 LE][code_len:u16 LE][code bytes]`. This is what a config
/// carries (hex-encoded) so one pipeline `.fmod` runs any Pipeline.
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
    let n = msg.fields.len();
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
        // A builtin's arena-backed result serializes as its bytes.
        let value = resolve_scratch(f.value, scratch);
        let f = &Field {
            number: f.number,
            value,
        };
        let (ty, payload): (u8, [u8; 8]) = match f.value {
            Value::Int(i) => (TY_I64, i.to_le_bytes()),
            Value::Uint(u) => (TY_I64, (u as i64).to_le_bytes()),
            Value::Bool(b) => (TY_I64, (b as i64).to_le_bytes()),
            _ => (TY_BYTES, [0u8; 8]),
        };
        // header: number:u8, type:u8, len:u16 LE
        let (len, bytes): (usize, &[u8]) = match f.value {
            Value::Bytes(b) => (b.len(), b),
            Value::Str(s) => (s.len(), s.as_bytes()),
            Value::Null => (0, &[]),
            // Msg and Double have no typed frame representation; reject rather
            // than emit a zero-filled byte string mislabelled as data.
            Value::Msg(_) | Value::Double(_) => return Err(PipeError::Encode),
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
/// into `dst`. Returns the encoded length.
fn run_stage(stage: &Stage, src: &[u8], dst: &mut [u8]) -> Result<usize, PipeError> {
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
    match eval_full_scratch(
        stage.code,
        &params,
        &mut builder,
        &mut scratch,
        stage.max_cost,
    ) {
        Ok(EvalResult::Constructed) => encode_frame_scratch(&builder.message(), &scratch, dst),
        Ok(EvalResult::Scalar(_)) => Err(PipeError::NotConstructed),
        Err(e) => Err(PipeError::StageEval(e)),
    }
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
/// * **failure routing** — implemented here. A stage that fails evaluation
///   continues from its `on_failure` stage instead of aborting, which is
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
            run_stage(stage, input, buf_a)
        } else if latest_is_a {
            run_stage(stage, &buf_a[..cur_len], buf_b)
        } else {
            run_stage(stage, &buf_b[..cur_len], buf_a)
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
                // Only an EVALUATION failure is routable. A structural fault
                // (a truncated frame, an undersized buffer) is a defect in the
                // deployment, not a condition the pipeline author anticipated,
                // so it propagates whatever the policy says.
                let routable = matches!(e, PipeError::StageEval(_) | PipeError::NotConstructed);
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
    out[..cur_len].copy_from_slice(final_frame);
    Ok(cur_len)
}
