// Packing compiled bytecode into the on-device param containers, on device.
//
// The compiler emits programs; the modules read CONTAINERS. This is the step
// between them: the exact byte layouts a config carries as hex, written by the
// same node that compiled the programs. A node that can author an artefact can
// now also pack it into the form another node's `.fmod` loads, with no host in
// the loop.
//
// The layouts mirror the readers in `pipeline_core` (`stage_at`), `decision_core`
// (`run_decision`) and `agg_core` (`parse_def`), so anything written here
// round-trips through the module that consumes it. That is the whole contract:
// these functions have no freedom, they reproduce a format that already exists.
//
// Everything appends into a caller buffer and returns the length written — no
// allocator, and an output that does not fit is reported rather than truncated.
//
// Requires `pipeline_core`, which owns `ROUTE_NONE`. The route sentinel is the
// READER's vocabulary, and defining a second name for the same wire byte here
// would let the two drift apart silently.

/// Why packing failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackError {
    /// The output buffer cannot hold the container.
    TooLarge,
    /// More stages, rules or operators than the one-byte count can express.
    TooManyItems,
    /// A program's code is longer than the two-byte length prefix allows.
    ProgramTooLong,
}

/// One bytecode program as the containers carry it: a `max_cost` budget paired
/// with the code, written as `[max_cost:u32 LE][code_len:u16 LE][code]`.
#[derive(Clone, Copy)]
pub struct Prog<'a> {
    pub cost: u64,
    pub code: &'a [u8],
}

/// One aggregation operator: its kind byte (0=Count, 1=Sum, 2=Min, 3=Max,
/// 4=Avg, 5=Distinct, 6=TopK, 7=Quantile) and selector program (empty for Count).
#[derive(Clone, Copy)]
pub struct PackedOp<'a> {
    pub kind: u8,
    pub selector: Prog<'a>,
}

/// The five scalar fields every aggregation `def` container opens with.
///
/// `window_step` of 0 (or equal to `window_size`) is tumbling; a
/// `correction_horizon` of 0 disables retractable corrections, so late data is
/// dropped rather than folded back in.
#[derive(Clone, Copy)]
pub struct AggHeader {
    pub window_size: i64,
    pub lateness: i64,
    pub max_lanes: u32,
    pub window_step: i64,
    pub correction_horizon: i64,
}

fn pk_put(out: &mut [u8], p: usize, s: &[u8]) -> Result<usize, PackError> {
    if p + s.len() > out.len() {
        return Err(PackError::TooLarge);
    }
    out[p..p + s.len()].copy_from_slice(s);
    Ok(p + s.len())
}

fn pk_u8(out: &mut [u8], p: usize, v: u8) -> Result<usize, PackError> {
    pk_put(out, p, &[v])
}

fn pk_u16(out: &mut [u8], p: usize, v: u16) -> Result<usize, PackError> {
    pk_put(out, p, &v.to_le_bytes())
}

fn pk_u32(out: &mut [u8], p: usize, v: u32) -> Result<usize, PackError> {
    pk_put(out, p, &v.to_le_bytes())
}

fn pk_i64(out: &mut [u8], p: usize, v: i64) -> Result<usize, PackError> {
    pk_put(out, p, &v.to_le_bytes())
}

/// A one-byte count, refused rather than wrapped when it will not fit.
fn count_u8(n: usize) -> Result<u8, PackError> {
    if n > u8::MAX as usize {
        return Err(PackError::TooManyItems);
    }
    Ok(n as u8)
}

/// `[max_cost:u32 LE][code_len:u16 LE][code]`.
///
/// The cost is NARROWED to u32 here, exactly as the host does — the containers
/// carry a 32-bit budget and the compiler's `u64` never approaches it.
fn pk_prog(out: &mut [u8], p: usize, prog: &Prog) -> Result<usize, PackError> {
    if prog.code.len() > u16::MAX as usize {
        return Err(PackError::ProgramTooLong);
    }
    let mut p = pk_u32(out, p, prog.cost as u32)?;
    p = pk_u16(out, p, prog.code.len() as u16)?;
    pk_put(out, p, prog.code)
}

/// `[ir_len:u16 LE][flat_ir]` — an embedded IR program, with no cost field.
///
/// The device re-derives the cost bound by lowering the IR, so shipping one
/// would be shipping a number nobody has to trust.
fn pk_ir_prog(out: &mut [u8], p: usize, flat: &[u8]) -> Result<usize, PackError> {
    if flat.len() > u16::MAX as usize {
        return Err(PackError::ProgramTooLong);
    }
    let p = pk_u16(out, p, flat.len() as u16)?;
    pk_put(out, p, flat)
}

/// The route byte for stage `i`: the caller's entry, or `ROUTE_NONE` when
/// `routes` is shorter than `stages` (a short or empty slice leaves the
/// remaining stages unrouted).
fn route_at(routes: &[u8], i: usize) -> u8 {
    match routes.get(i) {
        Some(r) => *r,
        None => ROUTE_NONE,
    }
}

/// The pipeline `stages` container: `[nstages:u8]` then per stage
/// `[route:u8][max_cost:u32 LE][code_len:u16 LE][code]`.
///
/// Each stage is a Transformation's construction bytecode, ending in FINISH_MSG.
pub fn pack_stages(out: &mut [u8], stages: &[Prog], routes: &[u8]) -> Result<usize, PackError> {
    let mut p = pk_u8(out, 0, count_u8(stages.len())?)?;
    for (i, s) in stages.iter().enumerate() {
        p = pk_u8(out, p, route_at(routes, i))?;
        p = pk_prog(out, p, s)?;
    }
    Ok(p)
}

/// The pipeline `ir_stages` container: `[nstages:u8]` then per stage
/// `[route:u8][ir_len:u16 LE][flat_ir]`.
///
/// The IR-shipping counterpart of [`pack_stages`]: the device lowers each stage
/// at load rather than running bytecode it was simply handed.
pub fn pack_ir_stages(out: &mut [u8], stages: &[&[u8]], routes: &[u8]) -> Result<usize, PackError> {
    let mut p = pk_u8(out, 0, count_u8(stages.len())?)?;
    for (i, flat) in stages.iter().enumerate() {
        p = pk_u8(out, p, route_at(routes, i))?;
        p = pk_ir_prog(out, p, flat)?;
    }
    Ok(p)
}

/// The `decision` container `decision_core::run_decision` consumes:
/// `[nrules:u8]`, then per rule `[when prog][outcome prog]`, then the default.
///
/// `when` programs are Bool-typed; outcomes construct a message.
pub fn pack_decision(
    out: &mut [u8],
    rules: &[(Prog, Prog)],
    default: &Prog,
) -> Result<usize, PackError> {
    let mut p = pk_u8(out, 0, count_u8(rules.len())?)?;
    for (when, outcome) in rules {
        p = pk_prog(out, p, when)?;
        p = pk_prog(out, p, outcome)?;
    }
    pk_prog(out, p, default)
}

/// The aggregation `def` container:
///
/// ```text
///   [window:i64][lateness:i64][max_lanes:u32][window_step:i64][correction_horizon:i64]
///   [key prog][time prog][emit prog]
///   [nops:u8] then per op [kind:u8][selector prog]
/// ```
pub fn pack_agg_def(
    out: &mut [u8],
    hdr: &AggHeader,
    key: &Prog,
    time: &Prog,
    emit: &Prog,
    ops: &[PackedOp],
) -> Result<usize, PackError> {
    let mut p = pk_agg_header(out, hdr)?;
    p = pk_prog(out, p, key)?;
    p = pk_prog(out, p, time)?;
    p = pk_prog(out, p, emit)?;
    p = pk_u8(out, p, count_u8(ops.len())?)?;
    for op in ops {
        p = pk_u8(out, p, op.kind)?;
        p = pk_prog(out, p, &op.selector)?;
    }
    Ok(p)
}

/// The aggregation IR-`def` container — the IR analogue of [`pack_agg_def`].
///
/// Same 36-byte header; each program is a flat checked IR instead of pre-lowered
/// bytecode, which the aggregation module transcodes at load. "It lowered" means
/// "it can run it", and the cost bound comes from the IR rather than a param.
pub fn pack_agg_ir_def(
    out: &mut [u8],
    hdr: &AggHeader,
    key_ir: &[u8],
    time_ir: &[u8],
    emit_ir: &[u8],
    ops: &[(u8, &[u8])],
) -> Result<usize, PackError> {
    let mut p = pk_agg_header(out, hdr)?;
    p = pk_ir_prog(out, p, key_ir)?;
    p = pk_ir_prog(out, p, time_ir)?;
    p = pk_ir_prog(out, p, emit_ir)?;
    p = pk_u8(out, p, count_u8(ops.len())?)?;
    for (kind, sel_ir) in ops {
        p = pk_u8(out, p, *kind)?;
        p = pk_ir_prog(out, p, sel_ir)?;
    }
    Ok(p)
}

/// The 36-byte scalar header both `def` container forms share.
fn pk_agg_header(out: &mut [u8], hdr: &AggHeader) -> Result<usize, PackError> {
    let mut p = pk_i64(out, 0, hdr.window_size)?;
    p = pk_i64(out, p, hdr.lateness)?;
    p = pk_u32(out, p, hdr.max_lanes)?;
    p = pk_i64(out, p, hdr.window_step)?;
    pk_i64(out, p, hdr.correction_horizon)
}
