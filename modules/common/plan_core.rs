// Pipeline -> module graph lowering, on device.
//
// This is the layer that removes judgment from graph construction. Given an
// ordered plan — each stage either pure COMPUTE, a DECISION, or a connector
// EFFECT — the graph is a PURE FUNCTION of the stage kinds. No human decides
// what becomes a node; the policy is fixed:
//
//   * a maximal run of consecutive COMPUTE stages collapses into ONE `pipeline`
//     node whose `ir_stages` param runs them as a bytecode chain, so pure
//     compute never touches a channel;
//   * a DECISION becomes its own node (the VM cannot branch), which also breaks
//     the surrounding compute run;
//   * each EFFECT becomes a genuine connector node — the provider module its
//     binding names — wired into the transport;
//   * dataflow is threaded in stage order, `cli_in -> ... -> cli_out` on a host
//     profile, ends left open on an embedded one.
//
// The same plan always yields byte-identical YAML, so a graph is
// content-addressable exactly like the bytecode it carries.
//
// WHY THIS EMITS YAML DIRECTLY rather than building a `graph_core::GraphSpec`:
// a GraphSpec borrows its names, params and wires, and every one of those
// strings has to be BUILT here (node suffixes, hex params, `node.port` wire
// endpoints). Handing back a GraphSpec would mean a struct holding both an arena
// and slices into that same arena — self-referential, and not expressible safely.
// The cost is that the YAML layout now has two writers, so
// `plan_matches_graph_core_emission` pins them together: it lowers a plan and
// asserts the result equals what `graph_to_yaml` produces for the equivalent
// hand-built spec. If the two ever drift, that test fails.
//
// Requires `pack_core` (the `ir_stages` container), `hex_core` (param encoding)
// and `graph_core` (the `GraphError` vocabulary and the layout it defines).

/// Version tag of the sibling-owned provider modules this composes. Providers
/// publish under `<silicon>/<module>:<PROVIDER_VERSION>` in the fluxor OCI
/// store; the deploy driver pins that reference so the build resolves the module
/// from the store rather than from a Chronicle-owned `.fmod`.
pub const PROVIDER_VERSION: &[u8] = b"0.1.0";

/// A connector effect binding: which capability a Resource effect realizes, plus
/// its endpoint and params.
///
/// Each variant maps to exactly one provider module — the effect -> capability
/// -> provider choice is DATA, not judgment. An empty `password` on Redis means
/// no password, matching the host's `Option`.
#[derive(Clone, Copy)]
pub enum Connector<'a> {
    Redis {
        endpoint_hex: &'a [u8],
        password: &'a [u8],
    },
    Pg {
        endpoint_hex: &'a [u8],
        user: &'a [u8],
        database: &'a [u8],
        password: &'a [u8],
    },
    Kafka {
        endpoint_hex: &'a [u8],
        client_id: &'a [u8],
        topic: &'a [u8],
    },
    Mongo {
        endpoint_hex: &'a [u8],
        user: &'a [u8],
        database: &'a [u8],
        password: &'a [u8],
        collection: &'a [u8],
    },
}

impl Connector<'_> {
    /// The connector's kind tag — the base node INSTANCE name (and, repeated, the
    /// `<kind>N` suffix). The node's `type:` is the provider module, not this.
    ///
    /// Deliberately not `fn(&self) -> &'static [u8]`: a match returning static
    /// references compiles to a table of (pointer, len) pairs, and static pointer
    /// tables do not relocate in a PIC module. Every accessor here appends into a
    /// caller buffer for that reason.
    pub fn kind(&self, out: &mut [u8]) -> Result<usize, GraphError> {
        match self {
            Connector::Redis { .. } => gput(out, 0, b"redis"),
            Connector::Pg { .. } => gput(out, 0, b"pg"),
            Connector::Kafka { .. } => gput(out, 0, b"kafka"),
            Connector::Mongo { .. } => gput(out, 0, b"mongo"),
        }
    }

    /// The sibling-owned provider module that realizes this capability — the
    /// graph node's `type:`. Silicon-independent; [`provider_pin`] adds the
    /// silicon-scoped store reference.
    ///
    /// [`provider_pin`]: Connector::provider_pin
    pub fn provider_module(&self, out: &mut [u8]) -> Result<usize, GraphError> {
        match self {
            Connector::Redis { .. } => gput(out, 0, b"redis_client"), // Lattice
            Connector::Pg { .. } => gput(out, 0, b"pg_client"),       // Lattice
            Connector::Kafka { .. } => gput(out, 0, b"kafka_client"), // Quantum
            Connector::Mongo { .. } => gput(out, 0, b"mongo_client"), // Lattice
        }
    }

    /// The store pin to record so the build composes this capability's provider
    /// from the OCI store, scoped to `silicon` (e.g. `b"bcm2712"`):
    /// `<silicon>/<module>:<PROVIDER_VERSION>`.
    pub fn provider_pin(&self, silicon: &[u8], out: &mut [u8]) -> Result<usize, GraphError> {
        let mut m = [0u8; 32];
        let n = self.provider_module(&mut m)?;
        let mut p = gput(out, 0, silicon)?;
        p = gput(out, p, b"/")?;
        p = gput(out, p, &m[..n])?;
        p = gput(out, p, b":")?;
        gput(out, p, PROVIDER_VERSION)
    }

    /// This connector's `(data_in, data_out)` port names.
    fn ports(&self, inp: &mut [u8], outp: &mut [u8]) -> Result<(usize, usize), GraphError> {
        match self {
            Connector::Redis { .. } | Connector::Pg { .. } => {
                Ok((gput(inp, 0, b"request_in")?, gput(outp, 0, b"reply_out")?))
            }
            Connector::Kafka { .. } | Connector::Mongo { .. } => {
                Ok((gput(inp, 0, b"publish_in")?, gput(outp, 0, b"status_out")?))
            }
        }
    }
}

/// One resolved stage of a pipeline, ready to lower.
#[derive(Clone, Copy)]
pub enum PlanStage<'a> {
    /// Pure compute: one stage's flat checked IR.
    Compute { stage_ir: &'a [u8] },
    /// A decision's pre-packed container.
    Decision { container: &'a [u8] },
    /// A connector effect.
    Effect(Connector<'a>),
}

/// What the graph is being lowered FOR: the fluxor target, the connector
/// transport, and whether the chain is `cli`-bracketed.
///
/// The node policy is profile-independent — the profile only decides the target,
/// the transport node the connectors reach the network through, and whether the
/// chain's ends are wired to a CLI or left open for a surrounding graph.
#[derive(Clone, Copy)]
pub struct TargetProfile<'a> {
    pub target: &'a [u8],
    pub transport: &'a [u8],
    pub host_cli: bool,
}

impl<'a> TargetProfile<'a> {
    /// The host profile: `linux` target, `linux_net` transport, `cli`-bracketed.
    pub fn host() -> TargetProfile<'a> {
        TargetProfile {
            target: b"linux",
            transport: b"linux_net",
            host_cli: true,
        }
    }

    /// An embedded profile: the chain's ends are left open for the surrounding
    /// graph to wire, so no `cli` edges and no `cli` platform capability.
    pub fn embedded(target: &'a [u8]) -> TargetProfile<'a> {
        TargetProfile {
            target,
            transport: b"ip",
            host_cli: false,
        }
    }
}

/// The largest chain this can lower — nodes are named `<kind>N`, and the plan is
/// walked twice (once for modules, once for wiring), so the chain is recorded
/// rather than rebuilt.
pub const MAX_CHAIN: usize = 32;

/// Longest node name plus port, e.g. `pipeline12.result_out`.
const NAME_CAP: usize = 48;

/// One node in the dataflow chain: its instance name and the ports the previous
/// and next stages connect to.
#[derive(Clone, Copy)]
struct ChainNode {
    name: [u8; NAME_CAP],
    name_len: u8,
    in_port: [u8; NAME_CAP],
    in_len: u8,
    out_port: [u8; NAME_CAP],
    out_len: u8,
}

impl ChainNode {
    fn blank() -> Self {
        ChainNode {
            name: [0u8; NAME_CAP],
            name_len: 0,
            in_port: [0u8; NAME_CAP],
            in_len: 0,
            out_port: [0u8; NAME_CAP],
            out_len: 0,
        }
    }
    fn name(&self) -> &[u8] {
        &self.name[..self.name_len as usize]
    }
    fn in_port(&self) -> &[u8] {
        &self.in_port[..self.in_len as usize]
    }
    fn out_port(&self) -> &[u8] {
        &self.out_port[..self.out_len as usize]
    }
}

fn set(dst: &mut [u8; NAME_CAP], len: &mut u8, s: &[u8]) -> Result<(), GraphError> {
    if s.len() > NAME_CAP {
        return Err(GraphError::TooLarge);
    }
    dst[..s.len()].copy_from_slice(s);
    *len = s.len() as u8;
    Ok(())
}

/// `<base>` for the first instance of a kind, `<base><n+1>` after that — the
/// host's stable-unique naming.
fn instance_name(base: &[u8], n: usize, out: &mut [u8]) -> Result<usize, GraphError> {
    let p = gput(out, 0, base)?;
    if n == 0 {
        return Ok(p);
    }
    gput_u32(out, p, (n + 1) as u32)
}

/// Emit `  <key>: "<value>"\n` — a quoted scalar param, at the module indent.
fn emit_param(out: &mut [u8], p: usize, key: &[u8], value: &[u8]) -> Result<usize, GraphError> {
    let mut p = gput(out, p, b"      ")?;
    p = gput(out, p, key)?;
    p = gput(out, p, b": \"")?;
    p = gput(out, p, value)?;
    gput(out, p, b"\"\n")
}

/// Emit a connector node's params, in the provider manifest's order.
fn emit_connector_params(out: &mut [u8], p: usize, c: &Connector) -> Result<usize, GraphError> {
    match c {
        Connector::Redis {
            endpoint_hex,
            password,
        } => {
            let p = emit_param(out, p, b"endpoint", endpoint_hex)?;
            if password.is_empty() {
                Ok(p)
            } else {
                emit_param(out, p, b"password", password)
            }
        }
        Connector::Pg {
            endpoint_hex,
            user,
            database,
            password,
        } => {
            let mut p = emit_param(out, p, b"endpoint", endpoint_hex)?;
            p = emit_param(out, p, b"user", user)?;
            p = emit_param(out, p, b"database", database)?;
            emit_param(out, p, b"password", password)
        }
        Connector::Kafka {
            endpoint_hex,
            client_id,
            topic,
        } => {
            let mut p = emit_param(out, p, b"endpoint", endpoint_hex)?;
            p = emit_param(out, p, b"client_id", client_id)?;
            emit_param(out, p, b"produce_topic", topic)
        }
        Connector::Mongo {
            endpoint_hex,
            user,
            database,
            password,
            collection,
        } => {
            let mut p = emit_param(out, p, b"endpoint", endpoint_hex)?;
            p = emit_param(out, p, b"user", user)?;
            p = emit_param(out, p, b"database", database)?;
            p = emit_param(out, p, b"password", password)?;
            emit_param(out, p, b"collection", collection)
        }
    }
}

/// Emit one `wiring:` edge, `  - from: <a>\n    to: <b>\n`.
fn emit_wire(
    out: &mut [u8],
    p: usize,
    from_node: &[u8],
    from_port: &[u8],
    to_node: &[u8],
    to_port: &[u8],
) -> Result<usize, GraphError> {
    let mut p = gput(out, p, b"  - from: ")?;
    p = gput(out, p, from_node)?;
    p = gput(out, p, b".")?;
    p = gput(out, p, from_port)?;
    p = gput(out, p, b"\n    to: ")?;
    p = gput(out, p, to_node)?;
    p = gput(out, p, b".")?;
    p = gput(out, p, to_port)?;
    gput(out, p, b"\n")
}

/// Lower `stages` into fluxor graph YAML for the host profile.
pub fn lower_pipeline(
    stages: &[PlanStage],
    target: &[u8],
    tick_us: u32,
    out: &mut [u8],
    scratch: &mut [u8],
) -> Result<usize, GraphError> {
    let mut profile = TargetProfile::host();
    profile.target = target;
    lower_pipeline_with(stages, &profile, tick_us, out, scratch)
}

/// Lower `stages` into fluxor graph YAML for `profile`.
///
/// `scratch` holds the packed `ir_stages` container for the largest compute run
/// before it is hex-encoded; it must be at least as large as that container.
/// Pure and deterministic: identical inputs render identical bytes.
pub fn lower_pipeline_with(
    stages: &[PlanStage],
    profile: &TargetProfile,
    tick_us: u32,
    out: &mut [u8],
    scratch: &mut [u8],
) -> Result<usize, GraphError> {
    let mut chain = [ChainNode::blank(); MAX_CHAIN];
    let mut n_chain = 0usize;
    let mut needs_net = false;

    // Per-kind instance counts, so repeated nodes get stable unique names.
    let (mut pipe_n, mut decision_n) = (0usize, 0usize);
    let (mut redis_n, mut pg_n, mut kafka_n, mut mongo_n) = (0usize, 0usize, 0usize, 0usize);

    // Modules are emitted as the plan is walked; wiring needs the whole chain, so
    // it is emitted afterwards from `chain`.
    let mut p = gput(out, 0, b"target: ")?;
    p = gput(out, p, profile.target)?;
    p = gput(out, p, b"\ntick_us: ")?;
    p = gput_u32(out, p, tick_us)?;
    p = gput(out, p, b"\n")?;
    // `accept_cycles` depends on whether any effect appears, which is not known
    // until the walk finishes — so the header is emitted last, into a prefix
    // buffer. Instead the walk runs first and the document is assembled after.
    let header_end = p;

    // ---- pass 1: walk the plan, recording the chain ----
    //
    // Two passes, because `scheduler.accept_cycles` and the `net` platform
    // capability both depend on whether ANY effect appears, and both are emitted
    // before the modules. Rather than buffer the module text somewhere and move
    // it, the walk records what each node is and pass 2 renders it.
    let mut effects = [None::<Connector>; MAX_CHAIN];
    let mut ir_spans = [(0usize, 0usize); MAX_CHAIN];
    let mut kinds = [0u8; MAX_CHAIN]; // 0 compute, 1 decision, 2 effect
    let mut decisions = [&[] as &[u8]; MAX_CHAIN];

    // The packed ir_stages containers for each compute run live end to end in
    // `scratch`; `ir_spans` records where each one is.
    let mut scratch_used = 0usize;

    let mut i = 0usize;
    while i < stages.len() {
        if n_chain >= MAX_CHAIN {
            return Err(GraphError::TooLarge);
        }
        match &stages[i] {
            PlanStage::Compute { .. } => {
                // Collapse the maximal run of consecutive compute stages into one
                // pipeline node — the policy that keeps pure compute off the wire.
                let mut run = [&[] as &[u8]; MAX_CHAIN];
                let mut n_run = 0usize;
                while let Some(PlanStage::Compute { stage_ir }) = stages.get(i) {
                    if n_run >= MAX_CHAIN {
                        return Err(GraphError::TooLarge);
                    }
                    run[n_run] = stage_ir;
                    n_run += 1;
                    i += 1;
                }
                let packed = pack_ir_stages(&mut scratch[scratch_used..], &run[..n_run], &[])
                    .map_err(|_| GraphError::TooLarge)?;
                ir_spans[n_chain] = (scratch_used, packed);
                scratch_used += packed;

                let mut name = [0u8; NAME_CAP];
                let nl = instance_name(b"pipeline", pipe_n, &mut name)?;
                pipe_n += 1;
                kinds[n_chain] = 0;
                let c = &mut chain[n_chain];
                set(&mut c.name, &mut c.name_len, &name[..nl])?;
                set(&mut c.in_port, &mut c.in_len, b"record_in")?;
                set(&mut c.out_port, &mut c.out_len, b"result_out")?;
                n_chain += 1;
            }
            PlanStage::Decision { container } => {
                let mut name = [0u8; NAME_CAP];
                let nl = instance_name(b"decision", decision_n, &mut name)?;
                decision_n += 1;
                kinds[n_chain] = 1;
                decisions[n_chain] = container;
                let c = &mut chain[n_chain];
                set(&mut c.name, &mut c.name_len, &name[..nl])?;
                set(&mut c.in_port, &mut c.in_len, b"record_in")?;
                set(&mut c.out_port, &mut c.out_len, b"result_out")?;
                n_chain += 1;
                i += 1;
            }
            PlanStage::Effect(binding) => {
                needs_net = true;
                let n = match binding {
                    Connector::Redis { .. } => &mut redis_n,
                    Connector::Pg { .. } => &mut pg_n,
                    Connector::Kafka { .. } => &mut kafka_n,
                    Connector::Mongo { .. } => &mut mongo_n,
                };
                let mut base = [0u8; NAME_CAP];
                let bl = binding.kind(&mut base)?;
                let mut name = [0u8; NAME_CAP];
                let nl = instance_name(&base[..bl], *n, &mut name)?;
                *n += 1;

                let (mut ip, mut op) = ([0u8; NAME_CAP], [0u8; NAME_CAP]);
                let (il, ol) = binding.ports(&mut ip, &mut op)?;

                kinds[n_chain] = 2;
                effects[n_chain] = Some(*binding);
                let c = &mut chain[n_chain];
                set(&mut c.name, &mut c.name_len, &name[..nl])?;
                set(&mut c.in_port, &mut c.in_len, &ip[..il])?;
                set(&mut c.out_port, &mut c.out_len, &op[..ol])?;
                n_chain += 1;
                i += 1;
            }
        }
    }

    // ---- pass 2: the document, now that `needs_net` is known ----
    let mut p = header_end;
    if needs_net {
        p = gput(out, p, b"scheduler:\n  accept_cycles: true\n")?;
    }

    // An EMPTY section is an explicit empty collection, never a bare key. A bare
    // `platform:` parses as null ("must be a mapping") and `wiring` is required
    // outright — an embedded compute-only graph has both empty and hit each in
    // turn, producing a graph fluxor refused to build.
    if !profile.host_cli && !needs_net {
        p = gput(out, p, b"\nplatform: {}\n")?;
    } else {
        p = gput(out, p, b"\nplatform:\n")?;
        if profile.host_cli {
            p = gput(out, p, b"  cli: {}\n")?;
        }
        if needs_net {
            p = gput(out, p, b"  net: {}\n")?;
        }
    }

    if n_chain == 0 {
        p = gput(out, p, b"\nmodules: []\n")?;
    } else {
        p = gput(out, p, b"\nmodules:\n")?;
    }
    for k in 0..n_chain {
        p = gput(out, p, b"  - name: ")?;
        p = gput(out, p, chain[k].name())?;
        p = gput(out, p, b"\n")?;
        match kinds[k] {
            0 => {
                // `type:` only when the instance name is not the module name.
                // The first compute node is `pipeline`, so the name implies the
                // type; a second run is `pipeline2`, and without an explicit
                // type fluxor looks for a module by that name and refuses the
                // graph. Same rule as the decision arm below.
                if chain[k].name() != b"pipeline" {
                    p = gput(out, p, b"    type: pipeline\n")?;
                }
                p = gput(out, p, b"    params:\n      ir_stages: \"")?;
                let (off, len) = ir_spans[k];
                p = put_hex(out, p, &scratch[off..off + len])?;
                p = gput(out, p, b"\"\n")?;
            }
            1 => {
                // `type:` only when the instance name is NOT the module name —
                // the first decision node is called `decision`, so the name
                // implies the type and it is omitted.
                if chain[k].name() != b"decision" {
                    p = gput(out, p, b"    type: decision\n")?;
                }
                p = gput(out, p, b"    params:\n      decision: \"")?;
                p = put_hex(out, p, decisions[k])?;
                p = gput(out, p, b"\"\n")?;
            }
            _ => {
                let c = effects[k].ok_or(GraphError::TooLarge)?;
                let mut m = [0u8; NAME_CAP];
                let ml = c.provider_module(&mut m)?;
                p = gput(out, p, b"    type: ")?;
                p = gput(out, p, &m[..ml])?;
                p = gput(out, p, b"\n    params:\n")?;
                p = emit_connector_params(out, p, &c)?;
            }
        }
    }
    // ---- wiring: source edge, inter-stage edges, sink edge, transport edges ----
    //
    // A single node on an embedded profile has NO edges at all: the ends are
    // left open for the surrounding graph. Emitting a bare `wiring:` key there
    // would be refused ("wiring must be a list"), so the section is counted
    // first and only opened if something goes in it.
    let mut n_wires = 0usize;
    if n_chain > 0 {
        if profile.host_cli {
            n_wires += 2;
        }
        n_wires += n_chain - 1;
    }
    for kind in kinds.iter().take(n_chain) {
        // Each effect adds a transport edge in each direction.
        if *kind == 2 {
            n_wires += 2;
        }
    }
    if n_wires == 0 {
        p = gput(out, p, b"\nwiring: []\n")?;
    } else {
        p = gput(out, p, b"\nwiring:\n")?;
    }
    if n_chain > 0 {
        if profile.host_cli {
            p = emit_wire(
                out,
                p,
                b"cli_in",
                b"stdin_out",
                chain[0].name(),
                chain[0].in_port(),
            )?;
        }
        for k in 0..n_chain.saturating_sub(1) {
            let (a, b) = (chain[k], chain[k + 1]);
            p = emit_wire(out, p, a.name(), a.out_port(), b.name(), b.in_port())?;
        }
        if profile.host_cli {
            let last = chain[n_chain - 1];
            p = emit_wire(
                out,
                p,
                last.name(),
                last.out_port(),
                b"cli_out",
                b"bytes_in",
            )?;
        }
    }
    // Transport edges last, in node order — the canonical order the host emits.
    for k in 0..n_chain {
        if kinds[k] != 2 {
            continue;
        }
        let name = chain[k].name();
        p = emit_wire(out, p, name, b"net_out", profile.transport, b"net_in")?;
        p = emit_wire(out, p, profile.transport, b"net_out", name, b"net_in")?;
    }
    Ok(p)
}

/// Append lowercase hex of `bytes` at `p`.
fn put_hex(out: &mut [u8], p: usize, bytes: &[u8]) -> Result<usize, GraphError> {
    if p + bytes.len() * 2 > out.len() {
        return Err(GraphError::TooLarge);
    }
    let n = hex_encode(bytes, &mut out[p..]).ok_or(GraphError::TooLarge)?;
    Ok(p + n)
}

/// The provider store pins a plan needs, deduplicated and in stage order.
///
/// The deploy driver records these into the project's `fluxor.lock` before
/// building, so slot-image resolves each provider module from the OCI store.
/// Pure: a function of the plan and the silicon — the pin-side mirror of the
/// graph itself.
///
/// Writes each pin into `out[i]`, returning how many were written.
pub fn plan_provider_pins(
    stages: &[PlanStage],
    silicon: &[u8],
    out: &mut [&mut [u8]],
    lens: &mut [usize],
) -> Result<usize, GraphError> {
    let mut n = 0usize;
    for st in stages {
        let PlanStage::Effect(binding) = st else {
            continue;
        };
        let mut pin = [0u8; 96];
        let pl = binding.provider_pin(silicon, &mut pin)?;

        let mut seen = false;
        for k in 0..n {
            if lens[k] == pl && out[k][..pl] == pin[..pl] {
                seen = true;
                break;
            }
        }
        if seen {
            continue;
        }
        if n >= out.len() || n >= lens.len() {
            return Err(GraphError::TooLarge);
        }
        if out[n].len() < pl {
            return Err(GraphError::TooLarge);
        }
        out[n][..pl].copy_from_slice(&pin[..pl]);
        lens[n] = pl;
        n += 1;
    }
    Ok(n)
}
