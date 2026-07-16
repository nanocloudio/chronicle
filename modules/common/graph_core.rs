// Graph-spec emission, on device — turning module instances and wiring into the
// fluxor YAML a runtime loads.
//
// This is the deployment half of authoring. A node that can compile and seal a
// document can now also emit the GRAPH that runs it: which modules to
// instantiate, what params they carry, and how their ports connect.
//
// Whether a node should plan its OWN graph is a separate question — normally a
// node receives a graph rather than authoring one. What this enables is a node
// authoring a graph for ANOTHER node: a controller that compiles a document and
// hands the result to a fleet, with no Linux build host anywhere in the loop.
//
// The output is byte-compatible with the host's `GraphSpec::to_yaml`, because a
// graph that differs by a byte is a graph that may parse differently. The block
// style, the blank lines between sections and the two-space indent are all part
// of that contract, not decoration.
//
// Emission is append-only into a caller buffer: no allocator, and a truncated
// document is reported rather than written.

/// Why graph emission failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphError {
    /// The output buffer cannot hold the rendered graph.
    TooLarge,
}

/// One module instance in the graph: an instance name, an optional module type
/// (absent means the name IS the type), and its params in declaration order.
#[derive(Clone, Copy)]
pub struct ModuleInst<'a> {
    pub name: &'a [u8],
    /// Empty when the instance name is also the module type.
    pub module_type: &'a [u8],
    /// `(key, value)` pairs, rendered in the order given — deterministic.
    pub params: &'a [(&'a [u8], &'a [u8])],
}

/// One directed connection between two module ports (`node.port`).
#[derive(Clone, Copy)]
pub struct Wire<'a> {
    pub from: &'a [u8],
    pub to: &'a [u8],
}

/// Everything a graph needs beyond its modules and wiring.
pub struct GraphSpec<'a> {
    pub target: &'a [u8],
    pub tick_us: u32,
    /// Emits a `scheduler.accept_cycles` block when set.
    pub accept_cycles: bool,
    /// Platform capabilities to enable (`cli`, `net`, …).
    pub platform: &'a [&'a [u8]],
    pub modules: &'a [ModuleInst<'a>],
    pub wiring: &'a [Wire<'a>],
}

fn gput(out: &mut [u8], p: usize, s: &[u8]) -> Result<usize, GraphError> {
    if p + s.len() > out.len() {
        return Err(GraphError::TooLarge);
    }
    out[p..p + s.len()].copy_from_slice(s);
    Ok(p + s.len())
}

fn gput_u32(out: &mut [u8], p: usize, mut v: u32) -> Result<usize, GraphError> {
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
    gput(out, p, &tmp[..n])
}

/// Render `spec` as fluxor YAML into `out`, returning its length.
///
/// Byte-for-byte identical to the host's `GraphSpec::to_yaml` — the layout is
/// part of the contract, so this mirrors it exactly rather than producing
/// equivalent-but-different YAML.
pub fn graph_to_yaml(spec: &GraphSpec, out: &mut [u8]) -> Result<usize, GraphError> {
    let mut p = gput(out, 0, b"target: ")?;
    p = gput(out, p, spec.target)?;
    p = gput(out, p, b"\ntick_us: ")?;
    p = gput_u32(out, p, spec.tick_us)?;
    p = gput(out, p, b"\n")?;
    if spec.accept_cycles {
        p = gput(out, p, b"scheduler:\n  accept_cycles: true\n")?;
    }

    // An EMPTY section is an explicit empty collection, never a bare key: a
    // bare `platform:` parses as null and fluxor refuses the graph, and `wiring`
    // is required outright. An embedded compute-only graph has both empty.
    if spec.platform.is_empty() {
        p = gput(out, p, b"\nplatform: {}\n")?;
    } else {
        p = gput(out, p, b"\nplatform:\n")?;
        for cap in spec.platform {
            p = gput(out, p, b"  ")?;
            p = gput(out, p, cap)?;
            p = gput(out, p, b": {}\n")?;
        }
    }

    if spec.modules.is_empty() {
        p = gput(out, p, b"\nmodules: []\n")?;
    } else {
        p = gput(out, p, b"\nmodules:\n")?;
    }
    for m in spec.modules {
        p = gput(out, p, b"  - name: ")?;
        p = gput(out, p, m.name)?;
        p = gput(out, p, b"\n")?;
        if !m.module_type.is_empty() {
            p = gput(out, p, b"    type: ")?;
            p = gput(out, p, m.module_type)?;
            p = gput(out, p, b"\n")?;
        }
        if !m.params.is_empty() {
            p = gput(out, p, b"    params:\n")?;
            for (k, v) in m.params {
                p = gput(out, p, b"      ")?;
                p = gput(out, p, k)?;
                p = gput(out, p, b": ")?;
                p = gput(out, p, v)?;
                p = gput(out, p, b"\n")?;
            }
        }
    }

    if spec.wiring.is_empty() {
        p = gput(out, p, b"\nwiring: []\n")?;
    } else {
        p = gput(out, p, b"\nwiring:\n")?;
    }
    for w in spec.wiring {
        p = gput(out, p, b"  - from: ")?;
        p = gput(out, p, w.from)?;
        p = gput(out, p, b"\n    to: ")?;
        p = gput(out, p, w.to)?;
        p = gput(out, p, b"\n")?;
    }
    Ok(p)
}
