// The activation sequence, on device — deciding whether a verified module can
// actually RUN here.
//
// `modsig_core` answers "is this module genuine?". That is necessary and not
// sufficient: a perfectly authentic module may still reference artefacts this
// node does not hold, depend on a module that is absent, need a capability the
// runtime lacks, or require a resource binding nobody supplied. Activating it
// anyway would fail later, in the middle of serving traffic, instead of here.
//
// So this runs the spec's ordered sequence and refuses at the first failing
// step, mirroring the host's `plan_activation`:
//
//   1. digest recomputed and checked      (modsig_core)
//   2. signature by a trusted signer      (modsig_core)
//   3. dependencies resolve in the registry
//   4. every pinned artefact reference resolves
//   5. required capabilities are supported
//   6. required resource bindings are bound
//
// ORDER IS PART OF THE CONTRACT, not an implementation detail: a caller that
// reports "capability unsupported" for a module whose signature was also invalid
// has told the operator the wrong thing, and — worse — has inspected the
// contents of a module it never established the provenance of. Authenticity is
// settled before anything else is read.
//
// Optionality is honoured in both places it appears: an OPTIONAL capability that
// is unsupported is fine (it is simply not satisfied), and a binding that is not
// `required` may be absent. Only a PINNED reference is resolvable — an unpinned
// one names no specific content, so there is nothing to look up.
//
// Bounded and allocation-free: the caller supplies the runtime's capability list
// and the registry's digest/name tables as slices, so this core owns no storage
// and imposes no ceiling of its own.
//
// Requires `pb_core` and `modsig_core`.

/// Module field numbers (proto/unified/v1/module.proto).
const F_SCHEMAS: u32 = 2;
const F_RESOURCE_CONTRACTS: u32 = 8;
const F_BINDING_REQUIREMENTS: u32 = 9;
const F_DEPENDENCIES: u32 = 10;

/// `ArtefactRef { name = 1, kind = 2, pinned = 3 }`.
const F_REF_KIND: u32 = 2;
const F_REF_PINNED: u32 = 3;
/// `CapabilityRequirement { capability = 1, optional = 2 }`.
/// `ResourceBindingRequirement { contract = 1, required = 2 }`.
const F_BIND_CONTRACT: u32 = 1;
const F_BIND_REQUIRED: u32 = 2;
/// `QualifiedName { package = 1, symbol = 2 }`.
const F_QN_PACKAGE: u32 = 1;
const F_QN_SYMBOL: u32 = 2;

/// Longest `package.symbol` this core will assemble for a binding lookup.
pub const CONTRACT_NAME_MAX: usize = 128;

/// What the node offers a module: the capabilities the runtime supports, and
/// what the local registry already holds.
///
/// Every field is a borrowed slice — the caller owns the storage, so this core
/// imposes no ceiling on how many artefacts a node may hold.
pub struct NodeState<'a> {
    /// Capability names the runtime supports.
    pub capabilities: &'a [&'a [u8]],
    /// Digests of artefacts held locally.
    pub artefacts: &'a [[u8; 32]],
    /// Digests of modules held locally.
    pub modules: &'a [[u8; 32]],
    /// Contract names (`package.symbol`) that have a binding.
    pub bound_contracts: &'a [&'a [u8]],
}

/// Why activation was refused. Ordered as the sequence encounters them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivationError {
    /// Step 1/2: the module is not authentic. Carries the verifier's reason.
    NotVerified(VerifyError),
    /// Step 3: a pinned dependency is not held locally.
    MissingDependency,
    /// Step 4: a pinned artefact reference does not resolve.
    UnresolvedArtefact {
        /// The `ArtefactKind` the reference expected.
        kind: i32,
    },
    /// Step 5: a REQUIRED capability is unsupported.
    CapabilityUnsupported,
    /// Step 6: a REQUIRED resource binding is absent.
    MissingBinding,
    /// The bytes are not a well-formed Module.
    Malformed,
}

impl From<PbError> for ActivationError {
    fn from(_: PbError) -> Self {
        ActivationError::Malformed
    }
}

/// What a successful activation established — the counts a caller wants for an
/// audit line, and the trusted key that vouched for the module.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActivationPlan {
    /// Index into the caller's trusted-key list of the signer that verified.
    pub signer: usize,
    /// Pinned dependencies resolved.
    pub dependencies: usize,
    /// Pinned artefact references resolved.
    pub artefacts: usize,
    /// Capabilities requested AND supported (optional ones may be absent).
    pub capabilities_satisfied: usize,
    /// Required bindings confirmed present.
    pub bindings: usize,
}

/// Read a `Digest`'s 32-byte value.
fn digest_value(bytes: &[u8]) -> Result<Option<[u8; 32]>, ActivationError> {
    let mut r = PbR::new(bytes);
    while let Some(f) = r.next_field()? {
        if f.number == F_DIGEST_VALUE && f.wire == WT_LEN {
            if f.bytes.len() != 32 {
                return Err(ActivationError::Malformed);
            }
            let mut d = [0u8; 32];
            d.copy_from_slice(f.bytes);
            return Ok(Some(d));
        }
    }
    Ok(None)
}

/// Assemble `package.symbol` from a `QualifiedName` into `out`, mirroring the
/// host's `contract_name` — including that an empty package yields the bare
/// symbol rather than a leading dot.
fn contract_name(
    bytes: &[u8],
    out: &mut [u8; CONTRACT_NAME_MAX],
) -> Result<usize, ActivationError> {
    let (mut pkg, mut sym): (&[u8], &[u8]) = (&[], &[]);
    let mut r = PbR::new(bytes);
    while let Some(f) = r.next_field()? {
        match f.number {
            F_QN_PACKAGE if f.wire == WT_LEN => pkg = f.bytes,
            F_QN_SYMBOL if f.wire == WT_LEN => sym = f.bytes,
            _ => {}
        }
    }
    let need = if pkg.is_empty() {
        sym.len()
    } else {
        pkg.len() + 1 + sym.len()
    };
    if need > out.len() {
        return Err(ActivationError::Malformed);
    }
    let mut n = 0;
    if !pkg.is_empty() {
        out[..pkg.len()].copy_from_slice(pkg);
        n = pkg.len();
        out[n] = b'.';
        n += 1;
    }
    out[n..n + sym.len()].copy_from_slice(sym);
    Ok(n + sym.len())
}

fn holds(set: &[[u8; 32]], d: &[u8; 32]) -> bool {
    set.iter().any(|x| x == d)
}

fn names_contain(set: &[&[u8]], name: &[u8]) -> bool {
    set.contains(&name)
}

/// Run the activation sequence against `module`.
///
/// `scratch` must be at least as large as `module` (it holds the pre-digest
/// reconstruction). Returns the plan, or the FIRST failing step's error.
pub fn plan_activation(
    module: &[u8],
    trusted: &[[u8; 32]],
    node: &NodeState,
    scratch: &mut [u8],
) -> Result<ActivationPlan, ActivationError> {
    // 1 + 2. Authenticity first: nothing below reads a module whose provenance
    // has not been established.
    let signer = module_verify(module, trusted, scratch).map_err(ActivationError::NotVerified)?;

    let mut plan = ActivationPlan {
        signer,
        dependencies: 0,
        artefacts: 0,
        capabilities_satisfied: 0,
        bindings: 0,
    };

    // The remaining steps run as SEPARATE PASSES, deliberately.
    //
    // Protobuf fields arrive in ascending field-number order, which here is
    // artefacts(2..8), bindings(9), dependencies(10), capabilities(11) — NOT the
    // sequence order. Checking them in wire order would report a different first
    // failure than the host for any module that fails more than one step, and
    // the first failure is exactly what an operator is told. Four passes over a
    // module cost nothing worth having; disagreeing with the host does.
    plan.dependencies = check_dependencies(module, node)?;
    plan.artefacts = check_artefacts(module, node)?;
    plan.capabilities_satisfied = check_capabilities(module, node)?;
    plan.bindings = check_bindings(module, node)?;
    Ok(plan)
}

/// Step 3: every pinned dependency must be a module this node holds.
fn check_dependencies(module: &[u8], node: &NodeState) -> Result<usize, ActivationError> {
    let mut n = 0;
    let mut r = PbR::new(module);
    while let Some(f) = r.next_field()? {
        if f.number != F_DEPENDENCIES || f.wire != WT_LEN {
            continue;
        }
        let mut dr = PbR::new(f.bytes);
        while let Some(df) = dr.next_field()? {
            if df.number == F_REF_PINNED && df.wire == WT_LEN {
                if let Some(d) = digest_value(df.bytes)? {
                    if !holds(node.modules, &d) {
                        return Err(ActivationError::MissingDependency);
                    }
                    n += 1;
                }
            }
        }
    }
    Ok(n)
}

/// Step 4: every pinned artefact reference must resolve. Walks the same set as
/// the host's `all_artefact_refs` — schemas..pipelines plus resource contracts.
///
/// Only a PINNED reference names specific content; an unpinned one has nothing
/// to resolve against, so it is not a failure.
fn check_artefacts(module: &[u8], node: &NodeState) -> Result<usize, ActivationError> {
    let mut n = 0;
    let mut r = PbR::new(module);
    while let Some(f) = r.next_field()? {
        let is_ref = (F_SCHEMAS..=7).contains(&f.number) || f.number == F_RESOURCE_CONTRACTS;
        if !is_ref || f.wire != WT_LEN {
            continue;
        }
        let mut kind = 0i32;
        let mut pinned: Option<[u8; 32]> = None;
        let mut ar = PbR::new(f.bytes);
        while let Some(af) = ar.next_field()? {
            match af.number {
                F_REF_KIND if af.wire == WT_VARINT => kind = af.value as i32,
                F_REF_PINNED if af.wire == WT_LEN => pinned = digest_value(af.bytes)?,
                _ => {}
            }
        }
        if let Some(d) = pinned {
            if !holds(node.artefacts, &d) {
                return Err(ActivationError::UnresolvedArtefact { kind });
            }
            n += 1;
        }
    }
    Ok(n)
}

/// Step 5: required capabilities must be supported. An unsupported OPTIONAL
/// capability is simply not satisfied, which is not a failure.
fn check_capabilities(module: &[u8], node: &NodeState) -> Result<usize, ActivationError> {
    let mut n = 0;
    let mut r = PbR::new(module);
    while let Some(f) = r.next_field()? {
        if f.number != F_CAPABILITIES || f.wire != WT_LEN {
            continue;
        }
        let mut name: &[u8] = &[];
        let mut optional = false;
        let mut cr = PbR::new(f.bytes);
        while let Some(cf) = cr.next_field()? {
            match cf.number {
                F_CAP_NAME if cf.wire == WT_LEN => name = cf.bytes,
                F_CAP_OPTIONAL if cf.wire == WT_VARINT => optional = cf.value != 0,
                _ => {}
            }
        }
        if names_contain(node.capabilities, name) {
            n += 1;
        } else if !optional {
            return Err(ActivationError::CapabilityUnsupported);
        }
    }
    Ok(n)
}

/// Step 6: every REQUIRED resource binding must be bound.
fn check_bindings(module: &[u8], node: &NodeState) -> Result<usize, ActivationError> {
    let mut n = 0;
    let mut r = PbR::new(module);
    while let Some(f) = r.next_field()? {
        if f.number != F_BINDING_REQUIREMENTS || f.wire != WT_LEN {
            continue;
        }
        let mut contract: &[u8] = &[];
        let mut required = false;
        let mut br = PbR::new(f.bytes);
        while let Some(bf) = br.next_field()? {
            match bf.number {
                F_BIND_CONTRACT if bf.wire == WT_LEN => contract = bf.bytes,
                F_BIND_REQUIRED if bf.wire == WT_VARINT => required = bf.value != 0,
                _ => {}
            }
        }
        if required {
            let mut buf = [0u8; CONTRACT_NAME_MAX];
            let len = contract_name(contract, &mut buf)?;
            if !names_contain(node.bound_contracts, &buf[..len]) {
                return Err(ActivationError::MissingBinding);
            }
            n += 1;
        }
    }
    Ok(n)
}
