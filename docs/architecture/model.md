# The Unified Deterministic Processing Model

## Overview

Chronicle expresses a dataplane as a small set of **content-addressed artefacts**.
Each artefact is a canonical Protobuf document whose identity is the digest of its
own canonical bytes (with the digest field cleared). Logic is checked once, ahead
of time, and lowered to bounded bytecode; the runtime executes that bytecode over a
fixed field layout and touches no clock, randomness, environment, or I/O except
through explicit effects. Determinism is a property of the structure, not of
discipline.

Three foundations are load-bearing:

- **Protobuf** is the canonical type system.
- **CEL** (a checked subset) is the canonical pure-expression language.
- **OCI** is the canonical distribution format; content digests are identity.

## The seven artefacts

| # | Artefact | Purpose | Proto |
|---|----------|---------|-------|
| 1 | Schema | portable types + structural compatibility | `schema.proto` |
| 2 | Expression | pure deterministic scalar computation (checked CEL) | `expression.proto` |
| 3 | Transformation | typed input → typed output message | `transformation.proto` |
| 4 | Decision | ordered rules → deterministic outcome | `decision.proto` |
| 5 | Aggregation | stateful event-time computation (windows / watermarks / lanes) | `aggregation.proto` |
| 6 | Pipeline | compose artefacts + effects into typed workflows | `pipeline.proto` |
| 7 | Module | immutable OCI deployment / compatibility unit | `module.proto` |

Two further protos — `common.proto` and `resource.proto` — are support types, not
artefacts. The Protobuf package is `unified.v1`: that is the **spec** type
namespace (the model unifies orchestration, deterministic state, and runtime distribution), distinct from the
`chronicle-*` crate names, which are the implementation. The split is deliberate —
`chronicle` is the engine, `unified.v1` is the spec vocabulary.

## The build-time / runtime split

The load-bearing choice is *where* checking happens.

```
authoring (.uproc / CEL)  ─▶  canonical Protobuf artefacts  ─▶  content digest (identity)
                                     │
   build time  ─────────────────────┤  descriptor closures + checked CEL, lowered to
                                     │  bounded no_std bytecode, packed into a module param
                                     ▼
   runtime     ─▶  generic Fluxor .fmod modules — bounded, no-alloc, deterministic —
                    param-driven, distributed as OCI Module artefacts
```

Type-checking and CEL-checking run **once**, at build time, producing sealed
artefacts. The runtime never links `libprotobuf` or a CEL interpreter. One generic
module per class runs *any* artefact of that class: the compiler emits the
bytecode, [`pack_core`](../../modules/common/pack_core.rs)
serializes it into a module param, and the module executes it — no rebuild. The
last hop is real: [`graph_core`](../../modules/common/graph_core.rs)
renders a runnable Fluxor graph whose params *are* the compiled programs.

## Content-digest identity

Every artefact, and the Module that bundles them, is identified by the sha256 of
its canonical serialization. This is what makes distribution and versioning
tractable: an OCI Module is fetched by digest and verified byte-for-byte; a
[module version](versioning.md) is a digest, so the same tag resolves to the same
bytecode on every instance in a fleet. The one map field that appears inside a
digested artefact (an Aggregation watermark override table) is encoded as a
`BTreeMap`, so insertion order cannot perturb the digest.

## Host and device: two implementations, one semantics

This is the most important structural fact to understand about the codebase.

The **on-device runtime** lives in `modules/common/*_core.rs`, which are
`no_std`, no-alloc, and — critically — **`include!`d verbatim by the `.fmod`
modules, by their test harnesses, and by the host differentials**. For the evaluator, the staged pipeline, the byte
codecs, the aggregation kernel, and the version table, host and device run *the
same source*. This is the "one source of truth" the core headers refer to, and it
is genuinely one implementation.

The duplication this section used to describe is **gone**. There were once
separate `chronicle-pipeline` and `chronicle-aggregation` crates carrying richer
`std` re-implementations of the same semantics, and pane assignment and watermark
maths were genuinely maintained in two places — a change to lateness rounding had
to land in both copies or determinism diverged silently.

Those crates were absorbed, and the device kernel is now the only implementation.
The variable-size Distinct / TopK / Quantile operators the host once owned alone
are on device too, as bounded sorted cells drawn from a fixed pool. There is one
copy of the pane maths, and it is the copy that runs in production.

There is no crates directory at all. The oracles were retired into golden
corpora — recorded answers, checked in beside the harness that reads each one —
and the crate that produced them was deleted with them. `tools/ci/shipping-surface.sh`
asserts that as a checked property rather than a convention: it fails CI if a
cargo manifest reappears, if a module includes from one, or if anything but
shipping sources lands in `modules/common/`.

That inverts the old risk. Instead of two implementations that must be kept in
step by discipline, there is one implementation and one oracle, and the tests
that compare them are the point of the oracle existing.

## Related Documentation

- [dataplane.md](dataplane.md) — the on-device modules and the record frame
- [connectors.md](connectors.md) — the byte codecs and the transport
- [versioning.md](versioning.md) — content-digest versions across a fleet
- [../guides/authoring.md](../guides/authoring.md) — authoring artefacts in `.uproc`
