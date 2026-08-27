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
artefacts. The Protobuf package is `unified.v1`: the type namespace of the model
(which unifies orchestration, deterministic state, and runtime distribution),
distinct from `chronicle`, the engine that implements it.

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

## One implementation, one oracle

This is the most important structural fact to understand about the codebase.

The **runtime** lives in `modules/common/*_core.rs`: `no_std`, no-alloc sources
that are **`include!`d verbatim by the `.fmod` modules and by the host test
harness**. For the evaluator, the staged pipeline, the byte codecs, the
aggregation kernel, the version table, and every other core, host tests and the
device run *the same source*. There is exactly one implementation, and it is the
one that runs in production.

Correctness rests on **golden corpora** rather than a second implementation:
recorded answers, checked in beside the harness suites that read them
(`tests/harness/tests/*/corpus.rs` and their `resources/`). A compiler or VM
change that alters a corpus answer is a breaking change to deployed programs and
is treated as one.

There is no cargo crate anywhere in the project. `tools/ci/shipping-surface.sh`
asserts that as a checked property rather than a convention: it fails CI if a
cargo manifest appears, if a module includes host code from one, or if anything
but shipping sources lands in `modules/common/`.

## Related Documentation

- [dataplane.md](dataplane.md) — the on-device modules and the record frame
- [connectors.md](connectors.md) — the byte codecs and the transport
- [versioning.md](versioning.md) — content-digest versions across a fleet
- [../guides/authoring.md](../guides/authoring.md) — authoring artefacts in `.uproc`
