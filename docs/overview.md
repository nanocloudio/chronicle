# Chronicle Documentation

Chronicle is a **fluxor-native deterministic dataplane engine**. Processing logic —
schemas, expressions, transformations, decisions, aggregations, pipelines — is
authored ahead of time, type- and CEL-checked once, and lowered to compact
bytecode that runs on Fluxor as position-independent `.fmod` modules. Nothing on
the device links a CEL interpreter or `libprotobuf`: it runs bounded bytecode over
a fixed field layout, so the same artefacts span microcontrollers through
server-class targets without changing the abstraction.

Every artefact has a canonical Protobuf form and a **content-digest identity**.
There is no hidden access to clocks, randomness, environment, filesystem, network,
or locale — external actions are explicit Pipeline effects. Determinism is
structural, not conventional.

Chronicle sits on Fluxor (the runtime, module graph, and channels — see
[`../../fluxor/docs/overview.md`](../../fluxor/docs/overview.md)) and implements the
**Unified Deterministic Processing** model, which folds three concerns —
orchestration/effects, deterministic event-time state, and OCI
distribution/activation — into one engine.

## Start Here

- [architecture/model.md](architecture/model.md) — the model, the seven artefacts, the build-time/runtime split, content-digest identity, and how host and device relate
- [architecture/dataplane.md](architecture/dataplane.md) — the on-device runtime: the generic modules, the record frame, param-driven pipelines, aggregation
- [guides/authoring.md](guides/authoring.md) — write a `.uproc` module and run it on device

## Architecture

The authoritative references for how the engine works.

- [architecture/model.md](architecture/model.md) — Unified Deterministic Processing: artefacts, identity, the build-time/runtime split, host-vs-device implementations
- [architecture/dataplane.md](architecture/dataplane.md) — generic `.fmod` modules, the typed record frame, staged pipelines, the aggregation engine, param-driven execution
- [architecture/connectors.md](architecture/connectors.md) — how effects bind to sibling-owned provider modules, the port vocabulary, and why protocols are not bytecode
- [architecture/versioning.md](architecture/versioning.md) — multiple versions per module, `X-Module-Version` selection, hot reload, release manifests, fleet propagation

## Guides

How to author and operate.

- [guides/authoring.md](guides/authoring.md) — the `.uproc` DSL: messages, expressions, transformations, decisions, aggregations, pipelines, connectors
- [guides/wire-codec.md](guides/wire-codec.md) — authoring connector encoders/decoders as wire templates
- [guides/envoy-mapping.md](guides/envoy-mapping.md) — mapping an Envoy service mesh onto the artefact model

## Conformance

Every normative claim maps to an executable test or an on-device run:
[`conformance.md`](conformance.md).
