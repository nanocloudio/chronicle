# Chronicle

> Deterministic processing as content-addressed artefacts: Schema, Expression,
> Transformation, Decision, Aggregation, Pipeline, Module — compiled ahead of
> time, executed on Fluxor as `.fmod` modules.

Chronicle is a **fluxor-native** deterministic dataplane engine, structured like
its sibling nanocloud projects (`../quantum`, `../lattice`, `../clustor`): a
Cargo/fluxor workspace of host crates plus a set of on-device `.fmod` modules.

It implements the **Unified Deterministic Processing** model, absorbing what were
three separate concerns into one engine:

- **Orchestration & effects** — connectors, pipelines, retries, compensation,
  telemetry (the **Pipeline** and **Resource** surfaces).
- **Deterministic state** — event-time windows, watermarks,
  keyed lanes, corrections, checkpoints (the **Aggregation** surface).
- **Runtime & distribution** (Fluxor) — position-independent `.fmod` modules,
  typed-channel graphs, OCI packaging, capability negotiation, atomic activation
  (the **Module** anchor).

Envoy's service mesh maps onto the same model: Listener → Resource, filter chain
→ Pipeline, route → Decision, Cluster → upstream Resource (see
[`docs/guides/envoy-mapping.md`](docs/guides/envoy-mapping.md)).

**Documentation:** [`docs/overview.md`](docs/overview.md) is the index — the
architecture references (model, dataplane, connectors, versioning) and the
authoring guides.

## The model

Protobuf is the canonical type system, CEL the canonical pure expression
language, OCI the canonical distribution format. Every artefact has a canonical
Protobuf form and a **content-digest identity**. There is no hidden access to
clocks, randomness, environment, filesystem, network, or locale — external
actions are explicit Pipeline effects on Resources.

```
authoring (.uproc / CEL) ─▶ canonical Protobuf artefacts ─▶ content digest (identity)
                                    │
        build time  ────────────────┤  descriptor closures + checked CEL, lowered
                                    │  to bounded no_std bytecode + packed into a param
                                    ▼
        runtime     ─▶ generic Fluxor .fmod modules (bounded, no-alloc, deterministic),
                        param-driven, distributed as OCI Module artefacts
```

The build-time / runtime split is the load-bearing choice: type- and CEL-checking
happen once, ahead of time, producing canonical checked artefacts. The runtime
never links `libprotobuf` or a CEL interpreter — it runs bounded bytecode over
fixed layouts, so the same artefacts span constrained targets through server
class. One generic module per class runs *any* artefact: the compiler emits the
bytecode, [`pack_core`](modules/common/pack_core.rs)
serializes it into a module param, and the module runs it — no rebuild.

The last hop is real too: [`graph_core`](modules/common/graph_core.rs)
renders a runnable Fluxor graph whose module params **are** the compiled programs,
so one `.uproc` goes **authored → compiled → param → running on device**. Every
bounded VM — the evaluator, the byte-serializer, the byte-deserializer, the staged
pipeline, and the aggregation engine — is fuzz-tested to **never panic** on
malformed input (a hard requirement for freestanding `.fmod` code); see
[`modules/app/aggregation/tests/robustness.rs`](modules/app/aggregation/tests/robustness.rs).

## The seven artefacts

| # | Artefact | Purpose | Canonical proto |
|---|----------|---------|-----------------|
| 1 | Schema | portable types + structural compatibility | `proto/unified/v1/schema.proto` |
| 2 | Expression | pure deterministic scalar computation (checked CEL) | `expression.proto` |
| 3 | Transformation | typed input → typed output | `transformation.proto` |
| 4 | Decision | ordered rules → deterministic outcome | `decision.proto` |
| 5 | Aggregation | stateful temporal computation (windows / watermarks / lanes) | `aggregation.proto` |
| 6 | Pipeline | compose artefacts + effects into typed workflows | `pipeline.proto` |
| 7 | Module | immutable OCI deployment / compatibility unit | `module.proto` |

## Layout

- `modules/common/` — the shared device cores: evaluator, staged pipeline,
  aggregation kernel, decision driver, byte codecs, version table, IR lowerer and
  the CEL front end. `include!`d verbatim by the modules **and** by the host
  crates, so there is one source of truth and no host/device drift.
- `modules/app/` — the param-driven on-device `.fmod`s (built for `bcm2712`):
  `expression`, `pipeline`, `decision`, `aggregation`, and `chronicle_cli` (the
  toolchain applet). No protocol is baked into a module; protocol I/O comes from
  sibling-owned providers composed as graph nodes.
- `tools/lib.sh`, `tools/support/` — the shared E2E harness the gates and
  drivers both source.
- `proto/unified/v1/` — the artefact schemas: the **7** canonical artefacts plus
  two support protos (`common`, `resource`) that are explicitly *not* artefacts.
  Package `unified.v1` is the **spec** type-namespace (the Unified Deterministic
  Processing model that unifies orchestration, deterministic state, and runtime distribution); the `chronicle-*` crates
  are the implementation. The two names are deliberate: `chronicle` = engine,
  `unified.v1` = spec types.
- `examples/` — runnable graphs.
- `tools/ci/` — structural gates (what the tree must BE).
- `tools/e2e/` — behavioural drivers (each boots a real graph and asserts real output).
- `docs/conformance.md` — the spec-dimension → test map.

## Connectors

A connector is three things: **encode** (record → wire request), **transport**
(send/receive), **decode** (reply → record) — and all three are param-driven, no
protocol module anywhere.

- **Encode & decode are symmetric byte VMs.** Building a request is bytecode
  ([`ser_core`](modules/common/ser_core.rs) — literals, field
  values, binary ints, varints, region framing for length/varint/CRC); parsing a
  reply back into a record is its mirror
  ([`deser_core`](modules/common/deser_core.rs) — a cursor with
  skip/until/take/seek/`h2msg`, binary + decimal int reads, then the message
  builder). Both run as the generic pipeline module's `encode` / `decode` params,
  so the read path is as first-class as the write path.
- **Protocols are not bytecode.** Framing a message is in scope; speaking a
  protocol is not. Connectors are provider `.fmod`s owned by the domain projects —
  lattice (Redis/Postgres/MySQL/Mongo/Cassandra), quantum (MQTT/Kafka/AMQP/NATS),
  wave (HTTP/WebSocket/RTP/SIP/SMTP), loam (S3) — pinned from the Fluxor OCI store
  and composed as graph nodes. Each is proven standalone in its owning repo
  against a real backend. See [connectors.md](docs/architecture/connectors.md).

## Build & run

One-time bootstrap, before anything below: `make -C ../fluxor install` — it
builds the `fluxor` CLI, publishes it into the local OCI store and puts the
resolving launcher on PATH. Every later `fluxor publish` supersedes the CLI in
place, so there is nothing to reinstall.

```bash
make ci          # fluxor ci — the full gate (lints, tests, strict module build)
protoc -I proto --include_imports -o /dev/null proto/unified/v1/*.proto   # schemas compile
make test        # module harnesses + the 22 graph E2Es (what CI runs)
make build       # the .fmod modules — this project has no cargo crate
tools/e2e/expression.sh   # one graph E2E on its own → cust-42

# a param-driven pipeline on device (transform doubles the amount):
printf '\x03\x01\x00\x05\x00ord-1\x02\x00\x06\x00cust-9\x03\x01\x08\x00\xfa\x00\x00\x00\x00\x00\x00\x00' \
  | fluxor run examples/pipeline/linux.yaml | xxd   # -> {ord-1, 250, 500}
```

The whole dataplane arc — authored `.uproc`/CEL → compiled → packed → module
param → composed on-device fluxor graph → live external write, with fan-out —
runs on real Fluxor hardware. `docs/conformance.md` maps every spec dimension
to its test / on-device proof.
