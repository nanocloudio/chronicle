# On-Device Dataplane

## Overview

Chronicle runs on Fluxor as four generic, param-driven `.fmod` modules. None bakes
any specific logic: each is one binary that executes *whatever* bytecode arrives in
its config params. A graph wires them together with typed channels; the compiler
emits the params.

| Module | Role | Param(s) |
|--------|------|----------|
| `app/expression` | one checked-CEL Expression | `program`, `max_cost` |
| `app/pipeline` | staged Transformations + encode/decode + versioning | `ir_stages` (or `versions`), `encode`, `decode` |
| `app/aggregation` | event-time stateful engine | `ir_def` |
| `app/decision` | first-hit rule container | `decision` |
| `app/chronicle_cli` | the toolchain CLI applet (`fluxor exec chronicle`) | — |

Protocol I/O is **not** here: connectors are provider `.fmod`s owned by the
sibling projects and composed as graph nodes (see
[connectors.md](connectors.md)).

The shared execution cores (`vm_core.rs`, `pipeline_core.rs`, `agg_core.rs`,
`ser_core.rs`, `deser_core.rs`, `version_core.rs`, `celc_core.rs`, …) live in
`modules/common/` and are `include!`d verbatim by the `.fmod` modules and by the
host test harness (`fluxor test`) — so a test and the device run identical code
(see [model.md](model.md#one-implementation-one-oracle)).

## The typed record frame

Every value crossing a channel between compute modules is a **typed record frame** —
the on-device serialization of a message:

```
[count:u8]  then count ×  [number:u8][type:u8][len:u16 LE][payload]
```

`type` is `0` (byte string) or `1` (`i64`, 8 bytes LE). The field number is a `u8`,
so field numbers are `1..=255`; the encoder rejects a wider number rather than
truncate it, and field `255` is reserved as the [version selector](versioning.md).
`encode_frame` / `decode_frame` in `pipeline_core.rs` are the single codec for this
format.

## The pipeline module

`app/pipeline` is the workhorse. Per record it runs, in order:

```
input ─▶ [decode] ─▶ select version ─▶ stages ─▶ [encode] ─▶ output
```

- **decode** (optional `decode` param): a [byte-deserialization](connectors.md)
  program that parses a raw protocol reply into a record frame before the stages.
- **version select**: the record's `X-Module-Version` selector (field 255) resolves
  to one of the loaded [versions](versioning.md); unknown ⇒ fail closed.
- **stages**: an ordered chain of Transformation bytecode. Each stage runs on the
  evaluator; its constructed message is *serialized* as the next stage's input
  (serialize-at-the-boundary), so stages compose with no shared mutable state.
- **encode** (optional `encode` param): a [byte-serialization](connectors.md)
  program that renders the final record as wire bytes (e.g. a Redis `SET`).

The stage table is a param container — `[nstages:u8]{[cost:u32][len:u16][code]}` —
not baked, so one pipeline binary runs any pipeline. An `ir_stages` param lowers
at load into a single default-version table; a `versions` param supplies a
ready-made multi-version table.

## The aggregation module

`app/aggregation` is the on-device event-time engine
(`agg_core.rs`), bounded and allocation-free:

- **Windows**: tumbling and sliding (pane-aligned by size + step). Multiple panes
  are open per lane at once, so out-of-order events land in the correct window.
- **Watermark**: `max_event_time − lateness`; a pane finalizes when its end falls at
  or below the watermark.
- **Lanes**: bounded keyed cardinality — a new key past the ceiling is dropped and
  audited, deterministically.
- **Monoids**: `Count`/`Sum`/`Min`/`Max`/`Avg` (fixed-size, all retractable).
- **Corrections**: a late event within `correction_horizon` re-folds into its
  finalized (still-retained) pane and re-emits; beyond the horizon it is dropped.
- **Emit**: the finished state is projected through checked-CEL bytecode over a
  synthesized `ctx = {key, state, window}` message.

`Distinct`/`TopK`/`Quantile` are on device too, alongside the fixed-size monoids.
They fold a bounded sorted cell drawn from a fixed pool indexed by a derived
`(lane, pane)` slot, so they cost no state at all in a Sum-only deployment. TopK
stays exact past the cell ceiling — it evicts the smallest, which it would have
discarded anyway — while Distinct and Quantile saturate and COUNT the loss via
`coll_overflows()` rather than passing a lower bound off as exact.

They are also NOT retractable: a late correction within the horizon folds the
monoids but freezes a collection cell, and the refusal is counted in
`non_retractable_drops()`. Silently folding late data into a Distinct would make
the answer depend on arrival order.

## Param-driven execution

Because logic is a param, deployment is data, not code. The authoring compiler emits
each artefact's bytecode; `pack_core` serializes the containers;
`graph_core` renders the graph config. Changing the logic means a
new param — no module rebuild — and, for pipelines, can be done on a running
instance (see [versioning.md](versioning.md)).

## Record lifecycle and accounting

The steady-state modules share one framed-admission and pending-delivery
discipline (`io_core.rs`): a whole typed frame is admitted non-destructively via
peek, at most one record is in flight per step, and a blocked output is retained
and re-driven — never dropped. Each module's step logic is a safe, host-testable
core (`expression_step_core.rs`, `decision_step_core.rs`); the `.fmod`
`module_step` is a thin ABI adapter over it.

Every observed record is classified into exactly one disposition by a common
accounting taxonomy (`accounting_core.rs`) whose invariants —
`observed = admitted + rejected`, `admitted = succeeded + dropped + failed +
in_flight` — hold by construction. The 14 baseline instruments plus each
module's own (an operating-mode gauge, failure-reason splits, work units) are
published over fluxor telemetry; every bound that shapes what a module admits is
recorded in the [limit register](limit_register.md), and the per-module port and
instrument surface in the [resource summary](resource_summary.md).

## Safety

The modules are `no_std` and must never panic on malformed input — a panic crashes
the `.fmod`. Every core uses checked slice access, `checked_*`/`wrapping_*`
arithmetic, and hand-rolled loops that avoid the formatted-panic paths a
freestanding module cannot link. This is proven by deterministic-fuzz property
tests (`tests/harness/tests/aggregation_suites/robustness.rs`) that hammer every
VM with random bytecode and inputs.

## Related Documentation

- [model.md](model.md) — the artefact model and the shared cores
- [connectors.md](connectors.md) — encode/decode byte VMs and the transport
- [versioning.md](versioning.md) — multi-version pipelines and hot reload
