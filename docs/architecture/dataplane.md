# On-Device Dataplane

## Overview

Chronicle runs on Fluxor as four generic, param-driven engine `.fmod` modules, plus
the `sensor_intake` adapter and the CLI applet. No engine bakes
any specific logic: each is one binary that executes *whatever* bytecode arrives in
its config params. A graph wires them together with typed channels; the compiler
emits the params.

| Module | Role | Param(s) |
|--------|------|----------|
| `app/expression` | one checked-CEL Expression | `program`, `max_cost` |
| `app/pipeline` | staged Transformations and Decisions + encode/decode + versioning | `ir_stages` (or `versions`), `stage_kinds`, `encode`, `decode`, `reply_decode` |
| `app/aggregation` | event-time stateful engine | `ir_def` |
| `app/decision` | first-hit rule container | `decision` |
| `app/sensor_intake` | `SensorSample` → typed record frame | — |
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

`type` is `0` (byte string), `1` (`i64`, 8 bytes LE) or `3` (a nested message,
whose payload is its own frame); `2` is unassigned and refused. The field number is a `u8`, so field
numbers are `1..=255`; the encoder rejects a wider number rather than truncate it.

The engines reserve `240..=255`. Three of those numbers have fixed meanings;
`240..=252` are held unassigned:

| Number | Type | Meaning |
|---|---|---|
| `255` | string or bytes | the [version selector](versioning.md) |
| `254` | message | the **carry** (`CARRY_FIELD`): the record's carried context, a nested message every connector returns unchanged, so a request built before an effect and the reply handled after it share no state but the record. On the [exchange surface](connectors.md#the-exchange-surface) it travels as the `msg_key` |
| `253` | int | the **exchange status** (`EXCHANGE_STATUS_FIELD`) on a record made from a provider's reply: `0` answered, `1..=15` the contract's refusals. A payload's own field 253 is replaced by it |

A field with one of these numbers has its reserved meaning whatever the schema
calls it, so the schema compiler accepts a reserved number only as its fixed type
and refuses `240..=252` outright (`ReservedField`). Data fields are numbered
below 240.

A frame never carries an absent field: `encode_frame` leaves it out. A **nested
message** crosses a frame as its own frame, typed `3`: a construction that is
another construction's field value packs into one (`FRAME_PACK`), a path through
a message-typed field reads it (`GET_FIELD`), and passing it on keeps its type. A
byte-string field is never read as a message, and a map stage's elements are
messages. A connector that returns the carry returns its type byte with it. `encode_frame` / `decode_frame` in `pipeline_core.rs`
are the single codec for this format.

## The pipeline module

`app/pipeline` is the workhorse. Per record it runs, in order:

```
input ─▶ [decode] ─▶ select version ─▶ stages ─▶ [encode] ─▶ output
```

- **decode** (optional `decode` param): a [byte-deserialization](../guides/wire-codec.md)
  program that parses a raw protocol reply into a record frame before the stages.
- **version select**: the record's `X-Module-Version` selector (field 255) resolves
  to one of the loaded [versions](versioning.md); unknown ⇒ fail closed.
- **stages**: an ordered chain of stage programs (see [stage kinds](#stage-kinds)).
  Each stage's constructed message is *serialized* as the next stage's input
  (serialize-at-the-boundary), so stages compose with no shared mutable state.
- **encode** (optional `encode` param): a [byte-serialization](../guides/wire-codec.md)
  program that renders the final record as wire bytes (e.g. a Redis `SET`).

The stage table is a param container — `[nstages:u8]{[cost:u32][len:u16][code]}` —
not baked, so one pipeline binary runs any pipeline. An `ir_stages` param lowers
at load into a single default-version table; a `versions` param supplies a
ready-made multi-version table.

### Stage kinds

A stage's `code` is Transformation bytecode by default. The optional
`stage_kinds` param — one hex byte per stage, parallel to the container —
names a different executor for a stage:

| Kind | `code` is | Executor |
|---|---|---|
| `00` compute | Transformation bytecode | the expression evaluator |
| `01` decision | a [Decision](model.md) container | the first-hit policy driver |
| `02` map | a map container (`MAP_HEADER` + a predicate) | the bounded map driver |

A Decision is one input, one output and the same decode/evaluate/encode step a
compute stage is; only the program format differs. Running it as a stage makes
a chain that routes mid-way a single node rather than
`pipeline -> decision -> pipeline`, so the two channel hops either side of the
decision are not paid — the saving the [hop register](hop_register.md) counts.

The kinds ride *alongside* the container rather than inside it, so the stage
format is unchanged and a container read on its own is all compute. Each kind
is validated at load by its own scanner — an unknown opcode fails a compute
stage, a malformed container fails a decision stage — and a stage whose kind is
not declared is scanned as compute, so a decision body can never be admitted
unchecked.

A decision stage's body is copied through the lowering verbatim rather than
transcoded, and its arms carry their own cost bounds; see `DECISION_STAGE_COST`
in the [limit register](limit_register.md).

A **map** stage applies one predicate to every element of a repeated field —
a field number the frame carries more than once, each occurrence a nested
message's frame — at most the declared `max` of them (more refuses the stage,
`TooMany`). Whatever an element is judged against travels in the element — a
producer pairs before the stage. The predicate reads `(element, record)` and
answers true, false or unknown; the driver
counts the three into declared int fields, so "every element holds" is
`false == 0 && unknown == 0` in whatever follows. Authored as a `.uproc`
`map` (see `examples/map_stage/selector.uproc`), sealed as an Expression plus
the calling Stage's `MapSpec`, and lowered into the surrounding compute run's
node — a map does not branch, so it is a stage, not a node. Its body is copied
verbatim like a decision's; see `MAP_STAGE_COST`.

The pipeline also speaks the ordered-ack exchange surface in both directions:
wire `publish_out`/`ack_in` and results leave as correlated publishes to any
provider of the surface; wire `publish_in`/`ack_out` and the pipeline is the
sink, taking each publish's payload as its record and acknowledging it once
its output is accepted downstream (see
[connectors.md](connectors.md#the-exchange-surface)).

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
`(lane, pane)` slot, so they cost no state at all in a Sum-only deployment.
TopK's `k` and Quantile's permille travel in the container with the operator.
TopK stays exact past the cell ceiling — it evicts the smallest, which it would
have discarded anyway — while a Distinct or Quantile cell that saturates has no
exact answer: its value is emitted absent and the loss counted in
`coll_overflows()`, never a lower bound passed off as exact. A Count, Sum or Avg
that overflows i64 is emitted absent the same way, counted in
`arith_overflows()`.

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
- [../guides/wire-codec.md](../guides/wire-codec.md) — the encode/decode byte VMs
- [connectors.md](connectors.md) — the provider modules and the exchange surface
- [versioning.md](versioning.md) — multi-version pipelines and hot reload
