# Chronicle limit register

Every bound that shapes what a Chronicle artefact may hold or do is recorded here
with its source, so a reader can see the whole resource envelope in one place and
`fluxor ci`'s `limit-register` phase can prove the document has not drifted from
the code.

The register is normative for every target Chronicle ships modules for: bcm2712
(Pi 5), rp2350 and rp2040. Where a bound differs between them it is written here
for all three, in the order `bcm2712 / rp2350 / rp2040` that
[`resource_summary.md`](resource_summary.md) uses for its columns.

Two rules govern every limit and are verified by tests, not by prose:

- **No hidden `min(...)`.** Where exceeding a declared input count would change
  meaning, the module REJECTS before mutating and reports a stable reason — it
  never silently clamps. (Enforced by the boundary+1 tests named per row.)
- **Output sizing is reject, not clip.** A result that does not fit its record
  buffer is `output_too_large`; it is never truncated into a successful-but-
  different record. (The `expr`/`decision`/`pipeline` too-large tests.)

Dimensions: **B**=bytes, **rec**=records/frames, **fld**=fields, **st**=stages,
**ver**=versions, **ln**=lanes, **pn**=panes, **op**=operators, **win**=windows,
**ms**=milliseconds, **inst**=instruments, **vmi**=VM instructions.

## Arena tiers

A ceiling that varies by target is keyed on the state arena the target actually
has, never on a die name. The arena is the reason the bound differs, so it is what
the expression tests; a target added later inherits the right tier without an
edit, and the constant compiles under any toolchain because
`abi::config::kernel::STATE_ARENA_SIZE` is in scope in every module.

| Tier | Predicate | Targets | `STATE_ARENA_SIZE` |
|---|---|---|---|
| full | neither below | bcm2712 | 256 MiB |
| small | `ARENA <= 512 KiB` | rp2350 | 240 KiB |
| tiny | `ARENA <= 64 KiB` | rp2040 | 64 KiB |

These are the arenas the kernel allocates on each target, and what the predicates
read. A deployment may additionally declare a lower capacity for itself — the
composer writes the graph's demand into the image and the kernel's resource ledger
holds the pool to it — and the lower of the two binds at load. Tier selection is a
compile-time choice, so it uses the figure above.

`TINY` and `SMALL` are the two predicates; `tiny` implies `small`, so a bound that
splits only at the top uses `SMALL` and a bound that singles out rp2040 uses
`TINY`. Three-way bounds are written `by_arena(tiny, small, full)`.

Aggregation's lane, pane, operator and collection ceilings reach the same answer
by a different route: they live in `agg_core`, which is `include!`d and cannot see
the arena, so the mounting site supplies them — see `AGG_CAP_*` below.

Because a tier is an expression rather than a `cfg`, one declaration covers every
target, and each row below records the whole expression alongside the values it
resolves to.

## Constraints

A limit's value is half of what makes it safe. The other half is its relationship
to the limits around it. `expression`, `decision` and `pipeline` each carry a
`REC_BUF` that is correct at any value on its own, and a node holding less than
the one feeding it would drop exactly the records that node had carried. No value
row states that, because it is not a fact about any one of them.

The block below states those relationships, and the gate checks each is enforced
rather than merely described. Two forms:

- `relation | <source path>` — the relation must be the CONDITION of an
  `assert!` in that file. Not its message: a coupling stated in an assert's
  failure text reads convincingly while enforcing nothing.
- `relation | derived` — for a relation no single compilation can see. The engines
  are separate PIC modules; no build has another's constants in scope, so there is
  no assert any of them could carry. Both sides are
  register rows named `NAME@path`, and the check is that both have the same
  recorded right-hand side. Identical derivation is stronger than the equality it
  stands in for: it says the two move together by construction rather than
  happening to agree.

```limit-constraints
REC_BUF@modules/app/decision/mod.rs == REC_BUF@modules/app/pipeline/mod.rs | derived
REC_BUF@modules/app/expression/mod.rs == REC_BUF@modules/app/decision/mod.rs | derived
BIN_BUF >= MAX_CONTAINER_BIN_FULL | modules/common/author_core.rs
MAX_SNAPSHOT >= SNAPSHOT_EXACT | modules/app/aggregation/mod.rs
MAX_SNAPSHOT <= u16::MAX as usize | modules/app/aggregation/mod.rs
MAX_FRAME <= REC_BUF | modules/app/sensor_intake/mod.rs
MAX_WIN_PER_EVENT <= MAX_PANES | modules/common/agg_core.rs
MAX_LANES >= 1 && MAX_PANES >= 1 && MAX_OPS >= 1 | modules/common/agg_core.rs
COLL_CAP >= 1 | modules/common/agg_core.rs
```

## Record / buffer capacities

| Limit | Value | Dim | Scope | Kind | Failure (input consumed?) | Memory/work | Rationale | Change rule | Tests |
|---|---|---|---|---|---|---|---|---|---|
| `REC_BUF` (expression, decision, pipeline) | 4096 / 4096 / 512 | B | per record, per module | capacity | oversized frame → `BoundaryLost` → `inputs_rejected`, channel reset (consumed: whole stream drained) | one in + one out buffer per module: 8 KiB at the full tier, 1 KiB at tiny | one typed frame. `expression`, `decision` and `pipeline` carry the SAME bound and the register couples them — the three sit on one channel family, so a node admitting more than its downstream would pass records that node must refuse. Each declares the matching per-target `max_record`, so the advertised ceiling and the buffer behind it are one number | fix-forward format change moves all producers+consumers together | `expression::an_oversized_frame…`, `decision::an_oversized_frame…` |
| `STAGE_SCRATCH_CAP` | 512 | B | per record | capacity | scratch exhaustion → eval error → `inputs_failed` (consumed) | on the step, not retained | one record's field-construction scratch | larger module variant | `pipeline` stage suites |
| `MAX_BUILD_FIELDS` | 32 | fld | per record | v1 invariant | field 33 → decode/build reject (consumed) | 32 × `Field` per decode frame | a typed record's field count; the same bound the codec admits | fix-forward format change | `pipeline_core` frame suites |
| `MAX_PIPE_FIELDS` | `MAX_BUILD_FIELDS` | fld | per record | v1 invariant | as `MAX_BUILD_FIELDS` — the alias exists so pipeline code names the bound it is subject to | none of its own | the record field ceiling seen from the pipeline core; one name, one value | tracks `MAX_BUILD_FIELDS` | `pipeline_core` frame suites |
| `STACK_CAP` | 32 | vmi | per program evaluation | capacity | operand 33 → `StackOverflow` → eval error → `inputs_failed` (consumed) | 32 × `Value` on the step, never retained | the operand stack one expression may build; with `MAX_LOCALS` it is the whole of a program's mutable state | fix-forward ISA change | none — the value is pinned by the `limit-register` gate; no boundary suite exercises depth 33 |

## Pipeline

| Limit | Value | Dim | Scope | Kind | Failure (input consumed?) | Memory/work | Rationale | Change rule | Tests |
|---|---|---|---|---|---|---|---|---|---|
| `MAX_STAGES` | 8 | st | per record | capacity | `stage_count > MAX_STAGES` → `inputs_failed`, never truncated (consumed) | 8 × `Stage` descriptor on the step | a bounded pipeline depth; a longer chain is a graph of nodes | larger variant | `pipeline::…over_cap_stage…` |
| `DECISION_STAGE_COST` | 100_000 | vmi | per decision stage | policy default | n/a — recorded, not consulted | none | the `max_cost` field the stage format requires; a decision stage's real bounds are the per-program costs inside its own container, enforced by `run_decision` | tracks the format | `tools/e2e/inline-decision.sh` |
| `INGRESS_BUF` | `PAYLOAD_MAX` (8192) | B | per module (pipeline) | capacity | publish payload > ceiling → OVERSIZE refusal, frame consumed (consumed: yes, counted) | one intake buffer | a sink for any producer of the exchange surface takes the contract's whole payload; the decoded record is still one `REC_BUF` frame | tracks the contract | `tools/e2e/pipeline-chain.sh`, `examples/reverser/reverser_pi5.yaml` (build fact check) |
| `MAX_INFLIGHT` | 8 | rec | per module | capacity | window full → no record admitted until the destination answers (backpressure, consumed: no) | one counter; the frames live downstream | publishes unacknowledged at once on the exchange surface | larger variant | `tools/e2e/pipeline-chain.sh` (more records than the window, all delivered) |
| `PUB_FRAME_MAX` | `3 + PUBLISH_OVERHEAD + REC_BUF` — 4112 / 4112 / 528 | B | per publish frame | capacity | a record that would frame larger is refused before the send; the window is never advanced on a frame that did not go | one publish buffer | one framed publish at this module's record ceiling — what `publish_out` declares as `max_record`, so the exchange peer sizes for exactly this | tracks `REC_BUF` and the contract overhead | `tools/e2e/pipeline-egress.sh` |
| `MAX_VERSIONS` | 8 | ver | per module | capacity | reload adding a 9th version → reload rejected, active table unchanged | version table in `VBIN_BUF` | concurrent blue/green + a few pinned generations | larger variant | `pipeline::hot_reload…` |
| `VERSION_TAG_CAP` | 24 | B | per version | capacity | tag > 24 B → reload rejected (control msg, not a record) | 24 B × versions | a version label, not a payload | fix-forward | `version_core` suites |
| `VERSION_DIGEST_LEN` | 8 | B | per version | v1 invariant | a reload frame whose entry is short of 8 digest bytes → reload rejected, active table unchanged | 8 B × versions | a content digest wide enough to name a program in a table of `MAX_VERSIONS`; the device resolves by tag and does not verify the digest on the fast path — it is the control plane's content identity, so one tag means one set of bytes across a fleet | fix-forward format change | `version_core` suites |
| `VERSION_SELECTOR_FIELD` | 255 | fld | per record | v1 invariant | a record whose selector names no loaded version → fails closed, never the default | none — a field number, not storage | the reserved field a request pins its version with; data fields are 1..=N, so 255 sits outside them | fix-forward format change | `version_core` suites |
| `VBIN_BUF` | `if TINY { 3072 } else { 8192 }` — 8192 / 8192 / 3072 | B | per module | capacity | candidate table past the ceiling → reload rejected, active table untouched | held in TWO copies, active and candidate, so it costs twice what it says: 16 KiB at the full tier, 6 KiB at tiny | the compiled version table, holding every stage of every loaded version. Staging a candidate beside the active table is what lets a reload be rejected without disturbing what is running. A tiny target carries one program plus headroom instead, since a node that size is reconfigured by OTA rather than by hot-swapping versions under load | larger variant | `pipeline_lifecycle` reload |
| `PROG_BUF` | 2048 | B | per program | capacity | program > 2 KiB → load fault (`faulted`) | per encoder/decoder slot | one compiled stage/encoder/decoder program | larger variant | pipeline load faults |

## Decision

| Limit | Value | Dim | Scope | Kind | Failure (input consumed?) | Memory/work | Rationale | Change rule | Tests |
|---|---|---|---|---|---|---|---|---|---|
| `CONT_BUF` | `if TINY { MAX_CONTAINER_BIN } else { MAX_CONTAINER_BIN_FULL }` — 16384 / 16384 / 6144 | B | per module | capacity | container past the buffer → `param_overflow` → FAULT; the node names it and refuses input | one container + its hex = 48 KiB state, 18 KiB on the floor tier | the largest container the authoring path can produce, TAKEN from `pipeline_core` rather than restated. Tiered because one number cannot serve both ends of the arena range: a 29-arm lifecycle table measures 7894 B, about 272 B an arm, so the floor tier cannot hold even 23 arms of that shape, while 48 KiB is 0.02% of a 256 MiB arena | derived — checked, not chosen; `author_core` asserts `BIN_BUF >= MAX_CONTAINER_BIN_FULL` | `decision` load faults, `tools/e2e/fault.sh`, `tools/e2e/arena-budget.sh` |
| `HEX_BUF` | `2 * CONT_BUF` — 32768 / 32768 / 12288 | B | per module | capacity | as `CONT_BUF` — the hex is what the param carries | see `CONT_BUF` | hex of a max container | derived — tracks `CONT_BUF` (2x) | `decision` load faults |

## Aggregation

| Limit | Value | Dim | Scope | Kind | Failure (input consumed?) | Memory/work | Rationale | Change rule | Tests |
|---|---|---|---|---|---|---|---|---|---|
| `AGG_CAP_LANES` / `AGG_CAP_PANES` / `AGG_CAP_OPS` / `AGG_CAP_COLL` | 16,8,8,16 full and small / 4,4,4,4 tiny | — | per mount | capacity source | n/a — they are where the four below are CHOSEN | sizes every lane table, the emission queue and the snapshot | `agg_core` is `include!`d and cannot see the arena, and is mounted by the host harness where no SDK is in scope, so the mounting site supplies these: the engine derives them from `STATE_ARENA_SIZE`, the harness states what its boundary assertions are written against. They dominate the engine's state because the emission queue, the snapshot buffer and its hex decode are all lane×pane products | arena tier; a new tier is an edit at the mount | `aggregation_reduced` (the whole suite: the tiny tier's boundaries) |
| `MAX_LANES` | `AGG_CAP_LANES` — 16 / 16 / 4 | ln | per module | capacity | a key past the ceiling → `lane_overflows`, event still folded (consumed) | lane cells = lanes × panes × ops | distinct keys held concurrently; overflow is counted, never silent | larger variant / partition the keyspace | `aggregation` lane suites |
| `MAX_PANES` | `AGG_CAP_PANES` — 8 / 8 / 4 | pn | per lane | capacity | pane beyond horizon → `pane_overflows` (consumed) | see lanes | sliding-window panes per key | larger variant | `aggregation` window suites |
| `MAX_OPS` | `AGG_CAP_OPS` — 8 / 8 / 4 | op | per module | capacity | an operator past the ceiling → build_spec reject (consumed: no) | ops × lane cells | operators per aggregation | larger variant | `aggregation` spec suites |
| `MAX_WIN_PER_EVENT` | `AGG_CAP_PANES` — 8 / 8 / 4 | win | per event | capacity | iteration bound on windows closed by one event | bounds per-event work | one late event cannot close unbounded windows | larger variant | `aggregation` correction suites |
| `COLL_CAP` | `AGG_CAP_COLL` — 16 / 16 / 4 | rec | per collection cell | capacity | a member past the ceiling → `coll_overflows` (consumed) | one `i64` per retained value, per lane-pane cell | bounded collection operators | larger variant | `aggregation` collection suites |
| `REC_BUF` (aggregation) | `by_arena(512, 1024, 4096)` — 4096 / 1024 / 512 | B | per event | capacity | oversized event frame → refused and counted, never truncated | one input buffer | one typed event frame. Three-way rather than the two the other engines use, because this module's other buffers already span two orders of magnitude and RP2350 can afford the middle size | arena tier | `aggregation` frame suites |
| `CONT_BUF` (aggregation) | `by_arena(1024, 2048, 4096)` — 4096 / 2048 / 1024 | B | per module | capacity | container past the buffer → `param_overflow` → FAULT; the node names it and refuses input | one container plus a same-sized IR scratch | the compiled aggregation definition. Smaller than `decision`'s because an aggregation definition is a key program, a time program, an emit program and a handful of operators — not 32 rule arms | arena tier | `aggregation` load faults |
| `HEX_BUF` (aggregation) | `by_arena(2048, 4096, 8192)` — 8192 / 4096 / 2048 | B | per module | capacity | as `CONT_BUF` — the hex is what the param carries | hex staging | twice `CONT_BUF`, because hex doubles | arena tier — tracks `CONT_BUF` | `aggregation` load faults |
| `KEY_CAP` | 48 | B | per key | capacity | key > 48 B → `AggError::KeyTooLong`, reject (consumed: no) | 48 B × lanes | a routing key, not a payload. NOT scaled with the tier: a key is an identity, so truncating one would merge two distinct keys into a single lane and aggregate across them, and 48 bytes is what a routing key needs however many of them a die can hold | fix-forward | `agg_core` key suites |
| `EMIT_FRAME_MAX` | `if SMALL { 128 } else { 512 }` — 512 / 128 / 128 | B | per emission | capacity | emission past the ceiling → `emit_overflow` → `outputs_failed` (consumed) | queue frame bound | one window-result frame | larger variant | `aggregation` emission suites |
| `EMIT_Q_CAP` | `EMIT_FRAMES_PER_EVENT * (2 + EMIT_FRAME_MAX)` — 131,584 / 33,280 / 4,160 | B | per event | capacity | queue full → `emit_overflow` → `outputs_failed` (consumed) | the queue, and the largest single buffer the engine holds at the full tier | a whole fan-out event's emissions, drained one/step | derived — checked, not chosen: a literal here can be cut below the fan-out it bounds, turning the proven no-drop invariant into a live `emit_overflow` path | `aggregation` fan-out suites |
| `MAX_SNAPSHOT` | `SNAPSHOT_EXACT` +15%, `div_ceil` to 4 KiB — 40,960 / 40,960 / 4,096 | B | per checkpoint | capacity | state > the bound → `snapshot` None → checkpoint skipped, counted | snapshot staging buffer | the largest admitted aggregation state | derived — checked, not chosen; asserted `>= SNAPSHOT_EXACT` and `<= u16::MAX` (the `Pending` cursor width) | `aggregation` checkpoint suites |
| `SNAP_HEX` | `2 * MAX_SNAPSHOT` — 81,920 / 81,920 / 8,192 | B | per checkpoint | capacity | restore hex > buffer → restore reject | hex-decode staging | hex of a max snapshot | derived — tracks `MAX_SNAPSHOT` | `aggregation` restore suites |
| `EMIT_FRAMES_PER_EVENT` | `MAX_LANES * MAX_PANES * 2` — 256 / 256 / 32 | fr | per event | derived | n/a — it is the fan-out, not a refusal point | sizes `EMIT_Q_CAP` | every live pane on an `OnProcessing` trigger, then again on finalization | derived — moves with the lane/pane capacities, which is the point | `aggregation` fan-out suites |
| `SNAPSHOT_EXACT` | `SNAPSHOT_GLOBAL_BYTES + MAX_LANES * (SNAPSHOT_LANE_BYTES + MAX_PANES * SNAPSHOT_PANE_BYTES)`  — 35,562 full and small, 2,042 tiny | B | per checkpoint | derived | n/a — the bound `MAX_SNAPSHOT` is asserted against | none of its own | the exact bound on what `AggState::snapshot` can emit, from its own wire layout | derived — moves with every capacity it names | `aggregation` checkpoint suites |
| `SNAPSHOT_GLOBAL_BYTES` | `1 + 8 + 6 * 4 + 1 + 8` (42, every tier) | B | per checkpoint | derived | n/a | none | the snapshot's fixed header: version, max event time, six counters, lane count, processing clock | tracks the `agg_core::snapshot` layout | `aggregation` checkpoint suites |
| `SNAPSHOT_LANE_BYTES` | `1 + 2 + KEY_CAP + 8 + 1` (60, every tier) | B | per lane | derived | n/a | none | one lane's header: key discriminant, key length, key, finalized high-water, pane count | tracks the `agg_core::snapshot` layout | `aggregation` checkpoint suites |
| `SNAPSHOT_PANE_BYTES` | `1 + 8 + 16 * MAX_OPS + 4 + 1 + 8 * COLL_CAP` — 270 full and small, 110 tiny | B | per pane | derived | n/a | none | one pane: finalized flag, window start, every accumulator pair, emit counter, and the length-prefixed collection cell | tracks the `agg_core::snapshot` layout | `aggregation` checkpoint suites |

## CLI / authoring

| Limit | Value | Dim | Scope | Kind | Failure (input consumed?) | Memory/work | Rationale | Change rule | Tests |
|---|---|---|---|---|---|---|---|---|---|
| `UPROC_BUF` | 65536 | B | per document | capacity | `.uproc` source > 64 KiB → refused before author (does not hang) | source staging | one document carrying a whole protocol surface: discovery, authn, mTLS, authz, admission and CRUD on one request path | larger variant, in state | `chronicle_cli` author suites |
| `ARGV_BUF` | `2 * UPROC_BUF` (131072) | B | per invocation | capacity | argv record past the buffer → bounded retry then ERROR, applet gets no argv | argv staging | the hex of a max document as one argv record; DERIVED so the two cannot drift apart and leave a document that compiles but never reaches the applet | tracks `UPROC_BUF` (2x) | `chronicle_cli` parse suites |
| `MAX_RULE` | 32 | rec | per decision (author) | capacity | 33rd rule arm → author reject | 2 × 16 KiB in module STATE (`RuleCode`), not the stack | a state machine's arm count follows its states: entry and exit conditions over a handful of states reach the twenties without padding | raise with the buffers in state, never on a PIC frame | `chronicle_cli` decision authoring |
| `RULE_CODE` | 512 | B | per rule program (author) | capacity | a `when` or outcome compiling past 512 B → author reject | `2 × MAX_RULE × RULE_CODE` = 32 KiB in state | one arm's predicate or constructed outcome; the outcome is the long one, since a pass-through arm sets every field of its record | larger variant, in state | `chronicle_cli` decision authoring |
| `BIN_BUF` | 16384 | B | per artefact (author) | capacity | artefact past the buffer → author reports and refuses | 3 work buffers in module state | one sealed artefact: its container, its lowered code, and the digest-free encoding the two-pass seal needs | larger variant, in state | `chronicle_cli` author suites |
| `MAX_CONTAINER_BIN` | 6144 | B | per artefact (format) | capacity | an engine handed a larger container FAULTs on `param_overflow`; the authoring path refuses first | sizes an engine's container buffer on a 64 KiB-arena target | the largest compiled artefact container a floor-tier target can hold. A target whose whole state arena is 65,536 B cannot spend 48 KiB of it on one engine, so the floor buys its headroom back and accepts a table of roughly 22 arms | chosen; bounded above by `MAX_CONTAINER_BIN_FULL` | `chronicle_cli` author suites, `decision` load faults |
| `MAX_CONTAINER_BIN_FULL` | 16384 | B | per artefact (format) | capacity | as `MAX_CONTAINER_BIN` — the engine FAULTs, the authoring path refuses first | sizes every engine's container buffer off the floor tier | the largest compiled artefact container that can exist. It is the number the authoring path and the runtime engines must agree on, so it lives in the core both `include!` and neither owns, and `BIN_BUF` is asserted against it. 16384 admits the 32 arms `MAX_RULE` already permits at the ~272 B an arm a real lifecycle table costs | chosen; raise with `author_core::BIN_BUF`, which asserts it is at least this | `chronicle_cli` author suites, `decision` load faults |
| `OUT_BUF` | `2 * BIN_BUF + 4096` (36864) | B | per invocation | capacity | reply past the buffer → "too large to print", exit 1 | stdout staging in state | an artefact printed as HEX (2x) plus the YAML `graph` wraps it in; the `stdout` port takes one whole reply as a record | tracks `BIN_BUF` (2x) | `chronicle_cli` output suites, `tools/e2e/cli.sh` |

## Authoring document and CLI

Ceilings on what one `.uproc` document may declare and what the authoring CLI
accepts. Every one refuses: a document past a ceiling is rejected with a named
error and compiles to nothing, so a deployment unit is never a truncated version
of the document that produced it.

| Limit | Value | Dim | Scope | Kind | Failure (input consumed?) | Memory/work | Rationale | Change rule | Tests |
|---|---|---|---|---|---|---|---|---|---|
| `MAX_ART` | 24 | op | per document | capacity | artefact 25 → `error: too many artefacts`, compile aborts | 24 × (digest + symbol + kind) on the compile stack | every sealed artefact becomes a module ref, so this is how many the document's deployment unit may name | fix-forward | `tools/e2e/author.sh` |
| `MAX_ST` | 8 | st | per pipeline, per document | capacity | stage 9 → `error: too many stages for this node` | compile-time only | a pipeline's stage count as the authoring path admits it; mirrors the module's own `MAX_STAGES` | raise with `MAX_STAGES` | `tools/e2e/author.sh` |
| `MAX_OP` | 8 | op | per aggregation, per document | capacity | operator 9 → `error: too many operators for this node` | compile-time only | an aggregation's operator count; mirrors `MAX_OPS` in the engine | raise with `MAX_OPS` | `tools/e2e/author.sh` |
| `MAX_BIND` | 8 | op | per document | capacity | resource 9 → `error: too many resources`; entry 9 → `error: too many entries` | 8 × `BindingSpec` + 8 × `EntrySpec` | the resource requirements and activatable entries one document may declare | fix-forward | `tools/e2e/author.sh` |
| `MAX_PLAN_STAGES` | 16 | st | per compiled plan | capacity | stage 17 → `error: too many stages` | 16 × (span, kind, connector) | stages a plan may carry after lowering, above `MAX_ST` so a pipeline plus its effects still fits | fix-forward | `tools/e2e/author.sh` |
| `MAX_BINDINGS` | 8 | op | per deployment | capacity | binding 9 → parse refuses, the deployment does not compile | 8 × `BindingSpec` | deployment bindings one graph may resolve | fix-forward | `tools/e2e/author.sh` |
| `MAX_BINDING_PARAMS` | 12 | fld | per binding | capacity | param 13 → parse refuses | 12 × param span per binding | a connector's parameter list | fix-forward | `tools/e2e/author.sh` |
| `MAX_ARGV` | 24 | op | per CLI invocation | capacity | argument 25 → `error: too many arguments`; `split_argv` counts past the buffer so the refusal is on the true count | 24 spans | the applet's argument vector | fix-forward | `tools/e2e/cli.sh` |
| `MAX_SET` | 8 | op | per comma-separated argument | capacity | item 9 → `error: too many capabilities` / `too many bindings`, or a refused digest list; `split_csv` counts past the buffer | 8 slices | one list-valued CLI argument | fix-forward | `tools/e2e/cli.sh` |
| `MAX_REFS` | 6 | op | per `slot` invocation | capacity | ref 7 → `error: too many refs` | 6 × (digest, kind) | module refs one slot image may be built from at the CLI | fix-forward | `tools/e2e/slot-verify.sh` |
| `MAX_TRUSTED` | 8 | op | per `verify` invocation | capacity | key 9 → `error: too many trusted keys` | 8 × 32 B | trusted signing keys one verification may be given | fix-forward | `tools/e2e/verify.sh` |
| `MAX_STAGES` (CLI) | 8 | st | per lowered program | capacity | stage 9 → `error: too many stages` | 8 × `Stage` | the stage table the CLI builds from a lowered program; the module ceiling it must match | raise with the module's `MAX_STAGES` | `tools/e2e/cli.sh` |

## Observability

| Limit | Value | Dim | Scope | Kind | Failure (input consumed?) | Memory/work | Rationale | Change rule | Tests |
|---|---|---|---|---|---|---|---|---|---|
| `ACCT_METRIC_COUNT` | 14 | inst | per module | v1 invariant | manifest not front-loading these 14 in order → CI fail | 14 baseline instruments | the common accounting block emitted as ids 0..13 | fix-forward (adds shift downstream ids) | `tools/ci/accounting-order.sh` |
| `TLM_INTERVAL_MS` | 5000 | ms | per module | policy default | n/a (throttle only) | one publish per 5 s when subscribed | a uniform collector cadence (matches the fluxor DNS module) | configuration change | `tools/e2e/telemetry.sh` |

## OTA slot

The slot geometry is **Fluxor's**, restated here because it bounds what a
Chronicle artefact may be: `slot_core.rs` names the Fluxor constant each one
mirrors (`GRAPH_SLOT_SIZE`, `GRAPH_SLOT_HEADER_SIZE`). The gate proves
Chronicle's copy matches Chronicle's source; it cannot see Fluxor, so a change
to the flash layout upstream is reconciled by hand. That is the one row pair in
this register whose authority lives in another project.

| Limit | Value | Dim | Scope | Kind | Failure (input consumed?) | Memory/work | Rationale | Change rule | Tests |
|---|---|---|---|---|---|---|---|---|---|
| `SLOT_SIZE` | 0x0008_0000 (512 KiB) | B | per slot image | capacity | an extent past the slot → `BadExtent`; the image is refused whole, never partially adopted | the deployable artefact's whole envelope | Fluxor's `GRAPH_SLOT_SIZE`; every pipeline, program and version an OTA carries fits inside it | follows Fluxor's flash layout, not Chronicle's choice | `slot` suites (`BadExtent`) |
| `SLOT_HEADER_SIZE` | 256 | B | per slot image | v1 invariant | a shorter image → `TooShort` before any field is read | the fixed preamble the magic, version, epoch and digest pin live in | Fluxor's `GRAPH_SLOT_HEADER_SIZE` | as `SLOT_SIZE` | `slot` suites (`TooShort`) |

## Machine-checked constants

The `limit-register` gate parses the block below and asserts each named constant
still has the recorded right-hand side at the recorded source path. Format per
line: `NAME | source_path | expected_rhs`, where the right-hand side is compared
as TEXT after whitespace normalisation — so a constant defined in terms of
another is pinned to that relationship rather than to a number that would quietly
stop tracking it.

Every ceiling-shaped constant in a file this block names must appear here or in
the exemption block; an unregistered one fails the gate rather than waiting to be
noticed. Keep it in step with the tables above: the tables are prose, this block
is what the build checks.

```limit-register
MAX_LANES | modules/common/agg_core.rs | AGG_CAP_LANES
MAX_PANES | modules/common/agg_core.rs | AGG_CAP_PANES
MAX_OPS | modules/common/agg_core.rs | AGG_CAP_OPS
MAX_WIN_PER_EVENT | modules/common/agg_core.rs | AGG_CAP_PANES
COLL_CAP | modules/common/agg_core.rs | AGG_CAP_COLL
KEY_CAP | modules/common/agg_core.rs | 48
AGG_CAP_LANES | modules/app/aggregation/mod.rs | if ARENA <= 64 * 1024 { 4 } else { 16 }
AGG_CAP_PANES | modules/app/aggregation/mod.rs | if ARENA <= 64 * 1024 { 4 } else { 8 }
AGG_CAP_OPS | modules/app/aggregation/mod.rs | if ARENA <= 64 * 1024 { 4 } else { 8 }
AGG_CAP_COLL | modules/app/aggregation/mod.rs | if ARENA <= 64 * 1024 { 4 } else { 16 }
MAX_STAGES | modules/app/pipeline/mod.rs | 8
PUB_FRAME_MAX | modules/app/pipeline/mod.rs | 3 + PUBLISH_OVERHEAD + REC_BUF
MAX_PIPE_FIELDS | modules/common/pipeline_core.rs | MAX_BUILD_FIELDS
MAX_STAGES | modules/app/chronicle_cli/mod.rs | 8
MAX_REFS | modules/app/chronicle_cli/mod.rs | 6
MAX_TRUSTED | modules/app/chronicle_cli/mod.rs | 8
MAX_ARGV | modules/common/author_core.rs | 24
MAX_SET | modules/common/author_core.rs | 8
MAX_ART | modules/common/author_core.rs | 24
MAX_ST | modules/common/author_core.rs | 8
MAX_OP | modules/common/author_core.rs | 8
MAX_BIND | modules/common/author_core.rs | 8
MAX_PLAN_STAGES | modules/common/author_core.rs | 16
MAX_BINDINGS | modules/common/author_core.rs | 8
MAX_BINDING_PARAMS | modules/common/author_core.rs | 12
DECISION_STAGE_COST | modules/common/lower_core.rs | 100_000
MAX_INFLIGHT | modules/app/pipeline/mod.rs | 8
INGRESS_BUF | modules/app/pipeline/mod.rs | PAYLOAD_MAX
MAX_BUILD_FIELDS | modules/common/vm_core.rs | 32
MAX_VERSIONS | modules/common/version_core.rs | 8
VERSION_TAG_CAP | modules/common/version_core.rs | 24
VERSION_DIGEST_LEN | modules/common/version_core.rs | 8
VERSION_SELECTOR_FIELD | modules/common/version_core.rs | 255
MAX_LOCALS | modules/common/vm_core.rs | 8
STACK_CAP | modules/common/vm_core.rs | 32
STAGE_SCRATCH_CAP | modules/common/pipeline_core.rs | 512
MAX_RULE | modules/common/author_core.rs | 32
RULE_CODE | modules/common/author_core.rs | 512
BIN_BUF | modules/common/author_core.rs | 16384
MAX_CONTAINER_BIN | modules/common/pipeline_core.rs | 6144
MAX_CONTAINER_BIN_FULL | modules/common/pipeline_core.rs | 16384
EMIT_FRAME_MAX | modules/app/aggregation/mod.rs | if SMALL { 128 } else { 512 }
REC_BUF | modules/app/expression/mod.rs | if TINY { 512 } else { 4096 }
REC_BUF | modules/app/pipeline/mod.rs | if TINY { 512 } else { 4096 }
REC_BUF | modules/app/decision/mod.rs | if TINY { 512 } else { 4096 }
REC_BUF | modules/app/aggregation/mod.rs | by_arena(512, 1024, 4096)
HEX_BUF | modules/app/aggregation/mod.rs | by_arena(2048, 4096, 8192)
CONT_BUF | modules/app/aggregation/mod.rs | by_arena(1024, 2048, 4096)
EMIT_Q_CAP | modules/app/aggregation/mod.rs | EMIT_FRAMES_PER_EVENT * (2 + EMIT_FRAME_MAX)
EMIT_FRAMES_PER_EVENT | modules/app/aggregation/mod.rs | MAX_LANES * MAX_PANES * 2
MAX_SNAPSHOT | modules/app/aggregation/mod.rs | ((SNAPSHOT_EXACT * 115) / 100).div_ceil(4096) * 4096
SNAPSHOT_EXACT | modules/app/aggregation/mod.rs | SNAPSHOT_GLOBAL_BYTES + MAX_LANES * (SNAPSHOT_LANE_BYTES + MAX_PANES * SNAPSHOT_PANE_BYTES)
SNAPSHOT_GLOBAL_BYTES | modules/app/aggregation/mod.rs | 1 + 8 + 6 * 4 + 1 + 8
SNAPSHOT_LANE_BYTES | modules/app/aggregation/mod.rs | 1 + 2 + KEY_CAP + 8 + 1
SNAPSHOT_PANE_BYTES | modules/app/aggregation/mod.rs | 1 + 8 + 16 * MAX_OPS + 4 + 1 + 8 * COLL_CAP
SNAP_HEX | modules/app/aggregation/mod.rs | 2 * MAX_SNAPSHOT
VBIN_BUF | modules/app/pipeline/mod.rs | if TINY { 3072 } else { 8192 }
PROG_BUF | modules/app/pipeline/mod.rs | 2048
ARGV_BUF | modules/app/chronicle_cli/mod.rs | 2 * UPROC_BUF
UPROC_BUF | modules/app/chronicle_cli/mod.rs | 65536
OUT_BUF | modules/app/chronicle_cli/mod.rs | 2 * tc::BIN_BUF + 4096
HEX_BUF | modules/app/decision/mod.rs | 2 * CONT_BUF
CONT_BUF | modules/app/decision/mod.rs | if TINY { dec::MAX_CONTAINER_BIN } else { dec::MAX_CONTAINER_BIN_FULL }
TLM_INTERVAL_MS | modules/common/telemetry_core.rs | 5000
ACCT_METRIC_COUNT | modules/common/accounting_core.rs | 14
SLOT_HEADER_SIZE | modules/common/slot_core.rs | 256
SLOT_SIZE | modules/common/slot_core.rs | 0x0008_0000
```
