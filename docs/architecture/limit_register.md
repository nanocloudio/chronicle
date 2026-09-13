# Chronicle limit register

Every bound that shapes what a Chronicle artefact may hold or
do is recorded here with its source, so a reader can see the whole resource
envelope in one place and a CI gate (`tools/ci/limit-register.sh`) can prove the
document has not drifted from the code. The register is normative for the
`edge` profile (Pi 5 / BCM2712), the only profile currently built.

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

## Record / buffer capacities

| Limit | Value | Dim | Scope | Kind | Failure (input consumed?) | Memory/work | Rationale | Change rule | Tests |
|---|---|---|---|---|---|---|---|---|---|
| `REC_BUF` | 4096 | B | per record, per module | capacity | oversized frame → `BoundaryLost` → `inputs_rejected`, channel reset (consumed: whole stream drained) | one in + one out buffer per module = 8 KiB state | one typed frame; matches the port `max_record` every graph declares | fix-forward format change moves all producers+consumers together | `expression::an_oversized_frame…`, `decision::an_oversized_frame…` |
| `STAGE_SCRATCH_CAP` | 512 | B | per record | capacity | scratch exhaustion → eval error → `inputs_failed` (consumed) | on the step, not retained | one record's field-construction scratch | larger module variant | `pipeline` stage suites |
| `MAX_BUILD_FIELDS` | 32 | fld | per record | v1 invariant | field 33 → decode/build reject (consumed) | 32 × `Field` per decode frame | a typed record's field count; the same bound the codec admits | fix-forward format change | `pipeline_core` frame suites |
| `MAX_PIPE_FIELDS` | `MAX_BUILD_FIELDS` | fld | per record | v1 invariant | as `MAX_BUILD_FIELDS` — the alias exists so pipeline code names the bound it is subject to | none of its own | the record field ceiling seen from the pipeline core; one name, one value | tracks `MAX_BUILD_FIELDS` | `pipeline_core` frame suites |
| `STACK_CAP` | 32 | vmi | per program evaluation | capacity | operand 33 → `StackOverflow` → eval error → `inputs_failed` (consumed) | 32 × `Value` on the step, never retained | the operand stack one expression may build; with `MAX_LOCALS` it is the whole of a program's mutable state | fix-forward ISA change | none — the value is pinned by `tools/ci/limit-register.sh`; no boundary suite exercises depth 33 |

## Pipeline

| Limit | Value | Dim | Scope | Kind | Failure (input consumed?) | Memory/work | Rationale | Change rule | Tests |
|---|---|---|---|---|---|---|---|---|---|
| `MAX_STAGES` | 8 | st | per record | capacity | `stage_count > MAX_STAGES` → `inputs_failed`, never truncated (consumed) | 8 × `Stage` descriptor on the step | a bounded pipeline depth; a longer chain is a graph of nodes | larger variant | `pipeline::…over_cap_stage…` |
| `DECISION_STAGE_COST` | 100_000 | vmi | per decision stage | policy default | n/a — recorded, not consulted | none | the `max_cost` field the stage format requires; a decision stage's real bounds are the per-program costs inside its own container, enforced by `run_decision` | tracks the format | `tools/e2e/inline-decision.sh` |
| `INGRESS_BUF` | `PAYLOAD_MAX` (8192) | B | per module (pipeline) | capacity | publish payload > ceiling → OVERSIZE refusal, frame consumed (consumed: yes, counted) | one intake buffer | a sink for any producer of the exchange surface takes the contract's whole payload; the decoded record is still one `REC_BUF` frame | tracks the contract | `tools/e2e/pipeline-chain.sh`, `examples/reverser/reverser_pi5.yaml` (build fact check) |
| `MAX_INFLIGHT` | 8 | rec | per module | capacity | window full → no record admitted until the destination answers (backpressure, consumed: no) | one counter; the frames live downstream | publishes unacknowledged at once on the exchange surface | larger variant | `tools/e2e/pipeline-chain.sh` (more records than the window, all delivered) |
| `PUB_FRAME_MAX` | `3 + PUBLISH_OVERHEAD + REC_BUF` | B | per publish frame | capacity | a record that would frame larger is refused before the send; the window is never advanced on a frame that did not go | one publish buffer | one framed publish at this module's record ceiling — what `publish_out` declares as `max_record`, so the exchange peer sizes for exactly this | tracks `REC_BUF` and the contract overhead | `tools/e2e/pipeline-egress.sh` |
| `MAX_VERSIONS` | 8 | ver | per module | capacity | reload adding a 9th version → reload rejected, active table unchanged | version table in `VBIN_BUF` | concurrent blue/green + a few pinned generations | larger variant | `pipeline::hot_reload…` |
| `VERSION_TAG_CAP` | 24 | B | per version | capacity | tag > 24 B → reload rejected (control msg, not a record) | 24 B × versions | a version label, not a payload | fix-forward | `version_core` suites |
| `VERSION_DIGEST_LEN` | 8 | B | per version | v1 invariant | a reload frame whose entry is short of 8 digest bytes → reload rejected, active table unchanged | 8 B × versions | a content digest wide enough to name a program in a table of `MAX_VERSIONS`; the device resolves by tag and does not verify the digest on the fast path — it is the control plane's content identity, so one tag means one set of bytes across a fleet | fix-forward format change | `version_core` suites |
| `VERSION_SELECTOR_FIELD` | 255 | fld | per record | v1 invariant | a record whose selector names no loaded version → fails closed, never the default | none — a field number, not storage | the reserved field a request pins its version with; data fields are 1..=N, so 255 sits outside them | fix-forward format change | `version_core` suites |
| `VBIN_BUF` | 8192 | B | per module | capacity | candidate table > 8 KiB → reload rejected, active unchanged | one active + one candidate = 16 KiB | the compiled version table (all stages of all versions) | larger variant | `pipeline_lifecycle` reload |
| `PROG_BUF` | 2048 | B | per program | capacity | program > 2 KiB → load fault (`faulted`) | per encoder/decoder slot | one compiled stage/encoder/decoder program | larger variant | pipeline load faults |

## Decision

| Limit | Value | Dim | Scope | Kind | Failure (input consumed?) | Memory/work | Rationale | Change rule | Tests |
|---|---|---|---|---|---|---|---|---|---|
| `CONT_BUF` | 20480 | B | per module | capacity | container past the buffer → `param_overflow` → FAULT; the node names it and refuses input | one container + its hex = 60 KiB state | a full `MAX_RULE` decision table, so the arm count is the bound an author meets | larger variant, in state | `decision` load faults, `tools/e2e/fault.sh` |
| `HEX_BUF` | 40960 | B | per module | capacity | as `CONT_BUF` — the hex is what the param carries | see `CONT_BUF` | hex of a max container | tracks `CONT_BUF` (2x) | `decision` load faults |

## Aggregation

| Limit | Value | Dim | Scope | Kind | Failure (input consumed?) | Memory/work | Rationale | Change rule | Tests |
|---|---|---|---|---|---|---|---|---|---|
| `MAX_LANES` | 16 | ln | per module | capacity | 17th key → `lane_overflows`, event still folded (consumed) | lane cells = lanes × panes × ops | distinct keys held concurrently; overflow is counted, never silent | larger variant / partition the keyspace | `aggregation` lane suites |
| `MAX_PANES` | 8 | pn | per lane | capacity | pane beyond horizon → `pane_overflows` (consumed) | see lanes | sliding-window panes per key | larger variant | `aggregation` window suites |
| `MAX_OPS` | 8 | op | per module | capacity | 9th operator in spec → build_spec reject (consumed: no) | ops × lane cells | operators per aggregation | larger variant | `aggregation` spec suites |
| `MAX_WIN_PER_EVENT` | 8 | win | per event | capacity | iteration bound on windows closed by one event | bounds per-event work | one late event cannot close unbounded windows | larger variant | `aggregation` correction suites |
| `COLL_CAP` | 16 | rec | per collection cell | capacity | 17th member → `coll_overflows` (consumed) | 16 × member per collection op | bounded collection operators | larger variant | `aggregation` collection suites |
| `KEY_CAP` | 48 | B | per key | capacity | key > 48 B → `AggError::KeyTooLong`, reject (consumed: no) | 48 B × lanes | a routing key, not a payload | fix-forward | `agg_core` key suites |
| `EMIT_FRAME_MAX` | 512 | B | per emission | capacity | emission > 512 B → `emit_overflow` → `outputs_failed` (consumed) | queue frame bound | one window-result frame | larger variant | `aggregation` emission suites |
| `EMIT_Q_CAP` | 256*(2+EMIT_FRAME_MAX) | B | per event | capacity | queue full → `emit_overflow` → `outputs_failed` (consumed) | ~131 KiB emission queue | a whole fan-out event's emissions, drained one/step | larger variant | `aggregation` fan-out suites |
| `MAX_SNAPSHOT` | 40960 | B | per checkpoint | capacity | state > 40 KiB → `snapshot` None → checkpoint skipped, counted | snapshot staging buffer | the largest admitted aggregation state | larger variant / chunked checkpoints | `aggregation` checkpoint suites |
| `SNAP_HEX` | 2*MAX_SNAPSHOT | B | per checkpoint | capacity | restore hex > buffer → restore reject | hex-decode staging | hex of a max snapshot | tracks `MAX_SNAPSHOT` | `aggregation` restore suites |

## CLI / authoring

| Limit | Value | Dim | Scope | Kind | Failure (input consumed?) | Memory/work | Rationale | Change rule | Tests |
|---|---|---|---|---|---|---|---|---|---|
| `UPROC_BUF` | 65536 | B | per document | capacity | `.uproc` source > 64 KiB → refused before author (does not hang) | source staging | one document carrying a whole protocol surface: discovery, authn, mTLS, authz, admission and CRUD on one request path | larger variant, in state | `chronicle_cli` author suites |
| `ARGV_BUF` | `2 * UPROC_BUF` (131072) | B | per invocation | capacity | argv record past the buffer → bounded retry then ERROR, applet gets no argv | argv staging | the hex of a max document as one argv record; DERIVED so the two cannot drift apart and leave a document that compiles but never reaches the applet | tracks `UPROC_BUF` (2x) | `chronicle_cli` parse suites |
| `MAX_RULE` | 32 | rec | per decision (author) | capacity | 33rd rule arm → author reject | 2 × 16 KiB in module STATE (`RuleCode`), not the stack | a state machine's arm count follows its states: entry and exit conditions over a handful of states reach the twenties without padding | raise with the buffers in state, never on a PIC frame | `chronicle_cli` decision authoring |
| `RULE_CODE` | 512 | B | per rule program (author) | capacity | a `when` or outcome compiling past 512 B → author reject | `2 × MAX_RULE × RULE_CODE` = 32 KiB in state | one arm's predicate or constructed outcome; the outcome is the long one, since a pass-through arm sets every field of its record | larger variant, in state | `chronicle_cli` decision authoring |
| `BIN_BUF` | 16384 | B | per artefact (author) | capacity | artefact past the buffer → author reports and refuses | 3 work buffers in module state | one sealed artefact: its container, its lowered code, and the digest-free encoding the two-pass seal needs | larger variant, in state | `chronicle_cli` author suites |
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

`tools/ci/limit-register.sh` parses the block below and asserts each named
constant still has the recorded right-hand side at the recorded source path.
Format per line: `NAME | source_path | expected_rhs`. Keep it in sync with the
tables above — the tables are prose, this block is the gate.

```limit-register
MAX_LANES | modules/common/agg_core.rs | 16
MAX_PANES | modules/common/agg_core.rs | 8
MAX_OPS | modules/common/agg_core.rs | 8
MAX_WIN_PER_EVENT | modules/common/agg_core.rs | 8
COLL_CAP | modules/common/agg_core.rs | 16
KEY_CAP | modules/common/agg_core.rs | 48
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
EMIT_FRAME_MAX | modules/app/aggregation/mod.rs | 512
EMIT_Q_CAP | modules/app/aggregation/mod.rs | 256 * (2 + EMIT_FRAME_MAX)
MAX_SNAPSHOT | modules/app/aggregation/mod.rs | 40960
SNAP_HEX | modules/app/aggregation/mod.rs | 2 * MAX_SNAPSHOT
VBIN_BUF | modules/app/pipeline/mod.rs | 8192
PROG_BUF | modules/app/pipeline/mod.rs | 2048
ARGV_BUF | modules/app/chronicle_cli/mod.rs | 2 * UPROC_BUF
UPROC_BUF | modules/app/chronicle_cli/mod.rs | 65536
OUT_BUF | modules/app/chronicle_cli/mod.rs | 2 * tc::BIN_BUF + 4096
HEX_BUF | modules/app/decision/mod.rs | 40960
CONT_BUF | modules/app/decision/mod.rs | 20480
TLM_INTERVAL_MS | modules/common/telemetry_core.rs | 5000
ACCT_METRIC_COUNT | modules/common/accounting_core.rs | 14
SLOT_HEADER_SIZE | modules/common/slot_core.rs | 256
SLOT_SIZE | modules/common/slot_core.rs | 0x0008_0000
```
