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
**ms**=milliseconds, **inst**=instruments.

## Record / buffer capacities

| Limit | Value | Dim | Scope | Kind | Failure (input consumed?) | Memory/work | Rationale | Change rule | Tests |
|---|---|---|---|---|---|---|---|---|---|
| `REC_BUF` | 4096 | B | per record, per module | capacity | oversized frame → `BoundaryLost` → `inputs_rejected`, channel reset (consumed: whole stream drained) | one in + one out buffer per module = 8 KiB state | one typed frame; matches the port `max_record` every graph declares | fix-forward format change moves all producers+consumers together | `expression::an_oversized_frame…`, `decision::an_oversized_frame…` |
| `STAGE_SCRATCH_CAP` | 512 | B | per record | capacity | scratch exhaustion → eval error → `inputs_failed` (consumed) | on the step, not retained | one record's field-construction scratch | larger module variant | `pipeline` stage suites |
| `MAX_BUILD_FIELDS` | 16 | fld | per record | v1 invariant | field 17 → decode/build reject (consumed) | 16 × `Field` per decode frame | a typed record's field count; the same bound the codec admits | fix-forward format change | `pipeline_core` frame suites |

## Pipeline

| Limit | Value | Dim | Scope | Kind | Failure (input consumed?) | Memory/work | Rationale | Change rule | Tests |
|---|---|---|---|---|---|---|---|---|---|
| `MAX_STAGES` | 8 | st | per record | capacity | `stage_count > MAX_STAGES` → `inputs_failed`, never truncated (consumed) | 8 × `Stage` descriptor on the step | a bounded pipeline depth; a longer chain is a graph of nodes | larger variant | `pipeline::…over_cap_stage…` |
| `MAX_VERSIONS` | 8 | ver | per module | capacity | reload adding a 9th version → reload rejected, active table unchanged | version table in `VBIN_BUF` | concurrent blue/green + a few pinned generations | larger variant | `pipeline::hot_reload…` |
| `VERSION_TAG_CAP` | 24 | B | per version | capacity | tag > 24 B → reload rejected (control msg, not a record) | 24 B × versions | a version label, not a payload | fix-forward | `version_core` suites |
| `VBIN_BUF` | 8192 | B | per module | capacity | candidate table > 8 KiB → reload rejected, active unchanged | one active + one candidate = 16 KiB | the compiled version table (all stages of all versions) | larger variant | `pipeline_lifecycle` reload |
| `PROG_BUF` | 2048 | B | per program | capacity | program > 2 KiB → load fault (`faulted`) | per encoder/decoder slot | one compiled stage/encoder/decoder program | larger variant | pipeline load faults |

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
| `UPROC_BUF` | 32768 | B | per document | capacity | `.uproc` source > 32 KiB → refused before author (does not hang) | source staging | a full IdP `.uproc` (the largest example document, ~25.5 KiB) | larger variant | `chronicle_cli` author suites |
| `ARGV_BUF` | 65536 | B | per invocation | capacity | argv record > 64 KiB → bounded retry then ERROR, applet gets no argv | argv staging (2 × `UPROC_BUF`) | the hex of a max document as one argv record | tracks `UPROC_BUF` (2×) | `chronicle_cli` parse suites |
| `MAX_RULE` | 8 | rec | per decision (author) | capacity | 9th rule arm → author reject | 2 × 2 KiB stack arrays | a decision table sized off the PIC stack budget | larger variant | `chronicle_cli` decision authoring |

## Observability

| Limit | Value | Dim | Scope | Kind | Failure (input consumed?) | Memory/work | Rationale | Change rule | Tests |
|---|---|---|---|---|---|---|---|---|---|
| `ACCT_METRIC_COUNT` | 14 | inst | per module | v1 invariant | manifest not front-loading these 14 in order → CI fail | 14 baseline instruments | the common accounting block emitted as ids 0..13 | fix-forward (adds shift downstream ids) | `tools/ci/accounting-order.sh` |
| `TLM_INTERVAL_MS` | 5000 | ms | per module | policy default | n/a (throttle only) | one publish per 5 s when subscribed | a uniform collector cadence (matches the fluxor DNS module) | configuration change | `tools/e2e/telemetry.sh` |

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
MAX_BUILD_FIELDS | modules/common/vm_core.rs | 16
MAX_VERSIONS | modules/common/version_core.rs | 8
VERSION_TAG_CAP | modules/common/version_core.rs | 24
MAX_LOCALS | modules/common/vm_core.rs | 8
STAGE_SCRATCH_CAP | modules/common/pipeline_core.rs | 512
MAX_RULE | modules/common/author_core.rs | 8
EMIT_FRAME_MAX | modules/app/aggregation/mod.rs | 512
EMIT_Q_CAP | modules/app/aggregation/mod.rs | 256 * (2 + EMIT_FRAME_MAX)
MAX_SNAPSHOT | modules/app/aggregation/mod.rs | 40960
SNAP_HEX | modules/app/aggregation/mod.rs | 2 * MAX_SNAPSHOT
VBIN_BUF | modules/app/pipeline/mod.rs | 8192
PROG_BUF | modules/app/pipeline/mod.rs | 2048
ARGV_BUF | modules/app/chronicle_cli/mod.rs | 65536
UPROC_BUF | modules/app/chronicle_cli/mod.rs | 32768
TLM_INTERVAL_MS | modules/common/telemetry_core.rs | 5000
ACCT_METRIC_COUNT | modules/common/accounting_core.rs | 14
```
