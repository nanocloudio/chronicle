# Bytecode growth policy

Chronicle carries a custom bytecode VM. This document fixes what it is for, what
it may never grow into, and how new capability is added instead. It exists
because the VM's value rests entirely on a property that is easy to destroy by
increments: **every program terminates, by construction**.

## What the VM is for

Exactly one job: **per-record pure compute**, shipped as data.

The requirement is logic-as-data — pipelines are loaded at runtime, delivered
OTA into a 512 KiB slot (`SLOT_SIZE`), and hot-reloaded by version — under
constraints that rule out the usual answers: `no_std`, no allocator, no `unsafe`
outside the syscall seam (`tools/ci/unsafe-seam.sh`), and a step short enough to
sit inside a cooperative scheduler tick. A small VM whose ISA cannot express
non-termination is the smallest thing that satisfies all of it.

### What the cost bound is, and is not

A program's opcode count is its **work** bound: `lower_flat` returns it and
`vm_core` meters against it, one unit per dispatched opcode plus a builtin's
arity. Because the ISA has no backward branch, that count is known before the
program runs, and exceeding it is `CostExceeded` rather than a long step. This is
the property the whole design rests on, and it holds on every target.

It is not a time bound, and the register records it in `vmi` (VM instructions)
rather than microseconds for that reason. The meter's unit is not uniform: a
`CALL` to `replace` over a 4 KiB operand and a `PUSH_BOOL` are three units apart
in the meter and orders of magnitude apart on the wire, and the substring family is
worse than linear — `find` is a naive window walk, so its work is the product of
its two operands where the meter charges one unit plus arity. What keeps those
bounded is the operands themselves: a record is at most `REC_BUF`, so every
builtin is a bounded scan over bounded values, which is enough for termination
and not enough to predict a duration.

Any microsecond figure for a step is therefore a **measured property of one
target**, not an enforced one. `fluxor.toml` builds a single target
(`bcm2712`, Pi 5), so a timing claim here is a claim about that board and
nothing else. Treat a tick budget as something to measure per target, and the
opcode count as the thing the runtime actually guarantees.

In scope:

- **Expressions / transformations** — a CEL subset, type-checked at compile time,
  lowered to flat IR, transcoded on device at load (`ir_stages`).
- **Decisions** — first-hit rule containers. Note these are *outside* the VM
  precisely because the VM cannot branch (see below).
- **Serialization framing** (`ser` / `deser`) for record ↔ wire payload edges.
- **Version tables** — multiple loaded programs, hot-swapped by tag.

Out of scope, permanently:

- **Protocol logic.** Protocols are stateful, multi-round-trip, and
  reply-dependent (SCRAM's client proof, MySQL's scramble response, Kafka's
  membership handshake). They live in compiled per-protocol `.fmod` modules owned
  by the domain project; do not build protocol codecs in the VM.
- **Aggregation.** A declarative monoid spec run by a native engine, not
  per-record bytecode.
- **I/O of any kind.** The VM sees a record and produces a record.

## Why not compile each pipeline to a wasm module instead

Fluxor compiles `.fmod` modules for `wasm32` as a first-class target: the
envelope is the same across targets and only the code payload changes
(`../fluxor/docs/architecture/wasm_platform.md`). So the alternative worth
answering is not "embed an interpreter": an in-process wasm interpreter wants an
allocator or an unsafe FFI surface, and this tree has neither to give it. It is:
**AOT-compile each pipeline to a wasm fmod and delete the VM entirely** —
`vm_core`, `lower_core`, `builtins_core`, the ISA, this document.

Taken seriously, that alternative keeps more than it looks like it should:

- **Termination is still decidable** — not by construction, but a wasm producer
  Chronicle controls can refuse to emit loops as easily as this ISA can refuse to
  encode them.
- **Logic is still data** — a wasm fmod is bytes, content-addressed, delivered
  OTA like anything else.
- **No interpreter** — no dispatch loop per record, and no interpreter footprint
  to defend.

What it does not keep is the reason the VM exists:

- **Per-record version selection.** `version_core` holds up to `MAX_VERSIONS`
  programs in one module instance and picks one *per record* from a tag in the
  request (`VERSION_SELECTOR_FIELD`), so blue/green and canary are a property of a
  single running graph. Swapping an fmod is a graph reconfigure: to serve two
  versions at once you must run two modules and route between them, which moves
  the decision from the record to the topology. This is the load-bearing
  difference, and no amount of wasm tooling recovers it.
- **The slot budget.** Everything deployable shares one 512 KiB slot
  (`SLOT_SIZE`). A program is a param bounded by `PROG_BUF` at 2 KiB, and a
  pipeline holds `MAX_VERSIONS` of them at once. The wasm equivalent is a module
  per pipeline per version, each carrying whatever codec and string code it uses
  rather than calling a builtin table the engine already has. Whether that many
  modules fit the slot is an arithmetic question, and it is the one to answer
  before reopening this.
- **The edit loop.** A logic change is a param write. On wasm it is a build —
  not the full build/sign/flash cycle, since an authoring module could in
  principle host the codegen, but a toolchain where there is now a table write.

**The summary:** AOT to wasm is a real option, and footprint is not what answers
it. What keeps the VM is per-record version selection inside one module instance,
and a slot budget the alternative has not been shown to fit. Those two are what a
proposal to remove the VM has to address; anything else is arguing against the
interpreter nobody is proposing.

## The fence: no branches, no loops, no calls

The ISA has no backward jump, no conditional jump, and no call **into program
code**. Termination is therefore a property of the *instruction set*, not of a
fuel counter, a verifier, or a timeout. Cost is derived per stage at load and
bounded before execution.

One clarification the extension surface (§ below) makes necessary: the `CALL`
opcode is **not a call in this fence's sense**. It transfers control to nothing
a program authored — it applies one *named pure operator* from a pinned,
append-only builtin table (`builtins_core.rs`), with fixed arity, work bounded
by operand length (the same class of bound as the byte-compare the ISA always
had), and errors as values. Likewise `cel.bind`'s `STORE_LOCAL`/`LOAD_LOCAL`
move values through a bounded slot file; they cannot express a jump. The fence
is about control flow, and control flow remains impossible.

**This fence is load-bearing and must not be opened.** The pressure to open it
always arrives disguised as a small, reasonable feature: "just a loop for
line-items", "just a conditional for the null case", "just a helper call". Each
one individually looks harmless; together they convert a
termination-by-construction VM into one that needs a verifier to be safe, and the
verifier is where this class of system goes wrong.

If you find yourself needing a branch or a loop, the answer is never a new
opcode. It is a new artefact kind.

## How capability is added instead: constrained artefact kinds

When a computation does not fit the VM, model it as a **new artefact kind** whose
shape is itself constrained and whose execution is a native engine. This is the
established pattern, used twice already:

| Need | Why the VM can't | The artefact kind |
| --- | --- | --- |
| Branch on rules | no conditional jump | **Decision** — first-hit `[when, outcome]` container, `run_decision` |
| Windowed accumulation over time | unbounded, stateful | **Aggregation** — monoid spec + pane engine |

A new artefact kind must:

1. **Be declaratively bounded.** Its shape must make the expensive property
   (iteration count, memory, time) statically visible — a rule count, a window
   size, a lane cap — not a program to be analysed.
2. **Have a native engine**, host-testable, `no_std`, no-alloc, never-panic.
3. **Be authored, type-checked, and content-addressed** like the others, so it
   inherits determinism and the OTA/hot-reload path.
4. **Be its own graph node when it cannot be a pipeline stage.** A decision is a
   separate node precisely because it branches; do not smuggle control flow into
   the pipeline node to avoid a channel hop.

So a hypothetical "for each line item, compute a fee" is not a loop opcode. It is
a bounded map-over-repeated-field artefact: a declared max element count, a
per-element pure expression the VM already runs, and a native driver that applies
it. The per-element logic stays inside the fence; the iteration is declarative
and bounded outside it.

## The pinned CEL extension surface

The expression language is a CEL subset, so its function library is not
invented here — it is a **pinned subset of CEL's standard library and its
versioned extension libraries** (`cel-go/ext`), adopted per extension and
gated per extension as top-level module features (Fluxor module variants).
The `full` variant (default, emits the unsuffixed `.fmod`)
carries all of them; later variants may compose subsets for constrained
targets. A program calling a builtin its engine build lacks fails closed at
runtime (`BadBuiltin`) — the load-time analogue is `LowerError::BadTag` for
ids outside the pinned table entirely.

| feature | upstream | adopted | result discipline |
| --- | --- | --- | --- |
| `strings` | CEL stdlib + `ext.strings` v3 | `size`, `contains`, `startsWith`, `endsWith`, `indexOf`, `lastIndexOf`, `charAt`, `substring`(1 and 2-arg), `trim`, `reverse`, `lowerAscii`, `upperAscii`, `replace`(3-arg) | predicates/indexes → scalars; `substring`/`trim`/`charAt` → zero-copy subslices; `reverse`/case/`replace` → scratch arena |
| `math` | `ext.math`, integer subset | `greatest`/`least` (2-arg pin), `abs`, `sign`, `bitAnd`, `bitOr`, `bitXor`, `bitShiftLeft`, `bitShiftRight` | scalars |
| `encoders` | `ext.encoders` | `base64.encode`, `base64.decode` (strict: canonical padding or error) | scratch arena |
| `bindings` | `ext.bindings` | `cel.bind(x, init, result)` | compiler + 2 slot opcodes, no runtime table entry |

**Deviations from upstream, pinned:** indices and `size` are BYTE offsets,
not code points — identical to CEL for ASCII, documented beyond it; `trim`
trims ASCII whitespace only (consistent with CEL's own ASCII-scoped case
functions); `reverse` is overloaded by STATIC type — code-point-wise on
`str`, byte-wise on `bytes` — resolved by the compiler, never by sniffing
content; `replace` has no limit overload; `charAt` returns a 1-byte slice.

**Exclusions, each traceable to one principle:** `split`, `join`, `lists`,
`sets`, `two_var_comprehensions`, `optional` — no collection or optional
types in the VM; `matches`, `ext.regex` — no unbounded matching engine in a
work-bounded PIC (pattern extraction is a compiled-module capability, per
the crypto precedent); `format`, `quote` — printf machinery, weak
power-to-weight; `ext.protos` — the codec layer (`pb_core`, `PBFIELD`)
already owns that problem at the right layer.

**Scratch arena:** writing builtins append into a bounded caller-owned arena
(`STAGE_SCRATCH_CAP` per stage/record) and return offset-addressed values,
resolved at serialization. Overflow fails the evaluation closed
(`ScratchOverflow`). The aggregation engine currently passes no arena — its
programs may use every non-writing builtin; a writing builtin there fails
closed rather than differently.

**Governance:** builtin ids are wire contract — append-only, never reorder,
never reuse, exactly like the content-type table. New entries follow "Adding
an opcode" below, plus a row in this table.

## Correctness obligations

The VM is a single implementation with no second implementation to differ
against, so correctness rests on:

- **Type-checking at compile time.** "It compiled" is the type proof; the device
  does not re-check types, it re-derives cost.
- **Never-panic fuzzing.** `tests/harness/tests/aggregation_suites/robustness.rs` drives
  every evaluator with deterministic pseudo-random programs and inputs. Malformed
  input must return a `Result`, never panic — a panic inside a `.fmod` takes the
  module down.
- **Golden conformance vectors.** The corpus suites
  (`tests/harness/tests/pipeline_suites/corpus.rs`,
  `tests/harness/tests/chronicle_cli_suites/corpus.rs`) pin source → bytecode →
  output against checked-in recorded answers. These are the semantic oracle: any change to
  the compiler or the VM that alters an existing vector's output is a breaking
  change to the meaning of already-deployed programs, and must be treated as one.

## Adding an opcode

Rarely legitimate, but not never — arithmetic and comparison operators are
in-fence. The bar:

- It is a **pure, total function** of its operands (no trap, no unbounded work).
  Division's zero case is a structured `DivByZero` error, as in CEL — never a
  trap.
- It **cannot** change control flow.
- It comes with conformance vectors and fuzz coverage in the same change.
- The host evaluator and the `.fmod` share one source (`*_core.rs`, `include!`d)
  so host tests and device execution cannot diverge.
