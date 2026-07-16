# Bytecode growth policy

Chronicle carries a custom bytecode VM. This document fixes what it is for, what
it may never grow into, and how new capability is added instead. It exists
because the VM's value rests entirely on a property that is easy to destroy by
increments: **every program terminates, by construction**.

## What the VM is for

Exactly one job: **per-record pure compute**, shipped as data.

The requirement is logic-as-data — pipelines are loaded at runtime, delivered
OTA into a 512 KB slot, and hot-reloaded by version — under constraints that
rule out the usual answers: `no_std`, no allocator, `forbid(unsafe_code)`, and a
bounded slice of a 100 µs scheduler tick. A WASM interpreter needs an allocator
(wasmi) or unsafe FFI (wasm3) and costs 10–100× the footprint; native per-pipeline
codegen turns every logic edit into a build/sign/flash cycle. A small VM whose
ISA cannot express non-termination is the smallest thing that satisfies all of it.

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
  by the domain project. Chronicle retired its entire bytecode-codec connector
  path for this reason; do not rebuild it.
- **Aggregation.** A declarative monoid spec run by a native engine, not
  per-record bytecode.
- **I/O of any kind.** The VM sees a record and produces a record.

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
gated per extension as top-level module features (fluxor RFC
module_variants). The `full` variant (default, emits the unsuffixed `.fmod`)
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
WCET-bounded PIC (pattern extraction is a compiled-module capability, per
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
- **Never-panic fuzzing.** `modules/app/aggregation/tests/robustness.rs` drives
  every evaluator with deterministic pseudo-random programs and inputs. Malformed
  input must return a `Result`, never panic — a panic inside a `.fmod` takes the
  module down.
- **Golden conformance vectors.** `modules/app/pipeline/tests/registry.rs`
  pins source → bytecode → output. These are the semantic oracle: any change to
  the compiler or the VM that alters an existing vector's output is a breaking
  change to the meaning of already-deployed programs, and must be treated as one.

## Adding an opcode

Rarely legitimate, but not never — arithmetic and comparison operators are
in-fence. The bar:

- It is a **pure, total function** of its operands (no trap, no unbounded work).
  Division must define its zero case as a value, not a fault.
- It **cannot** change control flow.
- It comes with conformance vectors and fuzz coverage in the same change.
- The host evaluator and the `.fmod` share one source (`*_core.rs`, `include!`d)
  so host tests and device execution cannot diverge.
