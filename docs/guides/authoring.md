# Writing a plan

A **plan** is the declarative definition of a process: a single human-authored
`.plan` document holding its type environment plus the logic and orchestration
artefacts. It compiles **deterministically** to the sealed canonical artefacts
plus the `Plan` that bundles them.

It is a thin front end, by design. The document parser
([`plan_core.rs`](../../modules/common/plan_core.rs)) recognizes only the
*structure*; every expression body is captured as a verbatim source span and
handed unchanged to the CEL compiler
([`celc_core.rs`](../../modules/common/celc_core.rs)). The DSL therefore inherits
the exact CEL-subset grammar, type-checker, lowerer, and content-digest identity
of the engine — a plan authored in text seals to digests pinned by the golden
corpus (`plan_identity_matches_the_prost_corpus`).

## Example

See [`examples/authoring/process_order.plan`](../../examples/authoring/process_order.plan)
— `commerce.process_order`, authored end to end. Abridged here: the full file also
declares `commerce.Money`, `commerce.NormalizedOrder` and `commerce.Route`.

```
plan commerce.process_order {
  message commerce.Order { id: string = 1; total: commerce.Money = 3; }
  enum AUTOMATIC = 0;
  enum MANUAL_REVIEW = 1;

  expression is_large(order: commerce.Order) -> bool {
    order.total.units >= 1000
  }
  transformation normalize(order: commerce.Order) -> commerce.NormalizedOrder {
    commerce.NormalizedOrder { id: order.id, total: order.total.units }
  }
  decision route(order: commerce.Order) -> commerce.Route {
    when order.total.units >= 1000 -> commerce.Route { kind: MANUAL_REVIEW, order_id: order.id };
    default -> commerce.Route { kind: AUTOMATIC, order_id: order.id };
  }

  resource orders_store required;

  pipeline process(order: commerce.Order) -> commerce.NormalizedOrder {
    call normalized = normalize(order);
    call routed = route(order);
    effect stored = @orders_store.put(normalized);
    commit after stored;
    return normalized;
  }
  entry process = process;
}
```

## Grammar (EBNF sketch)

```
document      := 'plan' QNAME '{' decl* '}'
decl          := message | enumconst | expression | transformation
               | decision | aggregation | map | resource | pipeline | entry
               | provenance
message       := 'message' QNAME '{' field* '}'
field         := IDENT ':' type '=' INT ';'
enumconst     := 'enum' IDENT '=' INT ';'
expression    := 'expression' IDENT '(' IDENT ':' type ')' '->' type '{' EXPR '}'
transformation:= 'transformation' IDENT '(' IDENT ':' type ')' '->' type '{' EXPR '}'
decision      := 'decision' IDENT '(' IDENT ':' type ')' '->' type '{' rule* '}'
rule          := 'when' EXPR '->' EXPR ';' | 'default' '->' EXPR ';'
resource      := 'resource' IDENT ['required'] ';'
pipeline      := 'pipeline' IDENT '(' IDENT ':' type ')' '->' type '{' stage* '}'
stage         := 'call' IDENT '=' QNAME '(' [IDENT (',' IDENT)*] ')' ';'
               | 'effect' IDENT '=' '@' IDENT ['.' IDENT] '(' IDENT ')' ';'
               | 'commit' 'after' IDENT ';' | 'return' IDENT ';'
entry         := 'entry' IDENT '=' IDENT ';'
aggregation   := 'aggregation' IDENT '(' IDENT ':' type ')' '->' type '{' aclause* '}'
aclause       := 'key' EXPR ';' | 'event_time' EXPR ';'
               | 'window' ('tumbling' INT | 'sliding' INT INT) ';'
               | 'lateness' INT ';' | 'guard' INT ';' | 'lanes' INT ';'
               | 'operator' IDENT '=' opkind ';' | 'emit' EXPR ';'
opkind        := 'count' | ('sum'|'avg'|'min'|'max'|'distinct') '(' EXPR ')'
               | ('topk'|'quantile') '(' INT ',' EXPR ')'
map           := 'map' IDENT '(' IDENT ':' type ')' '->' type '{' mclause* '}'
mclause       := 'over' IDENT '.' IDENT 'as' IDENT ':' type 'max' INT ';'
               | 'count' IDENT '.' IDENT ',' IDENT '.' IDENT ',' IDENT '.' IDENT ';'
               | 'when' EXPR ';'
provenance    := 'provenance' 'revision' STRING 'toolchain' STRING ';'
type          := 'int'|'uint'|'bool'|'string'|'bytes' | QNAME
EXPR          := <the CEL subset of celc_core, captured verbatim>
```

`//` line comments are allowed anywhere. Rule priorities are assigned by
declaration order (earlier binds tighter), matching the `first` hit policy.

## Absence, faults and `&&` / `||`

An expression meets two kinds of trouble at run time, and `&&` / `||` resolve
both the way CEL does — commutatively, whichever side it is on:

- **Absence.** A field the record does not carry is *unknown*, and so is
  anything computed from it (a comparison, arithmetic, a builtin). `has(x)`
  asks about presence without being undone by it.
- **Faults.** Integer division or remainder by zero, a result outside `int`
  (overflow), a value of the wrong type, selecting into something that is not
  a message.

`false && e` is `false` and `true || e` is `true` whatever `e` is — unknown or
faulted — and so are `e && false` and `e || true`. So a guard protects what it
guards, in either order:

```
b.rate > 0 && b.used / b.rate > 10      // rate 0: false, not a division fault
b.rate == 0 || b.used / b.rate < 5      // rate 0: true
```

When the other side does not decide, the result is unknown if either side is
unknown (unknowns take precedence over faults), otherwise the fault — the left
operand's when both fault. A decision whose `when` ends unknown or faulted decides
nothing: it fails closed — no later rule and no default fires — with its
reason (an absent field, or the fault). A fault anywhere else — under an arithmetic or comparison
operator with no `&&` / `||` above it, in a constructed field, in a `cel.bind`
value — is raised where it happens, unchanged.

Both operands are evaluated: there is no branching in the VM, so a guarded side
still costs its instructions (within the program's derived cost bound) — it
just cannot fail the expression. The compiled program is the same as before;
the VM carries a fault on the stack slot it would have filled until an `&&` /
`||` absorbs it or it reaches the result.

## Strings a decision builds

A string a program reads — a field, a literal, a `substring`, a `trim` — is
borrowed where it lies. A string it *builds* is written into the stage's
scratch arena: `+`, `lowerAscii` / `upperAscii`, `replace`, `reverse`,
`base64`, and every nested message a construction packs (`ctx: Ctx{ … }`
becomes one frame field, so it copies what it packs). The arena is
`STAGE_SCRATCH_CAP`: 12288 B on a 64-bit core — three full-tier records — and
512 B on a 32-bit one, where it shares a 2 KiB stack. It is the same arena in
the `decision` node, in a pipeline's decision stage, and in `chronicle decide`,
so a dry run and the node agree on what fits.

What a run spends:

- **A chain of `+` costs its result once.** `"{\"items\":[" + doc + "]}"`
  writes the opening, the document and the close once each: every `+` whose
  left side is the string the previous `+` built extends it in place. A `+`
  whose left side is something else (a field, a literal, an older built
  string) copies that side.
- **A predicate's strings are handed back** once it has answered. Every arm's
  `when` up to the one that matches is evaluated, and none of what they built
  stays under the outcome, so concatenating in many conditions costs only the
  largest of them.
- **A nested message copies its fields once more.** A kilobyte document inside
  a context message costs about two kilobytes; inside a context inside a
  context, three.

So an outcome the size of a record — 4096 B at the full tier, the most a
`decision` node emits — fits with two levels of nesting around its strings.

Running out is a named fault, never a clipped string:

- **On a record**, building past the arena fails the run closed. The `decision`
  node counts it in `inputs_failed` and logs `[decision] failed closed at rule
  <i>: scratch arena overflow: the strings this branch builds pass
  STAGE_SCRATCH_CAP`; `chronicle decide <decision_hex> <record_hex>` prints
  `error: decision failed at rule <i>: scratch arena overflow: …` for the same
  record, and `chronicle eval` names a compute stage's overflow the same way.
  An outcome that builds within the arena but encodes past one record is a
  different bound, named as such: `error: outcome frame larger than a decision
  node's record (REC_BUF 4096 B)` (the node counts it in `errors_encode`).
- **Before any record**, a branch whose *literals alone* pass the arena —
  whatever the record holds, with every field taken as empty — can never
  complete, so it is refused where it is written. `chronicle author` and
  `chronicle graph` print `error: <decision>: rule <i> outcome builds at least
  <N> B of strings on any record; a stage's scratch arena (STAGE_SCRATCH_CAP)
  holds 12288 B` (`a branch` from `graph`, `program <k>` from `decision`), and
  an engine handed such a container refuses it at load: `[decision] FAULT: a
  branch builds more strings than STAGE_SCRATCH_CAP on any record`. This is a
  floor, not an estimate of the record-dependent part: what a branch builds
  from its fields is bounded by the record, and only a run can measure it.

## Guarantees

- **Deterministic identity** — identical source ⇒ identical plan + artefact
  digests (`sealing_is_deterministic`, `plan_identity_matches_the_prost_corpus`).
- **Fidelity** — parsing and sealing are pinned against the golden corpus, so a
  parser or compiler change that alters a document's artefacts is a visible,
  breaking change (`document_parsing_matches_the_corpus`, `rejection_matches_the_corpus`).
- **Structured errors** — parse errors carry a 1-based line/column; reference and
  type errors are typed `PlanError` values, never panics. A body that does not
  compile is named with the compiler's reason and its place in the document —
  `error: compile failed for label (the body): unknown field 'nope' at line 12
  column 20` from `author`, `error: decision 'route' did not compile (rule 1
  when): …` from `graph` — and `compile`/`compile-source`/`compile-stages` say
  the same of a bare source (`CelcFault`: every refusal carries the byte it
  points at; `every_compile_refusal_says_where`,
  `author_names_the_reason_and_the_place_a_body_did_not_compile`,
  `graph_names_the_reason_and_the_place_a_stage_did_not_compile`).

## Aggregation

The Aggregation artefact (deterministic event-time state) is authored via an
`aggregation { … }` block that lowers to a sealed Aggregation artefact
(`artefact_core.rs::seal_aggregation`).
See [`examples/authoring/customer_totals.plan`](../../examples/authoring/customer_totals.plan).

```
aggregation customer_totals(order: commerce.Order) -> commerce.CustomerTotal {
  key order.customer_id;
  event_time order.created_at;
  window tumbling 100;          // or: window sliding <size> <step>;
  lateness 2000;                // watermark lateness allowance (ms)
  guard 200;                    // watermark guard floor (ms)
  lanes 16;                     // lane ceiling, capped at the engine's MAX_LANES
                                // (16; 4 on rp2040); a key past it is counted
                                // in lane_overflows
  operator order_count = count;
  operator gross_total = sum(order.total.units);
  // operator p90 = quantile(900, order.total.units);   // topk(k, …) likewise
  emit commerce.CustomerTotal { customer_id: ctx.key,
    order_count: ctx.state.order_count, gross_total: ctx.state.gross_total,
    window_start: ctx.window.start, window_end: ctx.window.end };
}
```

`key`, `event_time`, and operator selectors are checked over the input parameter.
The `emit` construction is checked over a synthesized **emit context** `ctx`:
`ctx.key` (the partition key), `ctx.state.<operator>` (each operator's output, in
declaration order), and `ctx.window.start` / `ctx.window.end`. Operator kinds:
`count` (no selector), `sum`/`avg`/`min`/`max`/`distinct` `(<selector>)`, and
`topk`/`quantile` `(<n>, <selector>)`: `topk`'s `n` is the number of largest
values summed, 1..=65535; `quantile`'s is a permille, 0..=1000. An aggregation
takes at most one of `distinct`, `topk` and `quantile`: each window pane holds
a single collection cell.

## Connector wire edges — NOT a DSL block

A dataplane's wire edges — the body of a request built from a record, the
body of an answer parsed back into one — are [`ser` / `rd` byte
programs](wire-codec.md) supplied as a generic `pipeline` module's `encode` /
`decode` / `reply_decode` params. The rest of an exchange — its id, method,
target, status and content type — maps to record fields by the pipeline's field
map, so no program frames an exchange record.

There is no `connector { … }` block in the DSL — writing one is a parse error.
Wire edges are pipeline params rather than sealed canonical artefacts, which is
also why they never appear in a Plan's artefact refs.

Protocol I/O itself is not a Chronicle concern at all: a connector is a
sibling-owned provider module composed as a graph node and resolved from the
fluxor OCI store by pin — see [connectors.md](../architecture/connectors.md) and
`graph_lower_core`'s `Connector`, which maps an effect's capability to the provider that
realizes it.

## Authoring on device

The whole chain runs inside fluxor. `chronicle_cli` is a `.fmod`, so a node needs
no cargo, no crates and no Linux build host to take a document to a runnable
graph — which is the point of the DSL existing at all:

| command | what it does |
| --- | --- |
| `chronicle parse <plan_hex>` | parse a document and summarise its declarations |
| `chronicle author <plan_hex>` | compile and SEAL every artefact, printing `<name> <digest>` |
| `chronicle graph <plan_hex> <pipeline> [target] [bindings]` | lower a pipeline to bootable graph YAML, binding each effect's resource from `[bindings]` (see [connectors.md](../architecture/connectors.md)) |
| `chronicle compile-source <schema> <param> <type> <source>` | type-check source handed over at runtime → `ir` hex |
| `chronicle compile-stages <schema> <param> <type> <src>…` | the same, as a chain → `ir_stages` hex |
| `chronicle release <default_tag> <tag>:<prog_hex>…` | build the multi-version `versions` param |
| `chronicle release-ctl add\|default\|remove <tag> [hex]` | hot-reload control message for a live instance |
| `chronicle slot-verify <image_digest_hex> [abi_hex]` | stream-check a stored OTA slot image |

`graph` defaults to the EMBEDDED profile — a node authoring a graph is normally
authoring it for a node, and host `cli` bracketing would make the result
un-embeddable. Pass `linux` for the host profile.

Two things it deliberately refuses rather than guesses. An `effect` stage names a
resource whose endpoint and credentials appear nowhere in the document, so
without a binding for it `graph` reports it instead of inventing one that would
produce a graph pointing at nothing. And `compile-stages` rejects a stage that does not construct
a message: the executor hands one stage's output frame to the next, so a scalar
has nothing to pass on — it would pack into a well-formed container and fail
inside the VM at runtime.

`compile-source` is the DYNAMIC path — "POST source, no build step" — as opposed
to compiling a document. A node holds a schema and type-checks source it has
never seen, emitting the same shippable checked IR the modules lower at load. The
target then proves it can run the result by lowering it.

Driven end to end by [`tools/e2e/graph.sh`](../../tools/e2e/graph.sh),
[`tools/e2e/dynamic-compile.sh`](../../tools/e2e/dynamic-compile.sh),
[`tools/e2e/release.sh`](../../tools/e2e/release.sh) and
[`tools/e2e/slot-verify.sh`](../../tools/e2e/slot-verify.sh).

## Scope

The DSL covers the artefact kinds with a builder — Expression, Transformation,
Decision, Aggregation, Pipeline (including its map stages) — plus the Plan
wrapper. Schema artefacts
(descriptor closures) are supplied out-of-band, referenced by the message closure
declared inline. Connector wire edges are pipeline PARAMS, not a DSL block (see
above).

## Related Documentation

- [../architecture/model.md](../architecture/model.md) — the artefact model and identity
- [../architecture/dataplane.md](../architecture/dataplane.md) — how authored artefacts run on device
- [wire-codec.md](wire-codec.md) — the `ser`/`rd` byte-codec programs
