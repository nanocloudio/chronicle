# The `.uproc` authoring DSL

A single human-authored `.uproc` document describes a processing **module** — its
type environment plus the logic and orchestration artefacts — and compiles
**deterministically** to the sealed canonical artefacts + a `Module`.

It is a thin front end, by design. The document parser
([`uproc_core.rs`](../../modules/common/uproc_core.rs)) recognizes only the
*structure*; every expression body is captured as a verbatim source span and
handed unchanged to the CEL compiler
([`celc_core.rs`](../../modules/common/celc_core.rs)). The DSL therefore inherits
the exact CEL-subset grammar, type-checker, lowerer, and content-digest identity
of the engine — a module authored in text seals to digests pinned by the golden
corpus (`module_identity_matches_the_prost_corpus`).

## Example

See [`examples/authoring/process_order.uproc`](../../examples/authoring/process_order.uproc)
— `commerce.process_order`, authored end to end.

```
module commerce.process_order {
  message commerce.Order { id: string = 1; total: commerce.Money = 3; }
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
document      := 'module' QNAME '{' decl* '}'
decl          := message | enumconst | expression | transformation
               | decision | resource | pipeline | entry | provenance
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
provenance    := 'provenance' 'revision' STRING 'toolchain' STRING ';'
type          := 'int'|'uint'|'double'|'bool'|'string'|'bytes' | QNAME
EXPR          := <the CEL subset of celc_core, captured verbatim>
```

`//` line comments are allowed anywhere. Rule priorities are assigned by
declaration order (earlier binds tighter), matching the `first` hit policy.

## Guarantees

- **Deterministic identity** — identical source ⇒ identical module + artefact
  digests (`sealing_is_deterministic`, `module_identity_matches_the_prost_corpus`).
- **Fidelity** — parsing and sealing are pinned against the golden corpus, so a
  parser or compiler change that alters a document's artefacts is a visible,
  breaking change (`document_parsing_matches_the_corpus`, `rejection_matches_the_corpus`).
- **Structured errors** — parse errors carry a 1-based line/column; reference and
  type errors are typed `UprocError` values, never panics.

## Aggregation

The Aggregation artefact (deterministic event-time state) is authored via an
`aggregation { … }` block that lowers to a sealed Aggregation artefact
(`artefact_core.rs::seal_aggregation`).
See [`examples/authoring/customer_totals.uproc`](../../examples/authoring/customer_totals.uproc).

```
aggregation customer_totals(order: commerce.Order) -> commerce.CustomerTotal {
  key order.customer_id;
  event_time order.created_at;
  window tumbling 100;          // or: window sliding <size> <step>;
  lateness 2000;                // watermark lateness allowance (ms)
  guard 200;                    // watermark guard floor (ms)
  lanes 64;                     // bounded cardinality (0 = unbounded)
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
`topk`/`quantile` `(<n>, <selector>)`.

## Connector wire edges — NOT a DSL block

A dataplane's wire edges — the request built from a record, the reply parsed back
into one — are [wire-codec templates](wire-codec.md) compiled to `ser` / `rd`
bytecode and supplied as a generic `pipeline` module's `encode` / `decode`
params.

There is no `connector { … }` block in the DSL — writing one is a parse error.
Wire edges are pipeline params rather than sealed canonical artefacts, which is
also why they never appear in a Module's artefact refs.

Protocol I/O itself is not a Chronicle concern at all: a connector is a
sibling-owned provider module composed as a graph node and resolved from the
fluxor OCI store by pin — see [connectors.md](../architecture/connectors.md) and
`plan_core`'s `Connector`, which maps an effect's capability to the provider that
realizes it.

## Authoring on device

The whole chain runs inside fluxor. `chronicle_cli` is a `.fmod`, so a node needs
no cargo, no crates and no Linux build host to take a document to a runnable
graph — which is the point of the DSL existing at all:

| command | what it does |
| --- | --- |
| `chronicle parse <uproc_hex>` | parse a document and summarise its declarations |
| `chronicle author <uproc_hex>` | compile and SEAL every artefact, printing `<name> <digest>` |
| `chronicle graph <uproc_hex> <pipeline> [target]` | lower a pipeline to bootable graph YAML |
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
`graph` reports it instead of inventing a binding that would produce a graph
pointing at nothing. And `compile-stages` rejects a stage that does not construct
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
Decision, Aggregation, Pipeline — plus the Module wrapper. Schema artefacts
(descriptor closures) are supplied out-of-band, referenced by the message closure
declared inline. Connector wire edges are pipeline PARAMS, not a DSL block (see
above).

## Related Documentation

- [../architecture/model.md](../architecture/model.md) — the artefact model and identity
- [../architecture/dataplane.md](../architecture/dataplane.md) — how authored artefacts run on device
- [wire-codec.md](wire-codec.md) — the connector encoder/decoder templates
