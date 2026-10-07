# Connectors

## Overview

A connector is a **provider `.fmod` owned by the domain project**, composed into a
Chronicle graph as a node. Chronicle does not implement protocols: an effect in a
pipeline lowers to a binding on a sibling-owned module, pinned from the Fluxor OCI
store and wired into the emitted graph.

| Protocol family | Owner | Modules |
|---|---|---|
| Redis, Postgres, MySQL, MongoDB, Cassandra | **lattice** | `redis_client`, `pg_client`, `mysql_client`, `mongo_client`, `cassandra_client` |
| MQTT, Kafka, AMQP, NATS | **quantum** | `mqtt_client`, `mqtt_sink`, `kafka_client`, `kafka_sink`, `amqp_client`, `amqp_sink`, `nats_client` |
| HTTP, WebSocket, RTP, SIP, SMTP, S3 | **wave** | `http`, `ws_stream`, `smtp`, `s3`, … |

Each provider is proven **standalone in its owning repo** against a real backend
(each repo's live-clients suite), not only through Chronicle — a client that only
worked as a Chronicle endpoint would be the wrong artefact.

## Why not bytecode

Real protocols are **stateful, multi-round-trip, and reply-dependent** — SCRAM's
client proof, MySQL's `caching_sha2` scramble, Kafka's group-membership handshake,
AMQP's channel negotiation. None of that is a stateless codec, so Chronicle
composes genuine protocol modules, and
[`bytecode_policy.md`](../bytecode_policy.md) fixes protocol logic as permanently
out of the VM's scope.

What the VM does own is **framing a message** at a pipeline edge — the `ser`/`rd`
byte programs, see [the byte-codec guide](../guides/wire-codec.md).
A program only ever frames a BODY: the exchange records around it — ids,
methods, targets, statuses — map to record fields and are written by the
pipeline itself, because framing them belongs to the contract rather than the
graph author.

## How an effect binds

A `.uproc` declares what a pipeline needs and never what serves it:

```
resource orders_store required;
...
effect stored = @orders_store.put(normalized);
```

An endpoint literal in a document would pin a deployment into the source, so
the deployment answers separately, one binding per resource, and
`chronicle graph <doc> <pipeline> [target] [bindings]` takes them on device:

```
<resource>,<kind>,<provider>,<version>,<method>,<params>
```

```
orders_store,pg,pg_client,0.1.0,POST,authority=127.0.0.1:5432;user=app;database=orders
feed,mqtt,mqtt_sink,0.1.0,PUBLISH,authority=10.0.0.1:1883;keepalive_s=#60;topic=orders
```

The module name, the version tag it publishes under and the param names are
the provider's; Chronicle transports them and checks the shape. `<method>` is
the exchange method each request asks — an HTTP method token, or `PUBLISH` for
a durable destination — and resolves through the exchange contract's own table,
so an unknown word refuses the binding. A value led by `#` is numeric and is
emitted unquoted, because a provider's `u32` decoder rejects `"60"`. Bindings
are joined by `|`, so `|`, `,`, `;` and `=` cannot appear inside a value. No
binding names a port: every provider takes requests on `request_in` and answers
on `response_out`.

The planner ([`plan_core.rs`](../../modules/common/plan_core.rs)) turns each
binding into a `Connector` — the graph node's `type:`, its store pin
`<silicon>/<provider>:<version>`, its method and its params. Each binding
carries its own version tag because siblings release independently.
`plan_provider_pins` computes the pins, the driver records them in `fluxor.lock`
and composes `fluxor slot-image`, which resolves them from the OCI store and
emits the OTA bundle, and
[`graph_core.rs`](../../modules/common/graph_core.rs) renders the wired graph.

An effect is a request the pipeline node before it asks. That node becomes the
**requester** (`method:` set, `request_out` to the provider's `request_in`,
the provider's `response_out` back to its `response_in`), and each answer
continues the chain from its `result_out`. The provider hangs off its
requester; it is not a link in the record chain. With no compute node free to
carry the effect — the plan starts with it, or it follows a decision or another
effect — the planner places a pipeline node with no stages to carry it. Every
provider answers every request once, so a stage may follow any effect: after a
query it reads the rows, after a publish an empty answer whose status is at
field 253.

An unbound resource is refused as a deployment error; the document is not at
fault for declining to name an endpoint.

The plan suite (`tests/harness/tests/pipeline_suites/plan.rs`) pins the
lowering, and `tools/e2e/graph.sh` asserts both halves — refusal without a
binding, lowering with one — through the real `.fmod`.

## The exchange surface

Every provider in the workspace speaks ONE contract, defined in the Fluxor SDK
at [`modules/sdk/contracts/exchange.rs`][exchange]: an HTTP server and its
application routes, an HTTP or S3 client, a database client, a broker sink, a
pipeline. A requester opens an exchange under a 14-byte id it chooses; the
provider echoes that id on every record of its answer.

```text
record          [kind:u8][flags:u8][id:14] payload           one channel record
request HEAD    [method:u8][target_len:u16][hdr_len:u16][peer_len:u16][resp_credit:u32]
                target | headers | peer | body…
response HEAD   [status:u16][ct_len:u8][hdr_len:u16] content_type | headers | body…
BODY            body bytes, MORE set while more follow
ABORT · CREDIT · DATAGRAM · LINK (response direction, zero id: DOWN / UP)
```

A HEAD carries the first body bytes inline; a body longer than one record
follows in BODY records, past the inline bytes only on the receiver's credit.
Statuses are HTTP codes for every provider: 200 answers (for a durable
destination: accepted, with an empty body), and a provider's own refusals are
400, 413, 500, 502, 503 and 504. `METHOD_PUBLISH` asks a durable destination to
accept the body as one record, its target the ordering key.

A provider declares the delivery terms it offers as a capability:

- `stream.ordered_ack.sink` — a destination that ACCEPTS records (an MQTT
  topic, a Kafka partition, an INSERT): a 200 means the record is durable.
- `stream.ordered_ack.exchange` — a destination that ANSWERS WITH DATA (an
  HTTP GET, a SELECT).

A consumer that only needs its requests answered requires the parent,
`stream.ordered_ack`, and accepts either. The ports are the same for both, so a
pipeline publishing to Kafka and one calling an HTTP endpoint are the same
wiring against a different pin.

Two rules matter to a consumer:

- **Answered exactly once.** An exchange ends at a terminal response record (a
  HEAD or BODY without MORE) or an ABORT, and a provider ends every exchange it
  is asked.
- **LINK reports a change, not a precondition.** A provider whose backend link
  drops writes LINK DOWN and, on reconnect, LINK UP. A requester does not wait
  for UP before its first request, and after an UP re-issues every exchange it
  holds open without an answer.

### Sizes

| Constant | Value |
|---|---|
| `RECORD_MAX` | 8192 — one exchange record, header included |
| `PAYLOAD_MAX` | 8192 — a body collected whole |
| `KEY_MAX` | 512 — an ordering key (a publish's target) |

A provider whose backend cannot accept the full ceiling declares the smaller
number as its `max_payload` capability fact, and the build checks it against
the producer's own `max_payload` fact — never against a port's `max_record`,
which frames a whole record rather than a payload. Exchange edges are mailbox
edges; Fluxor gives every framed edge a buffer group of its own, so a graph
never declares one.

### The pipeline on the surface

`app/pipeline` meets the contract in the role its wired ports choose
([`exchange_core.rs`](../../modules/common/exchange_core.rs)). The parts of an
exchange other than its body — id, method, target, headers, status, content
type — map to data fields named by the `id_field`, `method_field`,
`target_field`, `headers_field`, `status_field` and `content_type_field`
params, so stages read and write them like any other field and a codec only
ever reads or writes a body.

- **Provider** (`request_in` + `response_out`). Each request is collected
  whole — credit granted as it arrives, refused with 413 or 503 past a bound,
  never truncated — and its body becomes a record (through `decode`, or a body
  that is itself a record frame), with the mapped parts added. With `ack`
  (`stream.ordered_ack.sink`, `ack = "transport"`), the request is answered 200
  once its record's output is accepted downstream and 500 when the record
  fails. Without it, a node downstream answers the request by its id. That is
  what lets an application pipeline be the destination of a CDC feed directly,
  acknowledging what it actually consumed. Either way a request is never left
  waiting: one whose record fails is answered 500 here, and one whose stages
  set the mapped status field is answered here with that status — a stage
  refuses by saying how — its body through `encode` or empty.
- **Answer** (`response_out` with `answer`). The result record is the response
  to the exchange its id field names: status and content type from the mapped
  fields (or 200 and the `content_type` param), body through `encode` or the
  record frame itself.
- **Requester** (`request_out` + `response_in`). The result record is a request
  this node opens under an id of its own, asking `method` unless a mapped field
  names one. Its **carry** (field 254, [the frame](dataplane.md#the-typed-record-frame))
  stays in the node — a carry past `KEY_BUF` (512 B, 64 B on rp2040) is refused
  (`carry_refused`), never truncated. Each answer becomes a record on
  `result_out`: the body through `reply_decode` (without one, an empty body or
  one record frame), the carry restored at 254 and the status at 253. A 2xx
  body is the answer and must decode; any other status's body is the
  provider's own, and when it does not decode the record carries the status
  and the carry alone, so the stages after still answer at once. At most
  `WINDOW` exchanges are open at once, and a full window is backpressure —
  nothing is admitted until an answer frees a slot. Exchanges open at a LINK
  DOWN are re-sent after the LINK UP (`replayed`), and a request made while the
  link is down is held in its slot and sent then too, since a provider drops
  what arrives before its UP. The node holds nothing of
  the request but its carry, so request → effect → decoded answer is one node,
  which is the Pipeline artefact's effect step made concrete.
- **Relay** (all four exchange ports). A request HEAD becomes a record, its
  stages rewrite it, and it leaves under the same id with its flags and inline
  body; every other record of the exchange is forwarded untouched, requests
  one way and the provider's answers the other. A body of any length streams
  through, and the node holds no exchange state beyond the HEAD it is
  rewriting. A request it cannot pass on is answered 500 (400 for a HEAD that
  does not parse); one whose stages set the mapped status field is answered
  with that status — a stage refuses by saying how — its body through `encode`
  or empty. Every exchange the relay is asked is answered, so its requester is
  never left waiting.
- **Response relay** (`response_in` + `response_out` only). Another node's
  answers pass through, so several nodes can answer one requester through its
  single `response_in`.

`examples/exchange_carry/`, `examples/pipeline_egress/`,
`examples/http_exchange/` and `examples/pipeline_chain/` are the graphs;
`tools/e2e/exchange-carry.sh`, `tools/e2e/pipeline-egress.sh`,
`tools/e2e/http-exchange.sh` and `tools/e2e/pipeline-chain.sh` prove the
requester, publish, HTTP and provider roles live, with an oversized carry
refused.

## Port vocabulary

| Port | Direction | Meaning |
|---|---|---|
| `request_in` → `response_out` | in/out | a provider: takes requests, answers each once |
| `request_out` → `response_in` | out/in | a requester: asks, takes the answers |
| `net_in` / `net_out` | both | transport, to the platform's network provider |
| `message_out` | out | a subscribed stream |
| `status_out` | out | lifecycle, human-readable (`TextPlain`) |

`request_*` ports carry `ExchangeRequest` records and `response_*` ports
`ExchangeResponse` records. `status_out` is a human-readable line for an
operator, never an answer.

[exchange]: ../../../fluxor/modules/sdk/contracts/exchange.rs

## Related documentation

- [../guides/wire-codec.md](../guides/wire-codec.md) — the `ser`/`rd` framing VMs
- [../bytecode_policy.md](../bytecode_policy.md) — why protocols are out of VM scope
- [dataplane.md](dataplane.md) — the on-device modules and their params
- [model.md](model.md) — the artefact model and the shared cores
