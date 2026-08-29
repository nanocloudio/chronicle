# Connectors

## Overview

A connector is a **provider `.fmod` owned by the domain project**, composed into a
Chronicle graph as a node. Chronicle does not implement protocols: an effect in a
pipeline lowers to a binding on a sibling-owned module, pinned from the Fluxor OCI
store and wired into the emitted graph.

| Protocol family | Owner | Modules |
|---|---|---|
| Redis, Postgres, MySQL, MongoDB, Cassandra | **lattice** | `redis_client`, `pg_client`, `mysql_client`, `mongo_client`, `cassandra_client` |
| MQTT, Kafka, AMQP, NATS | **quantum** | `mqtt_client`, `kafka_client`, `amqp_client`, `nats_client` |
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
`examples/identity_provider/` uses `encode` to render the HTTP requests kagi's
modules consume. `examples/pipeline_egress/` needs no such program: publishing
on the ordered-ack surface renders no frame, because framing there belongs to
the contract rather than the graph author.

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
<resource>,<kind>,<provider>,<version>,<in_port>,<out_port>,<r|n>,<params>
```

```
orders_store,pg,pg_client,0.1.0,request_in,reply_out,r,endpoint=7f000001;user=app
feed,kafka,kafka_sink,0.1.0,publish_in,ack_out,n,broker_ip=#167772161;topic=orders
```

Every field is the provider's: the module name, the version tag it publishes
under, the port pair, and the param names. Chronicle transports them and
checks the shape. `r`/`n` says whether the provider answers with data a next
stage can read; a value led by `#` is numeric and is emitted unquoted, because
a provider's `u32` decoder rejects `"167772161"`. Bindings are joined by `|`,
so `|`, `,`, `;` and `=` cannot appear inside a value.

The planner ([`plan_core.rs`](../../modules/common/plan_core.rs)) turns each
binding into a `Connector` — the graph node's `type:`, its store pin
`<silicon>/<provider>:<version>`, its port pair and its params. Each binding
carries its own version tag because siblings release independently.
`plan_provider_pins` resolves the pins against the fluxor OCI store, the driver
composes `fluxor slot-image` to emit the OTA bundle, and
[`graph_core.rs`](../../modules/common/graph_core.rs) renders the wired graph.

Two refusals are deliberate. An unbound resource is a deployment error and is
reported as one; the document is not at fault for declining to name an
endpoint. And a stage after a non-replying effect (`n`) is refused
(`GraphError::EffectNotChainable`): that provider's output port carries an
acknowledgement or a status line, not a record, so wiring it into the next
node's `record_in` would build a graph that runs and feeds a record parser
bytes that are not a record.

The plan suite (`tests/harness/tests/pipeline_suites/plan.rs`) pins the
mapping, and `tools/e2e/graph.sh` asserts both halves — refusal without a
binding, lowering with one — through the real `.fmod`.

## The exchange surface

Providers of the ordered-ack surface share ONE contract, defined in the Fluxor
SDK at [`modules/sdk/contracts/exchange.rs`][exchange]. A provider declares
which role it plays:

- `stream.ordered_ack.sink` — a destination that only ACCEPTS records (an MQTT
  topic, a Kafka partition, an INSERT): `publish_in` + `ack_out`.
- `stream.ordered_ack.exchange` — a destination that ANSWERS WITH DATA (an
  HTTP GET, a SELECT): additionally `reply_out`.

A consumer that only publishes requires the parent, `stream.ordered_ack`, and
accepts either.

```text
publish_in (input):  [corr:u64][flags:u8][msg_key_len:u16][msg_key…]
                     [payload_len:u16][payload…]
ack_out    (output): [corr:u64][status:u8]
reply_out  (output): [corr:u64][status:u8][msg_key_len:u16][msg_key…]
                     [payload_len:u16][payload…]      — exchange only
```

Each frame travels behind a 3-byte envelope, `[tag:u8][len:u16 LE]`
(`MSG_PUBLISH`, `MSG_ACK`, `MSG_REPLY`). The frames and the correlation are
identical across roles; only the presence of an answer differs. That is what
makes a POST and a GET the same act, one capability apart — and why a pipeline
stage publishing to Kafka and one calling an HTTP endpoint are the same wiring
against a different pin.

Two rules matter to a consumer:

- **Answered exactly once.** A replying provider answers on `reply_out` (the
  reply carries the status, so it IS the ack) and `ack_out` then carries only
  the `corr = 0` link-state signals. A non-replying provider answers on
  `ack_out`. Never both.
- **The reply echoes `msg_key` unchanged.** A producer stage puts its join
  context in the key and the stage handling the reply gets it back beside the
  payload — so a request built in one stage and a response handled in the next
  share no state but a graph edge.

### Sizes

The contract fixes one payload ceiling, so a producer has a number it can hold
itself to:

| Constant | Value |
|---|---|
| `PAYLOAD_MAX` | 8192 |
| `KEY_MAX` | 512 |
| `PUBLISH_FRAME_MAX` | 8717 — `PUBLISH_OVERHEAD + KEY_MAX + PAYLOAD_MAX`, what a `publish_in` port takes as one record |

A provider whose backend cannot accept the full ceiling declares the smaller
number as its `max_payload` capability fact, and the build checks it against
the producer's own `max_payload` fact — never against a port's `max_record`,
which frames a whole record rather than a payload. The rows are registered in
Fluxor's `docs/architecture/limit_register.md`.

### The pipeline on the surface

`app/pipeline` speaks the surface in both directions.

As a **producer**, it publishes when a graph wires `publish_out` to a
provider's `publish_in` and `ack_out` back to `ack_in`; wiring neither leaves
results on `result_out`. Each result frame is wrapped in a `Publish` with a
fresh correlation id and no key; at most `MAX_INFLIGHT` publishes are
unacknowledged at once, and a full window is backpressure — nothing is admitted
until the destination answers. Typed refusals are counted. The pipeline is an
at-most-once producer: it keeps no replay log, so publishes outstanding at a
LINK_DOWN are counted as `invalidated`, not re-sent.

As a **sink** (`stream.ordered_ack.sink`, `ack = "transport"`), it takes each
publish's payload as its record when a producer is wired to `publish_in`, and
answers on `ack_out` once that record's output is accepted downstream — OK, or
OVERSIZE for a payload beyond the contract ceiling, or UNROUTABLE when the
payload cannot be processed. The intake takes any contract payload
(`PAYLOAD_MAX`); the decoded record is one typed frame. LINK_UP is announced once the module is ready to take
records. That is what lets an application pipeline be the destination of a
CDC feed directly, acknowledging what it actually consumed.

`examples/pipeline_egress/`, `examples/http_exchange/` and
`examples/pipeline_chain/` are the graphs; `tools/e2e/pipeline-egress.sh`,
`tools/e2e/http-exchange.sh` and `tools/e2e/pipeline-chain.sh` prove the
producer, exchange and sink roles live.

## Port vocabulary

Request/reply providers (`pg_client`, `redis_client`) and status-reporting
sinks (`mongo_client`) use these names:

| Port | Direction | Meaning |
|---|---|---|
| `net_in` / `net_out` | both | transport, to the platform's network provider |
| `request_in` → `reply_out` | in/out | request/reply protocols |
| `publish_in` | in | payload sink (fire-and-forget) |
| `message_out` | out | a subscribed stream |
| `status_out` | out | lifecycle/result, human-readable (`TextPlain`) |

`status_out` is a human-readable line, not a record, which is why a binding on
it is `n`.

[exchange]: ../../../fluxor/modules/sdk/contracts/exchange.rs

## Related documentation

- [../guides/wire-codec.md](../guides/wire-codec.md) — the `ser`/`rd` framing VMs
- [../bytecode_policy.md](../bytecode_policy.md) — why protocols are out of VM scope
- [dataplane.md](dataplane.md) — the on-device modules and their params
- [model.md](model.md) — the artefact model and the shared cores
