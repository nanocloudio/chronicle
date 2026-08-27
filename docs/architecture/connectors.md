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
`examples/mqtt_sink/` uses `encode` to render the publish frame that quantum's
`mqtt_client` consumes on `app_in`.

## How an effect binds

The planner ([`plan_core.rs`](../../modules/common/plan_core.rs)) turns each
effect into a `Connector` naming the provider module, its version, and the port
pair the graph wires:

| Effect kind | Provider | Ports |
|---|---|---|
| Redis / Postgres | `redis_client` / `pg_client` | `request_in` → `reply_out` |
| Kafka / Mongo | `kafka_client` / `mongo_client` | `publish_in` → `status_out` |

`plan_provider_pins` resolves each binding against the fluxor OCI store, and the
driver composes `fluxor slot-image` to emit the OTA bundle;
[`graph_core.rs`](../../modules/common/graph_core.rs) renders the wired graph.
The plan suite (`tests/harness/tests/pipeline_suites/plan.rs`) pins the mapping,
and `tools/e2e/graph.sh` boots a device-authored graph end to end.

## Port vocabulary

Providers share one vocabulary so a graph reads the same across domains:

| Port | Direction | Meaning |
|---|---|---|
| `net_in` / `net_out` | both | transport, to the platform's network provider |
| `request_in` → `reply_out` | in/out | request/reply protocols |
| `publish_in` | in | payload sink (fire-and-forget) |
| `message_out` | out | a subscribed stream |
| `status_out` | out | lifecycle/result, human-readable (`TextPlain`) |

## Related documentation

- [../guides/wire-codec.md](../guides/wire-codec.md) — the `ser`/`rd` framing VMs
- [../bytecode_policy.md](../bytecode_policy.md) — why protocols are out of VM scope
- [dataplane.md](dataplane.md) — the on-device modules and their params
- [model.md](model.md) — the artefact model and the shared cores
