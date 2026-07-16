# Connectors

## Overview

A connector is a **provider `.fmod` owned by the domain project**, composed into a
Chronicle graph as a node. Chronicle does not implement protocols: an effect in a
pipeline lowers to a binding on a sibling-owned module, pinned from the Fluxor OCI
store and wired into the emitted graph.

| Protocol family | Owner | Modules |
|---|---|---|
| Redis, Postgres, MySQL, MongoDB, Cassandra | **lattice** | `redis_client`, `pg_client`, … |
| MQTT, Kafka, AMQP, NATS | **quantum** | `mqtt_client`, `kafka_client`, … |
| HTTP/1.1, HTTP/2, WebSocket, RTP, SIP, SMTP | **wave** | `http`, `ws_stream`, `smtp`, … |
| S3 / object storage | **loam** | `s3_client` |

Each provider is proven **standalone in its owning repo** against a real backend
(`<repo>/scripts/live_clients.sh`), not only through Chronicle — a client that only
worked as a Chronicle endpoint would be the wrong artefact.

## Why not bytecode

An earlier design expressed connectors purely as bytecode: an `encode` program
built the request, a generic `tcp_client` module carried it, a `decode` program
parsed the reply. It covered six protocols and was retired deliberately.

Real protocols are **stateful, multi-round-trip, and reply-dependent** — SCRAM's
client proof, MySQL's `caching_sha2` scramble, Kafka's group-membership handshake,
AMQP's channel negotiation. None of that is a stateless codec, and a per-protocol
completion `mode` on a generic transport was the design admitting it. Chronicle now
composes genuine protocol modules instead, and
[`bytecode_policy.md`](../bytecode_policy.md) fixes protocol logic as permanently
out of the VM's scope.

What survives from that work is the part that was always in scope: the `ser`/`rd`
byte VMs for **framing a message** at a pipeline edge — see
[the byte-codec guide](../guides/wire-codec.md). `examples/mqtt_sink/` uses `encode`
to render the publish frame that quantum's `mqtt_client` consumes on `app_in`.

## How an effect binds

`chronicle-canonical`'s planner turns each effect into a `ConnectorBinding`
naming the provider module, its version, and the port pair the graph wires:

| Effect kind | Provider | Ports |
|---|---|---|
| Redis / Postgres | `redis_client` / `pg_client` | `request_in` → `reply_out` |
| Kafka / Mongo | `kafka_client` / `mongo_client` | `publish_in` → `status_out` |

`bundle.rs::pin_providers` resolves each binding against the store, and the driver
composes `fluxor slot-image` to emit the OTA bundle. `plan.rs` holds the mapping;
`tests/plan.rs` and `tests/deploy_e2e.rs` pin it — the latter deploying a real Redis
effect by composing the pinned provider.

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
- [model.md](model.md) — the host/device relationship
