# OCI registry — two tiers, all config

An OCI-compatible registry built the way chronicle builds everything: **generic
engines carrying params**, composed with connectors and services owned by other
projects. There is no registry module, and there should not be one — `/v2/` is a
`.uproc` that compiles to params, not a `.fmod`.

```sh
./run.sh --verify     # start both tiers, assert, tear down
./run.sh              # start both tiers and stay up
./run.sh --stop       # kill anything it left running
```

Ports and state are one knob each, so two registries can run side by side:

```sh
S3_PORT=19222 REGISTRY_PORT=15333 STATE=/tmp/oci-alt ./run.sh --verify
```

The graph reads `${REGISTRY_PORT}`, `${S3_HOST}`, `${S3_ACCESS_KEY}` and
`${S3_SECRET}`, which fluxor substitutes before parsing the YAML; `run.sh` sets
all four. The credentials default to empty, so the graph builds on its own,
and tier 1 refuses every request signed with them.

```
  ok   GET /v2/ is 200
  ok   GET /v2/ returns the body
  ok   a stored blob is served
  ok   a 256 KiB blob streams through whole
  ok   a missing blob is 404
  ok   an unrouted path is 404
  ok   tier 1 refuses an unsigned read

PASS oci registry — discovery, blob fetch, streaming and miss, over two tiers
```

Four files, and they are the whole registry:

| file | what it is |
|---|---|
| `run.sh` | starts both tiers, seeds discovery, verifies — the only entry point |
| `registry.uproc` | the dispatch logic, compiled to params |
| `chronicle_registry.yaml` | the graph: which engines, which params, which wires |
| `README.md` | this |

Both tiers run under `fluxor run` from this project, so the runtime, every
module and the `loam-s3` bundle resolve from the Fluxor OCI store through the
pins in `../../fluxor.lock`. Nothing is built from another project's tree.

---

## The two tiers

```
        docker / curl
             │  HTTP
             ▼
  ┌──────────────────────┐   tier 2 — the registry PROTOCOL
  │ chronicle graph      │   stateless; scale by adding replicas
  │  http      (wave)    │   terminates HTTP; each request is an exchange
  │  registry  pipeline  │   relay: registry.uproc's `to_s3` rewrites the
  │                      │   request HEAD; every other record passes through
  │  s3        (wave)    │   SigV4-signs and performs it
  └──────────┬───────────┘
             │  S3 over TCP, SigV4-signed  ← the only thing crossing the boundary
             ▼
  ┌──────────────────────┐   tier 1 — the STORAGE: loam's `loam-s3` bundle
  │  http, s3_serve      │   wave: terminates S3 and verifies SigV4
  │  object_provider     │   loam: each access key acts under its capability
  │  admin_gate/_router  │   loam: the capability-gated admin plane
  │  namespace_router    │   ┐
  │  object_index        │   │ loam's storage node
  │  body_store          │   ┘
  └──────────────────────┘
```

Splitting on the S3 API rather than on a channel is the point: tier 1 can become
loam's replicated shape (remote body members, `replicas: 2` — see loam's
`docs/running.md`) and tier 2 does not change, because the S3 surface is the same
either way.

**Access is a capability, not a password.** `run.sh` makes a mesh root for the
run and mints from it a capability whose scope is the `registry/` bucket. Tier
1's credentials bind the registry's access key to that capability, so every
request tier 2 signs acts under it and reaches nothing outside the bucket. An
unsigned request is refused `403`.

---

## Every resource this needs

| # | Resource | Where it lives |
|---|---|---|
| 1 | `fluxor` runtime, CLI, OCI store | fluxor |
| 2 | the exchange contract, `ExchangeRequest` / `ExchangeResponse` | fluxor SDK |
| 3 | `http` — methods, streamed bodies, application routes, h1+h2 | wave, pinned |
| 4 | `s3` — a SigV4-signing S3 client, one exchange per request | wave, pinned |
| 5 | `loam-s3` — the S3 storage service | loam, a pinned bundle |
| 6 | `pipeline`, `decision` engines | chronicle `modules/app/` |
| 7 | `registry.uproc` → the relay's `ir_stages` | this directory |

Nothing on that list is a registry module.

---

## Why it is one straight chain

**Every request path becomes an object key.** `/v2/name/blobs/<digest>` is a
perfectly good S3 key as it stands — an object key is opaque bytes — so nothing
parses the path, and the registry's namespace simply *is* the URL's.

That includes `/v2/` itself: its discovery body is a **seeded object**, written
by `run.sh` at startup, not a branch in the graph. Which matters because the
expression VM has arithmetic and comparison but **no conditional** — branching
is the `decision` construct, and a fork in the *dataflow* would need a second
graph. Making discovery data instead of logic keeps the chain a straight line.

**One exchange, end to end.** `http` opens an exchange for each request under
an id it chooses. The `registry` node is a RELAY: it maps the request HEAD's
id, method and target to fields (`id_field`, `method_field`, `target_field`),
runs `to_s3` over them as an inline decision stage, and sends the rewritten
HEAD to `s3` under the SAME id. `s3` answers that id, and its answer records
pass back through the relay to `http` untouched — so the answer reaches the
client that asked, and no node holds a table of who asked what.

**Bodies stream.** Only the HEAD is rewritten. A blob's body arrives from `s3`
as a HEAD and as many BODY records as it takes, each forwarded as it comes and
paced by the client's own credit, so a blob of any size is served without
being assembled anywhere.

**The status is the store's, verbatim.** A 404 for a missing blob is loam's 404.
The registry never learns whether an object exists, so it cannot disagree with
the store about it. A request the relay cannot pass on is still answered — 500
— so a client is never left waiting.

**A bound port is not a serving graph.** `run.sh --verify` polls the discovery
endpoint until it answers rather than sleeping a fixed interval — the chain has
to reach the point where a request crosses HTTP → engines → SigV4 → storage and
back.

---

## Bounds

**Reads only.** `docker push` needs the upload session —
`POST /v2/<name>/blobs/uploads/`, `PATCH` chunks, `PUT ?digest=` — which is more
than a key lookup: the session id must persist across requests, and the digest
must be verified before the blob is committed. The read path here is the half
that is pure lookup.
