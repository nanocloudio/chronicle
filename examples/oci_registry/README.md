# OCI registry — two tiers, all config

An OCI-compatible registry built the way chronicle builds everything: **generic
engines carrying params**, composed with connectors owned by other projects.
There is no registry module, and there should not be one — `/v2/` is a `.uproc`
that compiles to params, not a `.fmod`.

```sh
./run.sh --verify     # start both tiers, assert, tear down
./run.sh              # start both tiers and stay up
./run.sh --stop       # kill anything it left running
```

Ports and state are one knob each, so two registries can run side by side:

```sh
S3_PORT=19222 REGISTRY_PORT=15333 STATE=/tmp/oci-alt ./run.sh --verify
```

The graph reads `${REGISTRY_PORT}`, `${S3_ENDPOINT}` and `${S3_HOST}` — fluxor
substitutes `${VAR:-default}` before parsing the YAML, so a bare
`fluxor build` still works on the defaults. `s3_client`'s `endpoint` is
`[ip:4][port:2 LE]` packed as hex, which a decimal port cannot be interpolated
into, so `run.sh` DERIVES it from `S3_PORT` rather than carrying a second
literal that would drift the first time someone moved the port.

```
  ok   GET /v2/ is 200
  ok   GET /v2/ returns the body
  ok   a stored blob is served
  ok   a missing blob is 404
  ok   an unrouted path is 404

PASS oci registry — discovery, blob fetch and miss, over two tiers
```

Four files, and they are the whole registry:

| file | what it is |
|---|---|
| `run.sh` | starts both tiers, seeds discovery, verifies — the only entry point |
| `registry.uproc` | the dispatch logic, compiled to params |
| `chronicle_registry.yaml` | the graph: which engines, which params, which wires |
| `README.md` | this |

Nothing outside this directory is edited to run it. The one project-level change
is a dependency line (`loam = "0.0.1"` in `../../fluxor.toml`) so the storage
connector resolves from the store — the same pin every other connector uses.

---

## The two tiers

```
        docker / curl
             │  HTTP
             ▼
  ┌──────────────────────┐   tier 2 — the registry PROTOCOL
  │ chronicle graph      │   stateless; scale by adding replicas
  │  http (wave)         │   terminates HTTP, hands requests to graph nodes
  │  dec_http  pipeline  │   rd bytecode: envelope → Request
  │  route     decision  │   registry.uproc's `to_s3`
  │  enc_s3    pipeline  │   ser bytecode: S3Call → S3Request
  │  s3_client (loam)    │   SigV4-signs and performs it
  │  dec_s3    pipeline  │   rd bytecode: S3Response → S3Reply
  │  enc_http  pipeline  │   ser bytecode: S3Reply → HttpResponse
  └──────────┬───────────┘
             │  S3 over TCP  ← the only thing crossing the tier boundary
             ▼
  ┌──────────────────────┐   tier 1 — the STORAGE
  │ loam-server          │   stateful; scale by adding body nodes
  │  --s3-listen         │   S3 gateway: PUT/GET/HEAD/DELETE per object
  │  namespace_router    │   ┐
  │  object_index        │   │ ordinary loam PICs behind the gateway
  │  placement_router    │   │
  │  body_store (fleet)  │   ┘
  └──────────────────────┘
```

Splitting on the S3 API rather than on a channel is the point: tier 1 can become
a replicated fleet (`--fleet tcp:a:7100,tcp:b:7100 --replica-count 2`) and tier 2
does not change, because the S3 surface is identical either way.

---

## Every resource this needs

| # | Resource | Where it lives | Status |
|---|---|---|---|
| 1 | `fluxor` runtime, CLI, OCI store | `../../../fluxor` | ✅ |
| 2 | `HttpRequest` / `HttpResponse` content types | fluxor `contracts/src/lib.rs` | ✅ |
| 3 | `app: true` route key → `HANDLER_APP` | fluxor `tools/src/schema.rs` | ✅ |
| 4 | `http` — methods, bounded bodies, app fan-out, h1+h2 | wave, pinned | ✅ |
| 5 | `loam-server --s3-listen` — the S3 gateway | loam `tools/loam-cli/` | ✅ |
| 6 | `namespace_router`, `object_index`, `body_store`, … | loam PICs, hosted by tier 1 | ✅ |
| 7 | `pipeline`, `decision` engines | chronicle `modules/app/` | ✅ |
| 8 | `s3_client` driveable per request | loam, pinned | ✅ |
| 9 | `registry.uproc` → `ir_stages` / decision params | this directory | ✅ |

Nothing on that list is a new chronicle module.

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

**Correlation survives the storage round trip** because `conn_id` rides in the
connector's `cid` field and returns in the reply — the same trick
`(conn_id, stream_id)` plays across the HTTP fan-out.

**The status is the store's, verbatim.** A 404 for a missing blob is loam's 404.
The registry never learns whether an object exists, so it cannot disagree with
the store about it.

---

## What the code changes were

Two, both genuine, both in loam — chronicle gained no code at all:

**`s3_client` driveable per request** (a missing feature). It signed one
`GET /` at boot and reported the status; there was no way to ask for a different
object. It now takes an `S3Request` per graph message on `request_in` and answers
on `response_out` — the client-side mirror of wave's `HANDLER_APP`. 17 host
vectors, plus `loam/tools/e2e/s3_driven.sh` driving PUT/GET/HEAD/DELETE against a
real gateway.

**`s3_object_path` doubled a leading slash** (a bug). Given the key `/v2/x` it
produced `/registry//v2/x`, which addresses a *different* object — verified
against loam's own gateway, where `/registry/v2/x` returns 200 and
`/registry//v2/x` returns 404. It silently 404s instead of failing, which is the
failure mode that function's own doc warns about. Fixed, with two tests.

A registry whose client passes a URL path straight through as a key hits that
immediately, which is how it was found.

---

## What it does not do yet

**Authentication.** Tier 1 runs anonymous — no `--s3-credentials` — so the
SigV4 signature tier 2 computes is never verified. Fine on loopback, wrong for
anything else: making it real is passing `--s3-credentials` to `loam-server` and
matching keys in the graph. The credentials in the YAML today
(`minioadmin`/`minioadmin`) are the MinIO defaults and mean nothing to an
anonymous gateway.

**Writes.** `docker push` needs the upload session — `POST /v2/<name>/blobs/uploads/`,
`PATCH` chunks, `PUT ?digest=` — which is more than a key lookup: the session id
must persist across requests, and the digest must be verified before the blob is
committed. The read path here is the half that is pure lookup.

Large blobs are bounded at 16 KiB per object by the connector's fixed staging
buffers. Beyond that needs the chunked form the HTTP fan-out already uses (a
`MORE_BODY` flag across several records).

---

## Three things this taught

**A `.uproc` is bounded at 2 KB.** `chronicle author` decodes a document into a
2048-byte buffer, so the source has a hard size limit — which is why the
rationale lives here and `registry.uproc` carries only logic.

**A pipeline node with no program fails CLOSED — silently, from outside.** With
only a codec param and no `ir_stages`, the engine emits a
`{1:"VERSION_UNAVAILABLE"}` record rather than the decoded one. Correct
behaviour (a record pinned to a version the instance does not hold must never get
the wrong version), but downstream it looks like an ordinary record, so the
failure surfaces two hops away as a decision that will not fire. Every codec node
here therefore carries one of the document's identity stages.

**A bound port is not a serving graph.** `run.sh --verify` polls the discovery
endpoint until it answers rather than sleeping a fixed interval — the chain has
to reach the point where a request crosses HTTP → engines → SigV4 → storage and
back, and a magic sleep is how a gate becomes flaky on a slower machine.
