# The reference identity provider

Four files. `idp.uproc` is the dispatch, `chronicle_idp.yaml` is the graph,
`run.sh` starts it and verifies it, and this is the rationale.

It serves. `POST /oauth/introspect` with a compact JWS in the body answers
200 and the subject when kagi verifies it, 401 when the signature does not
check out, 400 when it is not a credential at all, and 503 when the verifier
holds no key. Every one of those statuses is a kagi verdict that chronicle
routed; not one of them is a decision chronicle made.

## There is no IdP module, and there should not be one

What runs is chronicle's **generic** engines — `pipeline` and `decision` —
carrying IdP-shaped params, plus kagi's identity modules pinned from the
Fluxor store. `idp.uproc` compiles to those params. Nothing about OAuth is
baked into a `.fmod`.

That is the same principle `examples/oci_registry` establishes, and it
carries over verbatim: an application is a **composition**, not a build.
Applications have no manifest, because a manifest is a module artefact.

## What the pipeline cannot do

`C14` is the assertion that the pipeline cannot bypass kagi. The way that
assertion is kept true is not vigilance — it is that **the pipeline has no
vocabulary for the things it must not do**.

Read `idp.uproc` and notice what is absent. There is no credential
construction, no signature, no claim, no authorization code, no consent
record, no redirect-URI comparison. The document could not express them if
someone wanted it to. Every one lives inside a kagi module, reached by a
typed request.

What the document *does* contain is two mappings: which kagi operation an
HTTP method names, and which HTTP answer a kagi verdict deserves. Both are
routing.

The refusal path makes the same point sharper than the success path does.
`to_kagi`'s default arm does **not** short-circuit — it forwards to kagi with
an *empty* credential, and kagi answers `MALFORMED`, which becomes 400. A
`GET` to this endpoint is refused by kagi, not by the graph, even though the
graph plainly knew. That is deliberate: the moment the pipeline is allowed to
answer on its own for the easy cases, it has an answering path, and `C14` is
the claim that it has none.

## The branch is a kagi verdict

The expression VM has no conditional. A single bytecode program constructs
one message and cannot select among several, so every branch has to be a
`decision` node — a first-hit table over an **input record**, matching a
value some other module produced.

That constraint looks like an obstacle and is actually the enforcement
mechanism. `route_ans` branches on `KagiReply.verdict`, which means the
condition the graph acts on is *kagi's verdict* rather than a pipeline
expression. **The pipeline cannot decide to admit something kagi refused,
because the only thing it can branch on is what kagi said.**

kagi already produces exactly these discriminants — `auth_wire::mint_err`,
`verify_err`, and `VerifiedIdentity.status`. The typed-next-step design was
a proposal when this application was planned; it is now the shipped surface.

`to_answer`'s last two arms are **ranges** (`<= V_SUITE_MISMATCH`,
`<= V_LOW_ASSURANCE`) rather than an arm per code, because the table is
first-hit: by the time control reaches them, `OK`, `MALFORMED`, `NO_KEY` and
`NO_CLOCK` have already matched, so the ranges are exactly the
authentication failures and exactly the policy failures. Ordering is
load-bearing — moving an arm changes which codes it covers.

One decision node per protocol junction, not one per condition. That is what
keeps the graph near-linear despite a protocol with many shapes.

## Fail-closed defaults, twice

Both decision tables end in a default, and both defaults refuse:

- `to_kagi` defaults to `OP_REFUSE`. A request this document cannot classify
  must not reach kagi *as though it had been classified*.
- `to_answer` defaults to `503` — not `500`, and certainly not `200`. A
  verdict the document does not recognise is one it cannot act on, and the
  fail-closed answer is "cannot serve you".

The refusal arms carry no body. A refusal that carried one would let a client
that ignored the status read something credential-shaped — the same rule
`MintResponse` and `VerifiedIdentity` enforce on their own wires, restated
here because this is where it reaches a client.

## Long waits are state, not suspended executions

Email confirmation, consent, recovery and approval are durable lattice state
plus a later pipeline invocation. A chronicle execution is never suspended as
a workflow. External delivery is an **outbox effect after** the authoritative
transition, never before it — so a mail that is sent is a mail whose
transition committed.

## The surface, and why it is shaped that way

`POST /oauth/introspect`, with the compact JWS as the **request body**, and
`text/plain` back: the subject on 200, nothing on any refusal.

Two deliberate departures.

It is **not RFC 7662**, which posts a form and answers a JSON document.
Parsing that form and assembling that document are both chronicle handling
identity data, and the second one — building `{"active":true,"sub":...}` —
is chronicle *constructing an identity record* out of parts. The body of a
200 here is kagi's `VerifiedIdentity.subject` copied verbatim, and that is
the most this graph is permitted to say about anybody.

The credential rides in the **body, not the URL**, because a path is capped
at `abi::config::http::MAX_PATH` (200 bytes on this profile) and a real
compact JWS is longer. A credential in a URL is silently truncated, which is
the worst available failure: the request succeeds, the signature does not
verify, and nothing anywhere says why.

## Reading a request without parsing it

`dec_http` never parses. The `HttpRequest` envelope is
`[conn][stream][method][flags][path_len][hdr_len][body_len]` followed by
path, headers and body — so all three lengths are read *before* any of the
three values, and the byte VM's stack is LIFO with no locals and no swap.

The program reads `path_len`, reads `hdr_len`, **adds them**, and does one
`TAKEN` that consumes both variable regions together; `REST` is then exactly
the body. That is why `idp.Request` has a `head` field: the path and header
block come off as one value because taking them apart is not expressible,
and naming what was taken is better than discarding it silently. Nothing
downstream branches on it.

Every length used is wave's own framing. Not one of them is a length read
out of the request, so no request can make the program read past what
arrived.

## Buffers this example had to raise

Three, each because 512 or 8 was sized for the examples that existed rather
than for a workload — and each found by a real failure, not by inspection:

- **`pipeline`'s record buffers, 512 → 4096** (`REC_BUF`), and `decision`'s
  to match. A record too large is not truncated, it is **dropped**:
  `channel_read` stops at the buffer, the codec then reads a length the rest
  of the record was going to satisfy, fails, and the record vanishes into a
  counter. From the client that is a request that never answers — the
  hardest shape there is to diagnose from outside. A single compact JWS is
  ~350 bytes and the envelope carries the path and header block beside it.
- **`chronicle_cli`'s `u_enums`, 16 → 32.** A `decision` matches on a typed
  discriminant another module produced, so a document that routes a protocol
  names every value of every discriminant it routes on. `verify_err` alone
  is twelve.
- **`buffer_bytes: 4096` on every record-carrying edge** in the graph — a
  graph-level statement, not a module change, because it is this graph's
  bandwidth requirement. Stated on *every* such edge rather than the one
  that overflowed first: sizing one edge and leaving the next moves the hang
  rather than fixing it.

## Why this document is 5.7 KB

`chronicle author` decodes a `.uproc` into a fixed buffer, and that buffer was
**2048 bytes**. `examples/oci_registry/registry.uproc` is 1967 — 96% of it —
and an OCI registry is a far simpler protocol surface than this.

This document is **~7.6 KB**: 3.7× the old bound, and that is *after* the
configuration has been pushed out into params, the document reduced to
dispatch, and `to_answer` collapsed from twelve equality arms to six. Even
dispatch-only, an IdP has a decision per protocol junction where a registry
has a handful.

The authoring path has its own separate bound — `MAX_RULE = 8` arms per
decision, sized by two 2 KB stack arrays. That one was **not** raised:
chronicle deliberately keeps work buffers off the PIC stack, and 8 arms
turned out to be enough once the table used first-hit ordering instead of
enumerating every code. A cap that forces a better table is a cap worth
keeping.

So the bound was raised (`chronicle_cli`'s `UPROC_BUF`, 8192) rather than
worked around. Splitting the document would break the four-file shape these
examples establish; the cost of raising it is module state on a device with
plenty, since `chronicle_cli`, `pipeline` and `decision` are all
`hardware_targets = ["bcm2712"]`. The old value was not sized for a workload.

## Running it

```
./run.sh              # author, start, provision a keyset, stay up
./run.sh --verify     # introspect: author, start, assert every arm, tear down
./run.sh --token      # the /oauth/token device grant, end to end
./run.sh --authorize  # the OIDC authorization-code /authorize leg
./run.sh --exchange   # the OIDC authorization-code exchange leg
./run.sh --stop       # kill anything left running
```

Each mode stands up its own graph. `--verify` serves introspection through
`token_verify`; `--token` serves the device grant through `mint_admission`;
`--authorize` and `--exchange` serve the two legs of the OIDC
authorization-code flow through `authcode`. They are separate graphs because a
chronicle pipeline node has one decode program, so each operation's wire — its
request fields and its reply layout — needs its own codecs and its own linear
chain. What they share is the discipline: every graph routes to kagi's typed
surface, and the only thing any of them can branch on is the verdict kagi
returns.

`run.sh` authors the document **before** starting the graph. That is
deliberate: a graph whose params drifted from the document they were
generated from is a graph that does something the document does not say.

`--verify` drives **real HTTP round trips** and asserts the status of each,
including the refusals — a chain that only ever answers 200 has not shown it
can say no. It also asserts that the 200 carries kagi's subject verbatim and
that the 401 carries nothing at all.

An early draft asserted only that the graph stayed up, with `|| echo 000`
appended to curl's own `000`, producing `000000`, which did not match the
`000` guard: **it reported success on a total failure.** That is why the
check now captures each status and compares it exactly.

## The key ceremony

`token_verify` holds no key until one is given to it, and answers `NO_KEY`
until then — which this graph turns into 503, and which is the honest answer
for an issuer that has not been given an issuer.

The keyset arrives the way it arrives in kagi's own e2e graphs: a websocket
route (`/ws`) into `ws_stream` into `remote_channel`, with **channel 0 and
only channel 0** wired to `token_verify.verify_key`. `run.sh` writes out a
short operator client that generates a P-256 pair, pushes the **public** half
as a kagi `MSG_KEY_ADD`, and signs one short-lived ES256 credential with the
private half.

Two properties worth stating rather than inferring. The keyset is an
**operator** input, not a request input: it arrives on a different route,
through a different codec, and nothing on the serving path can reach it. And
the private key never enters the graph — a verifier holds public keys only,
which is why `MSG_KEY_ADD` carries a public point and why the ceremony is a
host script instead of a module.

## Why `token_verify` and not `token_endpoint`

kagi's HTTP endpoints (`token_endpoint`, `wellknown_endpoint`) speak
`HttpRequest`/`HttpResponse` directly. Wiring one of those would produce a
graph that serves — and a graph in which **chronicle does nothing**. The
endpoint would be the IdP and this example would be a passthrough
demonstrating none of its own point.

Routing through kagi's **typed** surface is what makes the example an
example: the graph has to encode a `MSG_VERIFY_REQ`, and the only thing it
can then branch on is the `verify_err` that comes back.

## What is not finished

Four operations are served end to end: introspection, the `/oauth/token`
device grant, and both legs of the OIDC authorization-code flow —
`/oauth/authorize` (which mints a single-use code) and the code exchange
(which redeems it for an access token and an ID token). Each is its own graph
routing to a kagi operation (`token_verify`, `mint_admission`, `authcode`),
and in every one the only branch is a kagi verdict.

The rest of the surface is not served yet: this is the dispatch skeleton and
the enforcement shape, not the full OIDC surface. The junctions `C14`
enumerates that remain — CSRF binding of the authorization server's own login
and consent session, client authentication, consent records, `acr`/`amr`/
`at_hash`/`c_hash`, discovery and JWKS, UserInfo, refresh rotation and reuse
detection, revocation, logout — each needs its kagi-side operation before it
can be routed to. The corpus should grow a pipeline that attempts each bypass
and is refused.

The authorization-code legs run as separate graphs, so `--authorize` issues a
code and `--exchange` redeems one the operator seeds (the artefact `/authorize`
would have created), each verified in isolation the way kagi's own e2e verifies
each leg. kagi's `authcode_e2e` proves the full `/authorize` → exchange chain
cryptographically end to end within kagi; here each leg's chronicle presentation
is proven over real HTTP.

What is established here is the shape that makes those additions safe: every
one of them arrives as a kagi operation with a typed verdict, and the graph
gains a `when` arm rather than any new ability to decide.
