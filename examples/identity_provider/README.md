# The reference identity provider

`idp.uproc` is the dispatch, the four `chronicle_*.yaml` files are the graphs
(one per operation), `run.sh` starts each and verifies it, the `*_client.py`
scripts are the operator's ceremonies and the clients, `control.py` is the
operator's carrier into a graph, and this is the rationale.

It serves. `POST /oauth/introspect` with a compact JWS in the body answers
200 and the subject when kagi verifies it, 401 when the signature does not
check out, 400 when it is not a credential at all, and 503 when the verifier
holds no key. Every one of those statuses is a kagi verdict that chronicle
routed; not one of them is a decision chronicle made.

## There is no IdP module, and there should not be one

What runs is chronicle's **generic** `pipeline` engine carrying IdP-shaped
params, plus kagi's identity modules pinned from the Fluxor store.
`idp.uproc` compiles to those params. Nothing about OAuth is baked into a
`.fmod`.

That is the same principle `examples/oci_registry` establishes, and it
carries over verbatim: an application is a **composition**, not a build.
Applications have no manifest, because a manifest is a module artefact.

## One exchange in, one exchange out

Every graph is the same three pipeline nodes around one kagi operation, and
every edge between them and the outside world is the workspace exchange
contract:

```
http ─request_out→ intake ──→ call ⇄ kagi operation ──→ reply ─response_out→ http
                     └──────── response_out ──────────→ ┘
```

- **`intake`** is the provider wave's `http` asks. The exchange id, method
  and target arrive as record fields; the body arrives whole and `decode`
  turns it into the credential fields. Its inline decision (`to_kagi`,
  `to_grant`, …) builds the typed call, and puts the HTTP exchange id in the
  record's **carry** (field 254).
- **`call`** (`verify`, `grant`, `authorize`, `redeem`) is the requester kagi
  answers. `encode` writes the call as one `auth_wire` envelope — the request
  body — and `reply_decode` reads the answer envelope back into fields. The
  carry stays in the node while the kagi exchange is open and comes back on
  its answer, so nothing holds a table of who asked.
- **`reply`** answers the HTTP exchange the carry names. Its inline decision
  (`to_answer`, …) maps kagi's verdict to a status and a body. It is also
  the relay for `intake`'s own answers, because `http.response_in` takes one
  producer.

kagi's typed operations are exchange providers too: a `POST` whose body is
one request envelope, answered 200 with the answer envelope — a refusal
verdict included. So the HTTP status a client sees is never kagi's transport
status passed through; it is this document's mapping of the verdict inside
the 200.

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
HTTP method and target name, and which HTTP answer a kagi verdict deserves.
Both are routing.

The refusal path makes the same point sharper than the success path does.
`to_kagi`'s default arm does **not** short-circuit — it forwards to kagi with
an *empty* credential, and kagi answers `MALFORMED`, which becomes 400. A
`GET` to this endpoint is refused by kagi, not by the graph, even though the
graph plainly knew. That is deliberate: the moment the pipeline is allowed to
answer on its own for the easy cases, it has an answering path, and `C14` is
the claim that it has none. So `intake` maps no status field: it has no
refusal of its own to give. The only answers it writes are the contract's —
a request it cannot collect (413, 503) or whose record fails (500).

## The branch is a kagi verdict

The expression VM has no conditional. A single bytecode program constructs
one message and cannot select among several, so every branch has to be a
`decision` — a first-hit table over an **input record**, matching a value
some other module produced. Here each decision runs inline, as a stage of the
pipeline node that needs it.

That constraint looks like an obstacle and is actually the enforcement
mechanism. `to_answer` branches on `KagiReply.verdict`, which means the
condition the graph acts on is *kagi's verdict* rather than a pipeline
expression. **The pipeline cannot decide to admit something kagi refused,
because the only thing it can branch on is what kagi said.** The answer side
does not even branch on the kagi exchange's status: a 200 carries the
verdict, and an answer without one reaches no decision at all (below).

kagi produces exactly these discriminants — `auth_wire::verify_err`,
`grant_err` and `authz_err` — as the first byte of each answer payload.

`to_answer`'s last two arms are **ranges** (`<= V_SUITE_MISMATCH`,
`<= V_LOW_ASSURANCE`) rather than an arm per code, because the table is
first-hit: by the time control reaches them, `OK`, `MALFORMED`, `NO_KEY` and
`NO_CLOCK` have already matched, so the ranges are exactly the
authentication failures and exactly the policy failures. Ordering is
load-bearing — moving an arm changes which codes it covers.

One decision per protocol junction, not one per condition. That is what
keeps the graph linear despite a protocol with many shapes.

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

An answer from kagi with **no** verdict — its own 400, 413 or 503, whose body
is empty — is the third fail-closed path. The requester's record then holds
kagi's status and the carry and no verdict field, so every verdict arm misses
and the default answers 503 at once; nothing downstream can mistake it for a
verdict. The graph never sends kagi a request it would answer that way —
always `POST`, always one envelope, always a full record of response credit —
so on this path it means kagi was busy.

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

The credential rides in the **body, not the URL**. A URL is logged by every
proxy on the way, and a request target is bounded — the pipeline collects at
most 512 bytes of one (`KEY_BUF`) and refuses a longer one rather than cut it
— while a real compact JWS is longer than that. The body is collected whole,
up to the contract's 8192 bytes, and never truncated.

## Reading a request without parsing it

Nothing in these graphs reads a length out of the request. The exchange
contract delivers it already framed: the id, method and target are fields of
the request HEAD, which `intake` maps to record fields (`id_field`,
`method_field`, `target_field`), and the body arrives whole, its extent the
contract's own. So `decode` sees only the body:

- introspection takes it whole — `REST` is the credential;
- the token, authorize and exchange bodies are newline-separated values none
  of which can contain a newline (compact JWSs, a client id, a URI, a scope,
  an opaque state, a PKCE value), so `UNTIL_OPT '\n'` takes each in turn and
  `REST` the last.

`UNTIL_OPT` rather than `UNTIL` because a short body must not fail the
record: a missing value reads as empty, the call reaches kagi with it empty,
and kagi says what that is worth. A decoder that failed would make the
pipeline the one refusing.

Matching the target against `/oauth/introspect` is a prefix test on a field
the contract framed — routing, not parsing.

## Sizes this example depends on

Each was found by a real failure, not by inspection:

- **The pipeline's record ports, 4096 bytes** (`record_in`/`result_out`,
  and `REC_BUF` inside the node). A record too large for a channel does not
  arrive truncated — it does not arrive at all, and from the client that is
  a request answered only by wave's deadline. Two compact JWSs and the eight
  authorize values with their carry fit with room, so no edge states a
  `buffer_bytes` of its own.
- **Response credit, 8192.** kagi refuses a request granting less than one
  answer record (`resp_credit` ≥ 8171) with a 400 rather than hold a verdict
  it could not deliver; the requester grants the contract's whole payload.
- **Exchange edges** are mailbox edges Fluxor frames itself, so no exchange
  edge declares a `buffer_group`. The two `http` ⇄ `ws_net` edges do: they
  carry `WsFrame` records, and without a group the channel is a byte FIFO.
- **`chronicle_cli`'s `u_enums`, 48.** A decision matches on a typed
  discriminant another module produced, so a document that routes a protocol
  names every value of every discriminant it routes on. This one names 35.

## The document's size

`chronicle author` decodes a `.uproc` into a fixed buffer, `chronicle_cli`'s
`UPROC_BUF` — 65536 bytes of source. This document is **~20 KB**, and that is
*after* the configuration has been pushed out into params and the document
reduced to dispatch: an IdP has a decision per protocol junction where a
registry has a handful.

`run.sh` checks the size before authoring, because exceeding the bound does
not fail — it hangs. The hex of the document is one `fluxor exec` argument;
an over-long one exceeds `ARGV_BUF` (twice `UPROC_BUF`) and is never
delivered, and the CLI waits for argv that cannot arrive.

The authoring path has its own bound — `MAX_RULE` arms per decision. The
widest table here has seven, because each uses first-hit ordering instead of
enumerating every code. A cap that forces a better table is a cap worth
keeping.

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
chronicle pipeline node has one decode program and one reply decoder, so each
operation's wire — its request fields and its answer layout — needs its own
codecs. What they share is the shape and the discipline: every graph routes
to kagi's typed surface, and the only thing any of them can branch on is the
verdict kagi returns.

`run.sh` authors the document **before** starting the graph. That is
deliberate: a graph whose params drifted from the document they were
generated from is a graph that does something the document does not say.

`--verify` drives **real HTTP round trips** and asserts the status of each,
including the refusals — a chain that only ever answers 200 has not shown it
can say no. It asserts that the 200 carries kagi's subject verbatim, that the
401 carries nothing at all, and that a `GET` is answered 400 by kagi's
`MALFORMED` rather than by the graph. Each status is captured and compared
exactly: a check that only asserted the graph stayed up, or that let curl's
own `000` fold into some other string, would report success on a total
failure.

## The key ceremony

`token_verify` holds no key until one is given to it, and answers `NO_KEY`
until then — which this graph turns into 503, and which is the honest answer
for an issuer that has not been given an issuer.

The keyset arrives the way it arrives in kagi's own e2e graphs: a websocket
route (`/ws`) into `ws_net` into `remote_channel`, with **channel 0 and only
channel 0** wired to `token_verify.verify_key`. `introspect_client.py`
generates a P-256 pair, pushes the **public** half as a kagi `MSG_KEY_ADD`
over that carrier (`control.py`), and signs one short-lived ES256 credential
with the private half.

Three properties worth stating rather than inferring. The keyset is an
**operator** input, not a request input: it arrives on a different route,
through a different carrier, and nothing on the serving path can reach it.
The private key never enters the graph — a verifier holds public keys only,
which is why `MSG_KEY_ADD` carries a public point and why the ceremony is a
host script instead of a module. And because the key and the request travel
independent paths, nothing orders them: the operator waits until
introspection stops answering 503 before handing the credential over, and the
grant clients wait for `token_mint`'s announcement of each key and the
ledger's acknowledgement of each record.

## Why `token_verify` and not `token_endpoint`

kagi's HTTP endpoints (`token_endpoint`, `wellknown_endpoint`) are exchange
providers that answer HTTP themselves: wired straight to `http.request_out`,
one would produce a graph that serves — and a graph in which **chronicle does
nothing**. The endpoint would be the IdP and this example would be a
passthrough demonstrating none of its own point.

Routing through kagi's **typed** operations is what makes the example an
example. They take the same contract, but their body is an `auth_wire`
request envelope and their 200 carries a verdict rather than an HTTP answer:
the graph has to encode a `MSG_VERIFY_REQ`, and the only thing it can then
branch on is the `verify_err` that comes back.

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
