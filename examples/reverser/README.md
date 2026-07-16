# The reverser — a whole application with zero application modules

Type a message into a browser; it comes back reversed:

```
http://127.0.0.1:15100/reverse?msg=hello      → "ok"       (stored durably)
http://127.0.0.1:15102/messages?max=10       → olleh …    (newest first, HTML)
```

Between those two URLs sit an HTTP gateway, a consensus-replicated SQL
database, a change-data-capture feed, an MQTT broker, and a stream processor —
and **not one line of application code**. Every module is a generic engine or
a protocol connector pinned from the Fluxor OCI store; the application exists
only as *configuration*: bytecode params compiled from `reverser.uproc` plus
hand-authored codec programs.

```
                      ┌────────────────────── tier 3: chronicle ───────────────────────┐
  browser ──HTTP──▶  A: http → dec → enc_sql ─────────────┐                            │
                      │                  ▲                │ [cid]INSERT                │
                      │                  └─ enc_http ◀─┐  ▼                            │
                      │                               dec_pg ◀─ pg_client ──SQL──┐    │
                      └────────────────────────────────────────────────────────── │ ───┘
                                                                                  ▼
   tier 2: lattice   pg_edge_anchor → relational_executor → router → consensus → WAL
                        `inbound(msg TEXT PRIMARY KEY)`         │ committed puts
                                                                ▼
                     cdc_pump (WAL-decoupled feed, at-least-once) → mqtt_sink
                                                                        │ QoS-1 PUBLISH
   tier 1: quantum                    MQTT broker  ◀────────────────────┘
                                          │ lattice/cdc/feed1
                      ┌───────────────────▼──────────── tier 3 ────────────────────────┐
                      │  B: mqtt_client → dec_evt → keep_puts → flip → enc_sql         │
                      │                             (decision)  (reverse())            │
                      └──────────────────────────────────│─────────────────────────────┘
                                                         ▼ INSERT
                     `reversed(ts BIGINT PRIMARY KEY, msg TEXT)`  ts = CDC commit_ts
                                                         ▲
  browser ◀──HTML── C: http → dec → enc_sql → pg_client ─┘ SELECT … ORDER BY ts DESC
```

## Run it

```
./run.sh            # start all four tiers, stay up (Ctrl-C stops everything)
./run.sh --verify   # start, push one message through the whole loop, tear down
./run.sh --stop     # kill anything a previous run left behind
```

Prerequisites: sibling checkouts of `fluxor` (built runtime), `quantum`
(broker graph), and the store publishes of `lattice`, `clustor`, `quantum`,
`wave` — chronicle's `fluxor.toml` pins all of them. `run.sh` wipes its state
dir on every start (`KEEP_STATE=1` to resume one).

## Run it on the rig — the whole application on one board

`./rig-verify.sh` netboots `reverser_pi5.yaml` onto the Pi 5 rig DUT
(192.168.1.9): wave http on **port 80**, all three chronicle pipelines, and
the lattice storage tier (consensus, WAL, NVMe, CDC) in ONE 32-module graph.
Then, from any machine on the bench LAN:

```
http://192.168.1.9/reverse?msg=hello
http://192.168.1.9/messages?max=10
```

The single-board shape is the distributed one with two deployment
substitutions and zero logic changes:

- the MQTT broker leg becomes an in-graph **tee** of the pump's publish
  channel — `loopback_sink` still consumes and acks it (the ordered_ack
  contract is untouched), pipeline B reads the same frames passively;
- the pg_client connections ride the fluxor ip module's **local-delivery
  fastpath**: a connect to 127.0.0.1 binds a conn pair against the local
  listener directly — no TCP, no ARP (a host cannot resolve itself through
  a switch). This was built for this example and is the general mechanism
  for composing a client module against its own board's listener.

**Known limit (lattice task #24)**: the store's version-scan drain is not
yet chunked, so once the first memtable spill exists (~240 messages total)
the pump's page walks saturate the FAT32 read path and SQL starves — the
board serves ~240 messages per boot, then needs a re-netboot (~3 min,
`clean_root` resets state). The fix (suspend/resume the scan across steps,
like the flush) is filed with full rig evidence.

Two size levers keep the image under the Pi 5 netboot ceiling (~3.6 MB —
images crossing it boot silent-dead with the appended module/config payload
unreadable; found by bisection on this bench): wave http's `app` variant
(h1 + the HANDLER_APP fan-out only) and dropping the metrics fan-in
(UDP telemetry still carries every heartbeat).

## Where the "application" actually lives

| concern | where it is expressed |
|---|---|
| message types, filter, compute | `reverser.uproc` (compiled on-device: `fluxor exec chronicle -- graph <hex> <entry> linux`) |
| the reverse itself | `flip`: `c.msg.reverse()` — the pinned CEL strings extension, one `CALL` opcode |
| put-vs-watermark filtering | `keep_puts` decision; non-puts construct the **empty message**, which the decision engine counts as `dropped` and routes nowhere |
| wire parsing/building | hand-authored `decode`/`encode` byte-VM programs in the graph YAMLs (annotated inline) |
| sequencing | none written: the store-minted CDC `commit_ts` **is** the sequence (`reversed.ts`), so C's `ORDER BY ts DESC` is replay order |
| correlation across the store hop | `pg_client`'s `cid_len` param: the first 4 request bytes are opaque, echoed at the front of the reply — wave's `(conn_id)` rides through the SQL round trip, and A answers "ok" only after the INSERT committed |

Two schema choices carry the whole design:

- **`inbound(msg TEXT PRIMARY KEY)`** — the CDC event's *key* is the message.
  Pipeline B never decodes a relational row image; `dec_evt` takes the raw key
  verbatim and the `keep_puts` outcome slices the text out of its framing
  (`[keyspace:4][table:4][present:1]<text>[term:2]`) with `substring()`.
- **`reversed(ts BIGINT PRIMARY KEY)`** — no SERIAL, no client-side id.

## Composition boundaries

The CDC RFC (`lattice/.context/rfc_cdc_egress.md`) forbids lattice depending
on quantum. The cross-project composition therefore lives HERE, in the
application project: `lattice_reverser.yaml` wires lattice's `cdc_pump`
(capability `stream.sink.ordered_ack`) to quantum's `mqtt_sink`, and
chronicle's `fluxor.toml` pins both projects (plus `clustor`, the consensus
substrate). Swap `mqtt_sink` for `kafka_sink` or `amqp_sink` and nothing in
lattice or in the pipelines changes.

`cdc_pump`'s keyrange prefix is `8000000200000001` =
`[KS_RELATIONAL_TABLE][table_id 1]`. `inbound` is the first table `run.sh`
creates, and the catalog allocator's first INCR returns 1, so the id is
deterministic — and scoping to it matters, because B's own inserts into
`reversed` (table 2) go through the same store and an unscoped feed would
loop them back into B. This is also why `run.sh` gates the DDL on the pump's
first checkpoint: a CREATE that times out during consensus warm-up still
burns a table id.

## What a platform would generate from this

Every value in these YAMLs is derivable from a single high-level resource.
This is the design artifact for acorn's implicit-provisioning story (name
still pending — `BackendApplication` as a placeholder):

```yaml
kind: BackendApplication
metadata:
  name: reverser
spec:
  tables:                       # → lattice instance, schema, CDC feeds
    - name: inbound
      columns: [{name: msg, type: text, primaryKey: true}]
      changeFeed: {topic: inbound.msg}          # → cdc_pump + broker + sink
    - name: reversed
      columns: [{name: ts, type: bigint, primaryKey: true},
                {name: msg, type: text}]
  pipelines:                    # → the chronicle graphs
    - name: ingest              # → graph A
      trigger: {http: {method: GET, path: /reverse, query: {msg: string}}}
      action:  {insert: {table: inbound, values: {msg: $msg}}}
    - name: reverse             # → graph B
      trigger: {changeFeed: inbound.msg}
      transformation: |         # verbatim .uproc CEL — the ONLY user logic
        rev.Change { ts: c.ts, msg: c.msg.reverse() }
      action:  {insert: {table: reversed, values: {ts: $ts, msg: $msg}}}
    - name: list                # → graph C
      trigger: {http: {method: GET, path: /messages, query: {max: int}}}
      action:  {select: {table: reversed, orderBy: {ts: desc}, limit: $max,
                render: {html: pre}}}
```

The reconciler's job is exactly what was done by hand here: allocate the
store + broker, compile the transformations, derive the codec programs from
the declared triggers, and emit the three graphs. No user ever sees a module
name.

## Deliberate simplifications (documented, not hidden)

- **No URL-decoding**: `msg=hello%20world` stores the literal `%20`. A real
  ingress would decode in the rd program or a stage.
- **No SQL quoting**: a `'` in msg breaks the INSERT (the statement errors;
  nothing downstream sees a partial write). Real deployments bind values.
- **Path prefixes are asserted, not routed**: any other path fails `dec`'s
  `LIT` (a counted decode error) and the client waits out wave's app timeout
  (504) instead of getting a 404.
- **Subscribe-then-insert ordering**: quantum's `mqtt_client` subscribes at
  QoS 0, so events published while B is down are not redelivered by the
  broker. The pump itself is at-least-once (durable checkpoint + replay), so
  a restarted FEED re-publishes unacked events; the demo keeps B up from
  before the first insert. Duplicates collapse: same key, same `ts`.
- **Put-only consumption**: deletes and resolved watermarks are counted drops
  in `keep_puts` (nothing ever deletes from `inbound` here).
- **Result-set ceiling**: a `/messages` reply that would overflow
  `pg_client`'s 1024-byte reply buffer is refused whole (never truncated) —
  the request times out rather than presenting a prefix as the full list.
- **The CDC topic is baked into `dec_evt`** (`SKIP 18` = the topic prefix on
  the subscriber frame), so changing the topic name means regenerating that
  one program.
- **Graph lowering gap**: `chronicle-authoring` lowers the *compute*
  containers from `reverser.uproc`, but does not yet emit ingress-headed or
  subscription-headed chains, so the graph YAMLs (codecs, connectors, wiring)
  are hand-authored — the same precedent as `examples/oci_registry`. When the
  lowering grows those ends, these YAMLs become its regression fixtures.
