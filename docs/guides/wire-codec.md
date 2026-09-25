# The byte-codec VMs (`ser` / `rd`)

Chronicle carries two byte-level VMs beside the expression evaluator: **`ser`**
builds a byte string from a record, and **`rd`** parses bytes back into a record.
They run inside the generic `pipeline` module as its `encode` / `decode` params —
hex bytecode, like every other param — so a record↔wire framing is data, not a
build.

Sources, `include!`d verbatim by both the host harness and the `.fmod`:
[`modules/common/ser_core.rs`](../../modules/common/ser_core.rs) and
[`deser_core.rs`](../../modules/common/deser_core.rs).

## Scope: framing, not protocols

These VMs frame a *message*. They do **not** speak a *protocol* — no handshake,
no reply-dependent state, no round trips. That distinction is load-bearing and
[`bytecode_policy.md`](../bytecode_policy.md) fixes it: protocol logic is
permanently out of scope for the VM and lives in compiled per-protocol `.fmod`
modules owned by the domain project (wave for HTTP/WS/RTP/SIP/SMTP/S3, quantum for
MQTT/Kafka/AMQP/NATS, lattice for the databases). Chronicle composes
those as graph nodes.

## Authoring a program

There is no template language. `encode` / `decode` programs are assembled from
the opcode constants below and hex-encoded into the param — see
[`tests/harness/tests/pipeline_suites/ser.rs`](../../tests/harness/tests/pipeline_suites/ser.rs)
and [`deser.rs`](../../tests/harness/tests/pipeline_suites/deser.rs) for worked
programs, and `examples/identity_provider/chronicle_authorize.yaml` for one in
service: it renders an HTTP `POST /oauth/authorize` request, length-framed body
included, for the IdP's authorize endpoint.

## `ser` — record → bytes

A program is terminated by `ser::FINISH`.

| opcode | operands | effect |
|---|---|---|
| `LIT` | `len:u16`, bytes | append a literal |
| `VAL` | — | pop a value → append (bytes raw, int decimal, bool `0`/`1`) |
| `INT` | `width:u8`, `endian:u8` | pop an int → append as a binary integer |
| `VARINT` | — | pop an int → append as a zig-zag varint |
| `LEN` | — | pop bytes/str → push `Int(byte length)` |
| `RGN_BEGIN` | — | open a region; the matching closer frames it |
| `RGN_END` | — | close: merge into the parent region, no prefix |
| `RGN_LEN` | `width:u8`, `endian:u8`, `delta:i8` | close: prepend a fixed-width length |
| `RGN_VARINT` | — | close: prepend an unsigned varint length |
| `RGN_ZIGVARINT` | — | close: prepend a zig-zag signed varint length |
| `RGN_CRC` | — | close: prepend the region's CRC-32C (4 bytes, big-endian) |
| `RGN_DECLEN` | — | close: prepend `<decimal len>\r\n` |
| `FINISH` | — | terminate |

Values reach the stack the same way the expression VM loads them
(`LOAD_PARAM`/`GET_FIELD`), so a codec inherits the evaluator's field access
without a second mechanism.

## `rd` — bytes → record

A program is terminated by `op::FINISH_MSG`.

| opcode | operands | effect |
|---|---|---|
| `SKIP` | `n:u16` | advance the cursor |
| `LIT` | `len:u16`, bytes | expect a literal (else a structured error) |
| `UNTIL` | `byte:u8` | read up to a delimiter |
| `UNTIL_OPT` | `byte:u8` | as `UNTIL`, but a missing delimiter yields the remainder and an exhausted cursor yields empty |
| `SEEK` | `len:u16`, bytes | advance past a multi-byte sequence |
| `TAKE` | `n:u16` | read `n` bytes |
| `TAKEN` | — | read `Int(top-of-stack)` bytes |
| `REST` | — | read to end of input |
| `INT` | `width:u8`, `endian:u8` | read a binary integer |
| `DECINT` | — | read ASCII decimal digits |
| `H2MSG` | — | walk HTTP/2 frames to DATA; push its gRPC message |
| `PBFIELD` | `n:u32` | pop a protobuf message, push field `n` (absent → null) |
| `FIELD` | `n:u8` | pop a Chronicle record frame, push field `n` (absent → null) |
| `JSONAT` | — | pop a path, pop a document, push the value there (absent → null): the path is data |
| `JSONDEF` | — | pop raw, pop a path, pop a document, push it with `"key":raw` inserted where the path (`k` or `parent.k`) has nothing; built in scratch |
| `PCTDEC` | — | pop a value, push it URL-decoded as a query string is (`%XX`, `+` → space); built in scratch |
| `BEFORE` | `len:u16`, bytes | pop a value, push the part before the first occurrence (the whole value if absent) |
| `AFTER` | `len:u16`, bytes | pop a value, push the part after the first occurrence (empty if absent) |
| `PART` | `delim:u8`, `index:u8` | pop a value, push its index-th part |
| `HDR` | `len:u16`, name | pop an HTTP header block, push that header's trimmed value (empty if absent) |
| `COALESCE` | — | pop b, pop a, push a if non-empty else b |
| `CAT` | `n:u8` | pop n values, push their concatenation; built in scratch |
| `HEX` | — | pop a value, push it as lowercase hex; built in scratch |
| `DROP` | `n:u16` | pop a value, push it without its first n bytes |
| `GATE` | — | pop cond, pop value, push value if cond is non-empty else empty |
| `PARTB` | `delim:u8`, `open:u8`, `close:u8`, `index:u8` | as `PART`, but a delimiter between `open` and `close` does not split; distinct brackets nest, equal ones toggle as quotes do |
| `PBJSON` | — | pop a descriptor, pop a protobuf message, push the message as JSON built in scratch: fields named and typed by the descriptor (`pbk` kinds — string, integer, bool, bytes as base64, message, map, timestamp, wrapper, tagged union — with repeated and omit-when-default flags), unknown fields skipped, absent ones left out; null when it does not decode, nests past `PBJ_MAX_FRAMES` or does not fit |
| `SET_FIELD` | `n:u32` | pop a value into a record field |
| `FINISH_MSG` | — | terminate |

Both VMs are bounded and never-panic: malformed input returns a structured error,
proven by the deterministic fuzzing in
[`robustness.rs`](../../tests/harness/tests/aggregation_suites/robustness.rs).

## Related documentation

- [`../bytecode_policy.md`](../bytecode_policy.md) — what the VM may and may not become
- [`../architecture/connectors.md`](../architecture/connectors.md) — how effects bind to sibling-owned provider modules
- [`authoring.md`](authoring.md) — the `.uproc` document and the compute artefacts
