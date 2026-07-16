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
modules owned by the domain project (wave for HTTP/WS/RTP/SIP/SMTP, quantum for
MQTT/Kafka/AMQP/NATS, lattice for the databases, loam for S3). Chronicle composes
those as graph nodes.

Historical note: an earlier design *did* try to be a connector — protocols
assembled purely from these opcodes, with a text front end
(`chronicle_canonical::wire`) and `.uproc` `connector { … }` blocks compiling
templates into them. That path was retired: a reply-code-driven, multi-round-trip
session is not a stateless codec. The front end and the connector blocks are gone;
the VMs remain, because message framing at a pipeline edge is genuinely in scope.

## Authoring a program today

There is no template language. `encode` / `decode` programs are assembled from
the opcode constants below and hex-encoded into the param — see
[`modules/app/pipeline/tests/ser.rs`](../../modules/app/pipeline/tests/ser.rs)
and [`deser.rs`](../../modules/app/pipeline/tests/deser.rs) for worked
programs, and `examples/mqtt_sink/linux.yaml` for one in service: it renders the
`[topic_len:u8][topic]<id>=<amount>` publish frame that quantum's `mqtt_client`
takes on `app_in`.

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
| `RGN_LEN` | `width:u8`, `endian:u8`, `delta:i8` | close: prepend a fixed-width length |
| `RGN_VARINT` | — | close: prepend an unsigned varint length |
| `RGN_ZIGVARINT` | — | close: prepend a zig-zag signed varint length |
| `RGN_CRC` | — | close: append CRC-32C over the region |
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
| `SEEK` | `len:u16`, bytes | advance past a multi-byte sequence |
| `TAKE` | `n:u16` | read `n` bytes |
| `TAKEN` | — | read `Int(top-of-stack)` bytes |
| `REST` | — | read to end of input |
| `INT` | `width:u8`, `endian:u8` | read a binary integer |
| `DECINT` | — | read ASCII decimal digits |
| `H2MSG` | — | walk HTTP/2 frames to DATA; push its gRPC message |
| `PBFIELD` | `n:u32` | pop a protobuf message, push field `n` (absent → null) |
| `SET_FIELD` | `n:u32` | pop a value into a record field |
| `FINISH_MSG` | — | terminate |

Both VMs are bounded and never-panic: malformed input returns a structured error,
proven by the deterministic fuzzing in
[`robustness.rs`](../../modules/app/aggregation/tests/robustness.rs).

## Related documentation

- [`../bytecode_policy.md`](../bytecode_policy.md) — what the VM may and may not become
- [`../architecture/connectors.md`](../architecture/connectors.md) — how effects bind to sibling-owned provider modules
- [`authoring.md`](authoring.md) — the `.uproc` document and the compute artefacts
