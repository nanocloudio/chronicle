#!/usr/bin/env bash
# THE CARRY ACROSS AN EXCHANGE, live: a record's context (field 254) leaves as
# the publish's msg_key and comes back on the reply, restored as 254 on the
# reply record — with the decoded body (5) and the exchange status (253).
#
# Asserted exactly: the reply record is built independently here from the
# documented frame layout, and must equal what the graph emitted byte for byte.
#
# The bound is asserted from both sides, in one graph: a carry of exactly
# KEY_MAX bytes must round-trip, and one byte more must be REFUSED — seen as the
# pipeline's `carry_refused` counter reaching 1, not merely as silence, since a
# graph that emitted nothing for any other reason would also be silent.
. "$(dirname "$0")/../lib.sh"
modules_ready || { no carry "fluxor modules build failed"; finish; exit; }

mkdir -p "$E2E_GEN"
req_file=$E2E_GEN/carry-req.txt
portfile=$E2E_GEN/carry-port.txt
python3 tools/support/http_echo.py "$req_file" >"$portfile" 2>/dev/null &
SRV_PID=$!
E2E_PIDS="$E2E_PIDS $SRV_PID"
disown "$SRV_PID" 2>/dev/null || true
for _ in $(seq 1 60); do
  HTTP_PORT=$(cat "$portfile" 2>/dev/null)
  [ -n "$HTTP_PORT" ] && break
  sleep 0.25
done

# The origin gives up after 30 s without a connection, and the graph build can
# take longer than that. A bare connect-and-close resets that clock without
# recording a request (the origin records only a complete request head), so the
# origin's lifetime is this run's, not the build's.
if [ -n "${HTTP_PORT:-}" ]; then
  ( while :; do
      (exec 3<>"/dev/tcp/127.0.0.1/$HTTP_PORT") 2>/dev/null
      sleep 5
    done ) &
  E2E_PIDS="$E2E_PIDS $!"
  disown "$!" 2>/dev/null || true
fi

# frame(fields) -> hex; a field is (number, int | bytes | Msg). KEY_MAX is the
# exchange contract's msg_key ceiling, which the pipeline's KEY_BUF tracks.
read -r REC_HEX WANT_HEX EDGE_HEX EDGE_WANT_HEX BIG_HEX < <(python3 - <<'PY'
import struct
KEY_MAX = 512
class Msg(bytes):
    """A nested message: its own frame, written as type 3."""
def frame(fs):
    out = bytes([len(fs)])
    for n, v in fs:
        if isinstance(v, int):
            out += struct.pack('<BBH', n, 1, 8) + struct.pack('<q', v)
        else:
            ty = 3 if isinstance(v, Msg) else 0
            out += struct.pack('<BBH', n, ty, len(v)) + v
    return out
def carry(n):
    """A context frame of exactly n bytes: 1 count + 12 (int) + 4 header."""
    c = Msg(frame([(30, 7), (31, b"x" * (n - 17))]))
    assert len(c) == n
    return c
ctx = Msg(frame([(30, 7), (31, b"req-7")]))
rec = frame([(2, 1), (3, b"/carry"), (4, b""), (254, ctx)])
want = frame([(5, b"echo:/carry"), (254, ctx), (253, 0)])
edge_ctx = carry(KEY_MAX)
edge = frame([(2, 1), (3, b"/edge"), (4, b""), (254, edge_ctx)])
edge_want = frame([(5, b"echo:/edge"), (254, edge_ctx), (253, 0)])
big = frame([(2, 1), (3, b"/big"), (4, b""), (254, carry(KEY_MAX + 1))])
print(rec.hex(), want.hex(), edge.hex(), edge_want.hex(), big.hex())
PY
)

# The shipped graph plus `observe`, which drains the telemetry ring to stderr
# as MON_METRIC lines — how the refusal is seen. It has no ports, so the wiring
# is untouched.
OBSERVE='/^wiring:/i\  - name: observe\n    params:\n      interval_ms: 100'

# carry_refused is pipeline metric id 28 (modules/app/pipeline/manifest.toml).
# MON_METRIC names a module by its instance index, which the runtime's own
# load log gives (`module N caps: …` then `loaded pipeline`) — read, not assumed.
mod_index() { # <stderr file> <module name>
  awk -v m="$2" '/\[inst\] module [0-9]+ caps:/ { for (i = 1; i < NF; i++) if ($i == "module") n = $(i + 1) }
                 $0 ~ ("loaded " m "$") { print n; exit }' "$1" 2>/dev/null
}
refused_count() { # <stderr file> -> the highest carry_refused value reported
  local m
  m=$(mod_index "$1" pipeline)
  [ -n "$m" ] || return 0
  grep -oE "MON_METRIC mod=$m id=28 kind=1 val=[0-9]+" "$1" 2>/dev/null \
    | grep -oE '[0-9]+$' | sort -n | tail -1
}

# run_carry <input_hex> <stderr file>: one run long enough for the second
# telemetry publish (5 s), stdout as hex.
run_carry() {
  printf '%s' "$1" | xxd -r -p \
    | timeout -k 2 8 "$FL_BIN" --config "$CFG" --modules "$MODS" 2>"$2" \
    | xxd -p | tr -d '\n'
}

if [ -z "${HTTP_PORT:-}" ]; then
  no carry "origin did not start"
elif build_graph examples/exchange_carry/linux.yaml \
       "s|authority: \"127.0.0.1:[0-9]*\"|authority: \"127.0.0.1:$HTTP_PORT\"|" \
       "$OBSERVE"; then
  got=$(run_hex "$REC_HEX" 12)
  saw=$(cat "$req_file" 2>/dev/null)
  case "$saw" in
    "GET /carry"*) ok "the origin saw the request the record encoded" ;;
    *)             no carry "origin saw '$saw'" ;;
  esac
  if [ "$got" = "$WANT_HEX" ]; then
    ok "the reply record carries the body, the SAME carry, and status 0"
  else
    no carry "reply record: want $WANT_HEX, got '$got'"
  fi

  # AT the bound: a carry of exactly KEY_MAX bytes goes out and comes back
  # whole, and nothing is counted refused. Without this, the refusal below
  # could be a bound set one byte too low.
  : >"$req_file"
  edge=$(run_carry "$EDGE_HEX" "$E2E_GEN/carry-edge.err")
  saw=$(cat "$req_file" 2>/dev/null)
  edge_refused=$(refused_count "$E2E_GEN/carry-edge.err")
  if [ "$edge" = "$EDGE_WANT_HEX" ] && [ "${saw#GET /edge}" != "$saw" ] \
     && [ "${edge_refused:-}" = 0 ]; then
    ok "a carry of exactly KEY_MAX bytes round-trips whole"
  else
    no carry "at-limit carry: origin saw '$saw', carry_refused='${edge_refused:-none}', output '$edge'"
  fi

  # PAST the bound: refused before the send — never truncated, since a clipped
  # context would rejoin the wrong request. Nothing reaches the origin, nothing
  # is emitted, and the refusal is COUNTED.
  : >"$req_file"
  big=$(run_carry "$BIG_HEX" "$E2E_GEN/carry-big.err")
  saw=$(cat "$req_file" 2>/dev/null)
  big_refused=$(refused_count "$E2E_GEN/carry-big.err")
  if [ -z "$big" ] && [ -z "$saw" ] && [ "${big_refused:-}" = 1 ]; then
    ok "a carry one byte past KEY_MAX is refused and counted, not sent and not truncated"
  else
    no carry "oversized carry: carry_refused='${big_refused:-none}', output '$big', origin saw '$saw'"
  fi
else
  no carry "graph build failed"
fi
finish
