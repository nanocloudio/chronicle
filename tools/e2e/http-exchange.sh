#!/usr/bin/env bash
# AN HTTP CLIENT AS AN ANSWERING PROVIDER — the exchange contract, end to end,
# live.
#
# The mirror of `pipeline-egress.sh`. There the contract carried records to a
# destination that only accepts them; here the SAME requester, records and
# ports carry a request to one that answers. This is the test that makes
# `stream.ordered_ack.exchange` a fact rather than a manifest claim.
#
# Two assertions, because either alone is weak:
#   * the ORIGIN saw the request the record named (method and path) — proving
#     the field map built the request HEAD, not merely that a record went out;
#   * the ANSWER RECORD is exactly the body, the exchange status and the mapped
#     status and content type — built independently here from the documented
#     frame layout and compared byte for byte.
. "$(dirname "$0")/../lib.sh"
modules_ready || { no exchange "fluxor modules build failed"; finish; exit; }

req_file=$(mktemp)
portfile=$(mktemp)
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

# frame(fields) -> hex; a field is (number, int | bytes).
read -r REQ_HEX WANT_HEX < <(python3 - <<'PY2'
import struct
def frame(fs):
    out = bytes([len(fs)])
    for n, v in fs:
        if isinstance(v, int):
            out += struct.pack('<BBH', n, 1, 8) + struct.pack('<q', v)
        else:
            out += struct.pack('<BBH', n, 0, len(v)) + v
    return out
# METHOD_GET = 1, the exchange contract's code (fluxor
# modules/sdk/contracts/exchange.rs).
METHOD_GET = 1
req = frame([(2, METHOD_GET), (3, b"/hello"), (4, b"")])
want = frame([(5, b"echo:/hello"), (253, 200), (6, 200), (7, b"text/plain")])
print(req.hex(), want.hex())
PY2
)

if [ -z "${HTTP_PORT:-}" ]; then
  no exchange "origin did not start"
elif build_graph examples/http_exchange/linux.yaml \
       "s|authority: \"127.0.0.1:[0-9]*\"|authority: \"127.0.0.1:$HTTP_PORT\"|"; then
  got=$(run_hex "$REQ_HEX" 12)
  for _ in $(seq 1 20); do
    [ -s "$req_file" ] && break
    sleep 0.25
  done
  saw=$(cat "$req_file" 2>/dev/null)

  case "$saw" in
    "GET /hello"*) ok "exchange request  (origin saw the path the graph built)" ;;
    "")            no exchange "origin saw no request" ;;
    *)             no exchange "origin saw '$saw'" ;;
  esac

  if [ "$got" = "$WANT_HEX" ]; then
    ok "exchange answer   (body, status 200 and content type, as mapped fields)"
  elif [ -z "$got" ]; then
    no exchange "no answer record emitted"
  else
    no exchange "answer record: want $WANT_HEX, got '$got'"
  fi
else
  no exchange "graph build failed"
fi

rm -f "$req_file" "$portfile"
finish
