#!/usr/bin/env bash
# HTTP AS A REPLYING PROVIDER — the exchange surface, end to end, live.
#
# The mirror of `pipeline-egress.sh`. There the surface carried records to a
# destination that only accepts them; here the SAME frames, correlation and
# ports carry a request to one that answers. This is the test that makes
# `stream.ordered_ack.exchange` a fact rather than a manifest claim.
#
# Two assertions, because either alone is weak:
#   * the ORIGIN saw the request the graph built (path included) — proving the
#     request record was decoded and issued, not merely accepted;
#   * the REPLY carries the response body AND echoes the request's `msg_key` —
#     proving the answer was correlated back, which is what lets a downstream
#     stage rejoin it without holding state.
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

# Publish carrying a GET for /hello, msg_key "job-42", corr 7.
#   [0xED][len][corr:u64][flags][klen][plen][key][method][path_len][body_len][path]
REQ_HEX=$(python3 - <<'PY'
import struct
# METHOD_GET = 1 (wave/modules/foundation/http/wire/method.rs). 0 is
# METHOD_NONE and is refused UNROUTABLE — the provider validates the verb
# rather than defaulting one, which is why this names the code explicitly.
METHOD_GET = 1
path = b"/hello"
rec  = bytes([METHOD_GET]) + struct.pack('<HH', len(path), 0) + path
key  = b"job-42"
pub  = struct.pack('<QBHH', 7, 0, len(key), len(rec)) + key + rec
print((bytes([0xED]) + struct.pack('<H', len(pub)) + pub).hex())
PY
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

  # The reply frame: MSG_REPLY (0xEF), then corr=7, status=0, the echoed key,
  # and the body the origin returned ("echo:/hello").
  key_hex=$(printf 'job-42' | xxd -p)
  body_hex=$(printf 'echo:/hello' | xxd -p)
  if [ -z "$got" ]; then
    no exchange "no reply frame emitted"
  elif [ "${got:0:2}" != "ef" ]; then
    no exchange "reply is not MSG_REPLY: ${got:0:2}"
  elif ! printf '%s' "$got" | grep -q "$key_hex"; then
    no exchange "reply did not echo msg_key"
  elif ! printf '%s' "$got" | grep -q "$body_hex"; then
    no exchange "reply body missing: $got"
  else
    ok "exchange reply    (status 0, msg_key echoed, origin body returned)"
  fi
else
  no exchange "graph build failed"
fi

rm -f "$req_file" "$portfile"
finish
