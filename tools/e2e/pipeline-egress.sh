#!/usr/bin/env bash
# PIPELINE -> EXCHANGE CONTRACT -> QUANTUM mqtt_sink, end to end.
#
# What this proves: the pipeline publishes without naming a protocol. There is
# no `encode` program rendering an MQTT frame — the pipeline asks a PUBLISH
# exchange on `request_out` and the provider owns everything else. Swapping
# `mqtt_sink` for `kafka_sink` in the graph would change the destination and
# nothing in chronicle.
#
# The broker PUBACKs (QoS 1) and the sink answers 200 only then, so the answer
# record the pipeline emits is the durable acceptance, asserted exactly — the
# answer path is exercised, not just the publish.
. "$(dirname "$0")/../lib.sh"
modules_ready || { no egress "fluxor modules build failed"; finish; exit; }

got_file=$(mktemp)
portfile=$(mktemp)
python3 tools/support/mqtt_broker.py "$got_file" >"$portfile" 2>/dev/null &
BROKER_PID=$!
E2E_PIDS="$E2E_PIDS $BROKER_PID"
disown "$BROKER_PID" 2>/dev/null || true

for _ in $(seq 1 60); do
  MQTT_PORT=$(cat "$portfile" 2>/dev/null)
  [ -n "$MQTT_PORT" ] && break
  sleep 0.25
done

if [ -z "${MQTT_PORT:-}" ]; then
  no egress "broker did not start"
elif build_graph examples/pipeline_egress/linux.yaml "s|authority: \"127.0.0.1:[0-9]*\"|authority: \"127.0.0.1:$MQTT_PORT\"|"; then
  # {1:"ord-7", 2:250} -> pipeline doubles -> PUBLISH exchange -> mqtt_sink.
  # The payload on the wire is the RECORD FRAME, not a hand-rendered string:
  # the pipeline never rendered an MQTT publish. The answer comes back as a
  # record holding only the exchange status: {253: 200}.
  answer=$(run_hex 02010005006f72642d3702010800fa00000000000000 8)
  for _ in $(seq 1 20); do
    [ -s "$got_file" ] && break
    sleep 0.25
  done
  # Assert the EXACT bytes, topic and payload, not just that something
  # arrived:
  #
  #   63752f6f7264657273        "cu/orders"
  #   20                        separator
  #   02010005006f72642d37      record: {1: "ord-7"}
  #   02010800 f401000000000000 {2: 500}  <- 250 doubled by the IR stage
  #
  # The payload is the RECORD FRAME. Nothing rendered an MQTT publish: the
  # pipeline asked an exchange and the provider owned the protocol.
  want="63752f6f72646572732002010005006f72642d3702010800f401000000000000"
  got=$(xxd -p "$got_file" 2>/dev/null | tr -d '\n')
  if [ -z "$got" ]; then
    no egress "broker received nothing"
  elif [ "$got" != "$want" ]; then
    no egress "payload mismatch:
      want $want
      got  $got"
  elif [ "$answer" != "01fd010800c800000000000000" ]; then
    no egress "answer record: want {253: 200} (01fd010800c800000000000000), got '$answer'"
  else
    ok "egress (pipeline -> PUBLISH -> quantum mqtt_sink -> broker, PUBACKed and answered 200)"
  fi
else
  no egress "graph build failed"
fi

rm -f "$got_file" "$portfile"
finish
