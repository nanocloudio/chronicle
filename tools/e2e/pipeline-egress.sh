#!/usr/bin/env bash
# PIPELINE -> ORDERED-ACK EXCHANGE SURFACE -> QUANTUM mqtt_sink, end to end.
#
# What this proves: the pipeline publishes without naming a protocol. There is no `encode` program rendering an MQTT frame —
# the pipeline emits a `SinkPublish` on `publish_out` and the provider owns
# everything else. Swapping `mqtt_sink` for `kafka_sink` in the graph would
# change the destination and nothing in chronicle.
#
# The broker PUBACKs (QoS 1), so the pipeline's in-flight window is genuinely
# released — the ack path is exercised, not just the publish.
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
  # {1:"ord-7", 2:250} -> pipeline doubles -> SinkPublish -> mqtt_sink PUBLISH.
  # The payload on the wire is the RECORD FRAME, not a hand-rendered string:
  # the pipeline never rendered an MQTT publish.
  run_hex 02010005006f72642d3702010800fa00000000000000 8 >/dev/null
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
  # pipeline emitted a `Publish` on the surface and the provider owned the
  # protocol.
  want="63752f6f72646572732002010005006f72642d3702010800f401000000000000"
  got=$(xxd -p "$got_file" 2>/dev/null | tr -d '\n')
  if [ -z "$got" ]; then
    no egress "broker received nothing"
  elif [ "$got" = "$want" ]; then
    ok "egress (pipeline -> Publish -> quantum mqtt_sink -> broker, QoS 1 PUBACKed)"
  else
    no egress "payload mismatch:
      want $want
      got  $got"
  fi
else
  no egress "graph build failed"
fi

rm -f "$got_file" "$portfile"
finish
