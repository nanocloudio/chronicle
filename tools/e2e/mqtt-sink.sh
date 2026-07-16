#!/usr/bin/env bash
# MQTT sink E2E, self-contained (no container runtime): chronicle COMPOSES
# quantum's mqtt_client (Quantum owns MQTT/Kafka/AMQP). The pipeline doubles
# order.amount (an IR stage), the ser `encode` stage renders the `app_in`
# publish frame, and mqtt_client owns CONNECT/keepalive/PUBLISH.
# `support/mqtt_broker.py` is the counterparty and captures the delivered
# PUBLISH, so the assertion is the exact topic AND payload that arrived.
#
# The broker binds port 0 and reports its port, so there is no free-port guess
# to race and concurrent runs cannot collide.
. "$(dirname "$0")/../lib.sh"
modules_ready || { no mqtt "fluxor modules build failed"; finish; exit; }

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
  no mqtt "broker did not start"
elif build_graph examples/mqtt_sink/linux.yaml "s|broker_port: [0-9]*|broker_port: $MQTT_PORT|"; then
  # {1:"ord-7", 2:250} -> pipeline doubles -> encode -> PUBLISH cu/orders "ord-7=500"
  run_hex 02010005006f72642d3702010800fa00000000000000 8 >/dev/null
  for _ in $(seq 1 20); do
    [ -s "$got_file" ] && break
    sleep 0.25
  done
  got=$(cat "$got_file")
  if [ "$got" = "cu/orders ord-7=500" ]; then
    ok "mqtt   (pipeline -> encode -> composed quantum mqtt_client PUBLISH received)"
  else
    no mqtt "broker got='$got'"
  fi
else
  no mqtt build
fi

rm -f "$got_file" "$portfile"
finish
