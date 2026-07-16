#!/usr/bin/env bash
# SMTP submission E2E, self-contained (no container runtime). The smtp .fmod is
# WAVE's — Wave owns protocol mechanics, Conclave owns message meaning — and
# chronicle composes it as a graph node. `support/smtp_sink.py` is the
# counterparty: it speaks the lockstep ESMTP the module drives and captures the
# delivered message, so the assertion is both the module's status line AND the
# message arriving intact.
#
# The sink binds port 0 and reports the port it got, so there is no free-port
# guess to race and concurrent runs cannot collide.
. "$(dirname "$0")/../lib.sh"
modules_ready || { no smtp "fluxor modules build failed"; finish; exit; }

inbox=$(mktemp)
portfile=$(mktemp)
python3 tools/support/smtp_sink.py "$inbox" >"$portfile" 2>/dev/null &
SINK_PID=$!
E2E_PIDS="$E2E_PIDS $SINK_PID"
# Off the job table: cleanup kills it, and the shell's async "Killed" notice
# for a helper we deliberately stopped is noise in the test output.
disown "$SINK_PID" 2>/dev/null || true

# Wait for the sink to announce its port.
for _ in $(seq 1 60); do
  SMTP_PORT=$(cat "$portfile" 2>/dev/null)
  [ -n "$SMTP_PORT" ] && break
  sleep 0.25
done

if [ -z "${SMTP_PORT:-}" ]; then
  no smtp "sink did not start"
elif build_graph examples/smtp_sink/linux.yaml \
       "s|endpoint: \"[0-9a-f]*\"|endpoint: \"$(endpoint_hex "$SMTP_PORT")\"|"; then
  status=$(run_text "" 8)
  # The sink serves until killed; poll for the delivered message.
  for _ in $(seq 1 20); do
    [ -s "$inbox" ] && break
    sleep 0.25
  done
  got=$(cat "$inbox")
  if want "$status" "smtp: delivered" && want "$got" "hello from chronicle"; then
    ok "smtp   (composed wave smtp.fmod -> message delivered intact)"
  else
    no smtp "status='$status' delivered=$(printf %s "$got" | wc -c) bytes"
  fi
else
  no smtp build
fi

rm -f "$inbox" "$portfile"
finish
