#!/usr/bin/env bash
# Telemetry E2E: a chronicle module's
# DECLARED instruments actually reach a subscribed consumer AND carry the right
# VALUES end to end — not merely that some line was emitted.
#
# The graph wires `observe` (fluxor's console exporter) beside `decision`.
# `observe` subscribes to the kernel telemetry ring (TLM_SUBSCRIBE), which flips
# the producer-side enabled gate so `decision`'s throttled `tlm_tick` emits; it
# then drains each record (TLM_DRAIN) and renders it as a `MON_METRIC` line.
#
# We feed exactly ONE record (routed 2000 -> 100 and delivered to cli_out) and
# assert the Accounting block reports it: after the module backdates its
# publish clock the FIRST tick fires at startup (all zero), and a SECOND tick
# ~5 s later — after the record has been admitted, processed and delivered —
# carries the real counts. So we run long enough for that second publish and
# assert the delivered-record values, which proves the whole accounting path
# (admit -> succeed -> deliver) and the module_mode gauge end to end.
#
# Metric ids are the manifest array index (accounting-order.sh pins the layout):
#   id 2  inputs_observed   id 5  inputs_succeeded
#   id 10 outputs_delivered id 14 module_mode (kind 2 gauge; Ready == 1)
. "$(dirname "$0")/../lib.sh"
modules_ready || { no telemetry "fluxor modules build failed"; finish; exit; }

build_graph examples/telemetry_probe/linux.yaml || { no telemetry "build"; finish; exit; }

err=$(mktemp)
# One record; run past the 5 s publish interval so the post-processing tick lands.
echo -n "0101010800d007000000000000" | xxd -r -p \
  | timeout 8 "$FL_BIN" --config "$CFG" --modules "$MODS" 2>"$err" >/dev/null

nmetric=$(grep -c "MON_METRIC" "$err")

# module_mode (id 14, kind 2 = up-down) reporting Ready (1): the module is neither
# faulted, awaiting config, nor output-blocked — the single most operationally
# important instrument, and proof the record path is healthy.
if grep -qE "MON_METRIC mod=[0-9]+ id=14 kind=2 val=1" "$err"; then
  ok "decision publishes module_mode=Ready to a subscribed consumer"
else
  no telemetry "no module_mode=Ready gauge (id 14) in $nmetric MON_METRIC lines"
fi

# The Accounting VALUES for the one delivered record: observed, succeeded and
# delivered each reach 1. This is the account-for-every-received-record exit gate
# proven end to end, not just that the instruments were named.
check_val() { # id, name
  if grep -qE "MON_METRIC mod=[0-9]+ id=$1 kind=1 val=1" "$err"; then
    ok "$2 (id $1) == 1 for the one delivered record"
  else
    no telemetry "$2 (id $1) never reported 1 in $nmetric MON_METRIC lines"
  fi
}
check_val 2 "inputs_observed"
check_val 5 "inputs_succeeded"
check_val 10 "outputs_delivered"

rm -f "$err"
finish
