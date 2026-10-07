#!/usr/bin/env bash
# THE REVERSER, all four tiers, as a CI gate.
#
# `examples/reverser/run.sh --verify` drives one message through the whole
# loop: an HTTP request answered only after its INSERT committed in lattice,
# the committed row carried by CDC to the MQTT broker, reversed by pipeline B,
# stored, and read back as HTML from pipeline C.
#
# It runs here because an example only stays correct while something checks it:
# these graphs name params and ports owned by four sibling projects, and a
# rename in any of them leaves a graph that still builds and no longer works.
#
# This wrapper adapts the runner's exit status to the harness. The runner owns
# the assertions; duplicating them here would give two places to update.
. "$(dirname "$0")/../lib.sh"
modules_ready || { no reverser "fluxor modules build failed"; finish; exit; }

if ! command -v curl >/dev/null 2>&1; then
  no reverser "curl is not available"
  finish
  exit
fi

log=$(mktemp)
if timeout 900 bash examples/reverser/run.sh --verify >"$log" 2>&1; then
  ok "reverser ($(grep -m1 '^PASS' "$log" | sed 's/^PASS: //'))"
else
  no reverser "run.sh --verify failed:
$(grep -E '^FAIL|^error|never' "$log" 2>/dev/null | head -4)"
fi
# The runner tears its tiers down on exit; this catches one that did not.
bash examples/reverser/run.sh --stop >/dev/null 2>&1 || true
rm -f "$log"

finish
