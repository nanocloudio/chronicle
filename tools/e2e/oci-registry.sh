#!/usr/bin/env bash
# OCI REGISTRY, both tiers, as a CI gate.
#
# `examples/oci_registry/run.sh --verify` asserts the whole chain: discovery
# served from a seeded object, a blob PUT into tier 1 and read back THROUGH the
# registry (HTTP → engines → SigV4 → storage and back), and a missing object
# carried as the store's own 404.
#
# It runs here because an example only stays correct while something checks it:
# these graphs name params and ports owned by sibling projects, and a rename
# there leaves a graph that still builds and no longer works.
#
# This wrapper adapts the runner's exit status to the harness. The runner owns
# the assertions; duplicating them here would give two places to update.
. "$(dirname "$0")/../lib.sh"
modules_ready || { no oci-registry "fluxor modules build failed"; finish; exit; }

if ! command -v curl >/dev/null 2>&1; then
  no oci-registry "curl is not available"
  finish
  exit
fi

log=$(mktemp)
if timeout 240 bash examples/oci_registry/run.sh --verify >"$log" 2>&1; then
  # Report what the runner checked, so a pass names its evidence.
  n=$(grep -c '^  ok   ' "$log" 2>/dev/null || echo 0)
  ok "oci-registry (both tiers, $n checks: discovery, blob round trip, 404 passthrough)"
else
  no oci-registry "run.sh --verify failed:
$(grep -E '^  FAIL|^error|died|never' "$log" 2>/dev/null | head -4)"
fi
rm -f "$log"

finish
