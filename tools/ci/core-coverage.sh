#!/usr/bin/env bash
# Shipping-core coverage inventory. Every
# `modules/common/*_core.rs` must be one of:
#   (1) directly mounted and tested by a named host suite;
#   (2) compile-only — a pure table transitively mounted by a tested core;
#   (3) explicitly exempt with a reason and a production wrapper E2E;
#   (4) a KNOWN GAP that still needs a suite, tracked with a reason.
# A core that fits none of these fails the build, so a NEW core cannot ship
# without a coverage decision. This mechanises "directly mount every shipping
# core or document a checked exemption."
set -u
here="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$here" || exit 1

# (3) Syscall-bound cores that cannot run in a host harness (they need a live
#     SyscallTable); each is covered end to end by a production wrapper E2E.
declare -A EXEMPT=(
  [syschan_core.rs]="the one production Chan impl over SyscallTable (the only unsafe); host-driven via the ScriptChan fake, exercised live by every *_e2e graph"
  [telemetry_core.rs]="emit helpers over dev_telemetry_* syscalls; covered end to end by tools/e2e/telemetry.sh, which asserts emitted VALUES through a subscribed consumer"
)
# (4) Cores that require provider-fake suites and that
#     do not yet have them. Tracked debt, not silent — printed prominently.
declare -A GAP=()

# A core is TESTED if some host suite include!s it directly.
is_tested() { grep -rql "include!(\"../../../modules/common/$1\")" tests/harness/tests/ 2>/dev/null; }
# A core is a TABLE if it is include!d by another core that is itself TESTED
# (transitively compiled and exercised through that core's suite).
is_table() {
  # any include! of this core (any relative path form) from another core that is
  # itself directly tested by a host suite.
  for host in $(grep -rlE "include!\(\"[^\"]*/$1\"\)" modules/common/*_core.rs 2>/dev/null); do
    [ "$(basename "$host")" = "$1" ] && continue
    is_tested "$(basename "$host")" && return 0
  done
  return 1
}

fail=0; ntested=0; ntable=0; nexempt=0; ngap=0
for path in modules/common/*_core.rs; do
  c="$(basename "$path")"
  if is_tested "$c"; then ntested=$((ntested+1));
  elif is_table "$c"; then ntable=$((ntable+1)); echo "  table    $c (compiled+exercised via a tested core)";
  elif [ -n "${EXEMPT[$c]:-}" ]; then nexempt=$((nexempt+1)); echo "  exempt   $c — ${EXEMPT[$c]}";
  elif [ -n "${GAP[$c]:-}" ]; then ngap=$((ngap+1)); echo "  GAP      $c — ${GAP[$c]}";
  else echo "  FAIL     $c is UNCLASSIFIED: add a host suite, or classify it (table/exempt/gap) in core-coverage.sh"; fail=1;
  fi
done

echo "  ---- $ntested tested, $ntable table, $nexempt exempt, $ngap tracked-gap"
if [ "$fail" -eq 0 ]; then
  echo "  PASS  core-coverage: every shipping core is tested, transitively covered, exempt, or a tracked gap"
fi
exit $fail
