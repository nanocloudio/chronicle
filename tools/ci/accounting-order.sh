#!/usr/bin/env bash
# Accounting-order guard. The common accounting taxonomy is
# emitted BY INDEX: `acct_emit` publishes the 14 baseline instruments as ids
# 0..13, and each module's own instruments follow at id 14+. That only holds if
# every steady-state module's manifest FRONT-LOADS the 14 baseline names in the
# canonical order (the field order in accounting_core.rs), so the array index
# equals the emit id. A reordered or renamed baseline entry would silently
# mislabel every downstream metric — a collector would read `outputs_delivered`
# under the name `inputs_rejected`. This check makes that drift a build failure.
set -u
here="$(cd "$(dirname "$0")/../.." && pwd)"
core="$here/modules/common/accounting_core.rs"

# The canonical baseline names, extracted from ACCT_METRIC_NAMES so this script
# and the core can never disagree: pull the string literals between the array's
# `[` and `]`.
mapfile -t canon < <(awk '
  /ACCT_METRIC_NAMES: \[/{grab=1; next}
  grab && /\];/{grab=0}
  grab{ while (match($0, /"[^"]+"/)) { s=substr($0,RSTART+1,RLENGTH-2); print s; $0=substr($0,RSTART+RLENGTH) } }
' "$core")
n=${#canon[@]}
if [ "$n" -ne 14 ]; then
  echo "  FAIL  accounting-order: expected 14 canonical baseline names, found $n in accounting_core.rs"
  exit 1
fi

fail=0
for m in expression decision pipeline aggregation; do
  man="$here/modules/app/$m/manifest.toml"
  [ -f "$man" ] || { echo "  FAIL  accounting-order: $m manifest missing"; fail=1; continue; }
  # The metrics array is `metrics = [ ... ]`, possibly multi-line. Slice from the
  # `metrics = [` to the closing `]`, then pull every quoted token in order.
  mapfile -t got < <(awk '
    /metrics *= *\[/{grab=1}
    grab{ while (match($0, /"[^"]+"/)) { s=substr($0,RSTART+1,RLENGTH-2); print s; $0=substr($0,RSTART+RLENGTH) } }
    grab && /\]/{exit}
  ' "$man")
  # Compare the first 14 entries to the canonical order.
  bad=0
  for i in $(seq 0 13); do
    if [ "${got[$i]:-<<missing>>}" != "${canon[$i]}" ]; then
      echo "  FAIL  accounting-order: $m metric[$i] is '${got[$i]:-<<missing>>}', expected '${canon[$i]}'"
      bad=1; fail=1
    fi
  done
  [ "$bad" -eq 0 ] && echo "  ok    $m front-loads the 14 baseline instruments in canonical order"
done

if [ "$fail" -eq 0 ]; then
  echo "  PASS  accounting-order: every steady-state module's manifest agrees with acct_emit's id layout"
fi
exit $fail
