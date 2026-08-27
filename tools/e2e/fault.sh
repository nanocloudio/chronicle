#!/usr/bin/env bash
# Fault-discipline E2E. The steady-state module
# WRAPPERS must fail closed on a broken CONFIG: a param that was provided but is
# unusable (bad hex, a truncated container) is a configuration fault — the module
# names it once at error level and then REFUSES INPUT (records do not silently
# vanish through a half-configured node). The host step-core suites cover runtime
# faults (a malformed record mid-stream); only a live graph covers the wrapper's
# module_new refusal, so this asserts it end to end: the named FAULT is logged and
# NOTHING is delivered downstream.
. "$(dirname "$0")/../lib.sh"
modules_ready || { no fault "modules build failed"; finish; exit; }

# A valid decision input record ({1:2000}); a healthy node would route it.
REC=0101010800d007000000000000

check_fault() { # <name> <graph> <sed-expr> <fault-substring>
  local name=$1 graph=$2 sed=$3 want=$4
  build_graph "$graph" "$sed" || { no "$name" "graph build failed"; return; }
  local err out
  err=$(mktemp)
  out=$(printf '%s' "$REC" | xxd -r -p \
    | timeout -k 2 4 "$FL_BIN" --config "$CFG" --modules "$MODS" 2>"$err" | xxd -p | tr -d '\n')
  if ! grep -qF "$want" "$err"; then
    no "$name fault-log" "expected '$want' in stderr"; rm -f "$err"; return
  fi
  if [ -n "$out" ]; then
    no "$name refusal" "a faulted node delivered output: $out"; rm -f "$err"; return
  fi
  ok "$name faults on a broken param and refuses input (delivers nothing)"
  rm -f "$err"
}

# Each module: break its driving param to invalid hex, assert its named FAULT.
check_fault decision   examples/decision/linux.yaml \
  's|decision: "[0-9a-f]*"|decision: "zzzz"|'   "[decision] FAULT"
check_fault expression examples/expression/linux.yaml \
  's|program: "[0-9a-f]*"|program: "zzzz"|'     "[expr] FAULT"
check_fault pipeline   examples/pipeline/linux.yaml \
  's|ir_stages: "[0-9a-f]*"|ir_stages: "zzzz"|' "[pipeline] FAULT"

finish
