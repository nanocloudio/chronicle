#!/usr/bin/env bash
# Decision E2E: first-hit routing on device. The `decision` param is the
# compiler-emitted [nrules][when,outcome][default] container: amount>1000 ->
# {1:100}, else -> {1:0}. Typed frame in, typed frame out.
. "$(dirname "$0")/../lib.sh"
modules_ready || { no decision "fluxor modules build failed"; finish; exit; }

build_graph examples/decision/linux.yaml || { no decision build; finish; exit; }

# {1:2000} -> rule hits -> {1:100}
got=$(run_hex 0101010800d007000000000000 4)
[ "$got" = "01010108006400000000000000" ] && ok "decision routes 2000 -> 100" \
  || no "decision hit" "got=$got"

# {1:500} -> default -> {1:0}
got=$(run_hex 0101010800f401000000000000 4)
[ "$got" = "01010108000000000000000000" ] && ok "decision defaults 500 -> 0" \
  || no "decision default" "got=$got"

finish
