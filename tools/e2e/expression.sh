#!/usr/bin/env bash
# Expression E2E: one evaluator .fmod runs ANY Expression, chosen purely by the
# `program` param — the same input record yields a different field per config.
# Input record {1:"o-1", 2:"cust-42"} as the canonical typed frame
# ([count] then per field [num][type=0 bytes][len:u16][bytes]).
. "$(dirname "$0")/../lib.sh"
modules_ready || { no expression "fluxor modules build failed"; finish; exit; }

IN=02010003006f2d3102000700637573742d3432

build_graph examples/expression/linux.yaml || { no "expr customer" build; finish; exit; }
got=$(run_hex $IN 4)
# order.customer_id (field 2) -> "cust-42"
[ "$got" = "637573742d3432" ] && ok "expression selects customer_id (param A)" \
  || no "expr customer" "got=$got"

build_graph examples/expression/id.yaml || { no "expr id" build; finish; exit; }
got=$(run_hex $IN 4)
# order.id (field 1) -> "o-1": same binary, different program param.
[ "$got" = "6f2d31" ] && ok "expression selects id (same .fmod, param B)" \
  || no "expr id" "got=$got"

finish
