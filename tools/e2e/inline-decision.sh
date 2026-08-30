#!/usr/bin/env bash
# A DECISION AS A PIPELINE STAGE — equivalence with the standalone node.
#
# A decision is one input, one output, and the same decode/evaluate/encode step
# a compute stage is; only the program format differs. Run as a stage it saves
# the two channel hops either side of a `decision` node, which is only worth
# having if the routing is otherwise identical.
#
# So the assertion is EQUIVALENCE, not merely "it produced something": the same
# decision program and the same inputs must give byte-identical output whether
# the routing happens in its own node or inside the pipeline. Both arms of the
# policy are driven, because a stage that only ever took the default would
# agree with the node by accident.
. "$(dirname "$0")/../lib.sh"
modules_ready || { no inline "fluxor modules build failed"; finish; exit; }

# {1: 2000} routes to {1: 100}; {1: 500} routes to {1: 0}.
run_case() {
  local hex=$1 want=$2 label=$3
  build_graph examples/decision/linux.yaml || { no inline "decision build"; return; }
  local node; node=$(run_hex "$hex" 8)
  build_graph examples/inline_decision/linux.yaml || { no inline "inline build"; return; }
  local stage; stage=$(run_hex "$hex" 8)

  if [ -z "$node" ]; then
    no inline "$label: standalone node produced nothing"
  elif [ "$node" != "$want" ]; then
    no inline "$label: node produced '$node', expected '$want'"
  elif [ "$stage" != "$node" ]; then
    no inline "$label: inline stage diverged
      node   $node
      inline $stage"
  else
    ok "inline decision $label (node and stage agree: $want)"
  fi
}

run_case 0101010800d007000000000000 01010108006400000000000000 "rule fires"
run_case 0101010800f401000000000000 01010108000000000000000000 "default fires"

finish
