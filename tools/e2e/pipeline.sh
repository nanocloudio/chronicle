#!/usr/bin/env bash
# Pipeline E2E via the IR-at-load path: the `ir_stages` param ships checked IR
# and the .fmod lowers it on device (lower_stages, cost re-derived). Two stages:
# normalize {id,amount} then enrich {id,amount,doubled}.
# Input {1:"o-1", 2:"cust-9", 3:250} -> output {1:"o-1", 2:250, 3:500}.
. "$(dirname "$0")/../lib.sh"
modules_ready || { no pipeline "fluxor modules build failed"; finish; exit; }

build_graph examples/pipeline/linux.yaml || { no pipeline build; finish; exit; }

got=$(run_hex 03010003006f2d3102000600637573742d3903010800fa00000000000000 4)
[ "$got" = "03010003006f2d3102010800fa0000000000000003010800f401000000000000" ] \
  && ok "pipeline lowers IR at load, normalize+enrich doubles amount" \
  || no pipeline "got=$got"

finish
