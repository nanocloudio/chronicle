#!/usr/bin/env bash
# AN ANSWERING NODE'S FAILED RECORD IS ANSWERED, 500. `reply` answers exchanges
# named by the records it is handed; one whose record fails in the stages must
# still be answered, or its requester waits out a deadline for nothing.
#
# {2: 1, 3: "/x", 4: ""} -> ask -> front -> reply (stage fails) -> {253: 500}
. "$(dirname "$0")/../lib.sh"
modules_ready || { no refusal "fluxor modules build failed"; finish; exit; }

build_graph examples/exchange_refusal/linux.yaml || { no refusal "graph build"; finish; exit; }

got=$(run_hex 03020108000100000000000000030002002f7804000000 6)
[ "$got" = "01fd010800f401000000000000" ] \
  && ok "refusal (a failed record's exchange is answered 500, not left waiting)" \
  || no refusal "want {253: 500} (01fd010800f401000000000000), got '$got'"

finish
