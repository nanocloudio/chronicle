#!/usr/bin/env bash
# PIPELINE -> EXCHANGE CONTRACT -> PIPELINE, end to end: the provider role.
#
# `first` is a requester; `second` is a `stream.ordered_ack.sink` provider and
# answers each request once its result is accepted downstream. Two things are
# asserted, and the count is the point:
#   * every record comes out transformed by BOTH stages (4x) in order — the
#     request body was taken as the record;
#   * MORE records than `first`'s in-flight window (WINDOW = 8) all arrive.
#     Without answers the window would fill after 8 and the 9th would never
#     leave, so a 9th output is the proof that `second` answered.
. "$(dirname "$0")/../lib.sh"
modules_ready || { no chain "fluxor modules build failed"; finish; exit; }

N=9
# {1:"ord-<i>", 2:250} as a typed frame; 250 doubled twice is 1000 (0x3e8).
in=""; want=""
for i in $(seq 1 $N); do
  id=$(printf 'ord-%d' "$i" | xxd -p)
  in="${in}0201000500${id}02010800fa00000000000000"
  want="${want}0201000500${id}02010800e803000000000000"
done

if build_graph examples/pipeline_chain/linux.yaml; then
  got=$(run_hex "$in" 6)
  if [ "$got" = "$want" ]; then
    ok "chain: $N records through two pipelines over the exchange, all answered and delivered (window is $((N-1)))"
  else
    no chain "expected $N x {id, 1000}:
      want $want
      got  $got"
  fi
else
  no chain "graph build failed"
fi
finish
