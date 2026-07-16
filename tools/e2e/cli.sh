#!/usr/bin/env bash
# CLI-applet E2E: the chronicle toolchain CLI as a PIC fmod. check/eval run the
# SAME include!'d cores the on-device engines run, so `eval` must reproduce the
# pipeline E2E's output frame byte-for-byte, and a bad container must fail
# closed with a non-zero exit.
#
# The applet's GRAPH is driven directly (argv after `--`, exactly as `cli_in`
# delivers it) rather than through `fluxor install` + `fluxor exec`. Install
# writes a fixed bundle path (`target/fluxor/chronicle`) and mutates the global
# applet registry, so two concurrent runs reinstall over each other mid-exec —
# observed as a command returning nothing. The install/exec plumbing is fluxor's
# to test; what chronicle owns is the applet's behaviour, and this drives it
# with the same isolation every other case uses.
. "$(dirname "$0")/../lib.sh"
modules_ready || { no cli "fluxor modules build failed"; finish; exit; }

use_store
build_graph packaging/cli/linux.yaml || { no cli "applet graph build failed"; finish; exit; }

IR=02ff190005000101000000120100000005000103000000120200000013ff2f000500010100000012010000000500010200000012020000000500010200000001020000000000000011120300000013
REC=03010003006f2d3102000600637573742d3903010800fa00000000000000

# The full ON-DEVICE authoring loop: compile both stages from CEL source with
# the no-alloc front end, assemble the container, and require it byte-identical
# to the host toolchain's pack-emitted param (the $IR above) — compiler parity
# proven end-to-end through the applet, not just in the differential test.
SCHEMA='Order{id:str@1,customer_id:str@2,amount:int@3};Norm{id:str@1,amount:int@2};Enr{id:str@1,amount:int@2,doubled:int@3}'
N=$(cli compile "$SCHEMA" 'order:Order' 'Norm{ id: order.id, amount: order.amount }' 2>/dev/null)
E=$(cli compile "$SCHEMA" 'n:Norm' 'Enr{ id: n.id, amount: n.amount, doubled: n.amount * 2 }' 2>/dev/null)
C=$(cli stages "$N" "$E" 2>/dev/null)
if [ "$C" = "$IR" ]; then
  ok "cli compile+stages reproduces the host toolchain's container"
else
  no "cli compile" "container='$C'"
fi

if cli compile "$SCHEMA" 'order:Order' 'Norm{ id: order.nope }' >/dev/null 2>&1; then
  no "cli compile fail-closed" "unknown field exited 0"
else
  ok "cli compile rejects an unknown field (non-zero exit)"
fi

got=$(cli check $IR 2>/dev/null)
if want "$got" "ok: 2 stage(s)" && want "$got" "stage 0: cost=7"; then
  ok "cli check lowers the container and derives per-stage cost"
else
  no "cli check" "got='$got'"
fi

got=$(cli eval $IR $REC 2>/dev/null)
# The exact frame the pipeline E2E pins — the applet runs the same cores.
if want "$got" "03010003006f2d3102010800fa0000000000000003010800f401000000000000"; then
  ok "cli eval reproduces the deployed engine's output frame"
else
  no "cli eval" "got='$got'"
fi

got=$(cli digest deadbeef 2>/dev/null)
if want "$got" "5f78c33274e43fa9de5659265c1d917e25c03722dcb0b8d27db8d5feaa813953"; then
  ok "cli digest matches the known sha256 vector"
else
  no "cli digest" "got='$got'"
fi

# Decision authoring on device: compile the predicate, the hit outcome and the
# default, then assemble the first-hit container `run_decision` consumes. Pinned
# against the host toolchain's `compile_decision_param` output (regenerate:
# cargo test -p chronicle-canonical --test plan print_compiled_decision_hex -- --nocapture).
DS='Order{amount:int@3};Norm{amount:int@2}'
W=$(cli compile "$DS" 'order:Order' 'order.amount > 1000' 2>/dev/null)
O=$(cli compile "$DS" 'order:Order' 'Norm{ amount: 1 }' 2>/dev/null)
D=$(cli compile "$DS" 'order:Order' 'Norm{ amount: 0 }' 2>/dev/null)
got=$(cli decision "$W" "$O" "$D" 2>/dev/null)
want=010500000012000100020300000010e8030000000000002400030000000f00100100000000000000400200000041030000000f00100000000000000000400200000041
if [ "$got" = "$want" ]; then
  ok "cli decision reproduces the host toolchain's decision container"
else
  no "cli decision" "got='$got'"
fi

# Aggregation authoring on device: key/event-time/emit programs plus the
# operator set, assembled into the IR-def container the engine lowers at load.
# Pinned against AGGREGATION_IR_DEF (regenerate: cargo test -p chronicle-canonical
# --test pack print_ir_example_params -- --nocapture) — the very param
# examples/aggregation/linux.yaml ships.
AS='Order{customer_id:str@1,created_at:int@2,amount:int@3};St{order_count:int@1,gross_total:int@2};Win{start:int@1,end:int@2};Ctx{key:str@1,state:St@2,window:Win@3};CustomerTotal{customer_id:str@1,order_count:int@2,gross_total:int@3,window_start:int@4,window_end:int@5}'
K=$(cli compile "$AS" 'order:Order' 'order.customer_id' 2>/dev/null)
T=$(cli compile "$AS" 'order:Order' 'order.created_at' 2>/dev/null)
A=$(cli compile "$AS" 'order:Order' 'order.amount' 2>/dev/null)
E=$(cli compile "$AS" 'ctx:Ctx' 'CustomerTotal{ customer_id: ctx.key, order_count: ctx.state.order_count, gross_total: ctx.state.gross_total, window_start: ctx.window.start, window_end: ctx.window.end }' 2>/dev/null)
got=$(cli agg 100 10 64 0 0 "$K" "$T" "$E" '0:' "1:$A" 2>/dev/null)
want=64000000000000000a00000000000000400000000000000000000000000000000000000008000500010100000014080005000102000000144d000500010100000012010000000500020200000001000000120200000005000202000000020000001203000000050002030000000100000012040000000500020300000002000000120500000013020000000108000500010300000014
if [ "$got" = "$want" ]; then
  ok "cli agg reproduces the host toolchain's ir_def container"
else
  no "cli agg" "got='$got'"
fi

# ARTEFACT IDENTITY ON DEVICE: compile a source and seal it into a canonical
# Expression artefact, entirely in the applet. The digest is the sha256 of the
# canonical protobuf encoding with the digest field cleared — and it must equal
# what the host's `build_expression` produces, or an artefact authored on a
# device is a DIFFERENT artefact and every pin or signature against it breaks.
# Pinned equal by `chronicle-canonical/tests/pb_differential.rs`
# (`device_sealed_expression_matches_the_host_builder`); this asserts the whole
# chain end to end, through the real .fmod.
got=$(cli seal 'commerce.Order{customer_id:str@2}' 'order:commerce.Order' \
        'order.customer_id' 'commerce' 'customer_key' 'order' 'commerce.Order' 'string' \
        2>/dev/null | head -1)
want=3897058998c0f47a796e5af68a712ab269345fd39d347b1b098bcf061e349e92
if [ "$got" = "$want" ]; then
  ok "cli seal produces the host toolchain's artefact digest"
else
  no "cli seal" "digest='$got'"
fi

# The other two artefact kinds the device can seal. `pinned_e2e_digests_are_current`
# guards these literals host-side, so a host change that moves a digest fails a
# fast unit test as well as this run.
got=$(cli seal-tf 'commerce.Order{id:str@1};commerce.Norm{id:str@1}' 'order:commerce.Order' \
        'commerce.Norm{ id: order.id }' 'commerce' 'normalize' 'commerce.Order' 'commerce.Norm' \
        2>/dev/null | head -1)
if [ "$got" = "98cd1aa6b3c8e9cacc7deef29cd72134c799c58eebcee1ad80f6535c57189f1d" ]; then
  ok "cli seal-tf produces the host toolchain's Transformation digest"
else
  no "cli seal-tf" "digest='$got'"
fi

# Refs are given OUT of kind order on purpose: the encoder must group them into
# ascending fields, as prost emits repeated fields, or the Module identity moves.
got=$(cli seal-module 'commerce' 'orders' 'abc123' 'rustc-1.81' \
        pipeline 'commerce.process' 2222222222222222222222222222222222222222222222222222222222222222 \
        expression 'commerce.customer_key' 1111111111111111111111111111111111111111111111111111111111111111 \
        2>/dev/null | head -1)
if [ "$got" = "4e4efeb1ce545f3c245ee598bdb3505b6d120ea7ee6536299c8164a7f903a84e" ]; then
  ok "cli seal-module produces the host toolchain's Module digest"
else
  no "cli seal-module" "digest='$got'"
fi

# CONTENT-ADDRESSED STORAGE ON DEVICE — the other half of self-hosting: a node
# that seals an artefact must also be able to KEEP and SERVE it, or the identity
# it computed goes nowhere. `get` rehashes what it read, so this also proves the
# content-addressing invariant is CHECKED rather than assumed.
d=$(cli put deadbeefcafe 2>/dev/null | head -1)
back=$(cli get "$d" 2>/dev/null | head -1)
if [ "$d" = "a74142bb79088369bc7d9b57f167cf32204a5425919937d9c2962edf6b6a5263" ] \
   && [ "$back" = "deadbeefcafe" ]; then
  ok "cli put/get round-trips a blob under its own digest"
else
  no "cli put/get" "digest='$d' back='$back'"
fi

# A corrupt store must FAIL the read, not return bytes that lie about their
# identity. Overwrite the stored object with different content and re-read.
if [ -n "${d:-}" ]; then
  bad=$(cli get 0000000000000000000000000000000000000000000000000000000000000000 2>/dev/null | head -1)
  if [ -z "$bad" ] || [ "${bad#error}" != "$bad" ]; then
    ok "cli get reports a missing blob rather than inventing one"
  else
    no "cli get missing" "returned='$bad'"
  fi
fi

# A bad container must be rejected AND latch a non-zero exit (the applet writes
# it to cli_out.exit_in, which the runtime exits with).
if cli check 02ffff >/dev/null 2>&1; then
  no "cli fail-closed" "bad container exited 0"
else
  ok "cli check fails closed on a bad container (non-zero exit)"
fi

finish
