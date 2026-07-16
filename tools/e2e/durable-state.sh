#!/usr/bin/env bash
# DURABLE STATE without a Linux toolchain — the full crash-recovery loop, every
# step on device:
#
#   aggregation --checkpoint_out--> snapshot bytes
#        -> chronicle put   (content-addressed into the store)
#        -> chronicle get   (read back, content address verified)
#        -> aggregation `state` param on a FRESH graph, which resumes
#
# Recovery is reached by COMPOSITION rather than by giving the aggregation
# engine a storage dependency: the engine emits state, the applet stores it, the
# engine restores it. Chronicle owns the state transition; fluxor's
# storage.object contract owns the bytes.
#
# The assertion is the one that matters for recovery: a run split across a
# checkpoint must produce the SAME totals as one uninterrupted run.
. "$(dirname "$0")/../lib.sh"
modules_ready || { no durable "fluxor modules build failed"; finish; exit; }

use_store

# Two alice events in the same [0,100) window: 100 then 200 -> count=2 sum=300.
EV1=0301000500616c696365020108000a00000000000000030108006400000000000000
EV2=0301000500616c69636502010800140000000000000003010800c800000000000000

# --- 1. uninterrupted run: both events, one graph -------------------------
build_graph examples/aggregation/linux.yaml || { no durable "agg build"; finish; exit; }
# A watermark-advancing event in the NEXT window forces the pane to finalize.
FLUSH=0301000300626f62020108009001000000000000030108000100000000000000
whole=$(run_hex "$EV1$EV2$FLUSH" 6)

# --- 2. split run: checkpoint after the first event -----------------------
build_graph examples/aggregation/checkpoint.yaml || { no durable "ckpt build"; finish; exit; }
snap=$(run_hex "$EV1" 6)
if [ -z "$snap" ]; then
  no durable "no checkpoint emitted"
  finish
  exit
fi

# --- 3. persist it content-addressed, then read it back -------------------
build_graph packaging/cli/linux.yaml || { no durable "cli build"; finish; exit; }
digest=$(cli put "$snap" 2>/dev/null | head -1)
restored=$(cli get "$digest" 2>/dev/null | head -1)
if [ "$restored" != "$snap" ]; then
  no durable "store round-trip lost the snapshot"
  finish
  exit
fi

# --- 4. resume a FRESH graph from the stored snapshot ---------------------
build_graph examples/aggregation/linux.yaml "s|ir_def: \"|state: \"$restored\"\\n      ir_def: \"|" \
  || { no durable "resume build"; finish; exit; }
resumed=$(run_hex "$EV2$FLUSH" 6)

# --- 5. CONTROL: the same second event with NO restore must differ, or the
#        assertion above would pass even if `state` were being ignored.
build_graph examples/aggregation/linux.yaml || { no durable "control build"; finish; exit; }
nostate=$(run_hex "$EV2$FLUSH" 6)

if [ -z "$whole" ]; then
  no durable "uninterrupted run produced nothing"
elif [ "$resumed" != "$whole" ]; then
  no durable "uninterrupted='$whole' resumed='$resumed'"
elif [ "$nostate" = "$whole" ]; then
  no durable "control matched too — the state param is not being applied"
else
  # uninterrupted: alice count=2 sum=300; without restore: count=1 sum=200.
  ok "durable state (checkpoint -> store -> restore resumes identically)"
  ok "durable state control (no restore genuinely differs)"
fi

finish
