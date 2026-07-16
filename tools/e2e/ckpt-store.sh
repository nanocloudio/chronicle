#!/usr/bin/env bash
# DURABLE LOCAL CHECKPOINT BACKEND (`state.local.checkpoint.v1`) on device.
#
# `durable-state-e2e.sh` proves the state TRANSITION — checkpoint, store,
# restore, resume identically. But it hands the digest from one run to the next,
# which a crashed node has no one to do for it. This proves the RECOVERY path:
#
#   chronicle ckpt-save <hex>   -> store under sha256, move `latest`
#   chronicle ckpt-load         -> follow `latest`, read back VERIFIED
#
# The pointer is what makes a node able to recover by itself.
. "$(dirname "$0")/../lib.sh"
modules_ready || { no ckpt "fluxor modules build failed"; finish; exit; }

export FLUXOR_STORE_DIR="$PWD/$E2E_GEN/store"
mkdir -p "$FLUXOR_STORE_DIR"

build_graph packaging/cli/linux.yaml || { no ckpt "cli build"; finish; exit; }
cli() { run_text "" 10 -- "$@"; }

# Before anything is saved, recovery must say so rather than invent a state.
got=$(cli ckpt-load 2>/dev/null | head -1)
case "$got" in
  *"no checkpoint saved"*) ok "ckpt-load reports an empty store rather than inventing state" ;;
  *) no ckpt "empty store did not report cleanly: '$got'" ;;
esac

C1=0102030405060708
C2=aabbccddeeff0011

d1=$(cli ckpt-save "$C1" 2>/dev/null | head -1)
case "$d1" in
  [0-9a-f]*) ok "ckpt-save returns a content digest" ;;
  *) no ckpt "save did not return a digest: '$d1'"; finish; exit ;;
esac

# Recovery with no prior knowledge of the digest.
got=$(cli ckpt-load 2>/dev/null | head -1)
[ "$got" = "$C1" ] && ok "ckpt-load recovers the checkpoint via the latest pointer" \
  || no ckpt "load gave '$got', expected '$C1'"

# A newer checkpoint moves the pointer.
d2=$(cli ckpt-save "$C2" 2>/dev/null | head -1)
got=$(cli ckpt-load 2>/dev/null | head -1)
if [ "$d2" = "$d1" ]; then
  no ckpt "different checkpoints produced the same digest"
elif [ "$got" = "$C2" ]; then
  ok "a newer checkpoint moves the latest pointer"
else
  no ckpt "after the second save, load gave '$got'"
fi

# Identical bytes dedupe to the same digest — content addressing, not a log.
again=$(cli ckpt-save "$C1" 2>/dev/null | head -1)
[ "$again" = "$d1" ] && ok "an identical checkpoint dedupes to one object" \
  || no ckpt "re-saving identical bytes changed the digest"

finish
