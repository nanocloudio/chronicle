#!/usr/bin/env bash
# AUTHORING END TO END ON DEVICE — a `.uproc` document in, sealed artefacts out:
#
#   chronicle author <uproc_hex>   -> "<name> <digest>" per artefact
#
# Every step runs on device: parse the document (`uproc_core`), assemble the
# type environment from its own declarations (`uproc_lower_core`), compile each
# body (`celc_core`), lower it, and seal it (`artefact_core`). No host is
# involved, and the digests are the ones the host toolchain produces for the
# same source — pinned by `chronicle-canonical/tests/device_uproc.rs`, which
# proves the device assembles a byte-identical schema text.
. "$(dirname "$0")/../lib.sh"
modules_ready || { no author "fluxor modules build failed"; finish; exit; }

build_graph packaging/cli/linux.yaml || { no author "cli build"; finish; exit; }
cli() { run_text "" 10 -- "$@"; }
hex_of() { python3 -c 'import sys;print(open(sys.argv[1],"rb").read().hex())' "$1"; }

DOC=$(hex_of examples/authoring/process_order.uproc)

# The document declares `is_large`; sealing it must yield a 64-char digest.
got=$(cli author "$DOC" 2>/dev/null | head -1)
name=${got%% *}
digest=${got##* }
if [ "$name" = "is_large" ] && [ ${#digest} -eq 64 ]; then
  ok "device authors a sealed artefact straight from the document"
else
  no author "unexpected first line: '$got'"
fi

# Authoring is DETERMINISTIC — the same source must seal to the same identity,
# or a digest could not be a pin.
again=$(cli author "$DOC" 2>/dev/null | head -1)
[ "$again" = "$got" ] && ok "authoring the same document twice is deterministic" \
  || no author "second run differed: '$again' vs '$got'"

# The digest must match what `seal` produces for the same expression compiled
# by hand — i.e. `author` is doing the real thing, not printing a placeholder.
hand=$(cli seal 'commerce.Money{units:int@1};commerce.Order{id:str@1,customer_id:str@2,total:commerce.Money@3};commerce.NormalizedOrder{id:str@1,total:int@2};commerce.Route{kind:int@1,order_id:str@2};commerce.Ack{order_id:str@1,route:int@2};AUTOMATIC=0;MANUAL_REVIEW=1' \
  'order:commerce.Order' 'order.total.units >= 1000' commerce is_large order commerce.Order bool 2>/dev/null | head -1)
if [ "$hand" = "$digest" ]; then
  ok "authored digest equals the hand-sealed one (same identity, fewer steps)"
else
  no author "author=$digest hand=$hand"
fi

# EVERY declared artefact kind is sealed, not just expressions: the document
# declares 2 expressions, a transformation, a decision and a pipeline.
# The document declares exactly four: an expression, a transformation, a
# decision and a pipeline — one of every kind it uses.
names=$(cli author "$DOC" 2>/dev/null | awk '{print $1}' | tr '\n' ' ')
lines=$(cli author "$DOC" 2>/dev/null | grep -c .)
if [ "$names" = "is_large normalize route process MODULE " ]; then
  ok "every kind is sealed, then the MODULE that contains them"
else
  no author "got '$names'"
fi

# Each line must carry a distinct 64-char digest — distinct artefacts cannot
# share an identity.
uniq=$(cli author "$DOC" 2>/dev/null | awk '{print $2}' | sort -u | grep -c .)
if [ "$uniq" = "$lines" ]; then
  ok "each artefact seals to its own distinct digest"
else
  no author "$lines artefacts but only $uniq distinct digests"
fi

# Aggregations seal too, including the `emit` expression — which reads a
# SYNTHESIZED context (operator state + window bounds) that appears nowhere in
# the document, so sealing one exercises the whole lowering path.
agg=$(cli author "$(hex_of examples/authoring/customer_totals.uproc)" 2>/dev/null | head -1)
aggname=${agg%% *}
aggdigest=${agg##* }
HOST_AGG=09f2d3c2a61cce57ee695af86402ec1cee5c013ba035018ad0b210b12d69c005
if [ "$aggname" = "customer_totals" ] && [ "$aggdigest" = "$HOST_AGG" ]; then
  ok "the aggregation digest equals the host toolchain's (emit context included)"
else
  no author "aggregation: dev=$aggdigest want=$HOST_AGG"
fi

# THE claim: a digest the device authors must equal the one the HOST toolchain
# produces for the same source — not merely "a digest" but THE digest, the
# identity every pin, signature and registry entry is made against.
#
# Every kind in this document matches the host. Each artefact kind seals a
# DIFFERENT field set — its own capability string, and per-kind metadata like
# rule names/priorities/explain and stage target kinds — so a kind matching is
# a real statement about the whole path, not a coincidence.
#
# Fixtures come from the host:
#   cargo test -p chronicle-canonical --test device_uproc print_host_authored_digests -- --nocapture
HOST_IS_LARGE=e3d22a05ad69b7499282c461cabac4b090b342da76910361d998dedec53a0bea
HOST_NORMALIZE=0d3d0186abf2c3e0e3627a836f0b56d14a6879fdc1e26179620ba5e0068aa020
HOST_ROUTE=b27737f28f24177c561f05d62714465db8644bce50521006e72dbb4442a9112d

dev_all=$(cli author "$DOC" 2>/dev/null)
dev_is_large=$(echo "$dev_all" | awk '$1=="is_large"{print $2}')
dev_normalize=$(echo "$dev_all" | awk '$1=="normalize"{print $2}')
dev_route=$(echo "$dev_all" | awk '$1=="route"{print $2}')

if [ "$dev_is_large" = "$HOST_IS_LARGE" ]; then
  ok "the expression digest equals the host toolchain's"
else
  no author "expr=$dev_is_large want=$HOST_IS_LARGE"
fi

# Per-kind, so the state is never summarised into something misleading.
[ "$dev_normalize" = "$HOST_NORMALIZE" ] \
  && ok "the transformation digest equals the host toolchain's" \
  || no author "transformation: dev=$dev_normalize want=$HOST_NORMALIZE"

[ "$dev_route" = "$HOST_ROUTE" ] \
  && ok "the decision digest equals the host toolchain's" \
  || no author "decision: dev=$dev_route want=$HOST_ROUTE"

dev_process=$(echo "$dev_all" | awk '$1=="process"{print $2}')
HOST_PROCESS_D=49b9d6c69a1c6ae6ee4fa3a7b3877c90692bf64fef1bed989ab5199f487aa08b
[ "$dev_process" = "$HOST_PROCESS_D" ] \
  && ok "the pipeline digest equals the host toolchain's" \
  || no author "pipeline: dev=$dev_process want=$HOST_PROCESS_D"

# The MODULE is the deployment unit: refs to every contained artefact, the
# resource bindings the deployment must satisfy, and the entry points. Matching
# the host here means a device can author something a registry will resolve.
HOST_MODULE=bfb3115ddcf1a85374b65c7a27b0c8c1a161a6bb6c88a6d0380fb8ae8b5c76f6
dev_module=$(echo "$dev_all" | awk '$1=="MODULE"{print $2}')
[ "$dev_module" = "$HOST_MODULE" ] \
  && ok "the sealed MODULE digest equals the host toolchain's" \
  || no author "module: dev=$dev_module want=$HOST_MODULE"

# ...and for the aggregation document too, whose module contains an Aggregation
# ref rather than a pipeline — a different ref field in the sealed encoding.
HOST_AGG_MODULE=b348420f32199e77d04c93886abb813cff016f3a6a8346a1e1267f76cb93d385
dev_agg_module=$(cli author "$(hex_of examples/authoring/customer_totals.uproc)" 2>/dev/null | awk '$1=="MODULE"{print $2}')
[ "$dev_agg_module" = "$HOST_AGG_MODULE" ] \
  && ok "the aggregation module digest equals the host toolchain's" \
  || no author "agg module: dev=$dev_agg_module want=$HOST_AGG_MODULE"

# A malformed document still reports a position rather than crashing.
BAD=$(printf 'module a.b {\n  nonsense foo;\n}' | python3 -c 'import sys;print(sys.stdin.buffer.read().hex())')
got=$(cli author "$BAD" 2>/dev/null | head -1)
case "$got" in
  *"at line 2"*) ok "a malformed document reports its position" ;;
  *) no author "bad document: '$got'" ;;
esac

finish
