#!/usr/bin/env bash
# AUTHORING TO RUNNABLE GRAPH, ENTIRELY ON DEVICE — a `.uproc` document in, the
# graph YAML a fluxor runtime boots out:
#
#   chronicle graph <uproc_hex> <pipeline> [target]
#
# This is the last hop that used to need a Linux build host. `author-e2e.sh`
# proves a device can SEAL a document's artefacts; this proves it can DEPLOY
# one — resolve the named pipeline, compile each stage against the document's
# own schema (`registry_core`), pack the results into the params a module loads
# (`pack_core`), and lower the whole thing to graph YAML (`plan_core`).
#
# Nothing here shells out to cargo. The entire chain runs inside a `.fmod`.
. "$(dirname "$0")/../lib.sh"
modules_ready || { no graph "fluxor modules build failed"; finish; exit; }

build_graph packaging/cli/linux.yaml || { no graph "cli build"; finish; exit; }
cli() { run_text "" 10 -- "$@"; }
hex_of() { python3 -c 'import sys;print(open(sys.argv[1],"rb").read().hex())' "$1"; }

DOC=$(hex_of examples/authoring/compute_only.uproc)

# The whole point: source to a bootable graph, no host in the loop.
got=$(cli graph "$DOC" process 2>/dev/null)
case "$got" in
  target:*) ok "device lowers a document's pipeline to graph YAML" ;;
  *) no graph "expected a graph document, got: '$(printf '%s' "$got" | head -1)'" ;;
esac

# A compute run must COLLAPSE into one pipeline node — the policy that keeps
# pure compute off the wire. Two nodes here would mean serializing between
# stages that never needed to leave the module.
n_nodes=$(printf '%s\n' "$got" | grep -c '^  - name: ')
[ "$n_nodes" = "1" ] && ok "consecutive compute stages collapse into one node" \
  || no graph "expected 1 module node, got $n_nodes"

# The stages must actually be IN there as a packed container, not a placeholder.
printf '%s\n' "$got" | grep -q 'ir_stages: "[0-9a-f][0-9a-f]*"' \
  && ok "the node carries the compiled ir_stages container" \
  || no graph "no ir_stages param in the emitted graph"

# Deterministic: a graph is content-addressed like the bytecode it carries, so
# the same document must lower to the same bytes.
again=$(cli graph "$DOC" process 2>/dev/null)
[ "$again" = "$got" ] && ok "lowering the same document twice is deterministic" \
  || no graph "second lowering differed"

# The default profile is EMBEDDED — a node authoring a graph authors it for a
# node, and host `cli` bracketing would make the result un-embeddable.
printf '%s\n' "$got" | grep -q 'cli_in' \
  && no graph "the default profile should not bracket with cli" \
  || ok "the default profile leaves the chain ends open for embedding"

# ...and the host profile is available when the graph really is for a host.
host=$(cli graph "$DOC" process linux 2>/dev/null)
printf '%s\n' "$host" | grep -q 'cli_in.stdin_out' \
  && ok "the linux profile brackets the chain with cli" \
  || no graph "linux profile did not emit cli edges"

# Fail-closed, not fail-plausible: an effect names an endpoint and credentials
# that exist nowhere in the document, so a binding cannot be invented. Emitting
# a graph that points at nothing would be worse than refusing.
eff=$(hex_of examples/authoring/process_order.uproc)
out=$(cli graph "$eff" process 2>/dev/null)
case "$out" in
  *"connector binding is not in the document"*)
    ok "an unbound effect is refused rather than guessed" ;;
  *) no graph "expected an unbound-effect refusal, got: '$(printf '%s' "$out" | head -1)'" ;;
esac

# A decision splits the compute run, so the graph gets a SECOND pipeline node
# whose name no longer implies its module type. Emitting `pipeline2` with no
# `type:` produced a graph fluxor refused to load; this is that regression.
SPLIT=$(hex_of examples/authoring/split_pipeline.uproc)
split_yaml=$E2E_GEN/split-$E2E_RUN.yaml
cli graph "$SPLIT" process linux > "$split_yaml" 2>/dev/null

grep -q '^  - name: pipeline2$' "$split_yaml" \
  && ok "a decision splits the compute run into two pipeline nodes" \
  || no graph "expected a pipeline2 node in the split graph"

grep -A1 '^  - name: pipeline2$' "$split_yaml" | grep -q '^    type: pipeline$' \
  && ok "a repeated node names its module type explicitly" \
  || no graph "pipeline2 has no 'type: pipeline' — fluxor cannot resolve it"

# The real assertion: the generated graph LOADS. A graph that renders but does
# not boot is the failure this whole path exists to prevent, and only actually
# running it can tell the difference.
out=$(timeout -k 2 20 "$FLX" run "$split_yaml" </dev/null 2>&1)
rc=$?
if printf '%s' "$out" | grep -q 'not found'; then
  no graph "fluxor refused the authored graph: $(printf '%s' "$out" | head -1)"
elif [ $rc -eq 124 ] || [ $rc -eq 137 ]; then
  ok "the authored graph loads and runs in fluxor"
else
  no graph "unexpected exit $rc: $(printf '%s' "$out" | head -1)"
fi

# THE EMBEDDED GRAPH MUST ACTUALLY BUILD. An embedded compute-only graph has an
# empty platform AND empty wiring, and both were being emitted as bare keys —
# `platform:` parses as null ("must be a mapping") and `wiring` is required
# outright. The graph rendered perfectly and fluxor refused every one of them.
# Rendering is not the test; building is.
emb_yaml=$E2E_GEN/embedded-$E2E_RUN.yaml
emb_bin=$E2E_GEN/embedded-$E2E_RUN.bin
cli graph "$DOC" process bcm2712 > "$emb_yaml" 2>/dev/null
if "$FLX" build --emit image --output "$emb_bin" --epoch 3 --target bcm2712 "$emb_yaml" >/dev/null 2>&1; then
  ok "a device-authored embedded graph builds an OTA slot image"
else
  no graph "fluxor refused the authored embedded graph: $("$FLX" build --emit image --output "$emb_bin" --epoch 3 --target bcm2712 "$emb_yaml" 2>&1 | head -1)"
fi

# Empty sections must be explicit empty collections, never bare keys.
grep -q '^platform: {}$' "$emb_yaml" \
  && ok "an empty platform is an explicit empty mapping" \
  || no graph "empty platform is not 'platform: {}'"
grep -q '^wiring: \[\]$' "$emb_yaml" \
  && ok "an empty wiring is an explicit empty list" \
  || no graph "empty wiring is not 'wiring: []'"

# A pipeline the document does not declare is a structured error.
out=$(cli graph "$DOC" nosuch 2>/dev/null)
case "$out" in
  *"no pipeline named 'nosuch'"*) ok "an unknown pipeline is a structured error" ;;
  *) no graph "expected an unknown-pipeline error, got: '$(printf '%s' "$out" | head -1)'" ;;
esac

finish
