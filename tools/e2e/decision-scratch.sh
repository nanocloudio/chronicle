#!/usr/bin/env bash
# A decision that BUILDS strings, from a `.plan` to the running graph — and
# `chronicle decide` agreeing with that graph byte for byte.
#
# examples/authoring/scratch_decision.plan concatenates in every arm's
# predicate and wraps a field into a document inside a nested context message.
# The document is lowered on device (`chronicle graph`), the graph it emits is
# run, and kilobyte records are driven through it. For each one the CLI's dry
# run of the SAME decision container must print the frame the node emitted, or
# refuse what the node refuses: `decide` is the diagnosis path for that node,
# so a bound it holds differently would explain a failure the node never had.
#
# Last, a document whose literals alone pass the stage scratch is refused when
# it is lowered and when it is authored, with the floor and the bound named.
. "$(dirname "$0")/../lib.sh"
modules_ready || { no scratch "fluxor modules build failed"; finish; exit; }

build_graph packaging/applet/chronicle/linux.yaml || { no scratch "cli build"; finish; exit; }
hex_of() { python3 -c 'import sys;print(open(sys.argv[1],"rb").read().hex())' "$1"; }
DOC=$(hex_of examples/authoring/scratch_decision.plan)
yaml=$E2E_GEN/scratch-$E2E_RUN.yaml
mkdir -p "$E2E_GEN"
run_text "" 10 -- graph "$DOC" process linux > "$yaml" 2>/dev/null
grep -q '^target:' "$yaml" || { no scratch "the plan did not lower: $(head -1 "$yaml")"; finish; exit; }
# The container the node is handed, exactly as the graph carries it.
DEC=$(sed -n 's/^ *decision: {hex: "\([0-9a-f]*\)"}.*/\1/p' "$yaml")
[ -n "$DEC" ] || { no scratch "no decision param in the lowered graph"; finish; exit; }
CLIYAML_CFG=$CFG CLIYAML_MODS=$MODS
build_graph "$yaml" || { no scratch "scratch graph build"; finish; exit; }
GRAPH_CFG=$CFG GRAPH_MODS=$MODS

# A t.Req frame: {1: id, 2: name, 3: body}; a name or body written
# `<char>*<n>` is that character n times, anything else is itself.
rec() {
  python3 - "$@" <<'PY'
import struct, sys
def arg(a):
    c, _, n = a.partition("*")
    return c.encode() * int(n) if n else a.encode()
id_, name, body = int(sys.argv[1]), arg(sys.argv[2]), arg(sys.argv[3])
f = bytes([3])
f += bytes([1, 1]) + struct.pack("<H", 8) + struct.pack("<q", id_)
f += bytes([2, 0]) + struct.pack("<H", len(name)) + name
f += bytes([3, 0]) + struct.pack("<H", len(body)) + body
print(f.hex())
PY
}
on_graph() { CFG=$GRAPH_CFG MODS=$GRAPH_MODS run_hex "$1" 6; }
decide() { CFG=$CLIYAML_CFG MODS=$CLIYAML_MODS run_text "" 10 -- decide "$DEC" "$1" 2>&1; }

# agree <record> <arm decide must name> <case>: the node emits a frame, and the
# dry run names the arm and prints that frame.
agree() {
  local r=$1 arm=$2 what=$3 node cli
  node=$(on_graph "$r")
  cli=$(decide "$r")
  if [ -z "$node" ]; then
    no scratch "$what: the node emitted nothing (cli: ${cli:0:160})"
  elif [ "$cli" = "$arm $node" ]; then
    ok "$what (${#node} hex chars, cli and node agree)"
  else
    no scratch "$what: node '${node:0:80}…' cli '${cli:0:160}'"
  fi
}
# A 1.5 KB body through three concatenating predicates to the default's
# nested document.
agree "$(rec 7 'web*1' 'q*1500')" default \
  "every predicate concatenates a 1.5 KB field, the default nests it in a document"
# Name and body equal: the first arm, a kilobyte each in the name, the key and
# the nested document.
agree "$(rec 7 'n*600' 'n*600')" "rule 0" \
  "the first arm builds a nested document from 600-byte fields"
agree "$(rec 7 'web' 'web/b')" "rule 1" "a short record takes the second arm"

# An outcome past one record: the node refuses it (nothing on), and the dry
# run refuses it naming the node's bound.
r=$(rec 7 'x*2000' 'x*2000')
node=$(on_graph "$r")
cli=$(decide "$r")
if [ -z "$node" ] && want "$cli" "error: outcome frame larger than a decision node's record (REC_BUF 4096 B)"; then
  ok "an outcome past REC_BUF is refused by the node and named by decide"
else
  no scratch "oversized outcome: node '${node:0:40}' cli '${cli:0:200}'"
fi
# A record past what the node admits is named as that bound, not as bad hex.
cli=$(decide "$(rec 1 'a*1' 'b*4100')")
if want "$cli" "error: record frame larger than the engine admits (REC_BUF 4096 B)"; then
  ok "a record past REC_BUF is named as the bound"
else
  no scratch "oversized record: '${cli:0:200}'"
fi

# Literals that pass the arena on ANY record: refused when lowered and when
# authored, with the floor and STAGE_SCRATCH_CAP.
big=$E2E_GEN/scratch-big-$E2E_RUN.plan
python3 - examples/authoring/scratch_decision.plan "$big" <<'PY'
import sys
src = open(sys.argv[1]).read()
# 200 literal bytes doubled six times through `cel.bind`: a 12800-byte
# string from a program a few hundred bytes long.
lit = "w" * 100
grow = ('cel.bind(a, "%s" + "%s", cel.bind(b, a + a, cel.bind(c, b + b, '
        'cel.bind(d, c + c, cel.bind(e, d + d, cel.bind(f, e + e, f + f))))))' % (lit, lit))
src = src.replace('t.Req { id: 3, name: r.name }', 't.Req { id: 3, name: %s }' % grow)
open(sys.argv[2], "w").write(src)
PY
BIG=$(hex_of "$big")
CFG=$CLIYAML_CFG MODS=$CLIYAML_MODS
got=$(run_text "" 10 -- graph "$BIG" process linux 2>&1)
if want "$got" "error: route: a branch builds at least 12800 B of strings on any record; a stage's scratch arena (STAGE_SCRATCH_CAP) holds 12288 B"; then
  ok "chronicle graph refuses a branch whose literals pass STAGE_SCRATCH_CAP"
else
  no scratch "graph over the floor: '${got:0:240}'"
fi
got=$(run_text "" 10 -- author "$BIG" 2>&1)
if want "$got" "error: route: rule 2 outcome builds at least 12800 B of strings on any record"; then
  ok "chronicle author names the rule whose literals pass STAGE_SCRATCH_CAP"
else
  no scratch "author over the floor: '${got:0:240}'"
fi

finish
