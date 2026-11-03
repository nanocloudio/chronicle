#!/usr/bin/env bash
# A GUARDED DIVISOR, from a `.plan` to the running graph: `&&` / `||` absorb a
# fault the other operand decides (CEL's commutative error semantics).
#
# examples/authoring/guarded_decision.plan routes on `u.used / u.rate` behind a
# guard on the rate. The document is lowered on device (`chronicle graph`), the
# graph it emits is run, and records are driven through it: a zero rate must
# take the rule its guard decides — not fail the record with a division fault.
# The host suites prove the VM case by case (`cel_numeric`, `builtins`); this
# proves it holds on the shipped path a plan takes.
. "$(dirname "$0")/../lib.sh"
modules_ready || { no guarded "fluxor modules build failed"; finish; exit; }

build_graph packaging/applet/chronicle/linux.yaml || { no guarded "cli build"; finish; exit; }
hex_of() { python3 -c 'import sys;print(open(sys.argv[1],"rb").read().hex())' "$1"; }
DOC=$(hex_of examples/authoring/guarded_decision.plan)
yaml=$E2E_GEN/guarded-$E2E_RUN.yaml
mkdir -p "$E2E_GEN"
run_text "" 10 -- graph "$DOC" process linux > "$yaml" 2>/dev/null
grep -q '^target:' "$yaml" || { no guarded "the plan did not lower: $(head -1 "$yaml")"; finish; exit; }
build_graph "$yaml" || { no guarded "guarded graph build"; finish; exit; }

# {1: "a", 2: rate, 3: used} as a record frame.
i64() { python3 -c 'import sys,struct;print(struct.pack("<q",int(sys.argv[1])).hex())' "$1"; }
rec() { echo "03010001006102010800$(i64 "$1")03010800$(i64 "$2")"; }

# rate, used, the rule that must fire (its `used`), what the case proves.
check() {
  local got want
  got=$(run_hex "$(rec "$1" "$2")" 6)
  want=$(rec "$1" "$3")
  [ "$got" = "$want" ] && ok "$4" || no guarded "$4: got '$got', want '$want'"
}
check 0 50 2 "rate 0: \`rate > 0 && used / rate > 10\` is false, \`used / rate < 0 || rate == 0\` true"
check 2 50 1 "rate 2: the guarded division decides the first rule"
check 10 50 0 "rate 10: neither rule, the default"

finish
