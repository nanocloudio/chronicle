#!/usr/bin/env bash
# MAP STAGE, live: a label selector's requirements against a pod's labels.
#
# `examples/map_stage/selector.uproc` declares a `map` (a predicate applied to
# every element of a repeated field, bounded by a declared max, its verdicts
# counted) and a transformation reading the counts. The device lowers it with
# `chronicle graph` to ONE pipeline node — the map is a stage of the compute run
# (`stage_kinds`), not a node — and the graph is run on real records:
# The verdict is `misses` — requirements that did not hold; 0 means selected:
#   * every requirement holds                      -> misses = 0
#   * NotIn / DoesNotExist on an ABSENT key hold   -> upstream semantics
#   * In on a value outside the set fails          -> misses = 1
#   * exactly the declared 16 requirements         -> evaluated, misses = 0
#   * one more than declared (17)                  -> refused and COUNTED
#     (`over_bound`), nothing emitted
. "$(dirname "$0")/../lib.sh"
modules_ready || { no map "fluxor modules build failed"; finish; exit; }
build_graph packaging/cli/linux.yaml >/dev/null || { no map "cli graph build"; finish; exit; }

SRC=$(python3 -c 'import sys;print(open(sys.argv[1],"rb").read().hex())' examples/map_stage/selector.uproc)
yaml="$E2E_GEN/map-stage-$E2E_RUN.yaml"
cli graph "$SRC" select linux >"$yaml" 2>/dev/null
grep -q 'stage_kinds: "0200"' "$yaml" \
  && ok "the map lowers into the compute run's node (stage_kinds 02,00)" \
  || no map "no map stage kind in the lowered graph"

# frame(...) records: requirements at 60, each a nested message paired with
# the pod's value for its key (4, absent when the pod lacks the label)
records=$(python3 - <<'PY'
import struct
class Msg(bytes):
    """A nested message: its own frame, written as type 3."""
def frame(fs):
    out = bytes([len(fs)])
    for n, v in fs:
        if isinstance(v, int):
            out += struct.pack('<BBH', n, 1, 8) + struct.pack('<q', v)
        else:
            ty = 3 if isinstance(v, Msg) else 0
            out += struct.pack('<BBH', n, ty, len(v)) + v
    return out
labels = {b"app": b"web", b"tier": b"fe"}
def req(k, o, v):
    fs = [(1, k), (2, o), (3, v)] + ([(4, labels[k])] if k in labels else [])
    return (60, Msg(frame(fs)))
cases = [
  [req(b"app", b"In", b"web,api"), req(b"tier", b"NotIn", b"db"),
   req(b"tier", b"Exists", b""), req(b"zone", b"DoesNotExist", b"")],
  [req(b"zone", b"NotIn", b"a"), req(b"zone", b"DoesNotExist", b"")],
  [req(b"app", b"In", b"api")],
  [req(b"app", b"Exists", b"")] * 16,
  [req(b"app", b"Exists", b"")] * 17,
]
print(" ".join(frame(c).hex() for c in cases))
PY
)
read -r ALL ABSENT MISS ATMAX TOOMANY <<<"$records"
want0=$(python3 -c 'import struct;print((bytes([1])+struct.pack("<BBH",1,1,8)+struct.pack("<q",0)).hex())')
want1=$(python3 -c 'import struct;print((bytes([1])+struct.pack("<BBH",1,1,8)+struct.pack("<q",1)).hex())')

# over_bound is pipeline metric id 30 (modules/app/pipeline/manifest.toml),
# the count of records refused for exceeding the field table or a map stage's
# maximum. `observe` drains the telemetry ring to stderr as MON_METRIC lines,
# naming a module by its instance index, which the runtime's own load log gives
# (`module N caps: …` then `loaded pipeline`) — read, not assumed.
OBSERVE='/^wiring:/i\  - name: observe\n    params:\n      interval_ms: 100'
mod_index() { # <stderr file> <module name>
  awk -v m="$2" '/\[inst\] module [0-9]+ caps:/ { for (i = 1; i < NF; i++) if ($i == "module") n = $(i + 1) }
                 $0 ~ ("loaded " m "$") { print n; exit }' "$1" 2>/dev/null
}
over_bound() { # <stderr file> -> the highest over_bound value reported
  local m
  m=$(mod_index "$1" pipeline)
  [ -n "$m" ] || return 0
  grep -oE "MON_METRIC mod=$m id=30 kind=1 val=[0-9]+" "$1" 2>/dev/null \
    | grep -oE '[0-9]+$' | sort -n | tail -1
}
# run_counted <input_hex> <stderr file>: long enough for the second telemetry
# publish (5 s), stdout as hex.
run_counted() {
  printf '%s' "$1" | xxd -r -p \
    | timeout -k 2 8 "$FL_BIN" --config "$CFG" --modules "$MODS" 2>"$2" \
    | xxd -p | tr -d '\n'
}

if build_graph "$yaml" "$OBSERVE" >/dev/null; then
  [ "$(run_hex "$ALL" 5)" = "$want0" ] && ok "every requirement holds -> 0 misses" || no map "all-hold case"
  [ "$(run_hex "$ABSENT" 5)" = "$want0" ] && ok "NotIn/DoesNotExist hold for a pod lacking the key" || no map "absent-key case"
  [ "$(run_hex "$MISS" 5)" = "$want1" ] && ok "In outside the value set is one miss" || no map "miss case"
  # AT the declared maximum: evaluated, and nothing counted over bound.
  got=$(run_counted "$ATMAX" "$E2E_GEN/map-atmax.err")
  n=$(over_bound "$E2E_GEN/map-atmax.err")
  if [ "$got" = "$want0" ] && [ "${n:-}" = 0 ]; then
    ok "16 requirements against a declared 16 are evaluated"
  else
    no map "at-max case: output '$got', over_bound='${n:-none}'"
  fi
  # PAST it: refused and counted — not merely silent.
  got=$(run_counted "$TOOMANY" "$E2E_GEN/map-toomany.err")
  n=$(over_bound "$E2E_GEN/map-toomany.err")
  if [ -z "$got" ] && [ "${n:-}" = 1 ]; then
    ok "17 requirements against a declared 16 are refused and counted"
  else
    no map "over-max case: output '$got', over_bound='${n:-none}'"
  fi
else
  no map "lowered graph did not build"
fi
finish
