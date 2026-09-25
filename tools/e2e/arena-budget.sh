#!/usr/bin/env bash
# ARENA ADMISSION — the loader refuses exhaustion at module load, to the byte.
#
# `limit_register.md` says of the state arena: "Exhaustion is refused at module
# load, not at the allocation that overruns." This checks it against the real
# loader, using the packaging-time `capacity:` envelope to size the arena down
# to the graph's exact need.
#
# The two graphs differ by ONE BYTE of envelope:
#   fits  — 57,872 B, exactly what `sensor_intake` (328) + `decision` (57,544)
#           need. Must load and decide.
#   tight — 57,871 B. `decision` must be refused, and nothing must be emitted.
#
# 57,544 and 328 are the loader's carves: the module's state size plus a 4-byte
# canary, 8-byte aligned. The refusal names both (`need=`, `used=`), and the last
# check below holds the state figure `pack` recorded in each `.fmod` (whole
# 64-byte units, what compose-time admission charges: 57,536 and 320) to the
# carve the loader actually made — so the recorded numbers are the loader's own,
# not an estimate that happens to look right.
. "$(dirname "$0")/../lib.sh"
modules_ready || { no arena "fluxor modules build failed"; finish; exit; }

le64() {
  local v=$1 out="" i
  for i in 0 1 2 3 4 5 6 7; do out+=$(printf '%02x' $(((v >> (8 * i)) & 0xff))); done
  printf '%s' "$out"
}
sample() { printf '010000fd07000000%s%s' "$(le64 0)" "$(le64 "$1")"; }

run_one() {
  local g=$1
  build_graph "examples/arena_budget/$g.yaml" >/dev/null 2>&1 || return 1
  printf '%s' "$(sample 23500)" | xxd -r -p \
    | timeout -k 2 8 "$FL_BIN" --config "$CFG" --modules "$MODS" 2>"$E2E_GEN/$g.err" \
    | xxd -p | tr -d '\n'
}

fits=$(run_one fits)
if [ "$fits" = "01010108006400000000000000" ]; then
  ok "arena admission: a graph sized to its exact need loads and decides"
else
  no arena "graph at its exact need produced '$fits', expected the decided outcome"
fi

tight=$(run_one tight)
if [ -n "$tight" ]; then
  no arena "one byte under the need still produced '$tight' — the loader did not refuse"
elif ! grep -q "STATE ARENA EXHAUSTED" "$E2E_GEN/tight.err" 2>/dev/null; then
  no arena "one byte under the need emitted nothing, but the loader never said why"
else
  ok "arena admission: one byte under the need is refused at module load"
fi

# WHICH ceiling refused. Two guard this arena — the compiled `STATE_ARENA_SIZE`
# and the packaged capacity envelope — and here it is the envelope, by
# construction: the graph sets it one byte below the need while the compiled arena
# is untouched and vastly larger. A refusal that named the compiled constant would
# send a reader to rebuild a kernel over a number that had nothing to do with it,
# so the message has to distinguish them and this asserts that it does.
if grep -q "deployment envelope" "$E2E_GEN/tight.err" 2>/dev/null &&
   grep -q "cap=57871" "$E2E_GEN/tight.err" 2>/dev/null; then
  ok "arena admission: the refusal names the envelope and the cap actually in force"
else
  no arena "the loader refused but named the wrong ceiling: $(grep -o 'STATE ARENA EXHAUSTED.*' "$E2E_GEN/tight.err" 2>/dev/null | head -1)"
fi

# THE RECORDED FIGURES ARE THE LOADER'S OWN. The refusal reports the carve it
# could not make (`need=`, decision) and the offset already carved before it
# (`used=`, sensor_intake, loaded first). A carve is `align8(state + 4)`, so the
# module's state size lies in [carve-11, carve-4]; `pack` records that size
# rounded up to 64 bytes. Each `.fmod`'s recorded figure must be the 64-byte
# rounding of a size in that window, and the envelope that fits must be the
# two carves summed — so none of 57,544 / 328 / 57,872 is taken on trust.
refusal=$(grep -o 'deployment envelope.*' "$E2E_GEN/tight.err" 2>/dev/null | head -1)
need=$(printf '%s' "$refusal" | grep -oE 'need=[0-9]+' | grep -oE '[0-9]+')
used=$(printf '%s' "$refusal" | grep -oE 'used=[0-9]+' | grep -oE '[0-9]+')
recorded() { "$FLX" inspect "target/fluxor/bcm2712/modules/$1.fmod" 2>/dev/null \
  | grep -oE 'state: [0-9]+' | grep -oE '[0-9]+'; }
# 0 when `rec` is the 64-byte rounding of some state size a carve of `c` implies.
matches_carve() { # <recorded> <carve>
  local rec=$1 c=$2 sz
  for sz in $(seq $((c - 11)) $((c - 4))); do
    [ $(((sz + 63) / 64 * 64)) -eq "$rec" ] && return 0
  done
  return 1
}
dec_rec=$(recorded decision)
si_rec=$(recorded sensor_intake)
fits_cap=$(grep -oE 'state_arena: [0-9]+' examples/arena_budget/fits.yaml | grep -oE '[0-9]+')
if [ -z "$need" ] || [ -z "$used" ] || [ -z "$dec_rec" ] || [ -z "$si_rec" ]; then
  no arena "missing a figure: need='$need' used='$used' recorded decision='$dec_rec' sensor_intake='$si_rec'"
elif ! matches_carve "$dec_rec" "$need"; then
  no arena "decision.fmod records $dec_rec B, not the 64-byte rounding of the loader's carve $need"
elif ! matches_carve "$si_rec" "$used"; then
  no arena "sensor_intake.fmod records $si_rec B, not the 64-byte rounding of the loader's carve $used"
elif [ "$fits_cap" -ne $((need + used)) ]; then
  no arena "fits.yaml sizes the envelope at $fits_cap B, but the loader carves $need + $used = $((need + used))"
else
  ok "arena admission: recorded state ($dec_rec, $si_rec) is the loader's carve ($need, $used) rounded to 64 B, and fits = $((need + used))"
fi

finish
