#!/usr/bin/env bash
# ARENA ADMISSION — the loader refuses exhaustion at module load, to the byte.
#
# `limit_register.md` says of the state arena: "Exhaustion is refused at module
# load, not at the allocation that overruns." Nothing checked it. This does,
# against the real loader, using the packaging-time `capacity:` envelope to size
# the arena down to the graph's exact need.
#
# The two graphs differ by ONE BYTE of envelope:
#   fits  — 57,792 B, exactly what `sensor_intake` (272) + `decision` (57,520)
#           need. Must load and decide.
#   tight — 57,791 B. `decision` must be refused, and nothing must be emitted.
#
# 57,520 and 272 are also the check that the manifest figures are the loader's
# own: rounded up to 64-byte units they are 57,536 and 320, which is exactly what
# `pack` recorded and what compose-time admission charges.
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
   grep -q "cap=57791" "$E2E_GEN/tight.err" 2>/dev/null; then
  ok "arena admission: the refusal names the envelope and the cap actually in force"
else
  no arena "the loader refused but named the wrong ceiling: $(grep -o 'STATE ARENA EXHAUSTED.*' "$E2E_GEN/tight.err" 2>/dev/null | head -1)"
fi

finish
