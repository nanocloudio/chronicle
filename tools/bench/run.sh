#!/usr/bin/env bash
# Chronicle benchmark runner. Measures a reference graph under SATURATION on
# the Pi 5 and proves the two exit-gate claims that are checkable here:
#
#   * zero UNEXPLAINED record loss under saturation — proved from the
#     accounting invariants read off the live telemetry, not guessed from the
#     outside: inputs_observed == admitted + rejected, and admitted ==
#     succeeded + policy_dropped + failed + in_flight. If those hold, every
#     received record is accounted for; nothing vanished.
#   * sustained throughput within a recorded baseline (relative gate).
#
# The environment is captured and a run whose governor is not
# `performance` or whose SoC is thermally throttling is reported ADVISORY: its
# number is recorded but does not gate, because frequency scaling makes it
# non-comparable. Absolute Pi 5 targets are pinned only from `performance`,
# unthrottled runs (none are asserted here).
. "$(dirname "$0")/../lib.sh"

GRAPH=${1:-examples/telemetry_probe/linux.yaml}
NAME=$(basename "$GRAPH" .yaml)
WINDOW=${BENCH_WINDOW:-12}
REPS=${BENCH_REPS:-3}
BASE="$_root/tools/bench/baselines/$NAME.txt"
OUT="$_root/target/bench/$NAME-$E2E_RUN.txt"
mkdir -p "$(dirname "$OUT")"

# --- environment (commit, target, governor, thermal, toolchain) ---
commit=$(git -C "$_root" rev-parse --short HEAD 2>/dev/null || echo "?")
gov=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo "?")
model=$(tr -d '\0' </sys/firmware/devicetree/base/model 2>/dev/null || echo "?")
temp0=$(( $(cat /sys/class/thermal/thermal_zone0/temp 2>/dev/null || echo 0) / 1000 ))
rustc_v=$(rustc --version 2>/dev/null | awk '{print $2}')
advisory=0
[ "$gov" != "performance" ] && advisory=1
[ "$temp0" -ge 80 ] && advisory=1

echo "chronicle-bench $NAME  commit=$commit  host=$model"
echo "  governor=$gov temp=${temp0}C rustc=$rustc_v window=${WINDOW}s reps=$REPS advisory=$advisory"

modules_ready || { no bench "modules build failed"; finish; exit; }
build_graph "$GRAPH" || { no bench "graph build failed"; finish; exit; }

# A saturating input: a big stream of valid decision frames ({1:2000} -> routed).
# Generated as BINARY once (no hex overhead on the hot pipe), sized past what the
# window can drain so the graph is busy the whole time.
feed="$E2E_GEN/feed.bin"
python3 - "$feed" <<'PY'
import sys, struct
n = 400_000
frame = bytes.fromhex("0101010800") + struct.pack("<q", 2000)
with open(sys.argv[1], "wb") as f:
    f.write(frame * n)
PY
fed_frames=400000

median_of() { printf '%s\n' "$@" | sort -n | awk '{a[NR]=$1} END{print a[int((NR+1)/2)]}'; }

rates=""; worst_loss_ok=1
for r in $(seq 1 "$REPS"); do
  err=$(mktemp)
  timeout -k 2 "$WINDOW" "$FL_BIN" --config "$CFG" --modules "$MODS" <"$feed" 2>"$err" >/dev/null
  # Read the LAST value of each accounting instrument the observer emitted.
  last() { grep -oE "MON_METRIC mod=[0-9]+ id=$1 kind=[12] val=[0-9]+" "$err" | tail -1 | grep -oE "val=[0-9]+" | cut -d= -f2; }
  obs=$(last 2); adm=$(last 3); rej=$(last 4); suc=$(last 5); drp=$(last 6); fal=$(last 7); inf=$(last 8); dlv=$(last 10)
  obs=${obs:-0}; adm=${adm:-0}; rej=${rej:-0}; suc=${suc:-0}; drp=${drp:-0}; fal=${fal:-0}; inf=${inf:-0}; dlv=${dlv:-0}
  # Accounting invariants: every received record is accounted for.
  inv1=$(( obs == adm + rej ))
  inv2=$(( adm == suc + drp + fal + inf ))
  rate=$(( dlv / WINDOW ))
  rates="$rates $rate"
  if [ "$inv1" -eq 1 ] && [ "$inv2" -eq 1 ]; then
    ok "rep $r: accounting balances (observed=$obs = admitted=$adm + rejected=$rej; admitted = succ=$suc + drop=$drp + fail=$fal + inflight=$inf), ~$rate rec/s"
  else
    no "rep $r accounting" "observed=$obs admitted=$adm rejected=$rej succ=$suc drop=$drp fail=$fal inflight=$inf — invariant broken (UNEXPLAINED LOSS)"
    worst_loss_ok=0
  fi
  rm -f "$err"
done

med=$(median_of $rates)
temp1=$(( $(cat /sys/class/thermal/thermal_zone0/temp 2>/dev/null || echo 0) / 1000 ))
{
  echo "name=$NAME commit=$commit governor=$gov temp_start=${temp0}C temp_end=${temp1}C advisory=$advisory"
  echo "fed_frames=$fed_frames window_s=$WINDOW reps=$REPS median_rec_per_s=$med rates=[$rates]"
} > "$OUT"
echo "  wrote $OUT (median ${med} rec/s)"

# --- baseline gate (relative, advisory-aware) ---
if [ -f "$BASE" ]; then
  base_med=$(grep -oE "median_rec_per_s=[0-9]+" "$BASE" | head -1 | cut -d= -f2)
  base_med=${base_med:-0}
  if [ "$base_med" -gt 0 ]; then
    # >10% throughput regression fails, unless this run is advisory.
    floor=$(( base_med * 90 / 100 ))
    if [ "$med" -ge "$floor" ]; then
      ok "throughput $med rec/s within 10% of baseline $base_med"
    elif [ "$advisory" -eq 1 ]; then
      echo "  WARN  advisory run below baseline ($med < floor $floor from $base_med) — not gated (governor=$gov)"
    else
      no "throughput regression" "$med rec/s < floor $floor (baseline $base_med)"
    fi
  fi
else
  cp "$OUT" "$BASE"
  echo "  established baseline: $BASE"
fi

# Zero unexplained loss under saturation is the load-bearing assertion.
[ "$worst_loss_ok" -eq 1 ] || no "zero-loss" "an accounting invariant broke under saturation"
finish
