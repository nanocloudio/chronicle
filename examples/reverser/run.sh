#!/usr/bin/env bash
# Run the whole reverser — all four tiers — from this directory alone.
#
#   quantum broker (MQTT)  ←  lattice (pg + consensus + CDC pump + mqtt_sink)
#         │                        ↑                     ↑
#         └──→ chronicle B ────────┘      chronicle A ───┘   chronicle C ──┘
#
#   ./run.sh            start everything, seed nothing, stay up (Ctrl-C stops)
#   ./run.sh --verify   start, drive a message through the whole loop, tear down
#   ./run.sh --stop     kill anything this script left running
#
# Then, in a browser:
#   http://127.0.0.1:15100/reverse?msg=hello     insert (answers "ok")
#   http://127.0.0.1:15102/messages?max=10       newest reversed messages
#
# The modules come from the Fluxor OCI store by chronicle's pins (lattice,
# clustor, quantum, wave) — a COMPOSITION, not a build. Start order matters
# once: pipeline B must be subscribed before the first insert, because the
# broker does not replay QoS-0 subscriptions (README: delivery caveats).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT="$(cd "$HERE/../.." && pwd)"
FLUXOR="${FLUXOR_ROOT:-$(cd "$PROJECT/../fluxor" && pwd)}"
QUANTUM="${QUANTUM_ROOT:-$(cd "$PROJECT/../quantum" && pwd)}"

STATE="${STATE:-/tmp/reverser}"
REV_A_PORT="${REV_A_PORT:-15100}"
REV_C_PORT="${REV_C_PORT:-15102}"
REV_PG_PORT="${REV_PG_PORT:-15432}"
REV_BROKER_PORT="${REV_BROKER_PORT:-9090}"
# pg_client's endpoint is [ip:4][port:2 LE] hex — derived, single knob.
REV_PG_ENDPOINT="$(printf '7f000001%02x%02x' $((REV_PG_PORT & 0xff)) $((REV_PG_PORT >> 8)))"
export REV_A_PORT REV_C_PORT REV_PG_PORT REV_BROKER_PORT REV_PG_ENDPOINT

RUNTIME="${FLUXOR_LINUX:-$FLUXOR/target/aarch64-unknown-linux-gnu/release/fluxor-linux}"

# The tiers are grandchildren (subshell → fluxor run → fluxor-linux), so PID
# bookkeeping alone leaks them; the sweep patterns are what actually stop it.
PIDS=""
sweep() {
  pkill -f "fluxor-linux --config.*reverser" 2>/dev/null || true
  pkill -f "fluxor run.*lattice_reverser" 2>/dev/null || true
  pkill -f "fluxor run.*minimal.yaml" 2>/dev/null || true
  pkill -f "fluxor-linux --config.*minimal" 2>/dev/null || true
}
cleanup() {
  for p in $PIDS; do kill "$p" 2>/dev/null || true; done
  sweep
}
trap cleanup EXIT

die() {
  echo "FAIL: $1" >&2
  for log in broker lattice b a c; do
    [ -f "$STATE/$log/boot.log" ] && {
      echo "--- $log ---" >&2
      grep -v "MON_\|hb " "$STATE/$log/boot.log" | tail -8 >&2
    }
  done
  exit 1
}

await_port() { # port name
  for _ in $(seq 100); do
    (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null && { exec 3<&- 3>&-; return 0; }
    sleep 0.2
  done
  die "$2 never bound :$1"
}

if [ "${1:-}" = "--stop" ]; then
  trap - EXIT
  sweep
  echo "stopped"
  exit 0
fi

# A previous run's tiers would still own the ports — start from zero.
sweep
sleep 0.5

command -v fluxor >/dev/null || die "fluxor is not on PATH"
[ -x "$RUNTIME" ] || die "no fluxor-linux at $RUNTIME"

# Fresh state by default: the pump's keyrange hardcodes `inbound` = table 1,
# which only holds if this run's DDL performs the catalog's FIRST allocation.
# KEEP_STATE=1 reuses an existing state dir (whose ids are already correct).
[ "${KEEP_STATE:-}" = "1" ] || rm -rf "$STATE"

# ── tier 1: quantum, the MQTT broker ──────────────────────────────────────
mkdir -p "$STATE/broker"
echo "── tier 1: quantum MQTT broker on 127.0.0.1:$REV_BROKER_PORT"
( cd "$STATE/broker" && FLUXOR_PROJECT_ROOT="$QUANTUM" \
  fluxor run "$QUANTUM/examples/linux/minimal.yaml" >boot.log 2>&1 ) &
PIDS="$PIDS $!"
await_port "$REV_BROKER_PORT" "broker"

# ── tier 2: lattice, the durable store with CDC egress ────────────────────
mkdir -p "$STATE/lattice/wal" "$STATE/lattice/kv"
echo "── tier 2: lattice (pg :$REV_PG_PORT, CDC → lattice/cdc/feed1)"
( cd "$STATE/lattice" && FLUXOR_PROJECT_ROOT="$PROJECT" \
  fluxor run "$HERE/lattice_reverser.yaml" >boot.log 2>&1 ) &
PIDS="$PIDS $!"
await_port "$REV_PG_PORT" "lattice pg"

# ── schema: inbound FIRST (its catalog id, 1, is the pump's keyrange) ─────
# The pg port opens before consensus finishes warming up, and a CREATE that
# times out mid-warm-up still BURNS a table id (the allocator INCR precedes
# the descriptor write) — which would silently shift `inbound` off table 1.
# So gate on the pump's first durable checkpoint instead: `[cdc] ckpt=` in
# the boot log means a KV PUT went router → consensus → WAL → applied, i.e.
# the exact path the DDL needs. Then create ONCE. IF NOT EXISTS keeps a
# KEEP_STATE=1 restart (ids already assigned) idempotent.
for _ in $(seq 300); do
  grep -q "\[cdc\] ckpt=" "$STATE/lattice/boot.log" 2>/dev/null && break
  sleep 0.2
done
grep -q "\[cdc\] ckpt=" "$STATE/lattice/boot.log" \
  || die "lattice write path never came up (no CDC checkpoint)"
python3 "$HERE/sql.py" 127.0.0.1 "$REV_PG_PORT" \
  "CREATE TABLE IF NOT EXISTS inbound (msg TEXT PRIMARY KEY)" \
  "CREATE TABLE IF NOT EXISTS reversed (ts BIGINT PRIMARY KEY, msg TEXT)" \
  || die "schema DDL"
echo "   created inbound (table 1) and reversed (table 2)"

# ── tier 3: the three chronicle graphs ────────────────────────────────────
echo "── tier 3: building the chronicle graphs"
for g in a b c; do
  ( cd "$PROJECT" && fluxor build "examples/reverser/reverser_$g.yaml" ) \
    >/dev/null || die "fluxor build reverser_$g"
done

# B first: it must be SUBSCRIBED before the first insert.
for g in b a c; do
  mkdir -p "$STATE/$g"
  ( cd "$STATE/$g" && "$RUNTIME" \
      --config "$PROJECT/target/linux/reverser_$g/config.bin" \
      --modules "$PROJECT/target/linux/reverser_$g/modules.bin" \
      >boot.log 2>&1 ) &
  PIDS="$PIDS $!"
done
await_port "$REV_A_PORT" "pipeline A"
await_port "$REV_C_PORT" "pipeline C"
for _ in $(seq 50); do
  grep -q "suback ok" "$STATE/b/boot.log" 2>/dev/null && break
  sleep 0.2
done
grep -q "suback ok" "$STATE/b/boot.log" || die "pipeline B never subscribed"
echo "   A on :$REV_A_PORT, C on :$REV_C_PORT, B subscribed"

if [ "${1:-}" = "--verify" ]; then
  MSG="verify$$"
  WANT="$(echo "$MSG" | rev)"
  curl -sf -m 15 "http://127.0.0.1:$REV_A_PORT/reverse?msg=$MSG" | grep -q ok \
    || die "pipeline A did not answer ok"
  # Poll C until the reversed message shows up — the B leg is asynchronous
  # (commit → CDC window → MQTT → insert), so poll, never sleep a magic number.
  for _ in $(seq 75); do
    if curl -sf -m 15 "http://127.0.0.1:$REV_C_PORT/messages?max=10" \
        | grep -q "$WANT"; then
      echo "PASS: '$MSG' went HTTP → lattice → CDC → MQTT → reverse() → '$WANT'"
      exit 0
    fi
    sleep 0.2
  done
  die "reversed message '$WANT' never appeared on /messages"
fi

echo ""
echo "reverser is up:"
echo "  http://127.0.0.1:$REV_A_PORT/reverse?msg=hello"
echo "  http://127.0.0.1:$REV_C_PORT/messages?max=10"
echo "Ctrl-C to stop."
wait
