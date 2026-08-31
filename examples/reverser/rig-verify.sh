#!/usr/bin/env bash
# Verify the reverser on REAL silicon — the WHOLE application on one board.
# `reverser_pi5.yaml` netboots onto the rig DUT: wave http on :80, the three
# chronicle pipelines, and the lattice storage tier (consensus, WAL, NVMe,
# CDC) in ONE graph. This host only netboots, creates the schema, and drives
# a browser-shaped request across the LAN:
#
#   http://192.168.1.9/reverse?msg=hello      insert (answers "ok")
#   http://192.168.1.9/messages?max=10        newest reversed messages
#
#   ./rig-verify.sh          netboot + verify one message; DUT stays up
#   ./rig-verify.sh --check  no netboot: verify against the running DUT
#
# The DUT side is the diag scenario tests/hardware/reverser_pi5_diag.toml —
# its pass rule never matches; THIS script owns the verdict. A netbooted
# kernel has no teardown: the DUT keeps serving until the next power cycle.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT="$(cd "$HERE/../.." && pwd)"
DUT_IP="${DUT_IP:-192.168.1.9}"
STATE="${STATE:-/tmp/reverser-rig}"
RUNS="$HOME/.local/state/fluxor/labs/default/rigs/pi5-a/runs"

die() {
  echo "FAIL: $1" >&2
  [ -f "$STATE/rig.log" ] && { echo "--- rig ---" >&2; tail -12 "$STATE/rig.log" >&2; }
  R="$(ls -t "$RUNS" 2>/dev/null | head -1)"
  [ -n "$R" ] && [ -f "$RUNS/$R/telemetry.monitor_udp.log" ] && {
    echo "--- DUT telemetry ---" >&2
    grep -vE "MON_|hb |tlm " "$RUNS/$R/telemetry.monitor_udp.log" | tail -10 >&2
  }
  exit 1
}

command -v fluxor >/dev/null || die "fluxor is not on PATH"
mkdir -p "$STATE"

if [ "${1:-}" != "--check" ]; then
  echo "── DUT: fluxor rig test (netboot $DUT_IP, capture window open)"
  # --force: a previous run's kill can leave the rig lock behind; this
  # script is the only sanctioned writer on this bench.
  ( cd "$PROJECT" && fluxor rig test --force \
      --scenario tests/hardware/reverser_pi5_diag.toml >"$STATE/rig.log" 2>&1 ) &
  RIG_PID=$!

  # Boot gate 1: port 80 answers. Power cycle (15 s off-hold) + VPU/TFTP
  # (~20 s) + kernel boot — allow 180 s.
  for _ in $(seq 900); do
    timeout 1 bash -c "exec 3<>/dev/tcp/$DUT_IP/80" 2>/dev/null && break
    kill -0 "$RIG_PID" 2>/dev/null || die "rig test exited before the DUT booted"
    sleep 0.2
  done
  timeout 1 bash -c "exec 3<>/dev/tcp/$DUT_IP/80" 2>/dev/null \
    || die "DUT never opened :80"
  echo "   DUT http is up"

  # Boot gate 2: the pump's first durable checkpoint in the rig's UDP
  # telemetry capture — a KV PUT went router → consensus → WAL → applied,
  # i.e. the exact write path the DDL needs. Gating here avoids the
  # table-id burn (a timed-out CREATE still consumes a catalog id, and
  # the pump's keyrange hardcodes inbound = table 1).
  # `|| true`: under pipefail, `ls | head` dies of SIGPIPE (141) the moment
  # head closes the pipe — this exact line silently killed the script after
  # "http is up" on every run, so the ckpt gate and DDL never executed and a
  # rushed manual DDL then lost CREATEs to the warm-up window.
  R="$(ls -t "$RUNS" 2>/dev/null | head -n 1 || true)"
  for _ in $(seq 300); do
    grep -q "\[cdc\] ckpt=" "$RUNS/$R/telemetry.monitor_udp.log" 2>/dev/null && break
    sleep 0.2
  done
  # The monitor can die with a broken pipe mid-run; the gate is best-effort
  # and the final curl verdict catches a burned table id visibly anyway.
  grep -q "\[cdc\] ckpt=" "$RUNS/$R/telemetry.monitor_udp.log" 2>/dev/null \
    || { echo "   (no telemetry checkpoint seen — falling back to a settle wait)"; sleep 20; }

  # Schema: inbound FIRST (fresh FAT32 each netboot → table ids 1 and 2).
  #
  # READINESS-PROBE, then DDL ONCE. The storage tier (consensus election +
  # WAL replay + cold NVMe first touch) can lag the http listener by minutes
  # on a fresh netboot — but retrying the CREATEs against an unready tier is
  # WORSE than failing: a refused CREATE can still burn a catalog table id,
  # and the CDC pump's keyrange is pinned to `inbound` being table id 1 — a
  # burned id kills the reversed leg silently (observed: `[cdc] tmot=` with
  # both pipelines healthy). So the wait probes with a SELECT, which
  # allocates nothing, and the CREATEs run exactly once, against a tier that
  # has already answered.
  STORAGE_OK=0
  for attempt in $(seq 60); do
    OUT="$(python3 "$HERE/sql.py" "$DUT_IP" 5432 "SELECT msg FROM inbound" 2>&1 | tr -d '\0')" \
      && { STORAGE_OK=1; break; }
    case "$OUT" in
      # POSITIVE evidence only: an executor that answered about the TABLE is
      # up (the table may simply not exist yet). Everything else — storage
      # unavailable, connection refused, timeouts — keeps waiting; treating
      # "any other error" as readiness declared victory on a refused connect.
      *"no such table"*|*"unknown table"*|*"relation"*|*"not found"*) STORAGE_OK=1; break ;;
      *) ;;
    esac
    [ $((attempt % 6)) = 0 ] && echo "   (storage not ready — probe $attempt/60: $(echo "$OUT" | head -c 60))"
    sleep 5
  done
  [ "$STORAGE_OK" = 1 ] || die "storage tier never became ready"
  python3 "$HERE/sql.py" "$DUT_IP" 5432 \
    "CREATE TABLE IF NOT EXISTS inbound (msg TEXT PRIMARY KEY)" \
    "CREATE TABLE IF NOT EXISTS reversed (ts BIGINT PRIMARY KEY, msg TEXT)" \
    || die "schema DDL on the DUT"
  echo "   created inbound (table 1) and reversed (table 2)"
fi

# ── the verdict: one message through the whole board ──────────────────────
MSG="rig$$"
WANT="$(echo "$MSG" | rev)"
curl -sf -m 20 "http://$DUT_IP/reverse?msg=$MSG" | grep -q ok \
  || die "the DUT did not answer ok (INSERT did not complete)"
for _ in $(seq 150); do
  if curl -sf -m 20 "http://$DUT_IP/messages?max=10" | grep -q "$WANT"; then
    echo "PASS: '$MSG' went :80 → consensus/NVMe → CDC → reverse() → '$WANT', all on $DUT_IP"
    echo ""
    echo "the reverser is live on the DUT:"
    echo "  http://$DUT_IP/reverse?msg=hello"
    echo "  http://$DUT_IP/messages?max=10"
    exit 0
  fi
  sleep 0.2
done
die "reversed message '$WANT' never appeared on /messages"

# ── Telemetry acceptance (rfc_observability_surface §12.8 phase 1) ─────────
# With fluxor-collect running on this host against the graph's id-table:
#
#   fluxor id-table examples/reverser/reverser_pi5.yaml -o /tmp/reverser.idtable.json
#   fluxor-collect --id-table /tmp/reverser.idtable.json &
#
# drive load and assert the request-latency histogram is live with its p99
# INSIDE the declared ladder (not pinned to +Inf) — the exact unsound-ladder
# failure the per-instrument bounds exist to prevent. Skipped (not failed)
# when no collector is listening: the reverser verification above stands on
# its own, and telemetry acceptance needs the collector by design.
if curl -sf -m 2 "http://127.0.0.1:9464/metrics" >/dev/null 2>&1; then
  echo "── telemetry: driving 100 requests for histogram mass"
  for i in $(seq 100); do
    curl -sf -m 5 "http://$DUT_IP/reverse?msg=load-$i" >/dev/null || die "load request $i failed"
  done
  echo "   waiting one emit cadence"
  sleep 7
  M="$(curl -sf "http://127.0.0.1:9464/metrics")" || die "collector scrape failed"
  echo "$M" | grep -q "fluxor_requests_total{" || die "requests_total absent from the scrape"
  TOTAL="$(echo "$M" | grep 'fluxor_request_latency_us_count{' | grep -oE '[0-9]+$' | head -1)"
  [ -n "$TOTAL" ] && [ "$TOTAL" -ge 100 ] || die "latency histogram count '$TOTAL' < 100"
  INF="$(echo "$M" | grep 'fluxor_request_latency_us_bucket{.*le="+Inf"' | grep -oE '[0-9]+$' | head -1)"
  MID="$(echo "$M" | grep 'fluxor_request_latency_us_bucket{.*le="100000"' | grep -oE '[0-9]+$' | head -1)"
  [ -n "$INF" ] && [ -n "$MID" ] || die "latency buckets absent"
  # p99 mid-ladder: ≥99% of requests at or under the 100 ms bound.
  [ $((MID * 100)) -ge $((INF * 99)) ] \
    || die "p99 above 100ms (le=100000: $MID of $INF) — ladder or DUT regression"
  echo "   telemetry PASS: $TOTAL requests, p99 inside the ladder (≤100ms: $MID/$INF)"
else
  echo "── telemetry: no collector on :9464 — acceptance step skipped (see header)"
fi
