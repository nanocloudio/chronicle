#!/usr/bin/env bash
# Run the whole OCI registry — both tiers — from this directory alone.
#
# Everything the registry needs is here: the storage tier, the seed objects, the
# graph, and the verification. Nothing outside `examples/oci_registry/` is
# edited to run it; the modules come from the Fluxor OCI store by pin, which is
# what makes the registry a COMPOSITION rather than a build.
#
#   ./run.sh            start both tiers, seed, and stay up (Ctrl-C to stop)
#   ./run.sh --verify   start, assert the registry answers, tear down
#   ./run.sh --stop     kill anything this script left running
#
# Ports and state are overridable, so two copies can run side by side:
#   S3_PORT=19100  REGISTRY_PORT=15000  STATE=/tmp/oci-registry
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT="$(cd "$HERE/../.." && pwd)"
LOAM="${LOAM_ROOT:-$(cd "$PROJECT/../loam" && pwd)}"
FLUXOR="${FLUXOR_ROOT:-$(cd "$PROJECT/../fluxor" && pwd)}"

S3_PORT="${S3_PORT:-19100}"
REGISTRY_PORT="${REGISTRY_PORT:-15000}"
STATE="${STATE:-/tmp/oci-registry}"
BUCKET="${BUCKET:-registry}"

# The graph reads `${REGISTRY_PORT}`, `${S3_ENDPOINT}` and `${S3_HOST}` — fluxor
# substitutes `${VAR:-default}` before parsing the YAML, so the ports are set in
# exactly one place: here.
#
# `s3`'s `endpoint` is `[ip:4][port:2 LE]` packed as hex, which a decimal
# port cannot be interpolated into, so it is DERIVED rather than duplicated. A
# second literal would be a second source of truth, and the two would drift the
# first time someone moved the port.
S3_IP="${S3_IP:-127.0.0.1}"
S3_ENDPOINT="$(printf '%02x%02x%02x%02x%02x%02x' \
  "${S3_IP%%.*}" \
  "$(echo "$S3_IP" | cut -d. -f2)" \
  "$(echo "$S3_IP" | cut -d. -f3)" \
  "${S3_IP##*.}" \
  "$((S3_PORT & 0xff))" \
  "$((S3_PORT >> 8))")"
S3_HOST="$S3_IP:$S3_PORT"
export REGISTRY_PORT S3_ENDPOINT S3_HOST

LOAM_SERVER="$LOAM/target/aarch64-unknown-linux-gnu/release/loam-server"
RUNTIME="${FLUXOR_LINUX:-$FLUXOR/target/aarch64-unknown-linux-gnu/release/fluxor-linux}"
GRAPH="examples/oci_registry/chronicle_registry.yaml"
BUILT="$PROJECT/target/linux/chronicle_registry"

PIDS=""
cleanup() { for p in $PIDS; do kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT

die() {
  echo "FAIL: $1" >&2
  [ -f "$STATE/loam.log" ] && { echo "--- loam ---" >&2; tail -15 "$STATE/loam.log" >&2; }
  [ -f "$STATE/graph.log" ] && { echo "--- graph ---" >&2; grep -v MON_ "$STATE/graph.log" | tail -15 >&2; }
  exit 1
}
need() { command -v "$1" >/dev/null 2>&1 || die "$1 is not on PATH"; }

if [ "${1:-}" = "--stop" ]; then
  pkill -f loam-server 2>/dev/null || true
  pkill -f "chronicle_registry" 2>/dev/null || true
  echo "stopped"
  exit 0
fi

need curl
need fluxor
[ -x "$LOAM_SERVER" ] || die "no loam-server at $LOAM_SERVER
  Build it (a build, not a source change):
    cd $LOAM && cargo build --release --bin loam-server \\
      --target aarch64-unknown-linux-gnu"
[ -x "$RUNTIME" ] || die "no fluxor-linux at $RUNTIME
  Build it:
    cd $FLUXOR && cargo build --release --bin fluxor-linux \\
      --no-default-features --features host-linux \\
      --target aarch64-unknown-linux-gnu"

# ── tier 1: loam, serving S3 ──────────────────────────────────────────────
# `--fleet dir:PATH` is one local body_store. Replication is what the fleet is
# for — `--fleet tcp:a:7100,tcp:b:7100 --replica-count 2` spreads bodies across
# `--serve-body` nodes and NOTHING in tier 2 changes, because the S3 surface is
# identical either way. That independence is the reason for two tiers.
mkdir -p "$STATE/body"
echo "── tier 1: loam S3 on 127.0.0.1:$S3_PORT (state: $STATE)"
"$LOAM_SERVER" --s3-listen "127.0.0.1:$S3_PORT" \
  --ns-wal "$STATE/ns.wal" --obj-wal "$STATE/obj.wal" \
  --fleet "dir:$STATE/body" >"$STATE/loam.log" 2>&1 &
PIDS="$PIDS $!"
for _ in $(seq 80); do
  (exec 3<>"/dev/tcp/127.0.0.1/$S3_PORT") 2>/dev/null && { exec 3<&- 3>&-; break; }
  sleep 0.1
done
(exec 3<>"/dev/tcp/127.0.0.1/$S3_PORT") 2>/dev/null || die "loam never bound :$S3_PORT"
exec 3<&- 3>&-

# ── seed ──────────────────────────────────────────────────────────────────
# `GET /v2/` is the endpoint a client calls to learn this is a registry. Its
# body is an OBJECT, not a branch in the graph: the request path is the object
# key, so seeding `v2/` makes discovery answer without the chain ever forking.
# This is the whole reason tier 2 is a straight line.
curl -sf -o /dev/null -X PUT --data-binary '{}' \
  "http://127.0.0.1:$S3_PORT/$BUCKET/v2/" || die "could not seed the discovery object"
echo "   seeded /v2/ (discovery)"

# ── tier 2: chronicle, serving /v2/ ───────────────────────────────────────
echo "── tier 2: building $GRAPH"
( cd "$PROJECT" && fluxor build "$GRAPH" ) >/dev/null || die "fluxor build $GRAPH"
[ -s "$BUILT/config.bin" ] || die "$BUILT/config.bin missing"

echo "── tier 2: registry on 127.0.0.1:$REGISTRY_PORT"
"$RUNTIME" --config "$BUILT/config.bin" --modules "$BUILT/modules.bin" \
  >"$STATE/graph.log" 2>&1 &
PIDS="$PIDS $!"
for _ in $(seq 80); do
  (exec 3<>"/dev/tcp/127.0.0.1/$REGISTRY_PORT") 2>/dev/null && { exec 3<&- 3>&-; break; }
  sleep 0.1
done
(exec 3<>"/dev/tcp/127.0.0.1/$REGISTRY_PORT") 2>/dev/null \
  || die "the registry never bound :$REGISTRY_PORT"
exec 3<&- 3>&-

BASE="http://127.0.0.1:$REGISTRY_PORT"

if [ "${1:-}" != "--verify" ]; then
  echo
  echo "registry up:  $BASE/v2/"
  echo "  curl -i $BASE/v2/"
  echo "  curl -X PUT --data-binary @layer.tar \\"
  echo "       http://127.0.0.1:$S3_PORT/$BUCKET/v2/img/blobs/sha256:abc   # store"
  echo "  curl $BASE/v2/img/blobs/sha256:abc                               # serve"
  echo
  echo "Ctrl-C to stop."
  wait
  exit 0
fi

# ── verify ────────────────────────────────────────────────────────────────
#
# A bound port is not a serving registry: the graph has to reach the point where
# a request crosses HTTP → engines → SigV4 → storage and back. Poll the
# discovery endpoint until it answers rather than sleeping a magic number —
# a fixed sleep is how a gate becomes flaky on a slower machine.
ready=0
for _ in $(seq 40); do
  if [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "$BASE/v2/" 2>/dev/null)" = "200" ]; then
    ready=1
    break
  fi
  sleep 0.5
done
[ "$ready" = 1 ] || die "the registry never served /v2/ — the chain is not answering"

fail=0
check() { # <label> <expected> <actual>
  if [ "$2" = "$3" ]; then echo "  ok   $1"; else echo "  FAIL $1: want '$2', got '$3'"; fail=1; fi
}
code() { curl -s -o /dev/null -w '%{http_code}' --max-time 15 "$1"; }
body() { curl -s --max-time 15 "$1"; }

echo
# Discovery — a seeded object served through the whole chain.
check "GET /v2/ is 200"                200  "$(code "$BASE/v2/")"
check "GET /v2/ returns the body"      '{}' "$(body "$BASE/v2/")"

# A blob PUT into tier 1, then READ BACK THROUGH THE REGISTRY. This is the
# round trip that matters: the bytes cross HTTP → engines → SigV4 → storage and
# back, and a registry that answered from anywhere else would fail here.
BLOB="layer-bytes-$$"
curl -sf -o /dev/null -X PUT --data-binary "$BLOB" \
  "http://127.0.0.1:$S3_PORT/$BUCKET/v2/img/blobs/sha256-test" \
  || die "could not store the test blob in tier 1"
check "a stored blob is served"        "$BLOB" "$(body "$BASE/v2/img/blobs/sha256-test")"

# A missing object is the STORE's 404, carried verbatim — not a guess by the
# registry, which never learns whether an object exists.
check "a missing blob is 404"          404  "$(code "$BASE/v2/img/blobs/sha256-absent")"

# The route is mounted, not global: outside `/v2/` the gateway answers alone.
check "an unrouted path is 404"        404  "$(code "$BASE/nope")"

echo
if [ "$fail" = 0 ]; then
  echo "PASS oci registry — discovery, blob fetch and miss, over two tiers"
else
  echo "FAIL oci registry" >&2
fi
exit "$fail"
