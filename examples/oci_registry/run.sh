#!/usr/bin/env bash
# Run the whole OCI registry — both tiers — from this directory alone.
#
# Everything the registry needs is here: the storage tier's parameters, the seed
# objects, the graph, and the verification. Both tiers are fluxor graphs run by
# `fluxor run` from this project, so every module, the runtime and the loam-s3
# bundle come from the Fluxor OCI store by the pins in `fluxor.lock`: the
# registry is a COMPOSITION rather than a build.
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

S3_PORT="${S3_PORT:-19100}"
REGISTRY_PORT="${REGISTRY_PORT:-15000}"
STATE="${STATE:-/tmp/oci-registry}"
BUCKET="registry"
REGION="us-east-1"

# The graph reads `${REGISTRY_PORT}`, `${S3_HOST}` and the access key below —
# fluxor substitutes them before parsing the YAML, so each is set in exactly
# one place: here.
S3_HOST="127.0.0.1:$S3_PORT"
S3_ACCESS_KEY="AKREGISTRY0000000001"
S3_SECRET="registry-secret-$$"
export REGISTRY_PORT S3_HOST S3_ACCESS_KEY S3_SECRET

GRAPH="examples/oci_registry/chronicle_registry.yaml"

PIDS=""
cleanup() { for p in $PIDS; do kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT

die() {
  echo "FAIL: $1" >&2
  [ -f "$STATE/loam.log" ] && { echo "--- loam-s3 ---" >&2; grep -v MON_ "$STATE/loam.log" | tail -15 >&2; }
  [ -f "$STATE/graph.log" ] && { echo "--- graph ---" >&2; grep -v MON_ "$STATE/graph.log" | tail -15 >&2; }
  exit 1
}
need() { command -v "$1" >/dev/null 2>&1 || die "$1 is not on PATH"; }

if [ "${1:-}" = "--stop" ]; then
  pkill -f "fluxor run loam-s3" 2>/dev/null || true
  pkill -f "fluxor run $GRAPH" 2>/dev/null || true
  echo "stopped"
  exit 0
fi

need curl
need fluxor
grep -q '^name = "loam-s3"' "$PROJECT/fluxor.lock" \
  || die "fluxor.lock does not pin the loam-s3 bundle
  Pin it (it is published by loam):
    cd $PROJECT && fluxor store pin loam-s3:latest && fluxor sync"

# wait_port <port> <what>: bounded wait for a listener. `fluxor run` builds a
# graph before it serves, so the bound covers a cold build.
wait_port() {
  for _ in $(seq 480); do
    (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null && { exec 3<&- 3>&-; return 0; }
    sleep 0.25
  done
  die "$2 never bound :$1"
}

# ── tier 1: loam-s3, serving S3 ───────────────────────────────────────────
# One loam storage node behind wave's S3 server. Every request is SigV4-signed,
# and the access key acts under a capability that reaches only the registry
# bucket, minted here from a mesh root made for this run. Replicating tier 1 is
# loam's concern and changes nothing in tier 2, because the S3 surface is the
# same either way — that independence is the reason for two tiers.
mkdir -p "$STATE/bodies" "$STATE/spool"
MESH_ROOT=$(fluxor modules keygen --key "$STATE/root.seed" 2>/dev/null | tail -1)
now=$(date +%s)
CAP=$(fluxor modules cap mint --key "$STATE/root.seed" --scope "$BUCKET/" \
  --perms read_state,send_command --not-before $((now - 60)) --not-after $((now + 86400)) \
  2>/dev/null | tail -1)
[ -n "$MESH_ROOT" ] && [ -n "$CAP" ] || die "could not mint the registry's capability"
echo "$S3_ACCESS_KEY $S3_SECRET $BUCKET/ $CAP" >"$STATE/credentials"

echo "── tier 1: loam-s3 on $S3_HOST (state: $STATE)"
(cd "$PROJECT" && exec fluxor run loam-s3 \
  --param "port=$S3_PORT" --param "credentials=$STATE/credentials" \
  --param "mesh_roots=$MESH_ROOT" --param "region=$REGION" \
  --param "ns_wal=$STATE/ns.wal" --param "obj_wal=$STATE/obj.wal" \
  --param "body_dir=$STATE/bodies" --param "spool_dir=$STATE/spool") \
  >"$STATE/loam.log" 2>&1 &
PIDS="$PIDS $!"
wait_port "$S3_PORT" "loam-s3"

# s3 <curl args...>: a request signed with the registry's key.
s3() {
  curl -s --aws-sigv4 "aws:amz:$REGION:s3" --user "$S3_ACCESS_KEY:$S3_SECRET" "$@"
}

# ── seed ──────────────────────────────────────────────────────────────────
# `GET /v2/` is the endpoint a client calls to learn this is a registry. Its
# body is an OBJECT, not a branch in the graph: the request path is the object
# key, so seeding `v2/` makes discovery answer without the chain ever forking.
# This is the whole reason tier 2 is a straight line.
[ "$(s3 -o /dev/null -w '%{http_code}' -X PUT --data-binary '{}' "http://$S3_HOST/$BUCKET/v2/")" = 200 ] \
  || die "could not seed the discovery object"
echo "   seeded /v2/ (discovery)"

# ── tier 2: chronicle, serving /v2/ ───────────────────────────────────────
echo "── tier 2: registry on 127.0.0.1:$REGISTRY_PORT"
(cd "$PROJECT" && exec fluxor run "$GRAPH" </dev/null) >"$STATE/graph.log" 2>&1 &
PIDS="$PIDS $!"
wait_port "$REGISTRY_PORT" "the registry"

BASE="http://127.0.0.1:$REGISTRY_PORT"

if [ "${1:-}" != "--verify" ]; then
  echo
  echo "registry up:  $BASE/v2/"
  echo "  curl -i $BASE/v2/"
  echo "  store a blob in tier 1 (signed with the registry's key):"
  echo "    curl --aws-sigv4 aws:amz:$REGION:s3 --user $S3_ACCESS_KEY:$S3_SECRET \\"
  echo "         -X PUT --data-binary @layer.tar http://$S3_HOST/$BUCKET/v2/img/blobs/sha256:abc"
  echo "  curl $BASE/v2/img/blobs/sha256:abc"
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
[ "$(s3 -o /dev/null -w '%{http_code}' -X PUT --data-binary "$BLOB" \
  "http://$S3_HOST/$BUCKET/v2/img/blobs/sha256-test")" = 200 ] \
  || die "could not store the test blob in tier 1"
check "a stored blob is served"        "$BLOB" "$(body "$BASE/v2/img/blobs/sha256-test")"

# A blob many records long streams through: the relay passes every BODY record
# of the exchange on as it arrives, so its size is the store's limit, not the
# registry's. Compared by digest, so one byte out of place fails it.
dd if=/dev/urandom of="$STATE/big.bin" bs=1024 count=256 2>/dev/null
[ "$(s3 -o /dev/null -w '%{http_code}' -X PUT --data-binary @"$STATE/big.bin" \
  "http://$S3_HOST/$BUCKET/v2/img/blobs/sha256-big")" = 200 ] \
  || die "could not store the large test blob in tier 1"
check "a 256 KiB blob streams through whole" \
  "$(sha256sum <"$STATE/big.bin" | cut -d' ' -f1)" \
  "$(curl -s --max-time 30 "$BASE/v2/img/blobs/sha256-big" | sha256sum | cut -d' ' -f1)"

# A missing object is the STORE's 404, carried verbatim — not a guess by the
# registry, which never learns whether an object exists.
check "a missing blob is 404"          404  "$(code "$BASE/v2/img/blobs/sha256-absent")"

# The route is mounted, not global: outside `/v2/` the gateway answers alone.
check "an unrouted path is 404"        404  "$(code "$BASE/nope")"

# Tier 1 refuses what is not signed by a key it knows, so the registry's
# reads succeed only because tier 2 signs them.
check "tier 1 refuses an unsigned read" 403 \
  "$(curl -s -o /dev/null -w '%{http_code}' "http://$S3_HOST/$BUCKET/v2/")"

echo
if [ "$fail" = 0 ]; then
  echo "PASS oci registry — discovery, blob fetch, streaming and miss, over two tiers"
else
  echo "FAIL oci registry" >&2
fi
exit "$fail"
