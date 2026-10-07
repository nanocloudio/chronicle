#!/usr/bin/env bash
# Run the identity provider from this directory alone.
#
# Everything it needs is here: the dispatch document, the graphs, the operator
# clients, and the verification. Nothing outside `examples/identity_provider/`
# is edited to run it; the modules come from the Fluxor OCI store by pin,
# which is what makes the IdP a COMPOSITION rather than a build.
#
#   ./run.sh              author, start, provision, and stay up (Ctrl-C to stop)
#   ./run.sh --verify     introspect: assert every arm of the chain, tear down
#   ./run.sh --token      the /oauth/token device grant, end to end
#   ./run.sh --authorize  the OIDC authorization-code /authorize leg
#   ./run.sh --exchange   the OIDC authorization-code exchange leg
#   ./run.sh --stop       kill anything this script left running
#
# The port is overridable so two copies can run side by side:
#   IDP_PORT=15100 ./run.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT="$(cd "$HERE/../.." && pwd)"

IDP_PORT="${IDP_PORT:-15100}"
PIDFILE="/tmp/chronicle-idp.pid"

export IDP_PORT

port_open() {
  (exec 3<>"/dev/tcp/127.0.0.1/$IDP_PORT") 2>/dev/null
}

# A stopped graph drains before it exits, and it keeps its listener while it
# does — so `stop` waits for the port to close. Without that, the next mode
# on the same port finds the OLD graph listening and talks to it.
stop() {
  if [ -f "$PIDFILE" ]; then
    kill "$(cat "$PIDFILE")" 2>/dev/null || true
    rm -f "$PIDFILE"
    for _ in $(seq 1 100); do port_open || break; sleep 0.2; done
  fi
}

# Start a graph and wait for its listener rather than sleeping a guess: a
# fixed sleep is either too short on a loaded machine or wasted on an idle
# one. A port something else already holds is refused, not shared.
start_graph() { # <graph.yaml>
  if port_open; then
    echo "port $IDP_PORT is already in use"; exit 1
  fi
  ( cd "$PROJECT" && exec fluxor run "$1" ) &
  echo $! > "$PIDFILE"
  for _ in $(seq 1 150); do
    port_open && return 0
    sleep 0.2
  done
  echo "the graph never listened on :$IDP_PORT"; exit 1
}

case "${1:-}" in
  --stop) stop; echo "stopped"; exit 0 ;;
esac
trap stop EXIT

# ── the /oauth/token slice ──────────────────────────────────────────────────
#
# A SECOND graph in this example (chronicle_token.yaml) serving POST
# /oauth/token through mint_admission's GRANT operation. It needs the full grant
# substrate — a durable ledger, a vault signing key, an enrolled device — so
# `--token` stands it up with a store and a seal key and drives the ceremony
# in token_client.py: deliver the keys, seed a device, present a certificate +
# DPoP proof, and receive an access token that verifies.
if [ "${1:-}" = "--token" ]; then
  echo "== authoring idp.uproc (token) =="
  H="$(python3 -c 'import sys;print(open(sys.argv[1],"rb").read().hex())' "$HERE/idp.uproc")"
  ( cd "$PROJECT" && fluxor exec chronicle -- author "$H" ) >/dev/null 2>&1     || { echo "authoring failed"; exit 1; }

  WORK="$(mktemp -d /tmp/chronicle-token-XXXXXX)"
  export FLUXOR_STORE_DIR="$WORK/store"
  export FLUXOR_VAULT_DIR="$WORK/vault"
  # A vault that can actually persist the labelled signing key. Fixed, so a
  # restart would reopen the same key — the property a real deployment needs.
  export FLUXOR_SEAL_KEY="6b6167692d746f6b656e2d7365616c2d6b65792d666f722d64656d6f0000ffff"
  mkdir -p "$FLUXOR_STORE_DIR" "$FLUXOR_VAULT_DIR"

  echo "== starting the token endpoint on :$IDP_PORT =="
  start_graph "$HERE/chronicle_token.yaml"

  echo "== granting a token =="
  OUT="$(python3 "$HERE/token_client.py" "$IDP_PORT")"
  STATUS="$(printf '%s\n' "$OUT" | sed -n 's/^STATUS //p')"
  TOKEN="$(printf '%s\n' "$OUT" | tail -1)"
  fail=0
  if [ "$STATUS" = "200" ]; then
    echo "  ok   an enrolled device is granted a token (200)"
  else
    echo "  FAIL grant returned $STATUS"; fail=1
  fi
  # The token is a compact JWS — three dot-separated segments — and its
  # subject is the certificate's, which the client never named.
  segs="$(printf '%s' "$TOKEN" | awk -F. '{print NF}')"
  if [ "$segs" = "3" ]; then
    claims="$(printf '%s' "$TOKEN" | cut -d. -f2)"
    pad=$(( (4 - ${#claims} % 4) % 4 )); claims="$claims$(printf '=%.0s' $(seq 1 $pad))"
    decoded="$(printf '%s' "$claims" | tr '_-' '/+' | base64 -d 2>/dev/null || true)"
    case "$decoded" in
      *'"sub":"tenant:alice"'*) echo "  ok   the token carries the certificate's subject" ;;
      *) echo "  FAIL token subject: $decoded"; fail=1 ;;
    esac
    case "$decoded" in
      *'"aud":"https://rs.token.test"'*) echo "  ok   audience is the deployment's, not the client's" ;;
      *) echo "  FAIL token audience: $decoded"; fail=1 ;;
    esac
  else
    echo "  FAIL token is not a compact JWS: $TOKEN"; fail=1
  fi
  rm -rf "$WORK"
  [ "$fail" = 0 ] && echo "ok: /oauth/token minted a verifying token through the grant pipeline"                   || { echo "token verify FAILED"; exit 1; }
  exit 0
fi

# ── the OIDC /authorize leg ─────────────────────────────────────────────────
#
# A FOURTH graph (chronicle_authorize.yaml) serving POST /oauth/authorize
# through kagi's `authcode` module: it authenticates a device by its
# certificate + DPoP proof, establishes the subject, and returns a single-use
# code. Needs the grant substrate (durable ledger for the code + client
# registry) but no mint on the authorize path. authorize_client.py provisions
# the device-cert key and the client registry, then authorizes.
if [ "${1:-}" = "--authorize" ]; then
  echo "== authoring idp.uproc (authorize) =="
  H="$(python3 -c 'import sys;print(open(sys.argv[1],"rb").read().hex())' "$HERE/idp.uproc")"
  ( cd "$PROJECT" && fluxor exec chronicle -- author "$H" ) >/dev/null 2>&1     || { echo "authoring failed"; exit 1; }

  WORK="$(mktemp -d /tmp/chronicle-authorize-XXXXXX)"
  export FLUXOR_STORE_DIR="$WORK/store"
  export FLUXOR_VAULT_DIR="$WORK/vault"
  export FLUXOR_SEAL_KEY="6b6167692d746f6b656e2d7365616c2d6b65792d666f722d64656d6f0000ffff"
  mkdir -p "$FLUXOR_STORE_DIR" "$FLUXOR_VAULT_DIR"

  echo "== starting the authorize endpoint on :$IDP_PORT =="
  start_graph "$HERE/chronicle_authorize.yaml"

  echo "== authorizing a device =="
  OUT="$(python3 "$HERE/authorize_client.py" "$IDP_PORT")"
  STATUS="$(printf '%s\n' "$OUT" | sed -n 's/^STATUS //p')"
  CODE="$(printf '%s\n' "$OUT" | tail -1)"
  fail=0
  if [ "$STATUS" = "200" ]; then
    echo "  ok   an authenticated device is issued a code (200)"
  else
    echo "  FAIL authorize returned $STATUS"; fail=1
  fi
  # The code is a 22-char single-use id — established by kagi, not the pipeline.
  len="${#CODE}"
  if [ "$len" = "22" ]; then
    echo "  ok   the response body is a 22-char authorization code"
  else
    echo "  FAIL not a 22-char code (len=$len): $CODE"; fail=1
  fi
  rm -rf "$WORK"
  [ "$fail" = 0 ] && echo "ok: /oauth/authorize issued a code through the authcode pipeline"                   || { echo "authorize verify FAILED"; exit 1; }
  exit 0
fi

# ── the OIDC code-exchange leg ──────────────────────────────────────────────
#
# A THIRD graph (chronicle_exchange.yaml) serving POST /oauth/token through
# kagi's `authcode` module: it redeems a single-use authorization code + a PKCE
# verifier for an access token and an ID token, both minted inside kagi. It
# needs the same grant substrate as --token (durable ledger, vault signing
# keys) plus a seeded code — the artefact /authorize creates — so exchange_client.py
# provisions the keys, seeds a device and a code, and redeems it.
if [ "${1:-}" = "--exchange" ]; then
  echo "== authoring idp.uproc (exchange) =="
  H="$(python3 -c 'import sys;print(open(sys.argv[1],"rb").read().hex())' "$HERE/idp.uproc")"
  ( cd "$PROJECT" && fluxor exec chronicle -- author "$H" ) >/dev/null 2>&1     || { echo "authoring failed"; exit 1; }

  WORK="$(mktemp -d /tmp/chronicle-exchange-XXXXXX)"
  export FLUXOR_STORE_DIR="$WORK/store"
  export FLUXOR_VAULT_DIR="$WORK/vault"
  export FLUXOR_SEAL_KEY="6b6167692d746f6b656e2d7365616c2d6b65792d666f722d64656d6f0000ffff"
  mkdir -p "$FLUXOR_STORE_DIR" "$FLUXOR_VAULT_DIR"

  echo "== starting the token endpoint (exchange) on :$IDP_PORT =="
  start_graph "$HERE/chronicle_exchange.yaml"

  echo "== redeeming a code =="
  OUT="$(python3 "$HERE/exchange_client.py" "$IDP_PORT")"
  STATUS="$(printf '%s\n' "$OUT" | sed -n 's/^STATUS //p')"
  TOKEN="$(printf '%s\n' "$OUT" | tail -1)"
  fail=0
  if [ "$STATUS" = "200" ]; then
    echo "  ok   a valid code + PKCE verifier is exchanged for a token (200)"
  else
    echo "  FAIL exchange returned $STATUS"; fail=1
  fi
  # The access token is a compact JWS whose subject is the code's — established
  # at /authorize by kagi, never named by the client at exchange.
  segs="$(printf '%s' "$TOKEN" | awk -F. '{print NF}')"
  if [ "$segs" = "3" ]; then
    claims="$(printf '%s' "$TOKEN" | cut -d. -f2)"
    pad=$(( (4 - ${#claims} % 4) % 4 )); claims="$claims$(printf '=%.0s' $(seq 1 $pad))"
    decoded="$(printf '%s' "$claims" | tr '_-' '/+' | base64 -d 2>/dev/null || true)"
    case "$decoded" in
      *'"sub":"tenant:alice"'*) echo "  ok   the token carries the code's subject" ;;
      *) echo "  FAIL token subject: $decoded"; fail=1 ;;
    esac
    case "$decoded" in
      *'"aud":"https://rs.authcode.test"'*) echo "  ok   audience is the deployment's, not the client's" ;;
      *) echo "  FAIL token audience: $decoded"; fail=1 ;;
    esac
    case "$decoded" in
      *'"scope":"openid profile"'*) echo "  ok   scope is the code's clamped scope" ;;
      *) echo "  FAIL token scope: $decoded"; fail=1 ;;
    esac
  else
    echo "  FAIL token is not a compact JWS: $TOKEN"; fail=1
  fi
  rm -rf "$WORK"
  [ "$fail" = 0 ] && echo "ok: /oauth/token exchanged a code for a token through the authcode pipeline"                   || { echo "exchange verify FAILED"; exit 1; }
  exit 0
fi

# ── the operator client ────────────────────────────────────────────────────
#
# `introspect_client.py` is the KEY CEREMONY, and it is a host script because
# a ceremony belongs outside the graph. It provisions `token_verify`'s keyset
# over the control carrier (`control.py`) and mints one credential to
# introspect. The private half never reaches the graph — a verifier holds
# public keys only, which is the whole reason the keyset arrives as
# `MSG_KEY_ADD` carrying a public point.

# Author the document on device, which is also the check that it still
# compiles: every artefact digest below is one `chronicle author` sealed with
# no host toolchain involved at any step.
#
# It is run BEFORE the graph starts rather than as a separate step someone
# might skip: a graph whose params drifted from the document they were
# generated from is a graph that does something the document does not say.
echo "== authoring idp.uproc =="

# The document must fit `chronicle_cli`'s `UPROC_BUF` (65536 bytes of SOURCE
# text, hex-decoded into it), and it is checked HERE because exceeding it
# does not fail — it HANGS. The hex of the document is one `fluxor exec`
# argument, so an over-long document is an over-long argument record: it
# exceeds `ARGV_BUF` (2 x UPROC_BUF, the same bound seen from the other
# side), the record is never delivered, and the CLI waits for argv that
# cannot arrive.
#
# A person who has just added a paragraph of comments and watched authoring
# hang has no way to guess that. Measured against the source, before the
# hang, with the number that matters in the message.
UPROC_MAX=65536
size="$(wc -c < "$HERE/idp.uproc")"
if [ "$size" -gt "$UPROC_MAX" ]; then
  echo "idp.uproc is $size bytes, over chronicle's $UPROC_MAX-byte document bound."
  echo "Authoring would HANG rather than fail. Shorten the document (the"
  echo "comments are usually where the room is) or raise UPROC_BUF and"
  echo "ARGV_BUF together in modules/app/chronicle_cli/mod.rs."
  exit 1
fi

H="$(python3 -c 'import sys;print(open(sys.argv[1],"rb").read().hex())' "$HERE/idp.uproc")"
( cd "$PROJECT" && fluxor exec chronicle -- author "$H" ) 2>/dev/null \
  | grep -vE '^\[|^Running' \
  || { echo "authoring failed — the document does not compile"; exit 1; }

echo "== starting the identity provider on :$IDP_PORT =="
start_graph "$HERE/chronicle_idp.yaml"

# ── provision the keyset, and mint one credential ──────────────────────────
echo "== provisioning the verifying keyset =="
TOKEN="$(python3 "$HERE/introspect_client.py" "$IDP_PORT" | tail -1)"
[ -n "$TOKEN" ] || { echo "the key ceremony produced no credential"; exit 1; }

introspect() {
  post_to /oauth/introspect "$1"
}

# The same request against any path and with any method, so a route and a
# method can be tested rather than assumed.
post_to() { # <path> <body> [method]
  curl -s -o /tmp/chronicle-idp-body.$$ -w '%{http_code}' --max-time 10 \
       -X "${3:-POST}" --data-binary "$2" "http://127.0.0.1:$IDP_PORT$1"
}

if [ "${1:-}" = "--verify" ]; then
  echo "== verifying =="
  # Every assertion here is a REAL HTTP round trip whose status came out of a
  # kagi verdict. That is the whole claim the example makes, so it is the
  # whole thing the check tests — including the refusals, because a chain
  # that only ever answers 200 has not shown that it can say no.
  #
  # Each status is captured and compared exactly, and a mismatch is a
  # non-zero exit. A check that only asserted the graph stayed up — or that
  # folded curl's own `000` into some other string — would report success on
  # a total failure.
  fail=0
  check() { # <label> <expected> <body>
    got="$(introspect "$3")"
    if [ "$got" = "$2" ]; then
      echo "  ok   $1 -> $2"
    else
      echo "  FAIL $1 -> expected $2, got $got"
      fail=1
    fi
  }
  check "a valid credential"        200 "$TOKEN"
  check "a bad signature"           401 "aaa.bbb.ccc"
  check "an empty credential"       400 ""
  check "a credential kagi refuses" 400 "not-a-jws"

  # The wrong method on the served path is NOT refused by the graph: `to_kagi`
  # forwards it with an empty credential, and kagi's MALFORMED is the 400.
  got="$(post_to /oauth/introspect "$TOKEN" GET)"
  if [ "$got" = "400" ]; then
    echo "  ok   a GET with a valid credential -> 400, kagi's MALFORMED"
  else
    echo "  FAIL a GET answered $got"
    fail=1
  fi
  rm -f /tmp/chronicle-idp-body.$$

  # 200 must carry the subject kagi extracted, and nothing else. A status
  # alone would pass even if the body were empty or somebody else's.
  introspect "$TOKEN" >/dev/null
  sub="$(cat /tmp/chronicle-idp-body.$$)"; rm -f /tmp/chronicle-idp-body.$$
  if [ "$sub" = "spiffe://example/workload/demo" ]; then
    echo "  ok   the 200 carries kagi's subject verbatim"
  else
    echo "  FAIL the 200 body was '$sub'"
    fail=1
  fi

  # A valid credential on a path this IdP does not serve gets nothing.
  #
  # The refusal is wave's: `http` routes `/oauth/introspect` EXACTLY, so an
  # unserved path is answered 404 at the gateway and never reaches the
  # pipeline at all. Asserted anyway, because it is the property a reader
  # will assume and the one that would break silently — adding a second
  # `app: true` route is all it takes for requests to a new path to start
  # arriving at `to_kagi`, which is why that decision tests the target as
  # well as the method rather than trusting the route table to stay narrow.
  got="$(post_to /not-a-route "$TOKEN")"
  if [ "$got" = "404" ]; then
    echo "  ok   a valid credential on an unserved path -> 404"
  else
    echo "  FAIL an unserved path answered $got"
    fail=1
  fi
  rm -f /tmp/chronicle-idp-body.$$

  # A refusal must carry NOTHING. This is the rule `VerifiedIdentity`
  # enforces on its own wire, checked where it reaches a client.
  introspect "aaa.bbb.ccc" >/dev/null
  body="$(cat /tmp/chronicle-idp-body.$$)"; rm -f /tmp/chronicle-idp-body.$$
  if [ -z "$body" ]; then
    echo "  ok   the 401 carries no body"
  else
    echo "  FAIL the 401 carried a body: '$body'"
    fail=1
  fi

  stop
  [ "$fail" = 0 ] || { echo "FAIL"; exit 1; }
  echo "ok: authored, served, and every arm answered with a kagi verdict"
  exit 0
fi

echo "== up on http://127.0.0.1:$IDP_PORT/oauth/introspect =="
echo "   try: curl -X POST --data-binary '$TOKEN' \\"
echo "          http://127.0.0.1:$IDP_PORT/oauth/introspect"
echo "   (Ctrl-C to stop)"
wait
