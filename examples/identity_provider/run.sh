#!/usr/bin/env bash
# Run the identity provider from this directory alone.
#
# Everything it needs is here: the dispatch document, the graph, the operator
# client, and the verification. Nothing outside `examples/identity_provider/`
# is edited to run it; the modules come from the Fluxor OCI store by pin,
# which is what makes the IdP a COMPOSITION rather than a build.
#
#   ./run.sh            author, start, provision, and stay up (Ctrl-C to stop)
#   ./run.sh --verify   start, assert every arm of the chain, tear down
#   ./run.sh --stop     kill anything this script left running
#
# The port is overridable so two copies can run side by side:
#   IDP_PORT=15100 ./run.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT="$(cd "$HERE/../.." && pwd)"

IDP_PORT="${IDP_PORT:-15100}"
PIDFILE="/tmp/chronicle-idp.pid"
CLIENT="/tmp/chronicle-idp-client.$$.py"

export IDP_PORT

stop() {
  if [ -f "$PIDFILE" ]; then
    kill "$(cat "$PIDFILE")" 2>/dev/null || true
    rm -f "$PIDFILE"
  fi
  rm -f "$CLIENT"
}

case "${1:-}" in
  --stop) stop; echo "stopped"; exit 0 ;;
esac
trap stop EXIT

# ── the /oauth/token slice ──────────────────────────────────────────────────
#
# A SECOND graph in this example (chronicle_token.yaml) serving POST
# /oauth/token through mint_admission's grant mode. It needs the full grant
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
  ( cd "$PROJECT" && exec fluxor run "$HERE/chronicle_token.yaml" ) &
  echo $! > "$PIDFILE"
  for _ in $(seq 1 40); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$IDP_PORT") 2>/dev/null; then exec 3>&- 3<&-; break; fi
    sleep 0.25
  done

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
  ( cd "$PROJECT" && exec fluxor run "$HERE/chronicle_authorize.yaml" ) &
  echo $! > "$PIDFILE"
  for _ in $(seq 1 40); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$IDP_PORT") 2>/dev/null; then exec 3>&- 3<&-; break; fi
    sleep 0.25
  done

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
  ( cd "$PROJECT" && exec fluxor run "$HERE/chronicle_exchange.yaml" ) &
  echo $! > "$PIDFILE"
  for _ in $(seq 1 40); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$IDP_PORT") 2>/dev/null; then exec 3>&- 3<&-; break; fi
    sleep 0.25
  done

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
# Written out rather than shipped as a fifth file, because it is not part of
# the application: it is a KEY CEREMONY and a client, and both belong outside
# the graph. It provisions `token_verify`'s keyset over the websocket and
# mints one credential to introspect. The private half never reaches the
# graph — a verifier holds public keys only, which is the whole reason the
# keyset arrives as `MSG_KEY_ADD` carrying a public point.
cat > "$CLIENT" <<'PYEOF'
#!/usr/bin/env python3
"""Provision the IdP's verifying keyset and mint a credential to introspect.

This is the OPERATOR side of the example, and it is a host script on purpose:
it is what a deployment's key ceremony would do, and keeping it outside the
graph is the point — nothing on the serving path can reach `verify_key`.

It does three things, all with `openssl` and the standard library:
  1. generate a P-256 key pair,
  2. push the PUBLIC half to `token_verify` as a kagi `MSG_KEY_ADD` frame,
     over the websocket, on mux channel 0,
  3. sign a short-lived ES256 JWS with the PRIVATE half and print it.

The private key never leaves this script and never reaches the graph. That is
the shape a real issuer has: the verifier holds public keys only.
"""
import base64, hashlib, os, socket, struct, subprocess, sys, tempfile, time

MSG_KEY_ADD = 0x22
SUITE_ES256 = 1
PROFILE_ACCESS_TOKEN = 1
KEY_STATE_ACTIVE = 1
KEY_USE_VERIFY = 0x01
MUX_MAGIC = 0xFC

def b64u(b):     return base64.urlsafe_b64encode(b).rstrip(b"=")
def f8(b):       return bytes([len(b)]) + b
def f16(b):      return struct.pack("<H", len(b)) + b
def envelope(t, p): return bytes([t]) + struct.pack("<H", len(p)) + p

def openssl(args, **kw):
    return subprocess.run(["openssl", *args], check=True, capture_output=True, **kw).stdout

def keypair(path):
    """A P-256 key pair; returns the uncompressed SEC1 public point."""
    openssl(["ecparam", "-name", "prime256v1", "-genkey", "-noout", "-out", path])
    txt = openssl(["ec", "-in", path, "-text", "-noout"]).decode()
    # The `pub:` block is the uncompressed point, hex, one byte per group.
    take, hexes = False, []
    for line in txt.splitlines():
        if line.strip().startswith("pub:"):
            take = True; continue
        if take:
            if ":" not in line: break
            hexes += [x for x in line.strip().split(":") if x]
            if len(bytes.fromhex("".join(hexes))) >= 65: break
    pub = bytes.fromhex("".join(hexes))[:65]
    if len(pub) != 65 or pub[0] != 0x04:
        sys.exit("could not read an uncompressed P-256 public point")
    return pub

def key_add(issuer, kid, pub):
    """kagi `MSG_KEY_ADD` — a `KeyRecord` carrying the PUBLIC half only."""
    body = (f8(issuer) + struct.pack("<H", PROFILE_ACCESS_TOKEN) + f8(kid)
            + struct.pack("<H", SUITE_ES256)
            + bytes([KEY_STATE_ACTIVE, KEY_USE_VERIFY])
            + struct.pack("<I", 1)            # generation
            + struct.pack("<Q", 0)            # activate_after: immediately
            + struct.pack("<Q", 0)            # remove_after: no deadline
            + f16(pub))
    return envelope(MSG_KEY_ADD, body)

def der_to_raw(der):
    """DER SEQUENCE{INTEGER r, INTEGER s} -> the 64-byte r||s JOSE form."""
    assert der[0] == 0x30
    i = 2 if der[1] < 0x80 else 3 + (der[1] & 0x7F) - 1
    out = b""
    for _ in range(2):
        assert der[i] == 0x02
        n = der[i + 1]; v = der[i + 2:i + 2 + n]; i += 2 + n
        out += v.lstrip(b"\x00").rjust(32, b"\x00")
    return out

def sign_jws(key_path, kid, claims):
    hdr = b'{"alg":"ES256","typ":"JWT","kid":"' + kid + b'"}'
    payload = b"{" + b",".join(claims) + b"}"
    signing_input = b64u(hdr) + b"." + b64u(payload)
    with tempfile.NamedTemporaryFile(delete=False) as f:
        f.write(signing_input); tmp = f.name
    try:
        der = openssl(["dgst", "-sha256", "-sign", key_path, tmp])
    finally:
        os.unlink(tmp)
    return signing_input + b"." + b64u(der_to_raw(der))

def ws_push(host, port, path, payload, channel=0):
    """Open a websocket and send ONE mux frame. No library: the handshake is
    thirteen lines and a dependency here would be a dependency the example
    made someone install to read it."""
    key = base64.b64encode(os.urandom(16))
    s = socket.create_connection((host, port), timeout=5)
    s.sendall(b"GET " + path.encode() + b" HTTP/1.1\r\nHost: " + host.encode()
              + b"\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
              + b"Sec-WebSocket-Key: " + key + b"\r\nSec-WebSocket-Version: 13\r\n\r\n")
    head = b""
    while b"\r\n\r\n" not in head:
        chunk = s.recv(4096)
        if not chunk: sys.exit("the server closed during the websocket handshake")
        head += chunk
    if b"101" not in head.split(b"\r\n")[0]:
        sys.exit("websocket upgrade refused: " + head.split(b"\r\n")[0].decode())
    frame = bytes([MUX_MAGIC, channel]) + struct.pack("<H", len(payload)) + payload
    mask = os.urandom(4)
    masked = bytes(b ^ mask[i % 4] for i, b in enumerate(frame))
    hdr = bytes([0x82])                      # FIN + binary
    n = len(frame)
    hdr += bytes([0x80 | n]) if n < 126 else bytes([0x80 | 126]) + struct.pack(">H", n)
    s.sendall(hdr + mask + masked)
    time.sleep(0.5)                          # let the frame reach the module
    return s                                 # held open by the caller

if __name__ == "__main__":
    port = int(sys.argv[1])
    issuer, kid, sub = b"https://idp.example", b"k1", b"spiffe://example/workload/demo"
    with tempfile.TemporaryDirectory() as d:
        kp = os.path.join(d, "k.pem")
        pub = keypair(kp)
        sock = ws_push("127.0.0.1", port, "/ws", key_add(issuer, kid, pub))
        now = int(time.time())
        jws = sign_jws(kp, kid, [
            b'"iss":"' + issuer + b'"', b'"sub":"' + sub + b'"',
            b'"aud":"https://rs.example"', b'"scope":"read"',
            b'"jti":"demo-1"',
            b'"iat":' + str(now).encode(), b'"exp":' + str(now + 3600).encode(),
        ])
        print(jws.decode())
        sock.close()
PYEOF

# Author the document on device, which is also the check that it still
# compiles: every artefact digest below is one `chronicle author` sealed with
# no host toolchain involved at any step.
#
# It is run BEFORE the graph starts rather than as a separate step someone
# might skip: a graph whose params drifted from the document they were
# generated from is a graph that does something the document does not say.
echo "== authoring idp.uproc =="

# The document must fit `chronicle_cli`'s `UPROC_BUF` (32768 bytes of SOURCE
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
UPROC_MAX=32768
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
( cd "$PROJECT" && exec fluxor run "$HERE/chronicle_idp.yaml" ) &
echo $! > "$PIDFILE"

# Wait for the listener rather than sleeping a guess: a fixed sleep is either
# too short on a loaded machine or wasted on an idle one.
for _ in $(seq 1 150); do
  if (exec 3<>"/dev/tcp/127.0.0.1/$IDP_PORT") 2>/dev/null; then
    exec 3<&- 2>/dev/null || true
    break
  fi
  sleep 0.2
done

# ── provision the keyset, and mint one credential ──────────────────────────
echo "== provisioning the verifying keyset =="
TOKEN="$(python3 "$CLIENT" "$IDP_PORT" | tail -1)"
[ -n "$TOKEN" ] || { echo "the key ceremony produced no credential"; exit 1; }

introspect() {
  post_to /oauth/introspect "$1"
}

# The same request against any path, so a route can be tested rather than
# assumed.
post_to() {
  curl -s -o /tmp/chronicle-idp-body.$$ -w '%{http_code}' --max-time 10 \
       -X POST --data-binary "$2" "http://127.0.0.1:$IDP_PORT$1"
}

if [ "${1:-}" = "--verify" ]; then
  echo "== verifying =="
  # Every assertion here is a REAL HTTP round trip whose status came out of a
  # kagi verdict. That is the whole claim the example makes, so it is the
  # whole thing the check tests — including the refusals, because a chain
  # that only ever answers 200 has not shown that it can say no.
  #
  # An early draft of this script asserted only that the graph stayed up,
  # with `|| echo 000` appended to curl's own `000` — producing `000000`,
  # which did not match the `000` guard, so it REPORTED SUCCESS ON A TOTAL
  # FAILURE. Hence: capture the status, compare it exactly, and let a
  # non-zero exit be a failure.
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
  # arriving at `to_kagi`, which is why that decision now tests the path as
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
