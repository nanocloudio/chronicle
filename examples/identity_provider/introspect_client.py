#!/usr/bin/env python3
"""Provision the IdP's verifying keyset and mint a credential to introspect.

This is the OPERATOR side of the introspection graph, and it is a host script
on purpose: it is what a deployment's key ceremony would do, and keeping it
outside the graph is the point — nothing on the serving path can reach
`verify_key`.

It does three things, all with `openssl` and the standard library:
  1. generate a P-256 key pair,
  2. push the PUBLIC half to `token_verify` as a kagi `MSG_KEY_ADD` record,
     over the control carrier, on channel 0,
  3. sign a short-lived ES256 JWS with the PRIVATE half and print it, once
     the endpoint answers it with something other than "no key yet".

The private key never leaves this script and never reaches the graph. That is
the shape a real issuer has: the verifier holds public keys only.
"""
import base64, os, struct, subprocess, sys, tempfile, time

sys.dont_write_bytecode = True   # leave no __pycache__ in the example
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from control import Control, envelope, until_served  # noqa: E402

MSG_KEY_ADD = 0x22
SUITE_ES256 = 1
PROFILE_ACCESS_TOKEN = 1
KEY_STATE_ACTIVE = 1
KEY_USE_VERIFY = 0x01

def b64u(b):     return base64.urlsafe_b64encode(b).rstrip(b"=")
def f8(b):       return bytes([len(b)]) + b
def f16(b):      return struct.pack("<H", len(b)) + b

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

if __name__ == "__main__":
    port = int(sys.argv[1])
    issuer, kid, sub = b"https://idp.example", b"k1", b"spiffe://example/workload/demo"
    with tempfile.TemporaryDirectory() as d:
        kp = os.path.join(d, "k.pem")
        pub = keypair(kp)
        ctl = Control(port)
        ctl.send(0, key_add(issuer, kid, pub))
        now = int(time.time())
        jws = sign_jws(kp, kid, [
            b'"iss":"' + issuer + b'"', b'"sub":"' + sub + b'"',
            b'"aud":"https://rs.example"', b'"scope":"read"',
            b'"jti":"demo-1"',
            b'"iat":' + str(now).encode(), b'"exp":' + str(now + 3600).encode(),
        ])
        # The key and the request travel separate paths; introspection is
        # read-only, so asking until the verifier holds the key is safe.
        until_served(port, "/oauth/introspect", jws)
        ctl.close()
        print(jws.decode())
