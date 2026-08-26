#!/usr/bin/env python3
"""Operator + client for the OIDC /authorize leg, end to end over HTTP.

The OPERATOR half provisions the graph over the websocket mux, off the serving
path:
  ch1  the device-certificate VERIFY key (so authcode can authenticate the cert)
  ch2  the client registry record (redirect_uri + registered scope)

The CLIENT half is what a device does at /authorize: present its certificate, a
DPoP proof, the client id, the redirect_uri, the requested scope, an opaque
state, a PKCE S256 challenge and a nonce — as newline-separated body fields —
and receive a single-use authorization code. The subject is established by kagi
FROM the certificate; the client names none. The requested scope asks for more
than the client is registered for, to show authcode clamps it.

Ed25519 throughout, via openssl. The issuer-dc and device private keys never
leave this script; authcode never mints here — /authorize only issues a code.
"""
import base64, hashlib, json, os, socket, struct, subprocess, sys, tempfile, time

MSG_KEY_ADD = 0x22
MSG_STATE_PUT_ABS = 0x61
SUITE_ED25519 = 2
PROFILE_DEVICE_CERT = 2
STATE_ACTIVE = 1
KEY_USE_VERIFY = 0x01
NS_OAUTH_CLIENT = 8
CLIENT_ID_BYTE = 9
MUX_MAGIC = 0xFC

ISSUER = b"https://issuer.authcode.test"
DEVICE_ID = b"dev_authcode_alice"
CLIENT = b"client-web"
REDIRECT_URI = b"https://app.authcode.test/cb"
REG_SCOPE = b"openid profile"
STATE = b"xyz-state-123"
NONCE = b"n-0S6_WzA2Mj"
VERIFIER = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"

def b64u(b):  return base64.urlsafe_b64encode(b).rstrip(b"=")
def f8(b):    return bytes([len(b)]) + b
def f16(b):   return struct.pack("<H", len(b)) + b
def env(t, p): return bytes([t]) + struct.pack("<H", len(p)) + p

def openssl(args, inp=None):
    return subprocess.run(["openssl", *args], check=True, capture_output=True,
                          input=inp).stdout

def ed25519_keypair(path):
    openssl(["genpkey", "-algorithm", "ed25519", "-out", path])
    der = openssl(["pkey", "-in", path, "-pubout", "-outform", "DER"])
    return der[-32:]

def ed25519_sign(path, msg):
    with tempfile.NamedTemporaryFile(delete=False) as f:
        f.write(msg); tmp = f.name
    try:
        return openssl(["pkeyutl", "-sign", "-inkey", path, "-rawin", "-in", tmp])
    finally:
        os.unlink(tmp)

def jws(key_path, header_obj, claims_obj):
    h = b64u(json.dumps(header_obj, separators=(",", ":")).encode())
    p = b64u(json.dumps(claims_obj, separators=(",", ":")).encode())
    si = h + b"." + p
    return si + b"." + b64u(ed25519_sign(key_path, si))

def jwk_thumbprint(pub32):
    x = base64.urlsafe_b64encode(pub32).rstrip(b"=").decode()
    canonical = '{"crv":"Ed25519","kty":"OKP","x":"%s"}' % x
    return base64.urlsafe_b64encode(
        hashlib.sha256(canonical.encode()).digest()).rstrip(b"=").decode()

def device_jwk(pub32):
    x = base64.urlsafe_b64encode(pub32).rstrip(b"=").decode()
    return {"crv": "Ed25519", "kty": "OKP", "x": x}

def pkce_challenge():
    return base64.urlsafe_b64encode(
        hashlib.sha256(VERIFIER.encode()).digest()).rstrip(b"=").decode()

def key_record(profile, kid, suite, state, key_use, material):
    return env(MSG_KEY_ADD,
        f8(ISSUER) + struct.pack("<H", profile) + f8(kid)
        + struct.pack("<H", suite) + bytes([state, key_use])
        + struct.pack("<I", 1) + struct.pack("<Q", 0) + struct.pack("<Q", 0)
        + f16(material))

def put_abs(namespace, key, value):
    return env(MSG_STATE_PUT_ABS,
        struct.pack("<I", 1) + bytes([CLIENT_ID_BYTE, namespace]) + f8(key)
        + f16(value) + struct.pack("<Q", 0))

class Ws:
    def __init__(self, host, port, path="/ws"):
        key = base64.b64encode(os.urandom(16))
        self.s = socket.create_connection((host, port), timeout=5)
        self.s.sendall(b"GET " + path.encode() + b" HTTP/1.1\r\nHost: " + host.encode()
            + b"\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
            + b"Sec-WebSocket-Key: " + key + b"\r\nSec-WebSocket-Version: 13\r\n\r\n")
        head = b""
        while b"\r\n\r\n" not in head:
            head += self.s.recv(4096)
        if b"101" not in head.split(b"\r\n")[0]:
            sys.exit("ws upgrade refused: " + head.split(b"\r\n")[0].decode())
    def send(self, ch, payload):
        frame = bytes([MUX_MAGIC, ch]) + struct.pack("<H", len(payload)) + payload
        mask = os.urandom(4)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(frame))
        n = len(frame)
        hdr = bytes([0x82]) + (bytes([0x80 | n]) if n < 126
              else bytes([0x80 | 126]) + struct.pack(">H", n))
        self.s.sendall(hdr + mask + masked)
        time.sleep(0.2)
    def close(self):
        self.s.close()

def http_post(port, path, body):
    s = socket.create_connection(("127.0.0.1", port), timeout=10)
    req = (f"POST {path} HTTP/1.1\r\nHost: idp\r\nContent-Type: text/plain\r\n"
           f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n").encode() + body
    s.sendall(req)
    resp = b""
    while True:
        c = s.recv(4096)
        if not c: break
        resp += c
    s.close()
    head, _, payload = resp.partition(b"\r\n\r\n")
    status = int(head.split()[1])
    return status, payload

if __name__ == "__main__":
    port = int(sys.argv[1])
    with tempfile.TemporaryDirectory() as d:
        dc_key = os.path.join(d, "dc.pem")
        dev_key = os.path.join(d, "dev.pem")
        dc_pub = ed25519_keypair(dc_key)
        dev_pub = ed25519_keypair(dev_key)

        ws = Ws("127.0.0.1", port)
        # ch1: the device-cert verify key.
        ws.send(1, key_record(PROFILE_DEVICE_CERT, b"dc-k1", SUITE_ED25519,
                              STATE_ACTIVE, KEY_USE_VERIFY, dc_pub))
        # ch2: the client registry (redirect_uri + registered scope).
        client_rec = (b'{"redirect_uri":"' + REDIRECT_URI + b'","scope":"'
                      + REG_SCOPE + b'"}')
        ws.send(2, put_abs(NS_OAUTH_CLIENT, CLIENT, client_rec))
        time.sleep(1.0)
        ws.close()

        # The device certificate (dc+jwt), signed by the issuer-dc key.
        cert = jws(dc_key,
            {"alg": "EdDSA", "cty": "dc+jwt", "kid": "dc-k1"},
            {"cnf": {"jkt": jwk_thumbprint(dev_pub)},
             "device_id": DEVICE_ID.decode(), "exp": 9999999999, "iat": 100,
             "iss": "https://issuer.authcode.test", "sub": "tenant:alice"})
        # The DPoP proof, bound to POST /oauth/authorize.
        proof = jws(dev_key,
            {"typ": "dpop+jwt", "alg": "EdDSA", "jwk": device_jwk(dev_pub)},
            {"htm": "POST", "htu": "/oauth/authorize", "jti": "authz-1",
             "iat": int(time.time())})

        # The body: eight newline-separated fields. The requested scope asks
        # for MORE than registered, to show authcode clamps it into the code.
        body = b"\n".join([
            cert, proof, CLIENT, REDIRECT_URI,
            b"openid profile email", STATE, pkce_challenge().encode(), NONCE,
        ])
        status, code = http_post(port, "/oauth/authorize", body)
        print(f"STATUS {status}")
        print(code.decode(errors="replace"))
