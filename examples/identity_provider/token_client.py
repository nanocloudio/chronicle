#!/usr/bin/env python3
"""Operator + client for the /oauth/token slice, end to end over HTTP.

The OPERATOR half provisions the graph exactly as a deployment's key ceremony
would — over the websocket mux, off the serving path:
  ch0  a SIGNING key by LABEL (token_mint generates it in the vault and
       announces the public half back, which this reads to verify the token)
  ch1  the device-certificate VERIFY key (the issuer-dc public half)
  ch2  a device record seeded into the ledger

The CLIENT half is what a device does: present a device certificate and a
DPoP proof as the POST /oauth/token body, and receive an access token.

Ed25519 throughout, via openssl. The signing key's private half is generated
inside kagi's vault and never exists here; the issuer-dc and device private
keys never leave this script.
"""
import base64, hashlib, json, os, socket, struct, subprocess, sys, tempfile, time

MSG_KEY_ADD = 0x22
MSG_KEY_ACTIVATE = 0x23
MSG_STATE_PUT_ABS = 0x61
SUITE_ED25519 = 2
PROFILE_ACCESS_TOKEN = 1
PROFILE_DEVICE_CERT = 2
STATE_ADDED, STATE_ACTIVE = 0, 1
KEY_USE_VERIFY, KEY_USE_SIGN = 0x01, 0x02
NS_DEVICE = 2
CLIENT_ID = 9
MUX_MAGIC = 0xFC

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
    return der[-32:]  # Ed25519 SPKI is 44 bytes; the raw public key is the tail

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

def key_record(profile, kid, suite, state, key_use, material):
    return env(MSG_KEY_ADD,
        f8(b"https://issuer.token.test") + struct.pack("<H", profile) + f8(kid)
        + struct.pack("<H", suite) + bytes([state, key_use])
        + struct.pack("<I", 1) + struct.pack("<Q", 0) + struct.pack("<Q", 0)
        + f16(material))

def key_ref(profile, kid, arg):
    return env(MSG_KEY_ACTIVATE,
        f8(b"https://issuer.token.test") + struct.pack("<H", profile) + f8(kid)
        + struct.pack("<Q", arg))

def put_abs(key, value):
    return env(MSG_STATE_PUT_ABS,
        struct.pack("<I", 1) + bytes([CLIENT_ID, NS_DEVICE]) + f8(key)
        + f16(value) + struct.pack("<Q", 0))

def jwk_thumbprint(pub32):
    x = base64.urlsafe_b64encode(pub32).rstrip(b"=").decode()
    canonical = '{"crv":"Ed25519","kty":"OKP","x":"%s"}' % x
    return base64.urlsafe_b64encode(hashlib.sha256(canonical.encode()).digest()).rstrip(b"=").decode()

def device_jwk(pub32):
    x = base64.urlsafe_b64encode(pub32).rstrip(b"=").decode()
    return {"crv": "Ed25519", "kty": "OKP", "x": x}

class Ws:
    """A held-open websocket carrying mux frames; can send many and read one."""
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
        self.buf = head.split(b"\r\n\r\n", 1)[1]
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
    device_id = b"dev_token_alice"
    with tempfile.TemporaryDirectory() as d:
        dc_key = os.path.join(d, "dc.pem")      # device-cert issuer key
        dev_key = os.path.join(d, "dev.pem")    # the device's own key
        dc_pub = ed25519_keypair(dc_key)
        dev_pub = ed25519_keypair(dev_key)

        ws = Ws("127.0.0.1", port)
        # ch0: the mint's signing key by LABEL, then ACTIVATE.
        label = b"kagi/token/access-token"
        ws.send(0, key_record(PROFILE_ACCESS_TOKEN, b"mint-k1", SUITE_ED25519,
                              STATE_ADDED, KEY_USE_SIGN, label))
        ws.send(0, key_ref(PROFILE_ACCESS_TOKEN, b"mint-k1", 1))
        # ch1: the device-cert verify key (public).
        ws.send(1, key_record(PROFILE_DEVICE_CERT, b"dc-k1", SUITE_ED25519,
                              STATE_ACTIVE, KEY_USE_VERIFY, dc_pub))
        # ch2: seed the device record.
        ws.send(2, put_abs(device_id, b'{"status":"active"}'))
        time.sleep(1.0)  # let the mint open+announce and the ledger settle
        ws.close()

        # The device certificate (dc+jwt), signed by the issuer-dc key.
        cert = jws(dc_key,
            {"alg": "EdDSA", "cty": "dc+jwt", "kid": "dc-k1"},
            {"cnf": {"jkt": jwk_thumbprint(dev_pub)},
             "device_id": device_id.decode(), "exp": 9999999999, "iat": 100,
             "iss": "https://issuer.token.test", "sub": "tenant:alice"})
        # The DPoP proof, signed by the device key, bound to POST /oauth/token.
        proof = jws(dev_key,
            {"typ": "dpop+jwt", "alg": "EdDSA", "jwk": device_jwk(dev_pub)},
            {"htm": "POST", "htu": "/oauth/token", "jti": "token-1",
             "iat": int(time.time())})

        # Present both as the body: <certificate>\n<proof>.
        status, token = http_post(port, "/oauth/token", cert + b"\n" + proof)
        print(f"STATUS {status}")
        print(token.decode(errors="replace"))
