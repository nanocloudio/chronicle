#!/usr/bin/env python3
"""Operator + client for the OIDC code-exchange leg, end to end over HTTP.

The OPERATOR half provisions the graph over the websocket mux, off the serving
path, exactly as a deployment's key ceremony and an /authorize step would:
  ch0  two SIGNING keys by LABEL — the ACCESS_TOKEN and ID_TOKEN profiles;
       token_mint generates each in the vault and holds the private half
  ch1  the device-certificate VERIFY key (so authcode's keyset is non-empty)
  ch2  a device record, and a single-use CODE record — the artefact /authorize
       would have created, seeded here so the exchange leg can be exercised on
       its own (operator state, not pipeline construction: the C14 line is
       about what the PIPELINE builds, and it builds nothing)

The CLIENT half is what a device does at the token endpoint: POST the code, the
registered redirect_uri, the client id and the PKCE verifier, and receive an
access token and an ID token — both minted inside kagi over the subject the
code carries, neither named by the client.

Ed25519 throughout. Every signing key's private half lives in kagi's vault; the
device thumbprint the code binds is computed here for realism but authenticates
nothing at exchange — the code and the PKCE verifier are the proof.
"""
import base64, hashlib, json, os, socket, struct, subprocess, sys, tempfile, time

MSG_KEY_ADD = 0x22
MSG_KEY_ACTIVATE = 0x23
MSG_STATE_PUT_ABS = 0x61
SUITE_ED25519 = 2
PROFILE_ACCESS_TOKEN = 1
PROFILE_ID_TOKEN = 6
PROFILE_DEVICE_CERT = 2
STATE_ADDED, STATE_ACTIVE = 0, 1
KEY_USE_VERIFY, KEY_USE_SIGN = 0x01, 0x02
NS_DEVICE = 2
NS_OAUTH_CODE = 7
CLIENT_ID = 9
MUX_MAGIC = 0xFC

ISSUER = b"https://issuer.authcode.test"
DEVICE_ID = b"dev_authcode_alice"
CLIENT = b"client-web"
REDIRECT_URI = b"https://app.authcode.test/cb"
SCOPE = b"openid profile"
NONCE = b"n-0S6_WzA2Mj"
CODE = b"codeAUTHCODEexchange01"          # 22 chars, the code id key
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

def jwk_thumbprint(pub32):
    x = base64.urlsafe_b64encode(pub32).rstrip(b"=").decode()
    canonical = '{"crv":"Ed25519","kty":"OKP","x":"%s"}' % x
    return base64.urlsafe_b64encode(
        hashlib.sha256(canonical.encode()).digest()).rstrip(b"=").decode()

def pkce_challenge():
    return base64.urlsafe_b64encode(
        hashlib.sha256(VERIFIER.encode()).digest()).rstrip(b"=").decode()

def key_record(profile, kid, suite, state, key_use, material):
    return env(MSG_KEY_ADD,
        f8(ISSUER) + struct.pack("<H", profile) + f8(kid)
        + struct.pack("<H", suite) + bytes([state, key_use])
        + struct.pack("<I", 1) + struct.pack("<Q", 0) + struct.pack("<Q", 0)
        + f16(material))

def key_ref(profile, kid, arg):
    return env(MSG_KEY_ACTIVATE,
        f8(ISSUER) + struct.pack("<H", profile) + f8(kid) + struct.pack("<Q", arg))

def put_abs(namespace, key, value):
    return env(MSG_STATE_PUT_ABS,
        struct.pack("<I", 1) + bytes([CLIENT_ID, namespace]) + f8(key)
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
        jkt = jwk_thumbprint(dev_pub)  # 43 chars

        ws = Ws("127.0.0.1", port)
        # ch0: two signing keys by LABEL (access + id), each ACTIVATED.
        ws.send(0, key_record(PROFILE_ACCESS_TOKEN, b"at-k1", SUITE_ED25519,
                              STATE_ADDED, KEY_USE_SIGN, b"kagi/authcode/access-token"))
        ws.send(0, key_ref(PROFILE_ACCESS_TOKEN, b"at-k1", 1))
        ws.send(0, key_record(PROFILE_ID_TOKEN, b"id-k1", SUITE_ED25519,
                              STATE_ADDED, KEY_USE_SIGN, b"kagi/authcode/id-token"))
        ws.send(0, key_ref(PROFILE_ID_TOKEN, b"id-k1", 1))
        # ch1: the device-cert verify key (keyset must be non-empty to serve).
        ws.send(1, key_record(PROFILE_DEVICE_CERT, b"dc-k1", SUITE_ED25519,
                              STATE_ACTIVE, KEY_USE_VERIFY, dc_pub))
        # ch2: a device record, and the single-use code /authorize would create.
        ws.send(2, put_abs(NS_DEVICE, DEVICE_ID, b'{"status":"active"}'))
        code_record = (
            b'{"sub":"tenant:alice","did":"' + DEVICE_ID + b'","jkt":"'
            + jkt.encode() + b'","cid":"' + CLIENT + b'","ruri":"' + REDIRECT_URI
            + b'","cc":"' + pkce_challenge().encode() + b'","nonce":"' + NONCE
            + b'","scope":"' + SCOPE + b'","at":"100"}')
        ws.send(2, put_abs(NS_OAUTH_CODE, CODE, code_record))
        time.sleep(1.0)  # let the mint open+announce and the ledger settle
        ws.close()

        # The client redeems: <code>\n<redirect_uri>\n<client_id>\n<verifier>.
        body = (CODE + b"\n" + REDIRECT_URI + b"\n" + CLIENT + b"\n"
                + VERIFIER.encode())
        status, token = http_post(port, "/oauth/token", body)
        print(f"STATUS {status}")
        print(token.decode(errors="replace"))
