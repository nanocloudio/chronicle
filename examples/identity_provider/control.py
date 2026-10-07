"""The operator's control carrier: a websocket into the graph's `/ws` route,
and the `remote_channel` session it carries.

Every IdP graph takes its operator inputs — keys, ledger records — on this
carrier and never on the serving path. Bytes flow

    websocket binary frame → ws_net → remote_channel → one module channel

and `remote_channel` carries each channel's records whole, against the credit
its receiver grants:

    frame   [kind u8][channel u8][len u16 LE][body]
    HELLO   1  "FXRC"[count] then per channel [ct_len][content_type][max u32]
    REFUSE  2  [reason]                 the peer is ending the session
    CREDIT  3  [bytes u32]              room for that many more record bytes
    BEGIN   4  [record_len u32][first bytes]
    MORE    5  [bytes]                  the record in progress continues

The graph greets first; the client states the same table back, which opens
the session. Each side starts with one record's room per channel (its
`max_record` plus the four-byte charge every record costs), and CREDIT only
replenishes what the receiver has consumed.

No library: the websocket handshake is a dozen lines, and a dependency here
would be one the example made someone install to read it. Standard library
only.
"""
import base64, os, socket, struct, threading, time

HELLO, REFUSE, CREDIT, BEGIN, MORE = 1, 2, 3, 4, 5
CONTROL = 0xFF        # the channel byte HELLO and REFUSE name
RECORD_CHARGE = 4     # credit each record costs beyond its bytes


def envelope(msg_type, payload):
    """kagi's `auth_wire` envelope: [type u8][len u16 LE][payload]."""
    return bytes([msg_type]) + struct.pack("<H", len(payload)) + payload


class Control:
    """A held-open control session. A reader thread takes every record the
    graph sends back (key announcements, ledger acks) and returns its credit,
    so a channel the operator does not read is never what stalls the graph."""

    def __init__(self, port, host="127.0.0.1", path="/ws"):
        key = base64.b64encode(os.urandom(16))
        self.s = socket.create_connection((host, port), timeout=10)
        self.s.sendall(b"GET " + path.encode() + b" HTTP/1.1\r\nHost: "
                       + host.encode() + b"\r\nUpgrade: websocket\r\n"
                       + b"Connection: Upgrade\r\nSec-WebSocket-Key: " + key
                       + b"\r\nSec-WebSocket-Version: 13\r\n\r\n")
        head = b""
        while b"\r\n\r\n" not in head:
            chunk = self.s.recv(4096)
            if not chunk:
                raise SystemExit("the server closed during the websocket handshake")
            head += chunk
        if b" 101 " not in head.split(b"\r\n")[0] + b" ":
            raise SystemExit("websocket upgrade refused: "
                             + head.split(b"\r\n")[0].decode(errors="replace"))
        self.ws_buf = head.split(b"\r\n\r\n", 1)[1]
        self.mux_buf = b""
        self.lock = threading.Condition()
        self.send_lock = threading.Lock()   # the reader grants credit too
        self.hello = None
        self.error = None
        self.credit = [0] * 8
        self.partial = [None] * 8       # [remaining, bytes] per channel
        self.records = [[] for _ in range(8)]
        self.closed = False
        self.s.settimeout(0.2)
        self.reader = threading.Thread(target=self._pump, daemon=True)
        self.reader.start()
        self._open()

    # ── the session ─────────────────────────────────────────────────────────
    def _open(self):
        with self.lock:
            if not self.lock.wait_for(lambda: self.hello or self.error, timeout=10):
                raise SystemExit("the graph sent no remote_channel HELLO within 10 s")
            if self.error:
                raise SystemExit(self.error)
            hello = self.hello
        if hello[:4] != b"FXRC":
            raise SystemExit("malformed remote_channel HELLO")
        at = 5
        for ch in range(hello[4]):
            ct_len = hello[at]
            (max_record,) = struct.unpack_from("<I", hello, at + 1 + ct_len)
            at += 1 + ct_len + 4
            with self.lock:
                self.credit[ch] = max_record + RECORD_CHARGE
        self._frame(HELLO, CONTROL, hello)

    def send(self, ch, record, timeout=5.0):
        """Send one record on channel `ch`, within the credit the graph granted."""
        want = len(record) + RECORD_CHARGE
        with self.lock:
            if not self.lock.wait_for(lambda: self.credit[ch] >= want or self.error,
                                      timeout=timeout):
                raise SystemExit(f"channel {ch} granted no room for a {len(record)}-byte record")
            if self.error:
                raise SystemExit(self.error)
            self.credit[ch] -= want
        self._frame(BEGIN, ch, struct.pack("<I", len(record)) + record)

    def wait_records(self, ch, count, timeout=10.0):
        """Wait until `count` records have come back on channel `ch`."""
        with self.lock:
            if not self.lock.wait_for(lambda: len(self.records[ch]) >= count or self.error,
                                      timeout=timeout):
                raise SystemExit(f"channel {ch}: {len(self.records[ch])} of {count} "
                                 "records came back")
            if self.error:
                raise SystemExit(self.error)
            return list(self.records[ch])

    def close(self):
        self.closed = True
        self.reader.join(timeout=1)
        self.s.close()

    # ── the wire ────────────────────────────────────────────────────────────
    def _frame(self, kind, ch, body):
        """One remote_channel frame in one masked websocket binary frame."""
        frame = bytes([kind, ch]) + struct.pack("<H", len(body)) + body
        n = len(frame)
        hdr = bytes([0x82]) + (bytes([0x80 | n]) if n < 126
                               else bytes([0x80 | 126]) + struct.pack(">H", n))
        mask = os.urandom(4)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(frame))
        with self.send_lock:
            self.s.sendall(hdr + mask + masked)

    def _pump(self):
        while not self.closed:
            try:
                chunk = self.s.recv(65536)
            except socket.timeout:
                continue
            except OSError:
                return
            if not chunk:
                with self.lock:
                    self.error = self.error or "the graph closed the control websocket"
                    self.lock.notify_all()
                return
            self.ws_buf += chunk
            self._ws_frames()
            self._mux_frames()

    def _ws_frames(self):
        while len(self.ws_buf) >= 2:
            opcode, n = self.ws_buf[0] & 0x0F, self.ws_buf[1] & 0x7F
            at = 2
            if n == 126:
                if len(self.ws_buf) < 4:
                    return
                (n,) = struct.unpack_from(">H", self.ws_buf, 2); at = 4
            elif n == 127:
                if len(self.ws_buf) < 10:
                    return
                (n,) = struct.unpack_from(">Q", self.ws_buf, 2); at = 10
            if len(self.ws_buf) < at + n:
                return
            payload, self.ws_buf = self.ws_buf[at:at + n], self.ws_buf[at + n:]
            if opcode in (0x0, 0x1, 0x2):
                self.mux_buf += payload

    def _mux_frames(self):
        grants = []
        with self.lock:
            while len(self.mux_buf) >= 4:
                kind, ch = self.mux_buf[0], self.mux_buf[1]
                (n,) = struct.unpack_from("<H", self.mux_buf, 2)
                if len(self.mux_buf) < 4 + n:
                    break
                body, self.mux_buf = self.mux_buf[4:4 + n], self.mux_buf[4 + n:]
                if kind == HELLO:
                    self.hello = body
                elif kind == REFUSE:
                    self.error = ("the graph refused the remote_channel session, reason "
                                  f"{body[:1].hex()} (01 table, 02 protocol, 03 credit, "
                                  "04 oversize)")
                elif kind == CREDIT:
                    self.credit[ch] += struct.unpack_from("<I", body)[0]
                elif kind in (BEGIN, MORE):
                    record = self._take(kind, ch, body)
                    if record is not None:
                        self.records[ch].append(record)
                        grants.append((ch, len(record) + RECORD_CHARGE))
            self.lock.notify_all()
        # Give the room back, so the graph can send the next record.
        for ch, room in grants:
            self._frame(CREDIT, ch, struct.pack("<I", room))

    def _take(self, kind, ch, body):
        if kind == BEGIN:
            (total,) = struct.unpack_from("<I", body)
            self.partial[ch] = [total - (len(body) - 4), body[4:]]
        else:
            self.partial[ch][0] -= len(body)
            self.partial[ch][1] += body
        if self.partial[ch][0] == 0:
            record, self.partial[ch] = self.partial[ch][1], None
            return record
        return None


def http_post(port, path, body, method="POST"):
    """One HTTP/1.1 request; returns (status, body)."""
    s = socket.create_connection(("127.0.0.1", port), timeout=10)
    s.sendall((f"{method} {path} HTTP/1.1\r\nHost: idp\r\nContent-Type: text/plain\r\n"
               f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n").encode() + body)
    resp = b""
    while True:
        c = s.recv(4096)
        if not c:
            break
        resp += c
    s.close()
    head, _, payload = resp.partition(b"\r\n\r\n")
    return int(head.split()[1]), payload


def until_served(port, path, body, timeout=10.0):
    """POST until the answer is no longer 503, the status a kagi module's
    verdict maps to while it holds no key: a key travels the control carrier
    and a request the serving path, so nothing orders the two but this."""
    deadline = time.time() + timeout
    while True:
        status, payload = http_post(port, path, body)
        if status != 503 or time.time() >= deadline:
            return status, payload
        time.sleep(0.2)
