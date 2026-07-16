#!/usr/bin/env python3
"""A minimal MQTT 3.1.1 broker — the counterparty for mqtt-sink-e2e.sh.

Chronicle composes quantum's `mqtt_client`; this proves a graph's published
frame reaches a broker with the right topic and payload. Running mosquitto in a
container was a heavy dependency for that assertion and made the test collide
with concurrent runs, so this speaks just enough of the protocol:

    CONNECT  -> CONNACK        SUBSCRIBE -> SUBACK
    PUBLISH  (QoS 0, captured) PINGREQ   -> PINGRESP

Like the SMTP sink it binds port 0 and prints the port, so nothing has to guess
a free one, and it serves CONTINUOUSLY — the harness boots the graph once to
build its bundle and again to drive the case, so a one-shot broker would miss
the real publish. Each captured PUBLISH overwrites <outfile> as
`<topic> <payload>`.

Quantum owns a real broker (`configs/quantum-linux-minimal.yaml`, an MQTT-only
21-module graph); driving chronicle's client against THAT is a worthwhile
integration test, but it belongs in a cross-project suite — chronicle's own gate
should not fail because a sibling's broker graph regressed.

Usage:  mqtt_broker.py <outfile>   # prints "<port>", then serves
"""

import pathlib
import socket
import sys

TIMEOUT_S = 30
CONNECT, CONNACK, PUBLISH, SUBSCRIBE, SUBACK = 1, 2, 3, 8, 9
PINGREQ, PINGRESP, DISCONNECT = 12, 13, 14


def read_exactly(conn, n):
    buf = b""
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


def read_remaining_length(conn):
    """MQTT's variable-length integer: 7 bits per byte, high bit continues."""
    value, shift = 0, 0
    for _ in range(4):
        b = read_exactly(conn, 1)
        if b is None:
            return None
        value |= (b[0] & 0x7F) << shift
        if not b[0] & 0x80:
            return value
        shift += 7
    return None


def serve(conn, out):
    while True:
        head = read_exactly(conn, 1)
        if head is None:
            return
        ptype = head[0] >> 4
        length = read_remaining_length(conn)
        if length is None:
            return
        body = read_exactly(conn, length) if length else b""
        if body is None:
            return

        if ptype == CONNECT:
            # session-present = 0, return code 0 (accepted)
            conn.sendall(bytes([CONNACK << 4, 0x02, 0x00, 0x00]))
        elif ptype == SUBSCRIBE:
            packet_id = body[:2]
            conn.sendall(bytes([SUBACK << 4, 0x03]) + packet_id + bytes([0x00]))
        elif ptype == PUBLISH:
            tlen = int.from_bytes(body[:2], "big")
            topic = body[2 : 2 + tlen].decode("utf-8", "replace")
            # QoS 0 has no packet identifier, so the payload is the remainder.
            payload = body[2 + tlen :].decode("utf-8", "replace")
            out.write_text(f"{topic} {payload}")
        elif ptype == PINGREQ:
            conn.sendall(bytes([PINGRESP << 4, 0x00]))
        elif ptype == DISCONNECT:
            return


def main():
    out = pathlib.Path(sys.argv[1])

    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    srv.settimeout(TIMEOUT_S)
    print(srv.getsockname()[1], flush=True)

    while True:
        try:
            conn, _ = srv.accept()
        except socket.timeout:
            return 0
        conn.settimeout(TIMEOUT_S)
        try:
            serve(conn, out)
        except OSError:
            pass  # half-finished session (the build-phase boot) — keep serving
        finally:
            conn.close()


if __name__ == "__main__":
    sys.exit(main())
