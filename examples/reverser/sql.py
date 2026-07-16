#!/usr/bin/env python3
"""Minimal PostgreSQL v3 simple-query client (stdlib only) for run.sh.

lattice's pg_edge_anchor runs open (no password) by default, so the
exchange is: StartupMessage -> AuthenticationOk -> ReadyForQuery ->
Query -> ... -> ReadyForQuery. One statement per argument; prints each
DataRow's first column. Exits non-zero on any ErrorResponse.

Usage: sql.py HOST PORT "SQL" ["SQL" ...]
"""
import socket
import struct
import sys


def main() -> int:
    host, port = sys.argv[1], int(sys.argv[2])
    sock = socket.create_connection((host, port), timeout=15)
    params = b"user\x00lattice\x00database\x00lattice\x00\x00"
    body = struct.pack(">I", 196608) + params
    sock.sendall(struct.pack(">I", len(body) + 4) + body)

    buf = b""

    def msg():
        nonlocal buf
        while True:
            if len(buf) >= 5:
                tag = buf[0:1]
                ln = struct.unpack(">I", buf[1:5])[0]
                if len(buf) >= 1 + ln:
                    payload = buf[5 : 1 + ln]
                    buf = buf[1 + ln :]
                    return tag, payload
            d = sock.recv(8192)
            if not d:
                raise ConnectionError("server closed")
            buf += d

    # Drain to the first ReadyForQuery (auth + parameter status).
    while True:
        tag, payload = msg()
        if tag == b"E":
            print(payload.decode(errors="replace"), file=sys.stderr)
            return 1
        if tag == b"Z":
            break

    for sql in sys.argv[3:]:
        q = sql.encode() + b"\x00"
        sock.sendall(b"Q" + struct.pack(">I", len(q) + 4) + q)
        while True:
            tag, payload = msg()
            if tag == b"E":
                print(payload.decode(errors="replace"), file=sys.stderr)
                return 1
            if tag == b"D":
                ncols = struct.unpack(">H", payload[:2])[0]
                if ncols >= 1:
                    ln = struct.unpack(">i", payload[2:6])[0]
                    if ln >= 0:
                        print(payload[6 : 6 + ln].decode(errors="replace"))
            if tag == b"Z":
                break
    return 0


if __name__ == "__main__":
    sys.exit(main())
