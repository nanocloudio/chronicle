#!/usr/bin/env python3
"""Minimal HTTP/1.1 counterparty for the exchange E2E.

Binds port 0 and prints the chosen port, so concurrent runs cannot collide on
a guessed free port (same rule as `mqtt_broker.py`).

Answers any request with a body that ECHOES THE PATH, which is what makes the
test meaningful: a reply carrying the path proves the request the graph built
actually reached the origin, not merely that some response came back.

Records each request line to <outfile> so the driver can assert what was sent.
"""
import pathlib
import socket
import sys

TIMEOUT_S = 30


def serve(conn, out):
    conn.settimeout(TIMEOUT_S)
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = conn.recv(4096)
        if not chunk:
            return
        data += chunk

    head = data.split(b"\r\n", 1)[0]
    out.write_bytes(head)

    parts = head.split(b" ")
    path = parts[1] if len(parts) > 1 else b"/"
    body = b"echo:" + path
    conn.sendall(
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Type: text/plain\r\n"
        b"Content-Length: " + str(len(body)).encode() + b"\r\n"
        b"Connection: close\r\n"
        b"\r\n" + body
    )


def main():
    out = pathlib.Path(sys.argv[1])
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(4)
    srv.settimeout(TIMEOUT_S)
    print(srv.getsockname()[1], flush=True)

    while True:
        try:
            conn, _ = srv.accept()
        except socket.timeout:
            return
        try:
            serve(conn, out)
        except Exception:
            pass
        finally:
            conn.close()


if __name__ == "__main__":
    main()
