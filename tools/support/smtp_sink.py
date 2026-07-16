#!/usr/bin/env python3
"""A minimal ESMTP submission sink — the counterparty for smtp-e2e.sh.

Chronicle composes wave's `smtp` module; this proves a message authored by a
graph reaches a server and arrives intact. A full mail server (MailHog in a
container) was overkill for that and made the test depend on Docker, so this
speaks exactly the lockstep the module drives:

    220 greeting -> EHLO/250 -> MAIL FROM/250 -> RCPT TO/250
                 -> DATA/354 -> <message>.CRLF/250 -> QUIT/221

Two things it does that a real server makes awkward:

  * the EHLO reply is deliberately MULTI-LINE (`250-` continuations then a final
    `250 `), which is the shape `smtp_core::smtp_reply_line` has to get right and
    which nothing else exercises;
  * it binds port 0 and prints the assigned port, so the harness never has to
    guess a free one — no window between choosing a port and something binding
    it, and concurrent runs cannot collide.

It serves CONTINUOUSLY rather than once: the harness builds the graph bundle by
booting the graph (which connects), then runs it again to drive the case — so a
one-shot sink would miss the real delivery. Each completed message overwrites
<outfile>; the harness polls that file and kills the sink when done.

Usage:  smtp_sink.py <outfile>   # prints "<port>" on stdout, then serves
The received message (headers + body, dot-unstuffed) lands in <outfile>.
"""

import pathlib
import socket
import sys

TIMEOUT_S = 30


def reply(conn, text):
    conn.sendall(text.encode())


def main():
    out = pathlib.Path(sys.argv[1])

    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    srv.settimeout(TIMEOUT_S)
    # The harness reads this to build the graph's endpoint param.
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
            pass  # a half-finished conversation (the build-phase boot) — keep serving
        finally:
            conn.close()


def serve(conn, out):
    reply(conn, "220 chronicle-test ESMTP ready\r\n")

    buf = b""
    body = None
    in_data = False
    while True:
        try:
            chunk = conn.recv(4096)
        except socket.timeout:
            break
        if not chunk:
            break
        buf += chunk

        if in_data:
            # The message ends at CRLF '.' CRLF; everything before it is content.
            if b"\r\n.\r\n" in buf:
                raw, _, buf = buf.partition(b"\r\n.\r\n")
                # Undo RFC 5321 dot-stuffing.
                body = raw.replace(b"\r\n..", b"\r\n")
                in_data = False
                reply(conn, "250 2.0.0 Ok: queued\r\n")
            continue

        while b"\r\n" in buf and not in_data:
            line, _, buf = buf.partition(b"\r\n")
            verb = line[:4].upper()
            if verb in (b"EHLO", b"HELO"):
                # Multi-line on purpose — see the module docstring above.
                reply(
                    conn,
                    "250-chronicle-test greets you\r\n"
                    "250-SIZE 10240000\r\n"
                    "250-8BITMIME\r\n"
                    "250 HELP\r\n",
                )
            elif verb == b"MAIL":
                reply(conn, "250 2.1.0 Sender ok\r\n")
            elif verb == b"RCPT":
                reply(conn, "250 2.1.5 Recipient ok\r\n")
            elif verb == b"DATA":
                reply(conn, "354 End data with <CR><LF>.<CR><LF>\r\n")
                in_data = True
            elif verb == b"QUIT":
                reply(conn, "221 2.0.0 Bye\r\n")
                if body:
                    out.write_bytes(body)
                return
            elif verb == b"RSET":
                reply(conn, "250 2.0.0 Ok\r\n")
            else:
                reply(conn, "502 5.5.2 Command not implemented\r\n")

    if body:
        out.write_bytes(body)


if __name__ == "__main__":
    sys.exit(main())
