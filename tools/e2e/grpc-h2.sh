#!/usr/bin/env bash
# HTTP/2 composition E2E, fully self-contained: WAVE's http.fmod in server mode
# (h2c) on one graph, wave's http.fmod in client mode composed by a chronicle
# graph on the other. Real preface/SETTINGS/HPACK/WINDOW_UPDATE — the client
# auto-GETs / and the response body reaches stdout.
#
# The listen port is per-run and substituted into BOTH graphs (see lib.sh), so
# two concurrent runs never contend for one socket.
. "$(dirname "$0")/../lib.sh"
modules_ready || { no grpc-h2 "fluxor modules build failed"; finish; exit; }

H2_PORT=$(free_port)
PORT_SUB="s|port: [0-9]*|port: $H2_PORT|"

# The server graph runs for the duration; the client is a one-shot.
if ! start_graph examples/grpc_h2/_h2c_server.yaml "$PORT_SUB"; then
  no grpc-h2 "server build"; finish; exit
fi
SERVER_PID=$GRAPH_PID
wait_port "$H2_PORT" 30

if build_graph examples/grpc_h2/linux.yaml "$PORT_SUB"; then
  got=$(run_text "" 8)
  if want "$got" "Hello from Fluxor h2c"; then
    ok "grpc-h2 (composed wave http.fmod client: preface/SETTINGS/HPACK -> body)"
  else
    no grpc-h2 "got='$got'"
  fi
else
  no grpc-h2 "client build"
fi

reap_graph "$SERVER_PID"
finish
