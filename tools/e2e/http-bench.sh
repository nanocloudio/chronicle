#!/usr/bin/env bash
# THE HTTP BENCHMARK'S ENGINES, served on the host.
#
# `examples/http_bench/` is measured on the Pi 5 behind TLS, where CI cannot
# reach it. What it measures, though, is chronicle's: the engine nodes and the
# application-route wiring. This takes exactly those — the pipeline modules and
# the `http` ⇄ engine edges, copied out of each benchmark graph verbatim — and
# serves them from a plain-HTTP `http` on the host, so a graph that would
# answer wrongly, or not at all, on the rig fails here first.
#
# Asserted per graph: the status, the content type, the exact page the
# baseline route serves, and that more requests than the provider collects at
# once are all answered — each exchange is released when it is answered.
. "$(dirname "$0")/../lib.sh"
modules_ready || { no bench "fluxor modules build failed"; finish; exit; }

mkdir -p "$E2E_GEN"

# host_graph <bench yaml> <port> <out yaml>: the engines and their route wiring
# under a host `http`.
host_graph() {
  python3 - "$1" "$2" "$3" <<'PY'
import sys
src, port, out = sys.argv[1], sys.argv[2], sys.argv[3]
s = open(src).read()
# The engine modules: everything after the http module's own block.
start = s.index("\n  - name: ", s.index("  - name: http\n") + 1) + 1
engines = s[start:s.index("wiring:")]
route = s[s.index("  # The application route"):]
open(out, "w").write(f"""target: linux
tick_us: 100
scheduler:
  accept_cycles: true
platform:
  net: {{}}
modules:
  - name: http
    port: {port}
    host_tcp: 1
    routes:
      - path: "/"
        app: true
{engines}wiring:
  - from: linux_net.net_out
    to: http.net_in
  - from: http.net_out
    to: linux_net.net_in
{route}""")
PY
}

# The page every graph serves: the literal in the engines' `encode`, which is
# also the baseline route's body.
PAGE=$(python3 - <<'PY'
import re
s = open("examples/http_bench/one_node.yaml").read()
enc = re.search(r'encode: "([0-9a-f]+)"', s).group(1)
# LIT [len u16 LE] bytes, then FINISH.
assert enc[:2] == "60" and enc[-2:] == "6a"
n = int.from_bytes(bytes.fromhex(enc[2:6]), "little")
page = bytes.fromhex(enc[6:-2])
assert len(page) == n
print(page.decode())
PY
)

for g in one_node two_node; do
  port=$(free_port)
  yaml=$E2E_GEN/bench-$g.yaml
  host_graph "examples/http_bench/$g.yaml" "$port" "$yaml"
  if ! start_graph "$yaml"; then
    no bench "$g: graph build failed"
    continue
  fi
  if ! wait_port "$port" 30; then
    no bench "$g: never listened on $port"
    reap_graph "$GRAPH_PID"
    continue
  fi

  hdrs=$E2E_GEN/bench-$g.hdr
  body=$(curl -s --max-time 10 -D "$hdrs" "http://127.0.0.1:$port/")
  status=$(head -1 "$hdrs" | awk '{print $2}')
  ctype=$(grep -i '^content-type:' "$hdrs" | tr -d '\r' | awk '{print $2}')
  if [ "$status" = 200 ] && [ "$ctype" = "text/html" ] && [ "$body" = "$PAGE" ]; then
    ok "$g: 200, text/html and the exact page"
  else
    no bench "$g: status '$status', content type '$ctype', body ${#body} bytes (want ${#PAGE})"
  fi

  # Sequential and concurrent: more requests than the provider collects at
  # once, so a slot that was never released would leave the tail unanswered.
  codes=$(for _ in $(seq 1 24); do
            curl -s -o /dev/null -w '%{http_code}\n' --max-time 10 "http://127.0.0.1:$port/" &
          done; wait)
  n200=$(printf '%s\n' "$codes" | grep -c '^200$')
  if [ "$n200" = 24 ]; then
    ok "$g: 24 concurrent requests, all answered 200"
  else
    no bench "$g: $n200 of 24 concurrent requests answered 200"
  fi
  reap_graph "$GRAPH_PID"
done
finish
