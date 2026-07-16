# Shared harness for chronicle's CI gates and E2E drivers (tools/ci, tools/e2e)
# — sourced, not run.
#
# Each E2E builds an example graph bundle with `fluxor run` (killed once the
# bundle lands — the graph itself idles forever), then drives the fluxor-linux
# runtime directly so stdin/stdout are the graph's own cli ports with no build
# chatter mixed in. Hex in / hex out keeps binary frames printable and
# assertable with plain string compares.
#
# CONCURRENCY: these run in parallel — two worktrees on different branches, a
# human alongside CI, several cases at once. Nothing here may be a fixed global
# name, because a second run would tear down the first one's world mid-test and
# the symptom is an unrelated-looking protocol failure. Every shared resource is
# namespaced by $E2E_RUN:
#
#   bundle path   `fluxor run` derives it from the yaml BASENAME, so the graph
#                 is copied to `<name>-$E2E_RUN.yaml` → target/linux/<that>/
#   host ports    allocated from the kernel's ephemeral range, then substituted
#                 into the generated graph (never hard-coded in a config a
#                 second run could also bind)
#   helpers       support/*.py bind port 0 and REPORT the port, so there is no
#                 free-port guess to race
#   cleanup       PID-scoped: kill only what THIS run spawned. A `pkill -f` on
#                 a path pattern would reap a sibling run's runtime.
#
# Set E2E_RUN yourself to make a run's artefacts identifiable; it defaults to
# the shell's PID, which is unique among concurrent runs on one machine.
set -u
# No job-control monitoring: cleanup kills this run's own background helpers,
# and the shell's "Killed" notices for them are noise, not signal.
set +m
# Walk up to the project root by MARKER, not by relative depth. Callers sit at
# different depths (`tools/ci/`, `tools/e2e/`), and a `cd ../..` that silently
# lands one level off would leave every relative path in every script wrong.
_root=$(cd "$(dirname "$0")" && while [ ! -f fluxor.toml ] && [ "$PWD" != / ]; do cd ..; done; pwd)
[ -f "$_root/fluxor.toml" ] || { echo "lib.sh: no fluxor.toml above $0" >&2; exit 1; }
cd "$_root"
FLX=${FLUXOR_BIN:-fluxor}
FL_BIN=$PWD/target/aarch64-unknown-linux-gnu/release/fluxor-linux
E2E_RUN=${E2E_RUN:-$$}
E2E_GEN=target/e2e/$E2E_RUN

# Every process this run spawned, so cleanup can be exact.
E2E_PIDS=""

# LEAK GUARD: `fluxor run` spawns fluxor-linux as a CHILD; killing only the
# parent orphans the child, which runs its scheduler loop forever holding a
# ~100 MB arena. (The CLI now also sets PR_SET_PDEATHSIG on the child, so this
# is belt-and-braces for older CLIs.) Reap the child FIRST — `pkill -P` only
# works while the parent still exists; after it dies the child reparents.
reap_graph() { # <pid of the fluxor CLI>
  pkill -TERM -P "$1" 2>/dev/null
  kill "$1" 2>/dev/null
  wait "$1" 2>/dev/null
}

e2e_cleanup() {
  local p
  for p in $E2E_PIDS; do
    pkill -KILL -P "$p" 2>/dev/null
    kill -KILL "$p" 2>/dev/null
  done
  rm -rf "$E2E_GEN"
}
trap e2e_cleanup EXIT

pass=0
fail=0
ok() { echo "  PASS  $1"; pass=$((pass + 1)); }
no() { echo "  FAIL  $1: $2"; fail=$((fail + 1)); }
finish() { echo "== $pass passed, $fail failed =="; [ "$fail" -eq 0 ]; }

# The .fmod artefacts must exist and be current — a stale/cleaned target dir
# presents as a runtime "module not found" or, worse, runs old behaviour.
modules_ready() { timeout 900 "$FLX" modules build --target bcm2712 >/dev/null 2>&1; }

# A free TCP port, taken from the kernel rather than picked from a constant so
# two runs cannot choose the same one. Prefer a helper that binds port 0 and
# REPORTS its port (support/*.py do) — that has no window at all; this is for
# callers that must know the port before the listener exists.
free_port() {
  python3 -c 'import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()'
}

# `[ip:4][port:2 LE]` — the endpoint param form, for 127.0.0.1:<port>.
endpoint_hex() { printf '7f000001%02x%02x' $(($1 & 0xff)) $((($1 >> 8) & 0xff)); }

# build_graph <yaml> [sed-expr…]: copy the example to a run-unique name, apply
# any substitutions (ports), build its bundle, export CFG/MODS for run_*.
# The copy is what makes the bundle path unique — `fluxor run` keys the output
# dir on the yaml's basename.
build_graph() {
  local src=$1 base
  shift
  base=$(basename "$src" .yaml)
  local yaml=$E2E_GEN/$base-$E2E_RUN.yaml
  mkdir -p "$E2E_GEN"
  cp "$src" "$yaml"
  local e
  for e in "$@"; do sed -i "$e" "$yaml"; done

  CFG=target/linux/$base-$E2E_RUN/config.bin
  MODS=target/linux/$base-$E2E_RUN/modules.bin
  rm -f "$CFG" "$MODS"
  "$FLX" run "$yaml" </dev/null >/dev/null 2>&1 &
  local pid=$!
  E2E_PIDS="$E2E_PIDS $pid"
  for _ in $(seq 1 240); do
    [ -s "$CFG" ] && [ -s "$MODS" ] && break
    sleep 0.5
  done
  sleep 0.3
  reap_graph "$pid"
  [ -s "$CFG" ] && [ -s "$MODS" ]
}

# start_graph <yaml> [sed-expr…]: build, then leave the graph RUNNING (a server
# case). Exports GRAPH_PID; the caller stops it with reap_graph.
start_graph() {
  build_graph "$@" || return 1
  "$FL_BIN" --config "$CFG" --modules "$MODS" </dev/null >/dev/null 2>&1 &
  GRAPH_PID=$!
  E2E_PIDS="$E2E_PIDS $GRAPH_PID"
}

# run_hex <input_hex> <secs> [argv…]: drive the built bundle; stdout as hex.
# The runtime never exits on its own, so the timeout IS the run window.
run_hex() {
  local hex=$1 secs=$2
  shift 2
  printf '%s' "$hex" | xxd -r -p \
    | timeout -k 2 "$secs" "$FL_BIN" --config "$CFG" --modules "$MODS" "$@" 2>/dev/null \
    | xxd -p | tr -d '\n'
}

# run_text <input_hex> <secs> [argv…]: same, raw text stdout.
run_text() {
  local hex=$1 secs=$2
  shift 2
  printf '%s' "$hex" | xxd -r -p \
    | timeout -k 2 "$secs" "$FL_BIN" --config "$CFG" --modules "$MODS" "$@" 2>/dev/null
}

# `chronicle <args…>` against the built CLI applet graph (argv after `--`,
# exactly as `cli_in` delivers it). Build it first with
# `build_graph packaging/cli/linux.yaml`.
cli() { run_text "" 10 -- "$@"; }

# Route storage.object at a per-run directory, so concurrent runs never share
# blobs. The linux runtime only wires a real backend when this is set; unset,
# every put/get would (correctly) report no provider.
use_store() {
  export FLUXOR_STORE_DIR="$PWD/$E2E_GEN/store"
  mkdir -p "$FLUXOR_STORE_DIR"
}

# Wait for a TCP port to accept, bounded. Readiness must be the port itself:
# a container's control tool (rabbitmqctl, kafka-topics) reports up before the
# listener binds, and that gap reads as a protocol failure.
wait_port() { # <port> <secs>
  timeout "${2:-60}" bash -c "until (exec 3<>/dev/tcp/127.0.0.1/$1) 2>/dev/null; do sleep 0.5; done"
}

want() { case "$1" in *"$2"*) return 0 ;; *) return 1 ;; esac }
