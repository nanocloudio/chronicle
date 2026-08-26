#!/usr/bin/env bash
# The reference identity provider E2E: `examples/identity_provider/run.sh
# --verify`, run as a CI gate rather than left as something a reader might
# run by hand. An example nobody's CI executes is an example that rots, and
# this one composes four repos' artefacts — chronicle's engines, wave's http
# and ws_stream, kagi's token_verify and remote_channel — so it breaks for
# reasons none of them can see alone.
#
# `run.sh` owns the assertions (every arm of the chain, each status compared
# exactly, plus the 200's body and the 401's absence of one). This driver's
# job is to give it a free port and turn its exit code into the harness's
# PASS/FAIL. Duplicating the assertions here would put them in two places and
# they would drift.
. "$(dirname "$0")/../lib.sh"
modules_ready || { no identity-provider "fluxor modules build failed"; finish; exit; }

PORT=$(free_port)
log=$(mktemp)

# The example needs the `chronicle` applet installed, because it authors the
# document on device before starting. A tree that has not installed it is not
# broken — it just cannot run this gate, and saying so beats a failure that
# reads like the IdP is wrong.
if ! "$FLX" exec chronicle -- help >/dev/null 2>&1; then
  no identity-provider "the chronicle applet is not installed (fluxor install packaging/cli/workload.toml)"
elif ! IDP_PORT="$PORT" timeout 600 examples/identity_provider/run.sh --verify >"$log" 2>&1; then
  no identity-provider "introspect: $(grep -E '^  (ok|FAIL)|^FAIL' "$log" | tail -6 | tr '\n' ';')"
  examples/identity_provider/run.sh --stop >/dev/null 2>&1 || true
elif ! IDP_PORT="$PORT" timeout 600 examples/identity_provider/run.sh --token >"$log" 2>&1; then
  no identity-provider "token: $(grep -E '^  (ok|FAIL)|^FAIL' "$log" | tail -6 | tr '\n' ';')"
  examples/identity_provider/run.sh --stop >/dev/null 2>&1 || true
elif ! IDP_PORT="$PORT" timeout 600 examples/identity_provider/run.sh --authorize >"$log" 2>&1; then
  no identity-provider "authorize: $(grep -E '^  (ok|FAIL)|^FAIL' "$log" | tail -6 | tr '\n' ';')"
  examples/identity_provider/run.sh --stop >/dev/null 2>&1 || true
elif IDP_PORT="$PORT" timeout 600 examples/identity_provider/run.sh --exchange >"$log" 2>&1; then
  # All four operations: introspect (verify), the /oauth/token device grant,
  # and both legs of the OIDC authorization-code flow (/oauth/authorize +
  # code exchange). Every branch in every graph is a kagi verdict.
  ok "identity-provider   (introspect + token + authorize + exchange, every arm a kagi verdict)"
  examples/identity_provider/run.sh --stop >/dev/null 2>&1 || true
else
  no identity-provider "exchange: $(grep -E '^  (ok|FAIL)|^FAIL' "$log" | tail -6 | tr '\n' ';')"
  examples/identity_provider/run.sh --stop >/dev/null 2>&1 || true
fi

rm -f "$log"
finish
