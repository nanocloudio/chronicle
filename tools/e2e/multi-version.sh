#!/usr/bin/env bash
# Multi-version E2E: one pipeline instance holds v1 (pass-through, default) and
# v2 (amount*2) at once. Per-record selection via field 255, unknown versions
# fail CLOSED, and a control message flips the default live (blue-green, no
# restart). Params come from a ReleaseManifest.
. "$(dirname "$0")/../lib.sh"
modules_ready || { no multi-version "fluxor modules build failed"; finish; exit; }

build_graph examples/multi_version/linux.yaml || { no multi-version build; finish; exit; }

# {2:10}, no selector -> default v1 pass-through -> "10\n"
got=$(run_text 01020108000a00000000000000 4)
[ "$got" = "10" ] && ok "default version v1 passes amount through" \
  || no "mv default" "got='$got'"

# selector field 255 = "v2" -> pinned v2 doubles -> "20\n"
got=$(run_text 02020108000a00000000000000ff0002007632 4)
[ "$got" = "20" ] && ok "X-Module-Version pin selects v2 (amount*2)" \
  || no "mv pin v2" "got='$got'"

# unknown "v9" fails CLOSED, not through some default
got=$(run_text 02020108000a00000000000000ff0002007639 4)
[ "$got" = "VERSION_UNAVAILABLE" ] && ok "unknown version fails closed" \
  || no "mv unknown" "got='$got'"

# blue-green: SET_DEFAULT(v2) via the control port (cli argv), then default
# traffic runs v2 — a live flip, no restart, no re-deploy.
got=$(run_text 01020108000a00000000000000 4 -- $'\x02\x02v2')
[ "$got" = "20" ] && ok "blue-green control flip: default now v2" \
  || no "mv blue-green" "got='$got'"

finish
