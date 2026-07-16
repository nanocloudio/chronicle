#!/usr/bin/env bash
# OCI DISTRIBUTION without a Linux toolchain — a bundle published, tagged,
# resolved and fetched back, every step on device:
#
#   chronicle oci-init                 -> oci-layout + index.json
#   chronicle oci-push <hex> <tag>     -> blobs + manifest + index entry
#   chronicle oci-resolve <tag>        -> the bundle digest
#   chronicle oci-fetch <digest>       -> the layer bytes, content-address verified
#
# The layout written here is the standard OCI image layout — `oci-layout`,
# `index.json`, `blobs/sha256/<hex>` — so what a device publishes is what a
# registry serves, with no translation step.
#
# NOTE those are KEYS, not necessarily paths. `storage.object` abstracts where
# bytes live: on Linux with FLUXOR_STORE_DIR the backend is a versioned KV log,
# so nothing appears as a file on disk, while the host `OciStore` uses a real
# directory. Both agree on the key namespace, which is what makes them
# interoperable — pinned byte-for-byte by
# `chronicle-module/tests/oci_device_interop.rs` (device push -> host fetch and
# back, over a directory-backed store). This script proves the same code path
# runs on the real runtime over a real provider.
. "$(dirname "$0")/../lib.sh"
modules_ready || { no oci "fluxor modules build failed"; finish; exit; }

use_store

build_graph packaging/cli/linux.yaml || { no oci "cli build"; finish; exit; }

BODY=48656c6c6f2062756e646c65   # "Hello bundle"
BODY2=5365636f6e642062756e646c65 # "Second bundle"

cli oci-init >/dev/null 2>&1

digest=$(cli oci-push "$BODY" orders:0.1.0 2>/dev/null | head -1)
case "$digest" in
  [0-9a-f]*) ;;
  *) no oci "push did not return a digest: '$digest'"; finish; exit ;;
esac

# A freshly initialised store has a WELL-FORMED empty index: resolving any tag
# reports not-found rather than malformed, which it could not do if oci-init had
# failed to write a parseable index.json.
if cli oci-resolve absent:0 2>/dev/null | head -1 | grep -q 'not found'; then
  ok "oci-init writes a parseable empty index"
else
  no oci "a fresh store's index does not parse as an empty index"
fi

# A tag resolves to the bundle it was pushed under.
resolved=$(cli oci-resolve orders:0.1.0 2>/dev/null | head -1)
if [ "$resolved" = "$digest" ]; then
  ok "oci tag resolves to the pushed bundle"
else
  no oci "resolve gave '$resolved', expected '$digest'"
fi

# Fetching by digest returns the layer, content-address verified en route.
layer=$(cli oci-fetch "$digest" 2>/dev/null | head -1)
if [ "$layer" = "$BODY" ]; then
  ok "oci fetch by digest returns the verified layer"
else
  no oci "fetch gave '$layer', expected '$BODY'"
fi

# Tag mobility: re-pushing the tag moves the pointer; the old bundle stays
# addressable by digest, because content does not move — only names do.
digest2=$(cli oci-push "$BODY2" orders:0.1.0 2>/dev/null | head -1)
resolved2=$(cli oci-resolve orders:0.1.0 2>/dev/null | head -1)
old=$(cli oci-fetch "$digest" 2>/dev/null | head -1)
if [ "$digest2" = "$digest" ]; then
  no oci "a different bundle produced the same digest"
elif [ "$resolved2" != "$digest2" ]; then
  no oci "the tag did not move to the new bundle"
elif [ "$old" != "$BODY" ]; then
  no oci "the superseded bundle stopped being addressable by digest"
else
  ok "oci tag mobility (the tag moves, the old digest still resolves)"
fi

# An unknown tag is a clean not-found, not a crash or a bogus digest.
if cli oci-resolve nope:1.0 2>/dev/null | head -1 | grep -q '^error'; then
  ok "oci unknown tag reports not found"
else
  no oci "an unknown tag did not report an error"
fi

finish
