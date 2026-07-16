#!/usr/bin/env bash
# MODULE ACTIVATION without a Linux toolchain — a device deciding whether a
# verified module can actually RUN here:
#
#   chronicle activate <module_hex> <key_hex> <caps> <artefacts> <modules> <bindings>
#
# Verification answers "is this genuine?"; activation answers "can I run it?".
# An authentic module may still reference artefacts this node lacks, depend on an
# absent module, need an unsupported capability, or require an unbound resource.
# Refusing HERE beats failing later, mid-traffic.
#
# The ORDER assertions are the interesting ones. Protobuf serializes these in
# ascending field number — artefacts(2..8), bindings(9), dependencies(10),
# capabilities(11) — which is NOT the sequence order. A node that reported
# failures in wire order would tell the operator a different first cause than the
# host does, so the last two checks below pin the sequence, not just the refusal.
#
# Fixtures are generated:
#   cargo test -p chronicle-module --test device_activation print_activation_e2e_fixtures -- --nocapture
. "$(dirname "$0")/../lib.sh"
modules_ready || { no activation "fluxor modules build failed"; finish; exit; }

KEY=fd1724385aa0c75b64fb78cd602fa1d991fdebf76b13c58ed702eac835e9f618
MODULE=0a490a190a08636f6d6d65726365120d6f72646572735f6d6f64756c6510073a2a0a067368613235361220fea0bc59930db0004974defd7ef7440614f4086cd4ec3cf46b35ea086b493eab3a490a190a08636f6d6d65726365120d70726f636573735f6f7264657210061a2a0a06736861323536122002020202020202020202020202020202020202020202020202020202020202024a1d0a190a097265736f7572636573120c6f72646572735f73746f7265100152470a170a08636f6d6d65726365120b626173655f6d6f64756c6510071a2a0a06736861323536122003030303030303030303030303030303030303030303030303030303030303035a150a13706970656c696e652e656666656374732e76316a170a067265762d3432120b756e69666965642d302e311801726d0a07656432353531391240e1cbc699c948e4681a5d3d03762c7bd061c6fc2ede3ea771d13caa44c84dfd8a03286c7a1a7b4d2212c3a7c4659f73893f646d663f95cf24985870abcd0fba031a20fd1724385aa0c75b64fb78cd602fa1d991fdebf76b13c58ed702eac835e9f618
PIPE=0202020202020202020202020202020202020202020202020202020202020202
DEP=0303030303030303030303030303030303030303030303030303030303030303
CAP=pipeline.effects.v1
BIND=resources.orders_store

build_graph packaging/cli/linux.yaml || { no activation "cli build"; finish; exit; }

# Everything present -> activates, and reports what it resolved.
got=$(cli activate "$MODULE" "$KEY" "$CAP" "$PIPE" "$DEP" "$BIND" 2>/dev/null | head -1)
case "$got" in
  "ok: activate deps=1 artefacts=1 caps=1 bindings=1") ok "activation resolves deps, artefacts, capabilities and bindings" ;;
  *) no activation "full node did not activate: '$got'" ;;
esac

# Each check refuses on its own.
got=$(cli activate "$MODULE" "$KEY" "$CAP" "$PIPE" - "$BIND" 2>/dev/null | head -1)
case "$got" in
  *"missing dependency"*) ok "activation refuses a missing dependency" ;;
  *) no activation "missing dependency not caught: '$got'" ;;
esac

got=$(cli activate "$MODULE" "$KEY" "$CAP" - "$DEP" "$BIND" 2>/dev/null | head -1)
case "$got" in
  *"unresolved artefact"*) ok "activation refuses an unresolved artefact" ;;
  *) no activation "unresolved artefact not caught: '$got'" ;;
esac

got=$(cli activate "$MODULE" "$KEY" - "$PIPE" "$DEP" "$BIND" 2>/dev/null | head -1)
case "$got" in
  *"capability unsupported"*) ok "activation refuses an unsupported capability" ;;
  *) no activation "capability not caught: '$got'" ;;
esac

got=$(cli activate "$MODULE" "$KEY" "$CAP" "$PIPE" "$DEP" - 2>/dev/null | head -1)
case "$got" in
  *"missing binding"*) ok "activation refuses a missing binding" ;;
  *) no activation "binding not caught: '$got'" ;;
esac

# Authenticity is settled first: an untrusted key must report verification,
# never a downstream resolution failure, even with an empty node.
got=$(cli activate "$MODULE" "$PIPE" - - - - 2>/dev/null | head -1)
case "$got" in
  *"not verified"*) ok "activation settles authenticity before reading contents" ;;
  *) no activation "an unverified module was inspected: '$got'" ;;
esac

# ORDER: dependencies (field 10) are checked before artefacts (field 2..8),
# so a node missing BOTH must report the dependency — wire order would not.
got=$(cli activate "$MODULE" "$KEY" "$CAP" - - "$BIND" 2>/dev/null | head -1)
case "$got" in
  *"missing dependency"*) ok "sequence order wins: dependency before artefact" ;;
  *) no activation "wire order leaked into the reported cause: '$got'" ;;
esac

# ORDER: capabilities (field 11) are checked before bindings (field 9).
got=$(cli activate "$MODULE" "$KEY" - "$PIPE" "$DEP" - 2>/dev/null | head -1)
case "$got" in
  *"capability unsupported"*) ok "sequence order wins: capability before binding" ;;
  *) no activation "wire order leaked into the reported cause: '$got'" ;;
esac

finish
