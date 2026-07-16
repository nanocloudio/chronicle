#!/usr/bin/env bash
# MODULE VERIFICATION without a Linux toolchain — a device deciding whether to
# ACCEPT a module, not merely author one:
#
#   chronicle verify <module_hex> <trusted_pubkey_hex>...
#
# Two checks, both on device: the digest is RECOMPUTED from the module's own
# bytes and compared to the one it carries, then an ed25519 signature over that
# digest must verify under a trusted key. The crypto is the fluxor SDK's — the
# repo set's single crypto owner — composed here, not reimplemented.
#
# The assertions that matter are the REJECTIONS. Accepting a good module proves
# little; refusing a tampered body and an untrusted signer is the whole point,
# so each is checked to fail for its own distinct reason.
#
# Fixtures are generated (they depend on the encoder and the signing key):
#   cargo test -p chronicle-module --test device_verify print_verify_e2e_fixtures -- --nocapture
. "$(dirname "$0")/../lib.sh"
modules_ready || { no verify "fluxor modules build failed"; finish; exit; }

KEY=fd1724385aa0c75b64fb78cd602fa1d991fdebf76b13c58ed702eac835e9f618
GOOD=0a490a190a08636f6d6d65726365120d6f72646572735f6d6f64756c6510073a2a0a06736861323536122031ed419bf596d395f84d4676cbb7651ee5859f2140f5f13d0fc7b311b92d75b06a170a067265762d3432120b756e69666965642d302e311801726d0a07656432353531391240fd8eee66fc10cadcb9388782317f56f81589a16a755151f4896eaa2767a8fe464dc09deb27331f80b4398e46da08a9f743f4dd46d883cdd6fad4f1b4d7983f011a20fd1724385aa0c75b64fb78cd602fa1d991fdebf76b13c58ed702eac835e9f618
UNTRUSTED=0a490a190a08636f6d6d65726365120d6f72646572735f6d6f64756c6510073a2a0a06736861323536122031ed419bf596d395f84d4676cbb7651ee5859f2140f5f13d0fc7b311b92d75b06a170a067265762d3432120b756e69666965642d302e311801726d0a07656432353531391240a8806dffc536bc419782640a695e6e82a2f3af9b3889de672381977368a67751c68a95493d08b7b7752cfe319d8775439b1133ec7f2062724c3ef7dc8bfebc001a20ea4a6c63e29c520abef5507b132ec5f9954776aebebe7b92421eea691446d22c
TAMPERED=0a490a190a08636f6d6d65726365120d6f72646572735f6d6f64756c6510073a2a0a06736861323536122031ed419bf596d395f84d4676cbb7651ee5859f2140f5f13d0fc7b311b92d75b05a0a0a06736e65616b7910016a170a067265762d3432120b756e69666965642d302e311801726d0a07656432353531391240fd8eee66fc10cadcb9388782317f56f81589a16a755151f4896eaa2767a8fe464dc09deb27331f80b4398e46da08a9f743f4dd46d883cdd6fad4f1b4d7983f011a20fd1724385aa0c75b64fb78cd602fa1d991fdebf76b13c58ed702eac835e9f618

build_graph packaging/cli/linux.yaml || { no verify "cli build"; finish; exit; }

got=$(cli verify "$GOOD" "$KEY" 2>/dev/null | head -1)
case "$got" in
  ok:*) ok "verify accepts a module signed by a trusted key" ;;
  *) no verify "good module rejected: '$got'" ;;
esac

# Tampered body: caught by RECOMPUTATION, not by the signature — the signature
# here is still the genuine one over the original digest.
got=$(cli verify "$TAMPERED" "$KEY" 2>/dev/null | head -1)
case "$got" in
  *"digest mismatch"*) ok "verify rejects a tampered body as a digest mismatch" ;;
  *) no verify "tampered module not caught by digest: '$got'" ;;
esac

# Valid signature, unknown signer.
got=$(cli verify "$UNTRUSTED" "$KEY" 2>/dev/null | head -1)
case "$got" in
  *"trusted signer"*) ok "verify rejects a valid signature from an untrusted signer" ;;
  *) no verify "untrusted signer not caught: '$got'" ;;
esac

# Trusting nobody accepts nothing.
if cli verify "$GOOD" 2>/dev/null | head -1 | grep -q '^error'; then
  ok "verify with no trusted keys accepts nothing"
else
  no verify "an empty trust set accepted a module"
fi

# Junk is a clean structured error, not a crash.
got=$(cli verify ffffffffff "$KEY" 2>/dev/null | head -1)
case "$got" in
  error:*) ok "verify fails closed on malformed input" ;;
  *) no verify "junk did not produce a structured error: '$got'" ;;
esac

finish
