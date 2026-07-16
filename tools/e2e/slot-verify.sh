#!/usr/bin/env bash
# OTA SLOT VERIFICATION ON DEVICE — checking an image before writing it:
#
#   chronicle slot-verify <image_digest_hex> [abi_surface_hex]
#
# BUILDING a slot image stays fluxor's job: it compiles the graph's modules and
# self-supplies the ABI-surface pin, which only the target fluxor build can
# compute. Nothing in chronicle tries to construct one.
#
# CHECKING one is a device concern, and the reason is asymmetric risk. A node
# that writes a corrupt or wrong-ABI image over its working slot finds out at
# boot, with nothing left to refuse with. The check has to happen first.
#
# The image is addressed by its store DIGEST rather than passed as bytes: a slot
# is 512 KB, which fits neither argv nor a module buffer. The module streams it
# from `storage.object` in windows and hashes incrementally.
#
# SPLIT OF PROOF, because this script cannot carry all of it:
#   * here — the whole CLI path end to end (store read, streaming loop, verdict)
#     over a small image a shell can actually load into the store;
#   * `device_slot.rs` — the same verifier over a REAL 512 KB `fluxor build --emit=slot`
#     output, streamed in 2 KiB windows, plus every malformed-header case.
# There is no shell path to ingest half a megabyte into the object store, and
# inventing one purely to lengthen this script would prove nothing the
# differential does not already.
. "$(dirname "$0")/../lib.sh"
modules_ready || { no slot "fluxor modules build failed"; finish; exit; }

use_store
build_graph packaging/cli/linux.yaml || { no slot "cli build"; finish; exit; }
cli() { run_text "" 10 -- "$@"; }

# A small image with the layout fluxor emits: header, then modules, then config
# directly after it (the recorded hash is over their concatenation).
mkslot() { # <epoch> <modules> <config> <abi_byte_hex>
  python3 "$(dirname "$0")/mkslot.py" "$@"
}

IMG=$(mkslot 7 "MODULES-BLOB-CONTENTS" "config: yes" ab)

# `put` stores a blob under its own sha256 and prints that digest.
dg=$(cli put "$IMG" 2>/dev/null | head -1)
case "$dg" in
  [0-9a-f]*) ok "a slot image is stored under its content address" ;;
  *) no slot "put failed: '$dg'" ;;
esac

out=$(cli slot-verify "$dg" 2>/dev/null)
case "$out" in
  "ok epoch 7 payload 32 "*) ok "device streams a stored slot image and verifies it" ;;
  *) no slot "unexpected slot-verify output: '$out'" ;;
esac

# The pin must be ENFORCED when supplied: modules built against another surface
# would disagree with the runtime about the syscall ABI, unrecoverably at boot.
out=$(cli slot-verify "$dg" "$(printf '11%.0s' $(seq 32))" 2>/dev/null)
case "$out" in
  *"different fluxor ABI surface"*) ok "an ABI-surface mismatch is refused" ;;
  *) no slot "expected an ABI mismatch, got: '$out'" ;;
esac

# ...and the genuine pin passes, so the check is not simply always-fail.
out=$(cli slot-verify "$dg" "$(printf 'ab%.0s' $(seq 32))" 2>/dev/null)
case "$out" in
  "ok epoch 7 "*) ok "the matching ABI surface is accepted" ;;
  *) no slot "the genuine pin was rejected: '$out'" ;;
esac

# A corrupt payload under an intact header: the recorded sha256 must catch it.
# This is the device's own activate gate, applied before the write instead of
# after — which is the whole point of checking here.
OTHER=$(mkslot 7 "MODULES-BLOB-CONTENTS" "config: NO!" ab)
BAD="${IMG:0:512}${OTHER:512}"
bdg=$(cli put "$BAD" 2>/dev/null | head -1)
out=$(cli slot-verify "$bdg" 2>/dev/null)
case "$out" in
  *"does not match its recorded sha256"*) ok "a corrupted payload is refused" ;;
  *) no slot "expected a sha mismatch, got: '$out'" ;;
esac

# An image the store does not hold is reported, not treated as empty.
out=$(cli slot-verify "$(printf 'cd%.0s' $(seq 32))" 2>/dev/null)
case "$out" in
  *"not in the store"*) ok "an absent image is reported" ;;
  *) no slot "expected a not-found error, got: '$out'" ;;
esac

# A stored blob long enough for a header but not a slot image.
zdg=$(cli put "$(python3 -c 'print("00"*256)')" 2>/dev/null | head -1)
out=$(cli slot-verify "$zdg" 2>/dev/null)
case "$out" in
  *"not a slot image"*) ok "a stored blob with the wrong magic is refused" ;;
  *) no slot "expected a bad-magic refusal, got: '$out'" ;;
esac

# Argument shape, independent of the store.
out=$(cli slot-verify 00 2>/dev/null)
case "$out" in
  *"32-byte store digest"*) ok "a malformed digest argument is refused" ;;
  *) no slot "expected a digest-format error, got: '$out'" ;;
esac

out=$(cli slot-verify zzzz 2>/dev/null)
case "$out" in
  *"32-byte store digest"*) ok "a non-hex digest is a structured error" ;;
  *) no slot "expected a digest error, got: '$out'" ;;
esac

finish
