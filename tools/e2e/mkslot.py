"""Emit a small `graph_slot` slot image as hex, for `slot-verify-e2e.sh`.

Mirrors the layout `fluxor slot-image` produces — header, then the modules blob,
then the config blob directly after it — but at a size a shell can put into the
object store. `device_slot.rs` covers the real 512 KB article; this exists so the
CLI's store-read + streaming path can be driven end to end.

Usage: mkslot.py <epoch> <modules> <config> <abi_byte_hex>
"""

import hashlib
import sys

HEADER = 256
MAGIC = 0x4C53_5846  # "FXSL"
ABI_OFFSET = 64


def main() -> None:
    epoch = int(sys.argv[1])
    modules = sys.argv[2].encode()
    config = sys.argv[3].encode()
    abi = int(sys.argv[4], 16)

    h = bytearray(HEADER)
    h[0:4] = MAGIC.to_bytes(4, "little")
    h[4] = 1  # version
    h[8:16] = epoch.to_bytes(8, "little")
    h[16:20] = HEADER.to_bytes(4, "little")
    h[20:24] = len(modules).to_bytes(4, "little")
    h[24:28] = (HEADER + len(modules)).to_bytes(4, "little")
    h[28:32] = len(config).to_bytes(4, "little")
    # The activate gate: sha256 over `modules ++ config`.
    h[32:64] = hashlib.sha256(modules + config).digest()
    h[ABI_OFFSET : ABI_OFFSET + 32] = bytes([abi]) * 32

    sys.stdout.write((bytes(h) + modules + config).hex())


if __name__ == "__main__":
    main()
