#!/usr/bin/env bash
# UNSAFE LIVES ONLY ON THE SYSCALL SEAM.
#
# The cores under `modules/common/` are ordinary bounded Rust: no allocator, no
# panics, and no `unsafe`. The one place that cannot hold is where a core calls
# through the Fluxor `SyscallTable` — a raw function-pointer table the kernel
# fills in, so dereferencing it is an obligation the caller carries. Those cores
# declare `unsafe fn` and pass the obligation to their module.
#
# The property cannot be asserted in code: the cores are `include!` fragments, and
# `forbid(unsafe_code)` is an inner attribute, which an included file may not
# carry. A comment saying so enforces nothing. This gate is the enforcement the
# attribute cannot provide.
#
# Two assertions, because either alone rots:
#   * no core outside the seam allowlist contains `unsafe` — the property itself;
#   * every file ON the allowlist really does reach the syscall table — so the
#     list cannot quietly become the place unsafe goes to be forgiven.
#
# Scope is `modules/common/` deliberately. `modules/app/*/mod.rs` are the `.fmod`
# ABI entry points: `extern "C"` handles the kernel calls by raw pointer, and
# unsafe there is the contract, not a lapse.
. "$(dirname "$0")/../lib.sh"

# Files permitted to contain `unsafe`, each with the reason it is on the seam.
declare -A SEAM=(
  [syschan_core.rs]="the channel syscalls themselves — read/write/poll through the table"
  [blobstore_core.rs]="content-addressed blob put/get via the storage syscalls"
  [ckptstore_core.rs]="checkpoint save/load via the storage syscalls"
  [oci_core.rs]="registry push/pull and tag resolution via the store syscalls"
  [telemetry_core.rs]="counter/gauge emission into the kernel telemetry ring"
)

# Real `unsafe` — an unsafe block, fn, impl or trait — ignoring the word where it
# appears in a comment, which is how most of these files discuss the seam.
unsafe_hits() {
  grep -nE '^[^/]*\bunsafe\b[[:space:]]*(\{|fn |impl |trait )' "$1" 2>/dev/null
}

fail_local=0
for f in modules/common/*.rs; do
  base="$(basename "$f")"
  hits="$(unsafe_hits "$f")"
  if [ -n "$hits" ] && [ -z "${SEAM[$base]:-}" ]; then
    no unsafe-seam "$base holds unsafe but is not on the syscall seam:
$(echo "$hits" | head -3 | sed 's/^/        /')
        Move the syscall behind an existing seam core, or add $base to SEAM in
        $(basename "$0") with the reason it must reach the table."
    fail_local=1
  fi
done
[ "$fail_local" -eq 0 ] && ok "no core outside the syscall seam contains unsafe"

# The allowlist earns its keep only while every entry is really on the seam.
stale=0
for base in "${!SEAM[@]}"; do
  f="modules/common/$base"
  if [ ! -f "$f" ]; then
    no unsafe-seam "SEAM names $base, which no longer exists — drop the entry"
    stale=1
  elif ! grep -q "SyscallTable" "$f"; then
    no unsafe-seam "$base is allowlisted but never names SyscallTable — it is not on the seam any more, so its unsafe needs a reason or removal"
    stale=1
  elif [ -z "$(unsafe_hits "$f")" ]; then
    no unsafe-seam "$base is allowlisted but contains no unsafe — drop the entry so the list stays honest"
    stale=1
  fi
done
[ "$stale" -eq 0 ] && ok "every allowlisted core is genuinely on the syscall seam (${#SEAM[@]} files)"

finish
