#!/usr/bin/env bash
# Limit-register gate. The register at
# docs/architecture/limit_register.md is only useful if it cannot silently drift
# from the code. This parses the machine-checked block (fenced ```limit-register)
# and asserts each named constant STILL has the recorded right-hand side at the
# recorded source path. A value changed in code without updating the register —
# or vice versa — fails the build, so the documented resource envelope is always
# the real one.
set -u
here="$(cd "$(dirname "$0")/../.." && pwd)"
reg="$here/docs/architecture/limit_register.md"
[ -f "$reg" ] || { echo "  FAIL  limit-register: $reg missing"; exit 1; }

# Extract the fenced block body.
block="$(awk '/^```limit-register/{g=1;next} /^```/{if(g)exit} g{print}' "$reg")"
[ -n "$block" ] || { echo "  FAIL  limit-register: no \`\`\`limit-register block in the doc"; exit 1; }

norm() { # collapse runs of whitespace to a single space, trim ends
  echo "$1" | sed -E 's/[[:space:]]+/ /g; s/^ //; s/ $//'
}

fail=0; n=0
while IFS='|' read -r name src rhs; do
  name="$(echo "$name" | xargs)"; src="$(echo "$src" | xargs)"
  [ -z "$name" ] && continue
  rhs="$(norm "$rhs")"
  n=$((n+1))
  file="$here/$src"
  if [ ! -f "$file" ]; then
    echo "  FAIL  limit-register: $name source '$src' not found"; fail=1; continue
  fi
  # Find `const NAME ... = <rhs> ;` (allow indentation, pub, and a type annotation).
  line="$(grep -nE "const[[:space:]]+$name([[:space:]]*:|[[:space:]])" "$file" | head -1)"
  if [ -z "$line" ]; then
    echo "  FAIL  limit-register: const $name not found in $src"; fail=1; continue
  fi
  # Extract the RHS between '=' and ';'.
  actual="$(echo "$line" | sed -E 's/.*=[[:space:]]*//; s/[[:space:]]*;.*$//')"
  actual="$(norm "$actual")"
  if [ "$actual" != "$rhs" ]; then
    echo "  FAIL  limit-register: $name is '$actual' in $src, register says '$rhs'"; fail=1
  fi
done <<< "$block"

if [ "$fail" -eq 0 ]; then
  echo "  PASS  limit-register: all $n documented constants match their source"
fi
exit $fail
