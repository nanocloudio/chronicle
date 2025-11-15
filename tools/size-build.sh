#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root_dir"

run() {
  echo ">>> $*" >&2
  "$@"
}

BUILD_FEATURES="${BUILD_FEATURES:---features full}"
BUILD_PROFILE="${BUILD_PROFILE:---release}"
BIN_PATH="${BIN_PATH:-target/release/chronicle}"
SIZE_LIMIT_BYTES="${SIZE_LIMIT_BYTES:-8000000}"

run cargo build $BUILD_PROFILE $BUILD_FEATURES

if [[ ! -f "$BIN_PATH" ]]; then
  echo "error: expected binary '$BIN_PATH' not found. Override BIN_PATH if the target differs." >&2
  exit 1
fi

size_bytes=$(stat -c%s "$BIN_PATH")
size_human=$(du -h "$BIN_PATH" | cut -f1)

echo "Binary size: ${size_bytes} bytes (${size_human})"

if [[ "$SIZE_LIMIT_BYTES" -gt 0 && "$size_bytes" -gt "$SIZE_LIMIT_BYTES" ]]; then
  echo "error: binary exceeds limit of ${SIZE_LIMIT_BYTES} bytes" >&2
  exit 1
fi
