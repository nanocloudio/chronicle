#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root_dir"

run() {
  echo ">>> $*" >&2
  "$@"
}

require_tool() {
  local bin="$1"
  local install_hint="$2"
  if ! command -v "$bin" >/dev/null 2>&1; then
    echo "error: required tool '$bin' is not installed. ${install_hint}" >&2
    exit 1
  fi
}

AUDIT_FEATURES="${AUDIT_FEATURES:---all-features}"
TREE_FLAGS="${TREE_FLAGS:--d}"
UDEPS_FLAGS="${UDEPS_FLAGS:---all-targets ${AUDIT_FEATURES}}"
BLOAT_FLAGS="${BLOAT_FLAGS:---release --crates -n 20 ${AUDIT_FEATURES}}"

if [[ "${SKIP_TREE:-0}" != "1" ]]; then
  run cargo tree $TREE_FLAGS $AUDIT_FEATURES
else
  echo ">>> Skipping cargo tree because SKIP_TREE=1" >&2
fi

if [[ "${SKIP_UDEPS:-0}" != "1" ]]; then
  require_tool "cargo-udeps" "Install via 'cargo install cargo-udeps'."
  run cargo udeps $UDEPS_FLAGS
else
  echo ">>> Skipping cargo udeps because SKIP_UDEPS=1" >&2
fi

if [[ "${SKIP_BLOAT:-0}" != "1" ]]; then
  require_tool "cargo-bloat" "Install via 'cargo install cargo-bloat'."
  run cargo bloat $BLOAT_FLAGS
else
  echo ">>> Skipping cargo bloat because SKIP_BLOAT=1" >&2
fi
