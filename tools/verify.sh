#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root_dir"

run() {
  echo ">>> $*" >&2
  "$@"
}

run cargo fmt --all -- --check
run cargo clippy --all-targets --all-features -- -D warnings
run cargo test --all --all-features
run cargo check --no-default-features
run cargo check --features full

schema_configs=${SCHEMA_CONFIGS:-tests/fixtures/chronicle-integration.yaml}
if [[ -n "${schema_configs}" ]]; then
  run cargo run --quiet --bin schema_validate -- ${schema_configs}
fi

if [[ "${SKIP_AUDIT:-0}" != "1" ]]; then
  run ./tools/audit-deps.sh
else
  echo ">>> Skipping dependency audit because SKIP_AUDIT=1" >&2
fi

if [[ "${SKIP_SIZE_CHECK:-0}" != "1" ]]; then
  run ./tools/size-build.sh
else
  echo ">>> Skipping size check because SKIP_SIZE_CHECK=1" >&2
fi
