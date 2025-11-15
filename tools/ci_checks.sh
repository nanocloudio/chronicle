#!/usr/bin/env bash
set -eo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
cd "$root_dir"

run() {
  echo "\n>>> $*" >&2
  "$@"
}

run make verify

if [[ "${SKIP_HARNESS_TESTS:-0}" == "1" ]]; then
  echo "\n>>> Skipping integration harness tests because SKIP_HARNESS_TESTS=1" >&2
else
  if command -v docker >/dev/null 2>&1; then
    if docker compose version >/dev/null 2>&1 || command -v docker-compose >/dev/null 2>&1; then
      run ./e2e/harness.sh test
    else
      echo "\n>>> Skipping integration harness tests (docker compose not available)" >&2
    fi
  else
    echo "\n>>> Skipping integration harness tests (docker not available)" >&2
  fi
fi

if [[ "${SKIP_BENCH:-0}" != "1" ]]; then
  run cargo bench --bench record_flow -- --warm-up-time 1 --measurement-time 3
else
  echo "\n>>> Skipping benchmarks because SKIP_BENCH=1" >&2
fi
