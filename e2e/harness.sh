#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HARNESS_FILE=${HARNESS_FILE:-"${SCRIPT_DIR}/docker-compose.yaml"}
HARNESS_PROJECT=${HARNESS_PROJECT:-chronicle-harness}
CARGO_BIN=${CARGO:-cargo}
HARNESS_WAIT_TIMEOUT=${HARNESS_WAIT_TIMEOUT:-120}

if [[ -n "${HARNESS_CONTAINERS:-}" ]]; then
  IFS=' ' read -r -a HARNESS_CONTAINERS_ARRAY <<< "${HARNESS_CONTAINERS}"
else
  HARNESS_CONTAINERS_ARRAY=(chronicle-rabbitmq chronicle-mqtt chronicle-postgres chronicle-mongodb chronicle-redis)
fi

if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  COMPOSE_BIN=(docker compose)
  SUPPORTS_WAIT=1
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_BIN=(docker-compose)
  SUPPORTS_WAIT=0
else
  echo "docker compose is required to use the Chronicle harness" >&2
  exit 1
fi

compose() {
  "${COMPOSE_BIN[@]}" -p "${HARNESS_PROJECT}" -f "${HARNESS_FILE}" "$@"
}

wait_for_health() {
  local start elapsed status
  start=$(date +%s)
  while true; do
    local unhealthy=0
    for container in "${HARNESS_CONTAINERS_ARRAY[@]}"; do
      status=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "${container}" 2>/dev/null || echo "starting")
      if [[ "${status}" != "healthy" ]]; then
        unhealthy=1
        break
      fi
    done
    if [[ ${unhealthy} -eq 0 ]]; then
      return 0
    fi
    elapsed=$(( $(date +%s) - start ))
    if (( elapsed > HARNESS_WAIT_TIMEOUT )); then
      echo "Timed out waiting for harness services to become healthy" >&2
      exit 1
    fi
    sleep 2
  done
}

compose_up() {
  if [[ ${SUPPORTS_WAIT} -eq 1 ]]; then
    compose up -d --wait "$@"
  else
    compose up -d "$@"
    wait_for_health
  fi
}

compose_down() {
  compose down -v "$@"
}

run_tests() {
  local -a cargo_args=("$@")
  if [[ ${#cargo_args[@]} -eq 0 ]]; then
    cargo_args=(--all-features --test integration --test message_brokers --test redis_triggers --test postgres_phase --test documented_chronicles)
  fi
  "${CARGO_BIN}" test "${cargo_args[@]}"
}

command=${1:-help}
shift || true

case "${command}" in
  up)
    compose_up "$@"
    ;;
  down)
    compose_down "$@"
    ;;
  ps)
    compose ps "$@"
    ;;
  logs)
    compose logs -f "$@"
    ;;
  test)
    compose_up
    if [[ "${HARNESS_PRESERVE:-0}" != "1" ]]; then
      trap 'compose_down' EXIT
    fi
    run_tests "$@"
    ;;
  help|--help|-h)
    cat <<EOF
Chronicle harness helper

Usage:
  $(basename "$0") up          # start dependencies (waits for health)
  $(basename "$0") down        # stop and remove dependencies
  $(basename "$0") ps          # list harness containers
  $(basename "$0") logs [svc]  # stream service logs
  $(basename "$0") test [args] # start harness, run cargo test (defaults to integration suites)

Environment:
  HARNESS_FILE       path to docker-compose file (default: ${SCRIPT_DIR}/docker-compose.yaml)
  HARNESS_PROJECT    docker compose project name (default: chronicle-harness)
  HARNESS_PRESERVE   set to 1 to keep services running after tests
  HARNESS_CONTAINERS space-separated container names to monitor for health
  HARNESS_WAIT_TIMEOUT seconds to wait for health (default: 120)
  CARGO              cargo binary to invoke (default: cargo)
EOF
    ;;
  *)
    echo "Unknown command: ${command}" >&2
    exit 1
    ;;
esac
