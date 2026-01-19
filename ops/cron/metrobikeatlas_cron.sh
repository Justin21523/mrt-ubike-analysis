#!/usr/bin/env bash
set -euo pipefail

# User-level cron entrypoint for long-running Docker ops (no sudo required).
#
# Modes:
#   - start: ensure compose stack is up
#   - watchdog: restart collector if heartbeat is stale
#
# Usage:
#   ops/cron/metrobikeatlas_cron.sh start /abs/path/to/repo
#   ops/cron/metrobikeatlas_cron.sh watchdog /abs/path/to/repo

MODE="${1:-}"
REPO_ROOT="${2:-}"

if [ -z "${MODE}" ] || [ -z "${REPO_ROOT}" ]; then
  echo "usage: $0 <start|watchdog> <repo_root>" >&2
  exit 2
fi

if [ ! -d "${REPO_ROOT}" ]; then
  echo "repo_root not found: ${REPO_ROOT}" >&2
  exit 2
fi

cd "${REPO_ROOT}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker not found" >&2
  exit 2
fi

LOCK_DIR="${REPO_ROOT}/logs/locks"
mkdir -p "${LOCK_DIR}"

case "${MODE}" in
  start)
    exec flock -n "${LOCK_DIR}/cron_start.lock" docker compose up -d --remove-orphans
    ;;
  watchdog)
    HEARTBEAT_STALE_SECONDS="${HEARTBEAT_STALE_SECONDS:-1800}"
    exec flock -n "${LOCK_DIR}/cron_watchdog.lock" /bin/bash ops/watchdog/check_and_restart_collector.sh
    ;;
  *)
    echo "unknown mode: ${MODE}" >&2
    exit 2
    ;;
esac

