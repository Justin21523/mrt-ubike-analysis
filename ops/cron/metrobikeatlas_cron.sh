#!/usr/bin/env bash
set -euo pipefail

# User-level cron entrypoint for long-running Docker ops (no sudo required).
#
# Modes:
#   - start: ensure compose stack is up
#   - watchdog: restart collector if heartbeat is stale
#   - dq: run Bronze/Silver data quality checks
#   - archive: archive old Bronze snapshots (optional)
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
  dq)
    # Run DQ inside the API container for consistent dependencies.
    exec flock -n "${LOCK_DIR}/cron_dq.lock" /bin/bash -lc "\
      docker compose exec -T api python scripts/validate_bronze.py --bronze-dir data/bronze --out logs/dq/bronze_latest.json && \
      docker compose exec -T api python scripts/validate_silver_extended.py --silver-dir data/silver --out logs/dq/silver_latest.json \
    "
    ;;
  archive)
    # Archive older Bronze snapshots once per month (safe default: do NOT delete after archive).
    OLDER_THAN_DAYS="${ARCHIVE_BRONZE_OLDER_THAN_DAYS:-30}"
    exec flock -n "${LOCK_DIR}/cron_archive.lock" /bin/bash -lc "\
      docker compose exec -T api python scripts/archive_bronze.py \
        --bronze-dir data/bronze \
        --archive-dir data/archive/bronze \
        --older-than-days ${OLDER_THAN_DAYS} \
        --datasets tdx/bike/availability,tdx/bike/stations \
    "
    ;;
  *)
    echo "unknown mode: ${MODE}" >&2
    exit 2
    ;;
esac
