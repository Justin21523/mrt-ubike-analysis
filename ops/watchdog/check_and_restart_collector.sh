#!/usr/bin/env bash
set -euo pipefail

# Runs on the host to keep the collector healthy in long-running Docker mode.
# - If the collector heartbeat is stale (or container is missing), restart it.
#
# Requirements:
# - Run from the repo root (WorkingDirectory in systemd unit).
# - Docker daemon running.

HEARTBEAT_STALE_SECONDS="${HEARTBEAT_STALE_SECONDS:-1800}"  # 30 minutes

if ! command -v docker >/dev/null 2>&1; then
  echo "docker not found" >&2
  exit 2
fi

if ! docker compose ps >/dev/null 2>&1; then
  echo "docker compose not ready" >&2
  exit 2
fi

# If container doesn't exist or isn't running, try to start it.
collector_id="$(docker compose ps -q collector 2>/dev/null || true)"
if [ -z "${collector_id}" ]; then
  echo "collector container not found; starting..."
  docker compose up -d collector
  exit 0
fi

collector_state="$(docker inspect -f '{{.State.Status}}' "${collector_id}" 2>/dev/null || true)"
if [ "${collector_state}" != "running" ]; then
  echo "collector state=${collector_state}; restarting..."
  docker compose restart collector
  exit 0
fi

set +e
docker compose exec -T collector python scripts/collector_status.py \
  --repo-root /app \
  --heartbeat-stale-seconds "${HEARTBEAT_STALE_SECONDS}" \
  >/dev/null
status=$?
set -e

if [ "${status}" -ne 0 ]; then
  echo "collector unhealthy (exit=${status}); restarting..."
  docker compose restart collector
  exit 0
fi

echo "collector ok"

