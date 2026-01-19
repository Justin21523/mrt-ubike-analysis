# Ops

This folder contains operational templates and runbooks for running MetroBikeAtlas in long-running (non-demo) mode.

- `systemd/metrobikeatlas-compose.service`: systemd unit template to auto-start `docker compose` at boot.
- `systemd/metrobikeatlas-watchdog.service` + `.timer`: host watchdog to restart the collector when heartbeat is stale.
- `watchdog/check_and_restart_collector.sh`: watchdog script used by the systemd service.
