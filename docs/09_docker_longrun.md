# Docker long-run (real data)

This runbook sets up a long-running collector + API that:

- stays under conservative TDX rate limits
- keeps disk usage bounded (retention + emergency mode)
- restarts automatically (`restart: unless-stopped`)

## 1) Prepare `.env`

Copy and edit:

```bash
cp .env.example .env
```

Required:

- `TDX_CLIENT_ID`
- `TDX_CLIENT_SECRET`

Recommended (conservative throttling):

- `TDX_MIN_REQUEST_INTERVAL_S=0.6`
- `TDX_REQUEST_JITTER_S=0.2`

Recommended (retention):

- `BRONZE_RETAIN_AVAIL_FILES_PER_CITY=288`
- `BRONZE_RETAIN_AVAIL_DAYS=2`
- `BRONZE_RETAIN_STATIONS_FILES_PER_CITY=4`
- `BRONZE_CLEANUP_INTERVAL_SECONDS=600`

Optional (disk safety):

- `MIN_FREE_DISK_BYTES=10737418240` (10GB)

## 2) Start

```bash
docker compose up -d --build
```

This starts 3 long-running services:

- `api` (web + API)
- `collector` (TDX Bronze collection + optional Silver rebuild)
- `scheduler` (DQ gate + Gold rebuild + Bronze archiving)

## 3) Verify

```bash
docker compose ps
curl -fsS http://127.0.0.1:8000/status | jq .
curl -fsS http://127.0.0.1:8000/meta | jq .
```

Collector health summary (exit code suitable for healthcheck):

```bash
docker compose exec collector python scripts/collector_status.py --repo-root /app
```

## 4) Where data goes

- Bronze snapshots: `data/bronze/`
- Silver artifacts: `data/silver/`
- Heartbeat/metrics: `logs/collector_heartbeat.json`, `logs/collector_metrics.json`
- Jobs/logs: `logs/jobs/`

## 5) Autostart at boot (host systemd)

Docker containers already restart, but you still need to start the stack after reboot.

1) Enable Docker at boot:

```bash
sudo systemctl enable --now docker
```

2) Install the unit template:

```bash
sudo cp ops/systemd/metrobikeatlas-compose.service /etc/systemd/system/metrobikeatlas-compose.service
sudo sed -i "s|/ABSOLUTE/PATH/TO/mrt-ubike-analysis|$PWD|g" /etc/systemd/system/metrobikeatlas-compose.service
sudo systemctl daemon-reload
sudo systemctl enable --now metrobikeatlas-compose
```

3) Check:

```bash
systemctl status metrobikeatlas-compose --no-pager
docker compose ps
```

## 6) Watchdog (auto-restart if collector is stale)

This is optional but recommended for long-running ops. It restarts the collector container when its
`collector_heartbeat.json` stops updating (stuck process / network issues / rare edge cases).

1) Install + enable:

```bash
sudo cp ops/systemd/metrobikeatlas-watchdog.service /etc/systemd/system/metrobikeatlas-watchdog.service
sudo cp ops/systemd/metrobikeatlas-watchdog.timer /etc/systemd/system/metrobikeatlas-watchdog.timer
sudo sed -i "s|/ABSOLUTE/PATH/TO/mrt-ubike-analysis|$PWD|g" /etc/systemd/system/metrobikeatlas-watchdog.service
sudo systemctl daemon-reload
sudo systemctl enable --now metrobikeatlas-watchdog.timer
```

2) Inspect runs:

```bash
systemctl list-timers --all | rg metrobikeatlas-watchdog
journalctl -u metrobikeatlas-watchdog --no-pager -n 100
```

## 7) Alternative: user cron (no sudo)

If you don't want to use systemd (or you don't have sudo), you can use user crontab to:

- start the compose stack at boot (`@reboot`)
- run a watchdog every 2 minutes

Note: If you are already running the `scheduler` container, you do not need cron-based DQ/archive.

Install (includes autostart + watchdog; optional: add DQ + archive lines too):

```bash
chmod +x ops/cron/metrobikeatlas_cron.sh ops/watchdog/check_and_restart_collector.sh
crontab -l > /tmp/mba.cron || true
rg -n \"metrobikeatlas_cron\" /tmp/mba.cron >/dev/null || cat >> /tmp/mba.cron <<'CRON'
# MetroBikeAtlas (docker compose autostart + watchdog)
@reboot /bin/bash /ABSOLUTE/PATH/TO/mrt-ubike-analysis/ops/cron/metrobikeatlas_cron.sh start /ABSOLUTE/PATH/TO/mrt-ubike-analysis >> /ABSOLUTE/PATH/TO/mrt-ubike-analysis/logs/cron_start.log 2>&1
*/2 * * * * /bin/bash /ABSOLUTE/PATH/TO/mrt-ubike-analysis/ops/cron/metrobikeatlas_cron.sh watchdog /ABSOLUTE/PATH/TO/mrt-ubike-analysis >> /ABSOLUTE/PATH/TO/mrt-ubike-analysis/logs/cron_watchdog.log 2>&1
# MetroBikeAtlas: DQ gate (every 30 minutes)
*/30 * * * * /bin/bash /ABSOLUTE/PATH/TO/mrt-ubike-analysis/ops/cron/metrobikeatlas_cron.sh dq /ABSOLUTE/PATH/TO/mrt-ubike-analysis >> /ABSOLUTE/PATH/TO/mrt-ubike-analysis/logs/cron_dq.log 2>&1
# MetroBikeAtlas: archive old Bronze (monthly)
15 3 1 * * /bin/bash /ABSOLUTE/PATH/TO/mrt-ubike-analysis/ops/cron/metrobikeatlas_cron.sh archive /ABSOLUTE/PATH/TO/mrt-ubike-analysis >> /ABSOLUTE/PATH/TO/mrt-ubike-analysis/logs/cron_archive.log 2>&1
CRON
sed -i \"s|/ABSOLUTE/PATH/TO/mrt-ubike-analysis|$PWD|g\" /tmp/mba.cron
crontab /tmp/mba.cron
```

Verify:

```bash
crontab -l | rg metrobikeatlas
tail -n 50 logs/cron_watchdog.log
```

## 8) Operational tips

- Tune rate limiting: edit `.env` and `docker compose up -d` again.
- If CSV grows large: run `python scripts/build_silver.py --write-sqlite` once and set:
  - `METROBIKEATLAS_STORAGE=sqlite`
  - `METROBIKEATLAS_LAZY_BIKE_TS=true`
