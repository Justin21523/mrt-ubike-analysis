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

## 6) Operational tips

- Tune rate limiting: edit `.env` and `docker compose up -d` again.
- If CSV grows large: run `python scripts/build_silver.py --write-sqlite` once and set:
  - `METROBIKEATLAS_STORAGE=sqlite`
  - `METROBIKEATLAS_LAZY_BIKE_TS=true`

