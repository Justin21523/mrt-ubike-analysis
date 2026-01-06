# MetroBikeAtlas

MetroBikeAtlas is an MVP web app + data pipeline for urban mobility analysis in Taiwan:
Metro (MRT/rail) stations and flows (or feasible proxies when station-level flows are unavailable),
shared bike stations, and city factors (district/POI density, accessibility, etc.).

## Stage 1 (MVP) scope

- Reproducible project scaffold (config, logging, caching)
- TDX clients for metro + bike station metadata and bike availability snapshots
- Preprocessing utilities: temporal alignment and metro↔bike spatial join
- FastAPI backend + interactive map dashboard (control panels, inspector, keyboard shortcuts)

## Quickstart

1. Create venv: `python -m venv .venv && source .venv/bin/activate`
2. Install deps: `pip install -r requirements-dev.txt`
3. Run API + web (demo mode by default): `python scripts/run_api.py`

The web UI is served at `http://127.0.0.1:8000/`.

Tip: press `?` in the UI to see keyboard shortcuts (W/A/S/D pan, Q/E zoom, etc.).

## Demo mode vs real data mode

- Demo mode (`config/default.json` → `app.demo_mode=true`) works without TDX credentials and always shows a metro + bike chart.
- Real data mode (`app.demo_mode=false`) reads from `data/silver/` and requires you to build Silver tables first.

### Build Silver (real data)

1. Configure TDX credentials (never commit secrets): copy `.env.example` → `.env`
2. Fetch station metadata:
   - `python scripts/extract_metro_stations.py`
   - `python scripts/extract_bike_stations.py`
3. Collect bike availability snapshots over time (to form a time series):
   - One-shot: `python scripts/collect_bike_availability.py` (run repeatedly, e.g. every 5 minutes)
   - Loop helper: `python scripts/collect_bike_availability_loop.py --interval-seconds 300 --duration-seconds 3600`
4. Build Silver tables + metro↔bike links:
   - `python scripts/build_silver.py`

### One-command pipeline (real data)

Run Bronze → Silver → Gold in one go:

- `python scripts/run_pipeline_mvp.py --collect-duration-seconds 3600 --collect-interval-seconds 300`

Validate Silver (schema + basic sanity checks):

- `python scripts/validate_silver.py --strict`

### Build factors + analytics (Stage 3)

1. (Optional) Add external datasets:
   - `data/external/poi.csv`
   - `data/external/metro_station_district.csv`
2. Build station-level factors + targets:
   - `python scripts/build_features.py`
3. Run basic analytics (correlation/regression/clustering):
   - `python scripts/build_analytics.py`

When `data/gold/station_clusters.csv` exists, the map colors metro stations by cluster and the UI shows an
analytics summary (served from `/analytics/overview`).

### Optional: build POIs + districts (Stage 4)

- Generate POIs from OpenStreetMap (Overpass): `python scripts/fetch_poi_overpass.py`
- Map metro stations to districts using admin boundaries:
  - Provide `data/external/admin_boundaries.geojson`
  - Run `python scripts/build_station_district_map.py`

### Metro curve fallback

If `data/silver/metro_timeseries.csv` is not present, the API returns `metro_flow_proxy_from_bike_rent`
computed from bike availability deltas near each metro station. Provide `metro_timeseries.csv` (columns:
`station_id`, `ts`, `value`) to override the proxy with real ridership/flow (see `python scripts/import_metro_timeseries.py -h`).

## Dashboard controls (MVP)

- Spatial join: `buffer` radius (m) or `nearest` K bike stations
- Temporal alignment: `15min` / `hour` / `day`, and a rolling window (days)
- Similar stations: top-k, metric, and standardization
- Map layers: nearby bikes, buffer circle, link lines
- Live refresh: auto re-fetch station data at an interval

All UI settings persist in browser localStorage.

## API runtime parameters (selected)

- `GET /config`: default config values used by the web UI
- `GET /bike_stations`: bike station metadata (for overlays)
- `GET /station/{id}/timeseries`: supports `join_method`, `radius_m`, `nearest_k`, `granularity`, `window_days`
- `GET /station/{id}/nearby_bike`: supports `join_method`, `radius_m`, `nearest_k`, `limit`
- `GET /station/{id}/similar`: supports `top_k`, `metric`, `standardize`

## Environment variables (optional)

- `METROBIKEATLAS_CONFIG_PATH`: choose a config JSON (default: `config/default.json`)
- `METROBIKEATLAS_DEMO_MODE`: override demo mode (`true`/`false`)
- `TDX_BASE_URL`, `TDX_TOKEN_URL`: override TDX endpoints (rarely needed)

## Key paths

- App config: `config/default.json`
- Source code: `src/metrobikeatlas/`
- API: `src/metrobikeatlas/api/`
- Web (static): `web/`
- Local data lake: `data/{bronze,silver,gold}/` (gitignored)
