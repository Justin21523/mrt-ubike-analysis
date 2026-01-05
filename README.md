# MetroBikeAtlas

MetroBikeAtlas is an MVP web app + data pipeline for urban mobility analysis in Taiwan:
Metro (MRT/rail) stations and flows (or feasible proxies when station-level flows are unavailable),
shared bike stations, and city factors (district/POI density, accessibility, etc.).

## Stage 1 (MVP) scope

- Reproducible project scaffold (config, logging, caching)
- TDX clients for metro + bike station metadata and bike availability snapshots
- Preprocessing utilities: temporal alignment and metro↔bike spatial join
- FastAPI backend + minimal map UI (click a metro station → metro & bike charts)

## Quickstart

1. Create venv: `python -m venv .venv && source .venv/bin/activate`
2. Install deps: `pip install -r requirements-dev.txt`
3. Run API + web (demo mode by default): `python scripts/run_api.py`

The web UI is served at `http://127.0.0.1:8000/`.

## Demo mode vs real data mode

- Demo mode (`config/default.json` → `app.demo_mode=true`) works without TDX credentials and always shows a metro + bike chart.
- Real data mode (`app.demo_mode=false`) reads from `data/silver/` and requires you to build Silver tables first.

### Build Silver (real data)

1. Configure TDX credentials (never commit secrets): copy `.env.example` → `.env`
2. Fetch station metadata:
   - `python scripts/extract_metro_stations.py`
   - `python scripts/extract_bike_stations.py`
3. Collect bike availability snapshots over time (to form a time series):
   - `python scripts/collect_bike_availability.py` (run repeatedly, e.g. every 5 minutes)
4. Build Silver tables + metro↔bike links:
   - `python scripts/build_silver.py`

### Metro curve fallback

If `data/silver/metro_timeseries.csv` is not present, the API returns `metro_flow_proxy_from_bike_rent`
computed from bike availability deltas near each metro station. Provide `metro_timeseries.csv` (columns:
`station_id`, `ts`, `value`) to override the proxy with real ridership/flow.

## Key paths

- App config: `config/default.json`
- Source code: `src/metrobikeatlas/`
- API: `src/metrobikeatlas/api/`
- Web (static): `web/`
- Local data lake: `data/{bronze,silver,gold}/` (gitignored)
