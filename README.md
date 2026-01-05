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
3. Configure TDX credentials (never commit secrets): copy `.env.example` → `.env`
4. Run API + web: `python scripts/run_api.py`

The web UI is served at `http://127.0.0.1:8000/`.

## Key paths

- App config: `config/default.json`
- Source code: `src/metrobikeatlas/`
- API: `src/metrobikeatlas/api/`
- Web (static): `web/`
- Local data lake: `data/{bronze,silver,gold}/` (gitignored)
