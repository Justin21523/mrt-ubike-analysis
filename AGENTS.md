# Repository Guidelines

## Project Structure & Module Organization

- `src/metrobikeatlas/`: main Python package (production code)
  - `ingestion/`: data source clients (TDX, external fallbacks)
  - `preprocessing/`: temporal alignment + spatial joins
  - `features/`, `analytics/`: future stages (keep MVP simple first)
  - `api/`: FastAPI backend (serves endpoints + static web)
  - `schemas/`: core dataclasses and API schemas
  - `config/`: config loading and typed models
- `tests/`: pytest tests (fast, deterministic; no network calls)
- `docs/`: design notes and data strategy (Bronze/Silver/Gold, schemas, etc.)
- `data/`: local data lake layout (`bronze/`, `silver/`, `gold/`) — ignored by git (except `.gitkeep`)
- `scripts/`: runnable entrypoints that call into `src/`
- `notebooks/`: optional exploration only

## Build, Test, and Development Commands

- Create venv: `python -m venv .venv && source .venv/bin/activate`
- Install deps (dev): `pip install -r requirements-dev.txt`
- Run tests: `pytest -q`
- Lint: `ruff check .` (fix with `ruff check . --fix`)
- (Optional) Format: `ruff format .`
- Run API + web (demo mode by default): `python scripts/run_api.py`
- Build Silver (requires TDX creds): `python scripts/extract_metro_stations.py && python scripts/extract_bike_stations.py && python scripts/collect_bike_availability.py && python scripts/build_silver.py`
- Build factors + analytics: `python scripts/build_features.py && python scripts/build_analytics.py`

## Coding Style & Naming Conventions

- Python 3.10+, 4-space indentation, type hints where useful.
- No global variables/state; pass dependencies via function args or class constructors.
- Names: modules/functions `snake_case`, classes `PascalCase`, constants `UPPER_SNAKE_CASE`.
- Data modeling: prefer explicit “dim/fact” thinking (e.g., `dim_metro_station`, `fact_station_metric` in docs/schema).

## Testing Guidelines

- Framework: `pytest`. Files `tests/test_*.py`, functions `test_*`.
- Tests should not hit TDX; mock ingestion clients and focus on parsing/transform logic.

## Security & Configuration Tips

- Put credentials in `.env` (see `.env.example`) or environment variables; never commit real secrets.
- Treat `data/` as local artifacts; commit only structure + docs.

## Commit & Pull Request Guidelines

- No git history yet; use Conventional Commits (e.g., `feat(tdx): add station endpoint`, `fix(schema): tighten types`).
- PRs: explain the data source/endpoints, include a brief schema impact note, and add/adjust tests + docs when behavior changes.
