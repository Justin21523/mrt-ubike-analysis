from __future__ import annotations

# Allow running scripts without requiring an editable install (`pip install -e .`).
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_PATH))

# `argparse` provides a stable CLI interface for production-style scripts (no interactive prompts).
import argparse
# We stamp each Bronze file with a timezone-aware UTC timestamp for traceability and reproducibility.
from datetime import datetime, timezone
# `os.getenv` enables per-run tuning (rate limiting / caching) without changing code.
import os

# Config is loaded at runtime so we can change endpoints/cities without changing code (production-minded).
from metrobikeatlas.config.loader import load_config
# Bronze writer persists raw API payload + request metadata so we can rebuild Silver/Gold deterministically.
from metrobikeatlas.ingestion.bronze import write_bronze_json
# `TDXClient` handles OAuth tokens and retries; `TDXCredentials` reads secrets from env (never from git).
from metrobikeatlas.ingestion.tdx_base import TDXClient, TDXCredentials
# Optional on-disk cache reduces API calls for mostly-static station metadata.
from metrobikeatlas.utils.cache import JsonFileCache
# Centralized logging configuration keeps script output consistent across local runs and production jobs.
from metrobikeatlas.utils.logging import configure_logging


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return float(default)
    try:
        return float(raw)
    except ValueError:
        return float(default)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return bool(default)
    v = raw.strip().lower()
    if v in {"1", "true", "yes", "y", "on"}:
        return True
    if v in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


# Keep all side effects (config IO, network calls, filesystem writes) inside `main()` so the module is import-safe.
def main() -> None:
    # Build a CLI parser so users can run this as a repeatable command (cron/job runner friendly).
    parser = argparse.ArgumentParser(description="Fetch metro station metadata from TDX and write to Bronze.")
    # Optional config path makes it easy to switch environments without editing code.
    parser.add_argument("--config", default=None, help="Config JSON path.")
    # Bronze directory is a local data lake root; it is gitignored and treated as a runtime artifact.
    parser.add_argument("--bronze-dir", default="data/bronze")
    # Parse CLI arguments once at startup to keep control flow deterministic.
    args = parser.parse_args()

    # Load typed config (also loads `.env` when python-dotenv is installed).
    config = load_config(args.config)
    # Configure logging early so any later logs follow the same format/level.
    configure_logging(config.logging)
    # Read TDX credentials from environment variables (production practice: do not hardcode secrets).
    creds = TDXCredentials.from_env()
    cache = JsonFileCache(config.cache)
    use_cache = _env_bool("TDX_CACHE_STATIONS", True)

    # Ensure the Bronze root exists before writing any files.
    bronze_dir = Path(args.bronze_dir)
    bronze_dir.mkdir(parents=True, exist_ok=True)

    # Use a context manager so HTTP connections are closed cleanly even if the script fails.
    with TDXClient(
        # Base URL and token URL come from config/env overrides so deployment can swap endpoints safely.
        base_url=config.tdx.base_url,
        token_url=config.tdx.token_url,
        credentials=creds,
        min_request_interval_s=_env_float("TDX_MIN_REQUEST_INTERVAL_S", 0.2),
        request_jitter_s=_env_float("TDX_REQUEST_JITTER_S", 0.05),
    ) as tdx:
        # Iterate configured metro cities; each city is written into its own partitioned Bronze path.
        for city in config.tdx.metro.cities:
            # Build the API path from the configured template for this city.
            path = config.tdx.metro.stations_path_template.format(city=city)
            # Force JSON output for consistent downstream parsing.
            params = {"$format": "JSON"}
            # Capture retrieval time in UTC so we can compare files across machines/timezones.
            retrieved_at = datetime.now(timezone.utc)
            request_meta = {"path": path, "params": params, "cache": None}

            # Station metadata changes slowly; reuse cached payloads to reduce API calls.
            cache_key = cache.make_key("tdx:metro:stations", {"path": path, "params": params})
            cached = cache.get(cache_key) if use_cache else None
            if cached is not None:
                payload = cached
                request_meta["cache"] = "hit"
            else:
                # Fetch JSON payload (handles OData paging when needed).
                payload = tdx.get_json_all(path, params=params, max_pages=int(os.getenv("TDX_MAX_PAGES", "100")))
                if use_cache:
                    cache.set(cache_key, payload)
                request_meta["cache"] = "miss"

            # Persist the raw payload plus request metadata for full traceability (Bronze design principle).
            out = write_bronze_json(
                bronze_dir,
                source="tdx",
                domain="metro",
                dataset="stations",
                city=city,
                retrieved_at=retrieved_at,
                request=request_meta,
                payload=payload,
            )
            # Print the output path so job logs show what was produced (useful for debugging pipelines).
            print(f"Wrote {out}")


if __name__ == "__main__":
    # Guard to prevent accidental execution when imported by tests or other modules.
    main()
