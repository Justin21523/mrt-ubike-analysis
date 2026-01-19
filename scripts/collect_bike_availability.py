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
# `os.getenv` enables per-run tuning (rate limiting) without changing code.
import os

# Config is loaded at runtime so we can change endpoints/cities without changing code (production-minded).
from metrobikeatlas.config.loader import load_config
# Bronze writer persists raw API payload + request metadata so we can rebuild Silver/Gold deterministically.
from metrobikeatlas.ingestion.bronze import write_bronze_json
# `TDXClient` handles OAuth tokens and retries; `TDXCredentials` reads secrets from env (never from git).
from metrobikeatlas.ingestion.tdx_base import TDXClient, TDXCredentials
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


# Keep all side effects (config IO, network calls, filesystem writes) inside `main()` so the module is import-safe.
def main() -> None:
    # Build a CLI parser so users can run this as a repeatable command or schedule it via cron.
    parser = argparse.ArgumentParser(description="Collect a single bike availability snapshot from TDX (Bronze).")
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
        # Iterate configured bike cities; each snapshot is partitioned by city in Bronze.
        for city in config.tdx.bike.cities:
            # Build the realtime availability endpoint path for this city.
            path = config.tdx.bike.availability_path_template.format(city=city)
            # Force JSON output for consistent downstream parsing.
            params = {"$format": "JSON"}
            # Capture retrieval time in UTC so we can align snapshots across cities and runs.
            retrieved_at = datetime.now(timezone.utc)
            # Fetch JSON payload (handles OData paging when needed, but usually returns a single list).
            payload = tdx.get_json_all(path, params=params, max_pages=int(os.getenv("TDX_MAX_PAGES", "100")))

            # Persist the raw payload plus request metadata so we can build a time series later (Silver).
            out = write_bronze_json(
                bronze_dir,
                source="tdx",
                domain="bike",
                dataset="availability",
                city=city,
                retrieved_at=retrieved_at,
                request={"path": path, "params": params},
                payload=payload,
            )
            # Print the output path so job logs show what was produced (useful for debugging pipelines).
            print(f"Wrote {out}")


if __name__ == "__main__":
    # Guard to prevent accidental execution when imported by tests or other modules.
    main()
