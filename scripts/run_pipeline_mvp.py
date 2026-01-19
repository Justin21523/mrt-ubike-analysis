from __future__ import annotations

# Allow running scripts without requiring an editable install (`pip install -e .`).
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_PATH))

# `argparse` provides a stable CLI interface for orchestrating an end-to-end pipeline (repeatable runs).
import argparse
# `logging` is used to report which sub-commands run and to surface progress/errors in job logs.
import logging
# `os.environ` is used to pass config and secrets to subprocesses (standard pattern for pipeline runners).
import os
# `subprocess` runs the underlying scripts as separate processes to keep concerns separated.
import subprocess
# `sys.executable` ensures we invoke sub-scripts with the same Python interpreter/venv.
# `Optional` makes small helper functions explicit about "may be None" values.
from typing import Optional

# Config is loaded at runtime so endpoints/paths can be changed without modifying code.
from metrobikeatlas.config.loader import load_config
# Silver validation checks schema + basic invariants after we build Silver tables.
from metrobikeatlas.quality.silver import validate_silver_dir
# Centralized logging configuration keeps script output consistent across local runs and production jobs.
from metrobikeatlas.utils.logging import configure_logging


# Module-level logger is standard for consistent log formatting and handler configuration.
logger = logging.getLogger(__name__)


# Helper to parse boolean-ish env var values in a user-friendly way ("true/false/1/0/yes/no").
def _parse_bool_env(value: str | None) -> Optional[bool]:
    # Treat missing env vars as "no override".
    if value is None:
        return None
    # Normalize whitespace/casing so we can accept common truthy/falsey strings.
    v = value.strip().lower()
    # Return True for common truthy strings.
    if v in {"1", "true", "yes", "y", "on"}:
        return True
    # Return False for common falsey strings.
    if v in {"0", "false", "no", "n", "off"}:
        return False
    # Unknown strings return None so callers can ignore the override.
    return None


# Guard that ensures required credentials exist before we attempt any network calls to TDX.
def _require_tdx_credentials() -> None:
    # We only check presence (not validity) because secrets must not be printed or logged.
    if not os.getenv("TDX_CLIENT_ID") or not os.getenv("TDX_CLIENT_SECRET"):
        # Fail fast with a clear message so users know exactly what to configure.
        raise ValueError(
            "Missing TDX credentials. Set env vars TDX_CLIENT_ID and TDX_CLIENT_SECRET "
            "(see .env.example)."
        )


# Run a subprocess command with a controlled environment and fail if it exits non-zero.
def _run(cmd: list[str], *, env: dict[str, str]) -> None:
    # Log the command for observability; this is essential when debugging CI/cron runs.
    logger.info("Running: %s", " ".join(cmd))
    # `check=True` raises CalledProcessError on failure, which stops the pipeline (fail-fast behavior).
    subprocess.run(cmd, check=True, env=env)


# Keep all side effects (config IO, subprocess execution, filesystem writes) inside `main()` so import is safe.
def main() -> None:
    # Build a CLI parser so the pipeline runner is reproducible and can be used in cron/CI.
    parser = argparse.ArgumentParser(description="Run the end-to-end MVP pipeline (TDX → Bronze → Silver → Gold).")
    # Optional config path allows switching environments without editing code or exporting env vars.
    parser.add_argument("--config", default=None, help="Config JSON path (overrides METROBIKEATLAS_CONFIG_PATH).")
    # Pipeline directories are treated as local artifacts (gitignored) and can be overridden per run.
    parser.add_argument("--bronze-dir", default="data/bronze")
    parser.add_argument("--silver-dir", default="data/silver")
    parser.add_argument("--gold-dir", default="data/gold")
    # Cap availability files to avoid unbounded memory/time during Silver building.
    parser.add_argument("--max-availability-files", type=int, default=500)

    # Allow skipping station extraction when you already have recent Bronze station snapshots.
    parser.add_argument("--skip-extract-stations", action="store_true")
    # Optional collection window: if provided, we run the availability loop collector for N seconds.
    parser.add_argument("--collect-duration-seconds", type=int, default=None, help="If set, collect bike snapshots.")
    # Interval controls how often availability is polled during the collection window.
    parser.add_argument("--collect-interval-seconds", type=int, default=300)

    # Optional import lets you provide a real metro ridership CSV to override the bike-derived proxy.
    parser.add_argument("--import-metro-csv", default=None, help="Optional external metro ridership CSV to import.")
    # Column mapping args make the importer robust to different public data formats.
    parser.add_argument("--import-station-id-col", default="station_id")
    parser.add_argument("--import-ts-col", default="ts")
    parser.add_argument("--import-value-col", default="value")
    # Timestamp parsing options allow flexible inputs (format strings or integer time units).
    parser.add_argument("--import-ts-format", default=None)
    parser.add_argument("--import-ts-unit", default=None)
    # Timezone options support input data that is not UTC and ensure output aligns with app settings.
    parser.add_argument("--import-input-timezone", default=None)
    parser.add_argument("--import-output-timezone", default=None)
    # Optional alignment step can resample external ridership data to the app granularity.
    parser.add_argument("--import-align", action="store_true")
    # Granularity is constrained to values supported by our temporal alignment utilities.
    parser.add_argument("--import-granularity", default=None, choices=["15min", "hour", "day"])
    # Aggregation controls how multiple records in a bucket are combined.
    parser.add_argument("--import-agg", default="sum", choices=["sum", "mean"])

    # Gold steps can be skipped to focus on data collection and Silver validation during MVP.
    parser.add_argument("--skip-features", action="store_true")
    parser.add_argument("--skip-analytics", action="store_true")
    # Target metric lets you choose which column to analyze when building analytics outputs.
    parser.add_argument("--target-metric", default=None, help="Overrides analytics target metric.")
    # Parse CLI arguments once at startup to keep control flow deterministic.
    args = parser.parse_args()

    # Load typed config so this runner can configure logging and pass config paths to subprocesses.
    config = load_config(args.config)
    # Configure logging early so all subsequent log lines follow the same format/level.
    configure_logging(config.logging)

    # Resolve repo root so we can locate the `scripts/` directory reliably regardless of CWD.
    repo_root = Path(__file__).resolve().parents[1]
    # Script directory holds the smaller building blocks that this runner orchestrates.
    scripts_dir = repo_root / "scripts"

    # Start from the current environment so credentials and user settings flow into subprocesses.
    env = os.environ.copy()
    # If a config path is provided, set the env var so all subprocesses read the same config consistently.
    if args.config:
        env["METROBIKEATLAS_CONFIG_PATH"] = str(Path(args.config).resolve())

    # Optional: allow forcing demo mode via env when running the API later.
    # Note: this pipeline runner always builds real data artifacts; demo mode affects only the API/web layer.
    demo_env = _parse_bool_env(env.get("METROBIKEATLAS_DEMO_MODE"))
    if demo_env is not None:
        logger.info("METROBIKEATLAS_DEMO_MODE=%s (note: pipeline scripts ignore demo mode).", demo_env)

    # Convert directory args to strings so they can be passed cleanly to subprocess commands.
    bronze_dir = str(args.bronze_dir)
    silver_dir = str(args.silver_dir)
    gold_dir = str(args.gold_dir)

    # Step 1: fetch latest station snapshots to Bronze unless the user explicitly skips it.
    if not args.skip_extract_stations:
        # Credentials are required for network calls to TDX; fail early before running subprocesses.
        _require_tdx_credentials()
        # Extract metro stations for all configured cities (writes Bronze JSON files).
        _run([sys.executable, str(scripts_dir / "extract_metro_stations.py"), "--bronze-dir", bronze_dir], env=env)
        # Extract bike stations for all configured cities (writes Bronze JSON files).
        _run([sys.executable, str(scripts_dir / "extract_bike_stations.py"), "--bronze-dir", bronze_dir], env=env)

    # Step 2 (optional): collect availability snapshots for a time window to build a time series.
    if args.collect_duration_seconds is not None:
        # Credentials are required for network calls to TDX; fail early before running subprocesses.
        _require_tdx_credentials()
        # Run the availability collector loop for the requested duration/interval.
        _run(
            [
                sys.executable,
                str(scripts_dir / "collect_bike_availability_loop.py"),
                "--bronze-dir",
                bronze_dir,
                "--interval-seconds",
                str(int(args.collect_interval_seconds)),
                "--duration-seconds",
                str(int(args.collect_duration_seconds)),
            ],
            env=env,
        )

    # Step 3: build Silver tables (stations, bike time series, metro↔bike links) from Bronze files.
    _run(
        [
            sys.executable,
            str(scripts_dir / "build_silver.py"),
            "--bronze-dir",
            bronze_dir,
            "--silver-dir",
            silver_dir,
            "--max-availability-files",
            str(int(args.max_availability_files)),
        ],
        env=env,
    )

    # Step 4: validate Silver outputs so downstream steps don't silently operate on broken schemas.
    validate_silver_dir(Path(silver_dir), strict=True)

    # Determine whether a real metro ridership CSV exists (or will be imported) for target selection later.
    metro_ts_path = Path(silver_dir) / "metro_timeseries.csv"
    # Optional: import external metro ridership CSV into the Silver directory.
    if args.import_metro_csv:
        # Build the importer command with column/time options so it can handle common public datasets.
        import_cmd = [
            sys.executable,
            str(scripts_dir / "import_metro_timeseries.py"),
            args.import_metro_csv,
            "--output-csv",
            str(metro_ts_path),
            "--metro-stations-csv",
            str(Path(silver_dir) / "metro_stations.csv"),
            "--station-id-col",
            args.import_station_id_col,
            "--ts-col",
            args.import_ts_col,
            "--value-col",
            args.import_value_col,
            "--agg",
            args.import_agg,
        ]
        # Optional time parsing format for string timestamps.
        if args.import_ts_format:
            import_cmd += ["--ts-format", args.import_ts_format]
        # Optional time unit for integer timestamps (e.g., seconds, milliseconds).
        if args.import_ts_unit:
            import_cmd += ["--ts-unit", args.import_ts_unit]
        # Optional timezone normalization for inputs that are not in UTC.
        if args.import_input_timezone:
            import_cmd += ["--input-timezone", args.import_input_timezone]
        if args.import_output_timezone:
            import_cmd += ["--output-timezone", args.import_output_timezone]
        # Optional alignment step to resample the imported series to a target granularity.
        if args.import_align:
            import_cmd += ["--align"]
            if args.import_granularity:
                import_cmd += ["--granularity", args.import_granularity]
        # Run importer as a subprocess so its CLI stays testable and consistent with other scripts.
        _run(import_cmd, env=env)

    # Step 5 (optional): build station-level features (Gold) from Silver tables.
    if not args.skip_features:
        _run([sys.executable, str(scripts_dir / "build_features.py"), "--silver-dir", silver_dir], env=env)

    # Step 6 (optional): build analytics outputs (Gold) from features and targets.
    if not args.skip_analytics:
        # Prefer real ridership as target if present, unless overridden.
        target_metric = args.target_metric
        if target_metric is None:
            # If metro timeseries exists, use it; otherwise fall back to the bike-derived proxy target.
            target_metric = "metro_ridership" if metro_ts_path.exists() else "metro_flow_proxy_from_bike_rent"
        # Run analytics builder and write outputs into the chosen Gold directory.
        _run(
            [
                sys.executable,
                str(scripts_dir / "build_analytics.py"),
                "--target-metric",
                str(target_metric),
                "--out-dir",
                gold_dir,
            ],
            env=env,
        )

    # Final message helps users immediately move from data pipeline to interactive web exploration.
    logger.info("Pipeline complete. To run the web app: python scripts/run_api.py")


if __name__ == "__main__":
    # Guard to prevent accidental execution when imported by tests or other modules.
    main()
