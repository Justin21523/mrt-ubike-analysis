from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import Optional

from metrobikeatlas.config.loader import load_config
from metrobikeatlas.utils.logging import configure_logging


logger = logging.getLogger(__name__)


def _parse_bool_env(value: str | None) -> Optional[bool]:
    if value is None:
        return None
    v = value.strip().lower()
    if v in {"1", "true", "yes", "y", "on"}:
        return True
    if v in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _require_tdx_credentials() -> None:
    if not os.getenv("TDX_CLIENT_ID") or not os.getenv("TDX_CLIENT_SECRET"):
        raise ValueError(
            "Missing TDX credentials. Set env vars TDX_CLIENT_ID and TDX_CLIENT_SECRET "
            "(see .env.example)."
        )


def _run(cmd: list[str], *, env: dict[str, str]) -> None:
    logger.info("Running: %s", " ".join(cmd))
    subprocess.run(cmd, check=True, env=env)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the end-to-end MVP pipeline (TDX → Bronze → Silver → Gold).")
    parser.add_argument("--config", default=None, help="Config JSON path (overrides METROBIKEATLAS_CONFIG_PATH).")
    parser.add_argument("--bronze-dir", default="data/bronze")
    parser.add_argument("--silver-dir", default="data/silver")
    parser.add_argument("--gold-dir", default="data/gold")
    parser.add_argument("--max-availability-files", type=int, default=500)

    parser.add_argument("--skip-extract-stations", action="store_true")
    parser.add_argument("--collect-duration-seconds", type=int, default=None, help="If set, collect bike snapshots.")
    parser.add_argument("--collect-interval-seconds", type=int, default=300)

    parser.add_argument("--import-metro-csv", default=None, help="Optional external metro ridership CSV to import.")
    parser.add_argument("--import-station-id-col", default="station_id")
    parser.add_argument("--import-ts-col", default="ts")
    parser.add_argument("--import-value-col", default="value")
    parser.add_argument("--import-ts-format", default=None)
    parser.add_argument("--import-ts-unit", default=None)
    parser.add_argument("--import-input-timezone", default=None)
    parser.add_argument("--import-output-timezone", default=None)
    parser.add_argument("--import-align", action="store_true")
    parser.add_argument("--import-granularity", default=None, choices=["15min", "hour", "day"])
    parser.add_argument("--import-agg", default="sum", choices=["sum", "mean"])

    parser.add_argument("--skip-features", action="store_true")
    parser.add_argument("--skip-analytics", action="store_true")
    parser.add_argument("--target-metric", default=None, help="Overrides analytics target metric.")
    args = parser.parse_args()

    config = load_config(args.config)
    configure_logging(config.logging)

    repo_root = Path(__file__).resolve().parents[1]
    scripts_dir = repo_root / "scripts"

    env = os.environ.copy()
    if args.config:
        env["METROBIKEATLAS_CONFIG_PATH"] = str(Path(args.config).resolve())

    # Optional: allow forcing demo mode via env when running the API later.
    demo_env = _parse_bool_env(env.get("METROBIKEATLAS_DEMO_MODE"))
    if demo_env is not None:
        logger.info("METROBIKEATLAS_DEMO_MODE=%s (note: pipeline scripts ignore demo mode).", demo_env)

    bronze_dir = str(args.bronze_dir)
    silver_dir = str(args.silver_dir)
    gold_dir = str(args.gold_dir)

    if not args.skip_extract_stations:
        _require_tdx_credentials()
        _run([sys.executable, str(scripts_dir / "extract_metro_stations.py"), "--bronze-dir", bronze_dir], env=env)
        _run([sys.executable, str(scripts_dir / "extract_bike_stations.py"), "--bronze-dir", bronze_dir], env=env)

    if args.collect_duration_seconds is not None:
        _require_tdx_credentials()
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

    metro_ts_path = Path(silver_dir) / "metro_timeseries.csv"
    if args.import_metro_csv:
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
        if args.import_ts_format:
            import_cmd += ["--ts-format", args.import_ts_format]
        if args.import_ts_unit:
            import_cmd += ["--ts-unit", args.import_ts_unit]
        if args.import_input_timezone:
            import_cmd += ["--input-timezone", args.import_input_timezone]
        if args.import_output_timezone:
            import_cmd += ["--output-timezone", args.import_output_timezone]
        if args.import_align:
            import_cmd += ["--align"]
            if args.import_granularity:
                import_cmd += ["--granularity", args.import_granularity]
        _run(import_cmd, env=env)

    if not args.skip_features:
        _run([sys.executable, str(scripts_dir / "build_features.py"), "--silver-dir", silver_dir], env=env)

    if not args.skip_analytics:
        # Prefer real ridership as target if present, unless overridden.
        target_metric = args.target_metric
        if target_metric is None:
            target_metric = "metro_ridership" if metro_ts_path.exists() else "metro_flow_proxy_from_bike_rent"
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

    logger.info("Pipeline complete. To run the web app: python scripts/run_api.py")


if __name__ == "__main__":
    main()

