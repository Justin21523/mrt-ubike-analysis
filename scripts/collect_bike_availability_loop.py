from __future__ import annotations

import argparse
import logging
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from metrobikeatlas.config.loader import load_config
from metrobikeatlas.ingestion.bronze import write_bronze_json
from metrobikeatlas.ingestion.tdx_base import TDXClient, TDXCredentials
from metrobikeatlas.utils.logging import configure_logging


logger = logging.getLogger(__name__)


def _parse_cities(value: str | None) -> list[str] | None:
    if value is None:
        return None
    cities = [c.strip() for c in value.split(",")]
    return [c for c in cities if c]


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Continuously collect bike availability snapshots from TDX and write them to Bronze. "
            "Stop with Ctrl+C."
        )
    )
    parser.add_argument("--bronze-dir", default="data/bronze")
    parser.add_argument("--cities", default=None, help="Comma-separated list (overrides config).")
    parser.add_argument("--interval-seconds", type=int, default=300, help="Polling interval.")
    parser.add_argument("--duration-seconds", type=int, default=None, help="Stop after N seconds.")
    parser.add_argument("--max-iterations", type=int, default=None, help="Stop after N loops.")
    parser.add_argument("--jitter-seconds", type=float, default=0.0, help="Add random jitter to sleep.")
    parser.add_argument("--backoff-seconds", type=int, default=10, help="Sleep on failure (exponential).")
    parser.add_argument("--max-backoff-seconds", type=int, default=300, help="Max backoff on failure.")
    args = parser.parse_args()

    config = load_config()
    configure_logging(config.logging)

    creds = TDXCredentials.from_env()
    bronze_dir = Path(args.bronze_dir)
    bronze_dir.mkdir(parents=True, exist_ok=True)

    cities = _parse_cities(args.cities) or list(config.tdx.bike.cities)
    if not cities:
        raise ValueError("No bike cities configured or provided via --cities")

    interval_s = max(int(args.interval_seconds), 1)
    jitter_s = max(float(args.jitter_seconds), 0.0)

    stop_at = None
    if args.duration_seconds is not None:
        stop_at = datetime.now(timezone.utc) + timedelta(seconds=int(args.duration_seconds))

    backoff_s = max(int(args.backoff_seconds), 1)
    current_backoff_s = backoff_s

    with TDXClient(
        base_url=config.tdx.base_url,
        token_url=config.tdx.token_url,
        credentials=creds,
    ) as tdx:
        i = 0
        while True:
            if stop_at is not None and datetime.now(timezone.utc) >= stop_at:
                logger.info("Stopping: duration reached.")
                break
            if args.max_iterations is not None and i >= int(args.max_iterations):
                logger.info("Stopping: max iterations reached.")
                break
            i += 1

            loop_started = datetime.now(timezone.utc)
            ok = 0
            for city in cities:
                try:
                    path = config.tdx.bike.availability_path_template.format(city=city)
                    params = {"$format": "JSON"}
                    retrieved_at = datetime.now(timezone.utc)
                    payload = tdx.get_json(path, params=params)
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
                    ok += 1
                    logger.info("Wrote %s", out)
                except Exception:
                    logger.exception("Failed to fetch availability snapshot (city=%s)", city)

            if ok == 0:
                logger.warning("No snapshots collected; backing off %ss.", current_backoff_s)
                time.sleep(current_backoff_s)
                current_backoff_s = min(current_backoff_s * 2, int(args.max_backoff_seconds))
                continue

            current_backoff_s = backoff_s
            elapsed_s = (datetime.now(timezone.utc) - loop_started).total_seconds()
            sleep_s = max(interval_s - elapsed_s, 0.0)
            if jitter_s:
                sleep_s += random.random() * jitter_s
            if sleep_s > 0:
                logger.info("Sleeping %.1fs", sleep_s)
                time.sleep(sleep_s)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Stopped by user (KeyboardInterrupt).")
