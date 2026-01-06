from __future__ import annotations

# `argparse` provides a stable CLI interface for long-running ingestion jobs (cron/daemon friendly).
import argparse
# `logging` is used for continuous progress reporting and error visibility in long-running loops.
import logging
# `random` is used for jitter to avoid synchronized polling across machines (thundering herd).
import random
# `time.sleep` is used for simple scheduling between polling iterations.
import time
# We use timezone-aware UTC timestamps for consistent stop conditions and file metadata.
from datetime import datetime, timedelta, timezone
# `Path` keeps filesystem operations cross-platform and avoids manual string joins.
from pathlib import Path

# Config is loaded at runtime so we can change endpoints/cities without changing code (production-minded).
from metrobikeatlas.config.loader import load_config
# Bronze writer persists raw API payload + request metadata so we can rebuild Silver/Gold deterministically.
from metrobikeatlas.ingestion.bronze import write_bronze_json
# `TDXClient` handles OAuth tokens and retries; `TDXCredentials` reads secrets from env (never from git).
from metrobikeatlas.ingestion.tdx_base import TDXClient, TDXCredentials
# Centralized logging configuration keeps script output consistent across local runs and production jobs.
from metrobikeatlas.utils.logging import configure_logging


# Module-level logger is standard for consistent log formatting and handler configuration.
logger = logging.getLogger(__name__)


# Helper to parse an optional comma-separated city override from CLI args.
def _parse_cities(value: str | None) -> list[str] | None:
    # Treat missing value as "no override" so callers fall back to config.
    if value is None:
        return None
    # Split by comma and trim whitespace so users can pass "Taipei, NewTaipei".
    cities = [c.strip() for c in value.split(",")]
    # Filter out empty entries so accidental trailing commas don't produce invalid city codes.
    return [c for c in cities if c]


# Keep all side effects (config IO, network calls, filesystem writes) inside `main()` so the module is import-safe.
def main() -> None:
    # Build a CLI parser for a long-running polling loop (useful for on-demand collection and cron jobs).
    parser = argparse.ArgumentParser(
        description=(
            "Continuously collect bike availability snapshots from TDX and write them to Bronze. "
            "Stop with Ctrl+C."
        )
    )
    # Bronze directory is a local data lake root; it is gitignored and treated as a runtime artifact.
    parser.add_argument("--bronze-dir", default="data/bronze")
    # Optional per-run override lets you collect only selected cities without editing config files.
    parser.add_argument("--cities", default=None, help="Comma-separated list (overrides config).")
    # Interval controls how often we poll the availability endpoint (e.g., every 300s).
    parser.add_argument("--interval-seconds", type=int, default=300, help="Polling interval.")
    # Duration provides a wall-clock stop condition for temporary runs.
    parser.add_argument("--duration-seconds", type=int, default=None, help="Stop after N seconds.")
    # Max iterations provides a deterministic loop bound (useful for tests and quick experiments).
    parser.add_argument("--max-iterations", type=int, default=None, help="Stop after N loops.")
    # Jitter spreads requests over time to reduce bursty traffic (and avoid rate limiting).
    parser.add_argument("--jitter-seconds", type=float, default=0.0, help="Add random jitter to sleep.")
    # Backoff controls how long we sleep when all city requests fail in a loop iteration.
    parser.add_argument("--backoff-seconds", type=int, default=10, help="Sleep on failure (exponential).")
    # Max backoff caps exponential growth so the job eventually retries again.
    parser.add_argument("--max-backoff-seconds", type=int, default=300, help="Max backoff on failure.")
    # Parse CLI arguments once at startup to keep control flow deterministic.
    args = parser.parse_args()

    # Load config from default path or env override; this script intentionally does not take `--config`.
    config = load_config()
    # Configure logging early so every log line is consistent and includes timestamps/levels.
    configure_logging(config.logging)

    # Read TDX credentials from environment variables (production practice: do not hardcode secrets).
    creds = TDXCredentials.from_env()
    # Ensure the Bronze root exists before writing any files.
    bronze_dir = Path(args.bronze_dir)
    bronze_dir.mkdir(parents=True, exist_ok=True)

    # Choose which cities to collect: CLI override wins, otherwise we use config defaults.
    cities = _parse_cities(args.cities) or list(config.tdx.bike.cities)
    # Fail fast if no cities are configured, because an empty loop would hide misconfiguration.
    if not cities:
        raise ValueError("No bike cities configured or provided via --cities")

    # Enforce sane minimums so we never sleep a negative interval or spin in a tight loop.
    interval_s = max(int(args.interval_seconds), 1)
    # Jitter is optional and must be non-negative.
    jitter_s = max(float(args.jitter_seconds), 0.0)

    # `stop_at` provides an optional wall-clock stop time (UTC) for time-bounded runs.
    stop_at = None
    if args.duration_seconds is not None:
        stop_at = datetime.now(timezone.utc) + timedelta(seconds=int(args.duration_seconds))

    # Backoff settings handle repeated failures (e.g., network outage, TDX downtime).
    backoff_s = max(int(args.backoff_seconds), 1)
    # Current backoff grows exponentially when failures persist.
    current_backoff_s = backoff_s

    # Use a context manager so HTTP connections are closed cleanly even if the loop is interrupted.
    with TDXClient(
        # Base URL and token URL come from config/env overrides so deployment can swap endpoints safely.
        base_url=config.tdx.base_url,
        token_url=config.tdx.token_url,
        credentials=creds,
    ) as tdx:
        # Loop counter is useful for deterministic stopping and for log context.
        i = 0
        while True:
            # Stop when duration is reached (if configured).
            if stop_at is not None and datetime.now(timezone.utc) >= stop_at:
                logger.info("Stopping: duration reached.")
                break
            # Stop when max iterations is reached (if configured).
            if args.max_iterations is not None and i >= int(args.max_iterations):
                logger.info("Stopping: max iterations reached.")
                break
            # Increment loop counter at the start of each iteration.
            i += 1

            # Track start time so we can compute how long the work took and adjust sleep accordingly.
            loop_started = datetime.now(timezone.utc)
            # `ok` counts successful city snapshots so we can decide whether to back off.
            ok = 0
            # Collect one snapshot per city per loop iteration.
            for city in cities:
                try:
                    # Build the realtime availability endpoint path for this city.
                    path = config.tdx.bike.availability_path_template.format(city=city)
                    # Force JSON output for consistent downstream parsing.
                    params = {"$format": "JSON"}
                    # Capture retrieval time in UTC so we can align snapshots across cities.
                    retrieved_at = datetime.now(timezone.utc)
                    # Fetch raw JSON payload; token/retry logic is handled inside `TDXClient`.
                    payload = tdx.get_json(path, params=params)
                    # Persist the raw payload plus request metadata to Bronze for traceability.
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
                    # Mark this city as successful so the loop does not enter global backoff mode.
                    ok += 1
                    # Log output path for debugging and for observing ingestion progress.
                    logger.info("Wrote %s", out)
                except Exception:
                    # Log full stack trace so operators can debug transient network/API failures.
                    logger.exception("Failed to fetch availability snapshot (city=%s)", city)

            # If all cities failed, sleep using exponential backoff to reduce pressure and avoid tight loops.
            if ok == 0:
                logger.warning("No snapshots collected; backing off %ss.", current_backoff_s)
                time.sleep(current_backoff_s)
                current_backoff_s = min(current_backoff_s * 2, int(args.max_backoff_seconds))
                continue

            # Reset backoff after a successful loop so the next failure starts from the base backoff.
            current_backoff_s = backoff_s
            # Compute how long this iteration took so we can schedule the next poll close to `interval_s`.
            elapsed_s = (datetime.now(timezone.utc) - loop_started).total_seconds()
            # Sleep only the remaining time (never negative) so polling stays roughly periodic.
            sleep_s = max(interval_s - elapsed_s, 0.0)
            # Add optional jitter to spread requests across machines and reduce synchronized bursts.
            if jitter_s:
                sleep_s += random.random() * jitter_s
            # Sleep only if there is a positive duration; otherwise immediately start the next iteration.
            if sleep_s > 0:
                logger.info("Sleeping %.1fs", sleep_s)
                time.sleep(sleep_s)


if __name__ == "__main__":
    # Wrap `main()` so Ctrl+C produces a clean log line rather than a noisy stack trace.
    try:
        main()
    except KeyboardInterrupt:
        # KeyboardInterrupt is expected during local runs; we log it as a normal stop event.
        logger.info("Stopped by user (KeyboardInterrupt).")
