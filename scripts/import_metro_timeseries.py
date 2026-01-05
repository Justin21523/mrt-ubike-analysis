from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from metrobikeatlas.config.loader import load_config
from metrobikeatlas.preprocessing.metro_timeseries import normalize_metro_timeseries
from metrobikeatlas.preprocessing.temporal_align import align_timeseries
from metrobikeatlas.utils.logging import configure_logging


logger = logging.getLogger(__name__)


def _maybe_read_known_station_ids(path: Path) -> Optional[set[str]]:
    if not path.exists():
        return None
    df = pd.read_csv(path, dtype={"station_id": str})
    if "station_id" not in df.columns:
        return None
    return set(df["station_id"].astype(str))


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Import an external metro ridership/flow dataset into `data/silver/metro_timeseries.csv`.\n"
            "Expected output columns: station_id, ts (tz-aware), value."
        )
    )
    parser.add_argument("input_csv", help="Path to the source CSV.")
    parser.add_argument("--output-csv", default="data/silver/metro_timeseries.csv")
    parser.add_argument("--station-id-col", default="station_id")
    parser.add_argument("--ts-col", default="ts")
    parser.add_argument("--value-col", default="value")
    parser.add_argument("--ts-format", default=None, help="Optional datetime format string.")
    parser.add_argument("--ts-unit", default=None, help="Epoch unit (e.g., s, ms). If set, treats ts as epoch.")
    parser.add_argument("--input-timezone", default=None, help="Timezone for naive timestamps.")
    parser.add_argument("--output-timezone", default=None, help="Timezone for output timestamps.")
    parser.add_argument("--align", action="store_true", help="Align to a fixed granularity (resample).")
    parser.add_argument(
        "--granularity",
        default=None,
        choices=["15min", "hour", "day"],
        help="Granularity for --align (defaults to config.temporal.granularity).",
    )
    parser.add_argument(
        "--agg",
        default="sum",
        choices=["sum", "mean"],
        help="Aggregation used for --align and deduplication.",
    )
    parser.add_argument("--metro-stations-csv", default="data/silver/metro_stations.csv")
    parser.add_argument("--drop-unknown-stations", action="store_true")
    args = parser.parse_args()

    config = load_config()
    configure_logging(config.logging)

    input_path = Path(args.input_csv)
    if not input_path.exists():
        raise FileNotFoundError(input_path)

    output_path = Path(args.output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    input_tz = args.input_timezone or config.temporal.timezone
    output_tz = args.output_timezone or config.temporal.timezone

    raw = pd.read_csv(input_path)
    normalized = normalize_metro_timeseries(
        raw,
        station_id_col=args.station_id_col,
        ts_col=args.ts_col,
        value_col=args.value_col,
        ts_format=args.ts_format,
        ts_unit=args.ts_unit,
        input_timezone=input_tz,
        output_timezone=output_tz,
        deduplicate=True,
        dedup_agg=args.agg,
    )

    known_ids = _maybe_read_known_station_ids(Path(args.metro_stations_csv))
    if known_ids is not None:
        observed = set(normalized["station_id"].astype(str))
        unknown = sorted(observed - known_ids)
        if unknown:
            logger.warning(
                "Found %s station_id values not present in %s (e.g., %s)",
                len(unknown),
                args.metro_stations_csv,
                ", ".join(unknown[:10]),
            )
            if args.drop_unknown_stations:
                normalized = normalized[normalized["station_id"].astype(str).isin(known_ids)].copy()
                logger.warning("Dropped unknown station_id rows; remaining=%s", len(normalized))

    if args.align:
        granularity = args.granularity or config.temporal.granularity
        normalized = align_timeseries(
            normalized,
            ts_col="ts",
            group_cols=("station_id",),
            value_cols=("value",),
            granularity=granularity,
            timezone=output_tz,
            agg=args.agg,
        )

    normalized.to_csv(output_path, index=False)
    logger.info("Wrote %s (%s rows)", output_path, len(normalized))


if __name__ == "__main__":
    main()

