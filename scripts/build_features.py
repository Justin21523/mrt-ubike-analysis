from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from metrobikeatlas.config.loader import load_config
from metrobikeatlas.features.builder import (
    build_feature_artifacts,
    load_poi_csv,
    load_station_district_map_csv,
)
from metrobikeatlas.utils.logging import configure_logging


logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver-dir", default="data/silver")
    args = parser.parse_args()

    config = load_config()
    configure_logging(config.logging)

    silver_dir = Path(args.silver_dir)
    metro = pd.read_csv(silver_dir / "metro_stations.csv")
    bike = pd.read_csv(silver_dir / "bike_stations.csv")
    links = pd.read_csv(silver_dir / "metro_bike_links.csv")

    bike_ts_path = silver_dir / "bike_timeseries.csv"
    bike_ts = pd.read_csv(bike_ts_path, parse_dates=["ts"]) if bike_ts_path.exists() else pd.DataFrame()

    metro_ts_path = silver_dir / "metro_timeseries.csv"
    metro_ts = pd.read_csv(metro_ts_path, parse_dates=["ts"]) if metro_ts_path.exists() else None

    poi = None
    if config.features.poi is not None and config.features.poi.path.exists():
        poi = load_poi_csv(config.features.poi.path)
    else:
        logger.warning("POI file not found; skipping POI features.")

    district_map = None
    if config.features.station_district_map_path and config.features.station_district_map_path.exists():
        district_map = load_station_district_map_csv(config.features.station_district_map_path)
    else:
        logger.warning("Station→district map not found; skipping district feature.")

    artifacts = build_feature_artifacts(
        config=config,
        metro_stations=metro,
        bike_stations=bike,
        links=links,
        bike_timeseries=bike_ts,
        metro_timeseries=metro_ts,
        poi=poi,
        district_map=district_map,
    )

    config.features.station_features_path.parent.mkdir(parents=True, exist_ok=True)
    artifacts.station_features.to_csv(config.features.station_features_path, index=False)
    logger.info("Wrote %s", config.features.station_features_path)

    config.features.station_targets_path.parent.mkdir(parents=True, exist_ok=True)
    artifacts.station_targets.to_csv(config.features.station_targets_path, index=False)
    logger.info("Wrote %s", config.features.station_targets_path)


if __name__ == "__main__":
    main()
