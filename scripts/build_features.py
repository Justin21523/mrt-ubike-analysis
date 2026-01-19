from __future__ import annotations

import sys
from pathlib import Path

# Allow running scripts without requiring an editable install (`pip install -e .`).
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_PATH))

import argparse
import logging

import pandas as pd
import json
from datetime import datetime, timezone

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

    # Reproducibility meta (ties Gold outputs back to Silver build id/hash).
    try:
        silver_meta_path = silver_dir / "_build_meta.json"
        silver_meta = json.loads(silver_meta_path.read_text(encoding="utf-8")) if silver_meta_path.exists() else None
    except Exception:
        silver_meta = None
    run_meta = {
        "type": "gold_run_meta",
        "stage": "features",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "silver_build_id": (silver_meta or {}).get("build_id") if isinstance(silver_meta, dict) else None,
        "silver_inputs_hash": (silver_meta or {}).get("inputs_hash") if isinstance(silver_meta, dict) else None,
        "inputs": {"silver_dir": str(silver_dir)},
        "artifacts": {
            "station_features_path": str(config.features.station_features_path),
            "station_targets_path": str(config.features.station_targets_path),
        },
    }
    out_dir = config.features.station_features_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "_run_meta.json").write_text(json.dumps(run_meta, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Wrote %s", out_dir / "_run_meta.json")


if __name__ == "__main__":
    main()
