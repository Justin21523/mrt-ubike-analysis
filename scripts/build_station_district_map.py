from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from metrobikeatlas.config.loader import load_config
from metrobikeatlas.gis.boundaries import BoundaryIndex
from metrobikeatlas.utils.logging import configure_logging


logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver-dir", default="data/silver")
    parser.add_argument("--boundaries-geojson", default=None)
    parser.add_argument("--out", default=None)
    parser.add_argument("--name-property", default=None)
    args = parser.parse_args()

    config = load_config()
    configure_logging(config.logging)

    boundaries_path = (
        Path(args.boundaries_geojson)
        if args.boundaries_geojson
        else config.features.admin_boundaries_geojson_path
    )
    if boundaries_path is None:
        raise ValueError(
            "Missing boundaries GeoJSON path. Set config.features.admin_boundaries_geojson_path "
            "or pass --boundaries-geojson."
        )
    if not boundaries_path.exists():
        raise FileNotFoundError(boundaries_path)

    out_path = Path(args.out) if args.out else config.features.station_district_map_path
    if out_path is None:
        raise ValueError(
            "Missing output path. Set config.features.station_district_map_path or pass --out."
        )

    silver_dir = Path(args.silver_dir)
    metro = pd.read_csv(silver_dir / "metro_stations.csv")
    if not {"station_id", "lat", "lon"} <= set(metro.columns):
        raise ValueError("metro_stations.csv must include station_id, lat, lon")

    index = BoundaryIndex.from_geojson(boundaries_path, name_property=args.name_property)

    rows = []
    missing = 0
    for _, row in metro.iterrows():
        station_id = str(row["station_id"])
        district = index.lookup(lat=float(row["lat"]), lon=float(row["lon"]))
        if not district:
            missing += 1
        rows.append({"station_id": station_id, "district": district})

    out_df = pd.DataFrame(rows)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_path, index=False)
    logger.info("Wrote %s (missing=%s/%s)", out_path, missing, len(out_df))


if __name__ == "__main__":
    main()

