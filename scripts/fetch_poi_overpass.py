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

from metrobikeatlas.config.loader import load_config
from metrobikeatlas.ingestion.osm_overpass import (
    OverpassClient,
    OverpassSettings,
    build_bbox_from_points,
    build_overpass_query_for_category,
    elements_to_poi_rows,
)
from metrobikeatlas.utils.cache import JsonFileCache
from metrobikeatlas.utils.logging import configure_logging


logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver-dir", default="data/silver")
    parser.add_argument("--out", default=None)
    parser.add_argument("--categories", nargs="*", default=None)
    parser.add_argument("--bbox-padding-m", type=float, default=1500.0)
    parser.add_argument("--overpass-url", default=None)
    parser.add_argument("--timeout-s", type=int, default=180)
    parser.add_argument("--sleep-s", type=float, default=1.0)
    args = parser.parse_args()

    config = load_config()
    configure_logging(config.logging)

    silver_dir = Path(args.silver_dir)
    metro = pd.read_csv(silver_dir / "metro_stations.csv")
    if not {"lat", "lon"} <= set(metro.columns):
        raise ValueError("metro_stations.csv must include lat, lon")

    categories = args.categories or (config.features.poi.categories if config.features.poi else [])
    if not categories:
        raise ValueError("No categories specified. Pass --categories or set features.poi.categories in config.")

    out_path = Path(args.out) if args.out else (config.features.poi.path if config.features.poi else None)
    if out_path is None:
        raise ValueError("Missing output path. Pass --out or set features.poi.path in config.")

    bbox = build_bbox_from_points(
        lats=metro["lat"].astype(float).tolist(),
        lons=metro["lon"].astype(float).tolist(),
        padding_m=float(args.bbox_padding_m),
    )

    cache = JsonFileCache(config.cache)
    settings = OverpassSettings(
        url=args.overpass_url or OverpassSettings.url,
        timeout_s=int(args.timeout_s),
        user_agent=f"{config.app.name}/0.1.0",
        sleep_s=float(args.sleep_s),
    )
    client = OverpassClient(settings=settings, cache=cache)

    try:
        all_rows = []
        for cat in categories:
            query = build_overpass_query_for_category(category=cat, bbox=bbox, timeout_s=settings.timeout_s)
            logger.info("Fetching POIs for category=%s", cat)
            data = client.query(query, use_cache=True)
            elements = data.get("elements") or []
            rows = elements_to_poi_rows(elements, category=cat)
            all_rows.extend(rows)
            client.polite_sleep()
    finally:
        client.close()

    df = pd.DataFrame(all_rows)
    df = df.drop_duplicates(subset=["osm_type", "osm_id", "category"])
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    logger.info("Wrote %s (%s rows)", out_path, len(df))


if __name__ == "__main__":
    main()
