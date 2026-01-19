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
from metrobikeatlas.ingestion.osm_overpass import OverpassClient, OverpassSettings
from metrobikeatlas.utils.cache import JsonFileCache
from metrobikeatlas.utils.logging import configure_logging


logger = logging.getLogger(__name__)


def _build_query(*, bbox: tuple[float, float, float, float], timeout_s: int) -> str:
    south, west, north, east = bbox
    # Pragmatic query: metro/subway stations in a bbox.
    # Note: OSM tagging varies; we include both `station=subway|metro` and `railway=station` + `subway=yes`.
    return f"""
[out:json][timeout:{int(timeout_s)}];
(
  node["railway"="station"]["station"~"subway|metro"]({south},{west},{north},{east});
  way["railway"="station"]["station"~"subway|metro"]({south},{west},{north},{east});
  relation["railway"="station"]["station"~"subway|metro"]({south},{west},{north},{east});

  node["railway"="station"]["subway"="yes"]({south},{west},{north},{east});
  way["railway"="station"]["subway"="yes"]({south},{west},{north},{east});
  relation["railway"="station"]["subway"="yes"]({south},{west},{north},{east});

  node["public_transport"="station"]["station"~"subway|metro"]({south},{west},{north},{east});
  way["public_transport"="station"]["station"~"subway|metro"]({south},{west},{north},{east});
  relation["public_transport"="station"]["station"~"subway|metro"]({south},{west},{north},{east});
);
out center;
""".strip()


def _elements_to_rows(elements: list[dict[str, object]], *, city: str, default_system: str) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for el in elements:
        el_type = str(el.get("type") or "").strip()
        el_id = el.get("id")
        if not el_type or el_id is None:
            continue

        tags = el.get("tags") if isinstance(el.get("tags"), dict) else {}
        name = None
        if isinstance(tags, dict):
            name = tags.get("name") or tags.get("name:zh") or tags.get("name:en")
        if name is None:
            continue

        lat = el.get("lat")
        lon = el.get("lon")
        if lat is None or lon is None:
            center = el.get("center") if isinstance(el.get("center"), dict) else {}
            lat = center.get("lat") if isinstance(center, dict) else None
            lon = center.get("lon") if isinstance(center, dict) else None
        if lat is None or lon is None:
            continue

        system = default_system
        if isinstance(tags, dict):
            system = tags.get("network") or tags.get("operator") or tags.get("brand") or default_system

        rows.append(
            {
                "station_id": f"OSM_{el_type}_{el_id}",
                "name": str(name),
                "lat": float(lat),
                "lon": float(lon),
                "city": str(city),
                "system": str(system),
            }
        )
    return rows


def main() -> None:
    p = argparse.ArgumentParser(description="Fetch metro/subway stations from OSM Overpass into external CSV.")
    p.add_argument("--out", default="data/external/metro_stations.csv")
    p.add_argument(
        "--bbox",
        default="24.90,121.40,25.25,121.75",
        help="BBox as south,west,north,east (default targets Greater Taipei).",
    )
    p.add_argument("--city", default=None, help="City label to write in CSV (default: config.tdx.metro.cities[0]).")
    p.add_argument("--system", default="OSM", help="Fallback system label if OSM tags are missing.")
    p.add_argument("--overpass-url", default=None)
    p.add_argument("--timeout-s", type=int, default=180)
    p.add_argument("--sleep-s", type=float, default=1.0)
    args = p.parse_args()

    config = load_config()
    configure_logging(config.logging)

    parts = [p.strip() for p in str(args.bbox).split(",")]
    if len(parts) != 4:
        raise ValueError("--bbox must be 'south,west,north,east'")
    bbox = (float(parts[0]), float(parts[1]), float(parts[2]), float(parts[3]))

    city = str(args.city) if args.city else str(list(config.tdx.metro.cities)[0])

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    cache = JsonFileCache(config.cache)
    settings = OverpassSettings(
        url=args.overpass_url or OverpassSettings.url,
        timeout_s=int(args.timeout_s),
        user_agent=f"{config.app.name}/0.1.0",
        sleep_s=float(args.sleep_s),
    )
    client = OverpassClient(settings=settings, cache=cache)
    try:
        query = _build_query(bbox=bbox, timeout_s=settings.timeout_s)
        logger.info("Fetching OSM metro stations bbox=%s city=%s", args.bbox, city)
        data = client.query(query, use_cache=True)
    finally:
        client.close()

    elements = data.get("elements") if isinstance(data, dict) else None
    elements_list = list(elements) if isinstance(elements, list) else []
    rows = _elements_to_rows(elements_list, city=city, default_system=str(args.system))
    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("No OSM metro stations found for the given bbox. Try expanding --bbox.")

    df = df.drop_duplicates(subset=["station_id"])
    df = df.sort_values(["system", "name", "station_id"])
    df.to_csv(out, index=False)
    logger.info("Wrote %s (%s rows)", out, len(df))


if __name__ == "__main__":
    main()

