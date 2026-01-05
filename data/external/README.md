# `data/external/`

This folder is for optional, non-TDX datasets used to build city-factor features. Files here are **not**
version-controlled (except this README and `.gitkeep`).

## Admin boundaries (GeoJSON)

Path (default): `data/external/admin_boundaries.geojson`

- GeoJSON `FeatureCollection` with `Polygon` / `MultiPolygon` geometries.
- Each feature must have a district name in one of these properties (configurable):
  - `district` (preferred), `town`, `TOWNNAME`, `NAME`, or `name`

Used by: `python scripts/build_station_district_map.py`

## Station → district map (CSV)

Path (default): `data/external/metro_station_district.csv`

Required columns:
- `station_id` (matches `data/silver/metro_stations.csv`)
- `district` (string)

Used by: `python scripts/build_features.py` (adds a `district` factor).

## POI dataset (CSV)

Path (default): `data/external/poi.csv`

Required columns:
- `category` (string; should match `features.poi.categories` in `config/default.json`)
- `lat` (float)
- `lon` (float)

Optional columns:
- `poi_id`, `name`, `source`, `tags`

You can generate a baseline POI dataset from OpenStreetMap:
- `python scripts/fetch_poi_overpass.py`

