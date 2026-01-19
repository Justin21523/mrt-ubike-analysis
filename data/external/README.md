# `data/external/`

This folder is for optional, non-TDX datasets used to build city-factor features. Files here are **not**
version-controlled (except this README and `.gitkeep`).

## External calendar (CSV)

Path (default): `data/external/calendar.csv`

Required columns:
- `date` (YYYY-MM-DD)
- `is_holiday` (0/1 or true/false)

Optional columns:
- `name`

Used by: `python scripts/build_silver.py` (writes `data/silver/calendar.csv` when present).

Template: `docs/examples/external_calendar.csv`

## External weather hourly (CSV)

Path (default): `data/external/weather_hourly.csv`

Required columns:
- `ts` (timestamp; ISO8601 recommended)
- `city`
- `temp_c`
- `precip_mm`

Optional columns:
- `humidity_pct`

Used by: `python scripts/build_silver.py` (writes `data/silver/weather_hourly.csv` when present).

Template: `docs/examples/external_weather_hourly.csv`

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
