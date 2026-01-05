# Stage 3: City Factors & Basic Analytics (MVP)

This stage adds a minimal, reproducible feature layer (city factors + bike accessibility) and basic analytics for EDA.
It intentionally avoids complex ML; the goal is to support exploration and “similar station” browsing in the web app.

## Input data (local files)

### Required (Silver)

Built by `python scripts/build_silver.py`:

- `data/silver/metro_stations.csv`
- `data/silver/bike_stations.csv`
- `data/silver/metro_bike_links.csv`
- `data/silver/bike_timeseries.csv` (required for target metrics)

### Optional (External)

These are **not** committed to git. Put them under `data/external/`.

If you don’t already have curated datasets, you can generate a starting point:

- POIs from OpenStreetMap: `python scripts/fetch_poi_overpass.py`
- Station→district map from admin boundaries: `python scripts/build_station_district_map.py`

1) POI dataset: `data/external/poi.csv`

Required columns:
- `category` (string)
- `lat` (float)
- `lon` (float)

Optional columns:
- `poi_id`, `name`, `source`

2) Station → district mapping: `data/external/metro_station_district.csv`

Required columns:
- `station_id` (metro station id, matches `metro_stations.csv`)
- `district` (string)

3) Admin boundaries (GeoJSON): `data/external/admin_boundaries.geojson`

- GeoJSON `FeatureCollection` with Polygon/MultiPolygon geometries.
- Must contain a district name property (defaults: `district`, `town`, `TOWNNAME`, `NAME`, `name`).

## Feature outputs (Gold)

Built by `python scripts/build_features.py`:

- `data/gold/station_features.csv` (station-level factors; used by `/station/{id}/factors` and `/station/{id}/similar`)
- `data/gold/station_targets.csv` (EDA target metric; used by analytics scripts)

### Metro flow fallback (target metric)

If station-level metro ridership is not available, `station_targets.csv` uses:
- `metro_flow_proxy_from_bike_rent`: sum of `rent_proxy` from nearby bike stations over the last N days (configurable)

## Analytics outputs (Gold)

Built by `python scripts/build_analytics.py`:

- `data/gold/feature_correlations.csv` (Pearson correlation vs target)
- `data/gold/regression_coefficients.csv` (simple OLS coefficients, standardized X)
- `data/gold/station_clusters.csv` (K-means cluster labels; optional UI hint)

## Configuration

Edit `config/default.json`:

- `features.timeseries_window_days`
- `features.time_patterns` (peak windows used for time-pattern features)
- `features.accessibility.bike` (weights for the composite accessibility score)
- `features.poi.radii_m` and `features.poi.categories`
- `analytics.similarity.top_k` and `analytics.clustering.k`
