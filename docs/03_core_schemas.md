# Stage 1: Core Schemas (draft)

Stage 1 defines a minimal set of entities. City factors (district, POI density, accessibility) will be added later as dimension tables and joined by keys and geometry.

## 1) Metro stations (`dim_metro_station`)

Purpose: stable station keys + geospatial anchor for joins.

Suggested fields (v1): `station_id`, `system`, `name`, `name_en`, `lat`, `lon`, `city`.

## 2) Bike stations (`dim_bike_station`)

Purpose: stable station keys for availability snapshots and usage proxies.

Suggested fields (v1): `station_id`, `operator`, `name`, `lat`, `lon`, `city`, `capacity`.

## 3) Long-form metrics (`fact_station_metric`)

Purpose: unify metro/bike time series into a single pattern for aggregation and visualization.

Suggested fields: `entity_type` (`metro`/`bike`), `station_id`, `ts` (Asia/Taipei), `granularity`, `metric`, `value`, `source`.

### MVP note: metro flow availability

Station-level metro ridership is not consistently available across systems. For the MVP, the API can fall back to
`metro_flow_proxy_from_bike_rent` derived from bike availability deltas near the station. If you have a real metro
ridership dataset, provide `data/silver/metro_timeseries.csv` with columns `station_id`, `ts`, `value`.

## Python types

Defined in: `src/metrobikeatlas/schemas/core.py`
