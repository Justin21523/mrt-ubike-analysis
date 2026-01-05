# Stage 1: Cleaning & Integration Strategy (v0)

## Why write this first?

Integration is where projects drift. Writing down rules early keeps assumptions explicit and pipelines reproducible.

Key principles:
- Make data quality assumptions reviewable.
- Automate transforms (avoid “manual notebook fixes”).
- Preserve stable station keys and time alignment as the backbone.

## Layers

1. Bronze: persist raw JSON (request params, retrieval time, endpoint)
2. Silver: standardize (keys, naming, types, timezone, dedupe, missing data)
3. Gold: aggregates & features (peak/off-peak, weekday/weekend, lags, accessibility)

## Core cleaning rules (Silver)

### Stations (metro / bike)

- **Stable keys**: prefer source-stable IDs; introduce a mapping table if renames/merges happen.
- **Geometry quality**: validate `lat/lon` ranges; catch nulls and obvious outliers (e.g., 0,0).
- **District alignment**: backfill `city/district` via reverse-geocoding or official boundaries; avoid relying on free-text addresses.
- **Dedupe policy**: when the same station appears multiple times, keep the latest/most reliable record and track validity windows if needed later.

### Time series (flows / metrics)

- **Timezone**: convert everything to `Asia/Taipei` with tz-aware timestamps (avoid string time comparisons).
- **Granularity**: keep source granularity in Bronze; aggregate using explicit rules (sum/mean/last) in Silver/Gold.
- **Missingness**:
  - For counts (entries/exits/rents), missing data is not zero — distinguish “no event” vs “missing report”.
  - For realtime state (available bikes/docks), forward-fill can work, but cap the maximum gap.
- **Quality checks**: `(entity_type, station_id, ts, metric)` should be unique; values should be non-negative unless defined otherwise.

## Metro × Bike integration

The core is **space + time** alignment:

- **Space**: build a metro→bike mapping using a radius (e.g., 300m/500m) or nearest neighbors.
- **Time**: align to a common granularity (15min/hour/day) and derive:
  - weekday/weekend flags (via a calendar table)
  - peak/off-peak buckets (rule-based first, then calibrated later)
