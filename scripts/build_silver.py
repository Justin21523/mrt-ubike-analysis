from __future__ import annotations

# `argparse` provides a stable CLI interface for building Silver tables from Bronze (repeatable pipelines).
import argparse
# `asdict` converts dataclass records into plain dicts, which is convenient for building pandas DataFrames.
from dataclasses import asdict
# `Path` keeps filesystem operations cross-platform and avoids manual string joins.
from pathlib import Path
# `Any` is used for raw JSON dict payloads coming from Bronze.
from typing import Any

# `pandas` is used for tabular transformation and writing CSV outputs for the Silver layer.
import pandas as pd

# Config is loaded at runtime so city lists and paths can be changed without modifying code.
from metrobikeatlas.config.loader import load_config
# Bronze reader loads the wrapper JSON and returns a dict containing `retrieved_at`, `request`, and `payload`.
from metrobikeatlas.ingestion.bronze import read_bronze_json
# Parsing helpers normalize raw TDX station/availability JSON into stable dataclass schemas.
from metrobikeatlas.ingestion.tdx_bike_client import TDXBikeClient
from metrobikeatlas.ingestion.tdx_metro_client import TDXMetroClient
# Spatial join builds a metro↔bike link table used by the API and feature engineering later.
from metrobikeatlas.preprocessing.spatial_join import build_station_bike_links
# Temporal alignment builds a simple "rent/return proxy" from availability deltas for MVP analysis.
from metrobikeatlas.preprocessing.temporal_align import compute_rent_return_proxy


def _latest_file(dir_path: Path) -> Path:
    # List Bronze files in lexicographic order; our Bronze naming uses UTC timestamps so sorting works.
    files = sorted(dir_path.glob("*.json"))
    # Fail fast when Bronze is missing so users immediately know they must run the ingestion scripts first.
    if not files:
        raise FileNotFoundError(f"No Bronze files found in {dir_path}")
    # Return the newest file (latest timestamp) to represent the most recent station snapshot.
    return files[-1]


def main() -> None:
    # Build a CLI parser so Silver can be rebuilt deterministically from a chosen Bronze directory.
    parser = argparse.ArgumentParser()
    # Bronze is the raw data lake root written by `scripts/extract_*` and `scripts/collect_*` scripts.
    parser.add_argument("--bronze-dir", default="data/bronze")
    # Silver directory is where we write normalized CSVs used by the API and later analytics.
    parser.add_argument("--silver-dir", default="data/silver")
    # Cap the number of availability files to avoid unbounded memory usage in long-running collections.
    parser.add_argument("--max-availability-files", type=int, default=500)
    # Parse CLI arguments once at startup to keep control flow deterministic.
    args = parser.parse_args()

    # Load typed config to obtain the city lists and spatial settings (buffer radius / nearest K).
    config = load_config()
    # Resolve Bronze/Silver directories from CLI args.
    bronze_dir = Path(args.bronze_dir)
    silver_dir = Path(args.silver_dir)
    # Ensure the Silver output directory exists before writing CSVs.
    silver_dir.mkdir(parents=True, exist_ok=True)

    # Metro stations (latest per city)
    # We rebuild a station dimension table by reading the latest Bronze station snapshot for each city.
    metro_rows: list[dict[str, Any]] = []
    # Iterate configured metro cities so the pipeline is config-driven and reproducible.
    for city in config.tdx.metro.cities:
        # Bronze partition path matches the writer: source/domain/dataset/city=...
        city_dir = bronze_dir / "tdx" / "metro" / "stations" / f"city={city}"
        # Load the newest snapshot so we don't duplicate stations across multiple ingestion runs.
        bronze = read_bronze_json(_latest_file(city_dir))
        # Raw JSON records are under the `payload` key by Bronze wrapper convention.
        payload = bronze["payload"]
        # Normalize each raw record into a typed `MetroStation`, then convert to dict for DataFrame building.
        for item in payload:
            metro_rows.append(asdict(TDXMetroClient.parse_station(item, city=city)))

    # Build a DataFrame so we can write a single CSV for downstream steps (API, joins, analytics).
    metro_df = pd.DataFrame(metro_rows)
    # Silver file path uses a stable name so downstream code can locate it without scanning directories.
    metro_out = silver_dir / "metro_stations.csv"
    # Write without index to keep the schema clean and portable across tools.
    metro_df.to_csv(metro_out, index=False)
    # Print output path so logs show what was produced.
    print(f"Wrote {metro_out}")

    # Bike stations (latest per city)
    # Same pattern as metro stations: read latest Bronze snapshot per city and normalize to `BikeStation`.
    bike_rows: list[dict[str, Any]] = []
    for city in config.tdx.bike.cities:
        city_dir = bronze_dir / "tdx" / "bike" / "stations" / f"city={city}"
        bronze = read_bronze_json(_latest_file(city_dir))
        payload = bronze["payload"]
        for item in payload:
            bike_rows.append(asdict(TDXBikeClient.parse_station(item, city=city)))

    # Build a DataFrame for bike station metadata (used for map overlays and spatial joins).
    bike_df = pd.DataFrame(bike_rows)
    # Write a stable Silver CSV name.
    bike_out = silver_dir / "bike_stations.csv"
    bike_df.to_csv(bike_out, index=False)
    print(f"Wrote {bike_out}")

    # Bike availability snapshots (all files; capped)
    # Availability is time-varying, so we read multiple Bronze snapshot files to form a time series.
    availability_rows: list[dict[str, Any]] = []
    for city in config.tdx.bike.cities:
        city_dir = bronze_dir / "tdx" / "bike" / "availability" / f"city={city}"
        # Cap to the most recent N files so long-running collections don't blow up memory/time.
        files = sorted(city_dir.glob("*.json"))[-args.max_availability_files :]
        for f in files:
            # Each file is a wrapper with `payload` holding a list of station availability records.
            bronze = read_bronze_json(f)
            payload = bronze["payload"]
            for item in payload:
                # Normalize raw availability into a typed dataclass so timestamps and counts have stable types.
                record = TDXBikeClient.parse_availability(item)
                # Add city so multi-city collections remain distinguishable in a single DataFrame.
                availability_rows.append({**asdict(record), "city": city})

    # Only write `bike_timeseries.csv` if we actually collected availability snapshots.
    if availability_rows:
        # Build a DataFrame to support temporal operations and CSV export.
        availability_df = pd.DataFrame(availability_rows)
        # Compute a simple rent/return proxy from availability deltas (useful when true trip data is missing).
        availability_df = compute_rent_return_proxy(
            availability_df,
            station_id_col="station_id",
            ts_col="ts",
            available_bikes_col="available_bikes",
        )
        # Write the bike time series Silver table.
        ts_out = silver_dir / "bike_timeseries.csv"
        availability_df.to_csv(ts_out, index=False)
        print(f"Wrote {ts_out}")
    else:
        # This message explains why later steps that depend on bike time series may show empty curves.
        print("No bike availability data found; skipping bike_timeseries.csv")

    # Metro↔bike links
    # Build a station-to-station link table so the API can aggregate nearby bike stations per metro station.
    links_df = build_station_bike_links(
        # Use only the required columns to keep join logic focused and fast.
        metro_df[["station_id", "lat", "lon"]],
        bike_df[["station_id", "lat", "lon"]],
        # Spatial settings (buffer radius or nearest K) come from config for reproducibility.
        settings=config.spatial,
    )
    # Write links to a stable Silver CSV name so downstream code can locate it reliably.
    links_out = silver_dir / "metro_bike_links.csv"
    links_df.to_csv(links_out, index=False)
    print(f"Wrote {links_out}")


if __name__ == "__main__":
    # Guard to prevent accidental execution when imported by tests or other scripts.
    main()
