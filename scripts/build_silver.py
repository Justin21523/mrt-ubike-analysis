from __future__ import annotations

import argparse
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pandas as pd

from metrobikeatlas.config.loader import load_config
from metrobikeatlas.ingestion.bronze import read_bronze_json
from metrobikeatlas.ingestion.tdx_bike_client import TDXBikeClient
from metrobikeatlas.ingestion.tdx_metro_client import TDXMetroClient
from metrobikeatlas.preprocessing.spatial_join import build_station_bike_links
from metrobikeatlas.preprocessing.temporal_align import compute_rent_return_proxy


def _latest_file(dir_path: Path) -> Path:
    files = sorted(dir_path.glob("*.json"))
    if not files:
        raise FileNotFoundError(f"No Bronze files found in {dir_path}")
    return files[-1]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze-dir", default="data/bronze")
    parser.add_argument("--silver-dir", default="data/silver")
    parser.add_argument("--max-availability-files", type=int, default=500)
    args = parser.parse_args()

    config = load_config()
    bronze_dir = Path(args.bronze_dir)
    silver_dir = Path(args.silver_dir)
    silver_dir.mkdir(parents=True, exist_ok=True)

    # Metro stations (latest per city)
    metro_rows: list[dict[str, Any]] = []
    for city in config.tdx.metro.cities:
        city_dir = bronze_dir / "tdx" / "metro" / "stations" / f"city={city}"
        bronze = read_bronze_json(_latest_file(city_dir))
        payload = bronze["payload"]
        for item in payload:
            metro_rows.append(asdict(TDXMetroClient.parse_station(item, city=city)))

    metro_df = pd.DataFrame(metro_rows)
    metro_out = silver_dir / "metro_stations.csv"
    metro_df.to_csv(metro_out, index=False)
    print(f"Wrote {metro_out}")

    # Bike stations (latest per city)
    bike_rows: list[dict[str, Any]] = []
    for city in config.tdx.bike.cities:
        city_dir = bronze_dir / "tdx" / "bike" / "stations" / f"city={city}"
        bronze = read_bronze_json(_latest_file(city_dir))
        payload = bronze["payload"]
        for item in payload:
            bike_rows.append(asdict(TDXBikeClient.parse_station(item, city=city)))

    bike_df = pd.DataFrame(bike_rows)
    bike_out = silver_dir / "bike_stations.csv"
    bike_df.to_csv(bike_out, index=False)
    print(f"Wrote {bike_out}")

    # Bike availability snapshots (all files; capped)
    availability_rows: list[dict[str, Any]] = []
    for city in config.tdx.bike.cities:
        city_dir = bronze_dir / "tdx" / "bike" / "availability" / f"city={city}"
        files = sorted(city_dir.glob("*.json"))[-args.max_availability_files :]
        for f in files:
            bronze = read_bronze_json(f)
            payload = bronze["payload"]
            for item in payload:
                record = TDXBikeClient.parse_availability(item)
                availability_rows.append({**asdict(record), "city": city})

    if availability_rows:
        availability_df = pd.DataFrame(availability_rows)
        availability_df = compute_rent_return_proxy(
            availability_df,
            station_id_col="station_id",
            ts_col="ts",
            available_bikes_col="available_bikes",
        )
        ts_out = silver_dir / "bike_timeseries.csv"
        availability_df.to_csv(ts_out, index=False)
        print(f"Wrote {ts_out}")
    else:
        print("No bike availability data found; skipping bike_timeseries.csv")

    # Metro↔bike links
    links_df = build_station_bike_links(
        metro_df[["station_id", "lat", "lon"]],
        bike_df[["station_id", "lat", "lon"]],
        settings=config.spatial,
    )
    links_out = silver_dir / "metro_bike_links.csv"
    links_df.to_csv(links_out, index=False)
    print(f"Wrote {links_out}")


if __name__ == "__main__":
    main()

