from __future__ import annotations

# Allow running scripts without requiring an editable install (`pip install -e .`).
import sys
from pathlib import Path
import json
from datetime import datetime, timezone
import hashlib
import uuid

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_PATH))

# `argparse` provides a stable CLI interface for building Silver tables from Bronze (repeatable pipelines).
import argparse
# `asdict` converts dataclass records into plain dicts, which is convenient for building pandas DataFrames.
from dataclasses import asdict
# `Any` is used for raw JSON dict payloads coming from Bronze.
from typing import Any

# `pandas` is used for tabular transformation and writing CSV outputs for the Silver layer.
import pandas as pd
import sqlite3

# Config is loaded at runtime so city lists and paths can be changed without modifying code.
from metrobikeatlas.config.loader import load_config
# External metro stations CSV provides a fallback when TDX metro station endpoints are unavailable.
from metrobikeatlas.ingestion.external_inputs import (
    load_external_calendar_csv,
    load_external_metro_stations_csv,
    load_external_weather_hourly_csv,
    validate_external_calendar_df,
    validate_external_metro_stations_df,
    validate_external_weather_hourly_df,
)
# Bronze reader loads the wrapper JSON and returns a dict containing `retrieved_at`, `request`, and `payload`.
from metrobikeatlas.ingestion.bronze import read_bronze_json
# Parsing helpers normalize raw TDX station/availability JSON into stable dataclass schemas.
from metrobikeatlas.ingestion.tdx_bike_client import TDXBikeClient
from metrobikeatlas.ingestion.tdx_metro_client import TDXMetroClient
# Spatial join builds a metro↔bike link table used by the API and feature engineering later.
from metrobikeatlas.preprocessing.spatial_join import build_station_bike_links
# Temporal alignment builds a simple "rent/return proxy" from availability deltas for MVP analysis.
from metrobikeatlas.preprocessing.temporal_align import compute_rent_return_proxy
from metrobikeatlas.quality.contract import compute_schema_meta, write_json


def _artifact_status(path: Path) -> dict[str, object]:
    try:
        st = path.stat()
        return {
            "path": str(path),
            "exists": True,
            "mtime_utc": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat(),
            "size_bytes": int(st.st_size),
        }
    except FileNotFoundError:
        return {"path": str(path), "exists": False, "mtime_utc": None, "size_bytes": None}


def _emit_event(
    *,
    build_id: str | None = None,
    stage: str,
    progress_pct: int,
    message: str | None = None,
    artifacts: list[Path] | None = None,
    level: str = "info",
) -> None:
    """
    Emit a structured, stable event line for operational observability.

    The API job runner parses these lines to report stage/progress without relying on fragile log text.
    """

    payload: dict[str, object] = {
        "type": "mba_event",
        "job": "build_silver",
        "build_id": build_id,
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "stage": stage,
        "progress_pct": int(progress_pct),
    }
    if message:
        payload["message"] = str(message)
    if artifacts:
        payload["artifacts"] = [_artifact_status(Path(p)) for p in artifacts]
    print("MBA_EVENT " + json.dumps(payload, ensure_ascii=False))


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
    parser.add_argument("--write-sqlite", action="store_true", help="Write `data/silver/metrobikeatlas.db` for scalable reads.")
    # Optional external metro station fallback for cases where TDX metro endpoints are unavailable (404).
    parser.add_argument("--external-metro-stations-csv", default="data/external/metro_stations.csv")
    # Optional external datasets for policy/storytelling features.
    parser.add_argument("--external-calendar-csv", default="data/external/calendar.csv")
    parser.add_argument("--external-weather-hourly-csv", default="data/external/weather_hourly.csv")
    parser.add_argument(
        "--prefer-external-metro",
        action="store_true",
        help="Use external metro station CSV even if TDX metro Bronze exists.",
    )
    # Parse CLI arguments once at startup to keep control flow deterministic.
    args = parser.parse_args()

    build_id = uuid.uuid4().hex
    started_at = datetime.now(timezone.utc)
    _emit_event(build_id=build_id, stage="starting", progress_pct=0, message="build_silver started")

    # Load typed config to obtain the city lists and spatial settings (buffer radius / nearest K).
    config = load_config()
    # Resolve Bronze/Silver directories from CLI args.
    bronze_dir = Path(args.bronze_dir)
    silver_dir = Path(args.silver_dir)
    # Ensure the Silver output directory exists before writing CSVs.
    silver_dir.mkdir(parents=True, exist_ok=True)

    # Metro stations
    # Preferred source is TDX Bronze; fallback is `data/external/metro_stations.csv`.
    metro_df: pd.DataFrame
    metro_bronze_ok = False
    metro_source = "tdx_bronze"
    metro_station_inputs: list[dict[str, object]] = []
    external_metro_path: Path | None = None
    external_metro_row_count: int | None = None
    if not args.prefer_external_metro:
        try:
            metro_rows: list[dict[str, Any]] = []
            # Iterate configured metro cities so the pipeline is config-driven and reproducible.
            for city in config.tdx.metro.cities:
                city_dir = bronze_dir / "tdx" / "metro" / "stations" / f"city={city}"
                latest = _latest_file(city_dir)
                try:
                    st = latest.stat()
                    metro_station_inputs.append(
                        {
                            "city": city,
                            "path": str(latest),
                            "mtime_utc": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat(),
                            "size_bytes": int(st.st_size),
                        }
                    )
                except Exception:
                    metro_station_inputs.append({"city": city, "path": str(latest)})
                bronze = read_bronze_json(latest)
                payload = bronze["payload"]
                for item in payload:
                    metro_rows.append(asdict(TDXMetroClient.parse_station(item, city=city)))

            metro_df = pd.DataFrame(metro_rows)
            metro_bronze_ok = {"station_id", "name", "lat", "lon", "city", "system"} <= set(metro_df.columns)
        except FileNotFoundError:
            metro_bronze_ok = False
        except Exception:
            metro_bronze_ok = False

    if not metro_bronze_ok:
        metro_source = "external_csv"
        external_path = Path(args.external_metro_stations_csv)
        external_metro_path = external_path
        if not external_path.exists():
            raise FileNotFoundError(
                "Missing metro stations source. Either collect TDX metro station Bronze files, "
                f"or provide an external CSV at {external_path}."
            )
        metro_df = load_external_metro_stations_csv(external_path)
        external_metro_row_count = int(len(metro_df))
        issues = validate_external_metro_stations_df(metro_df)
        errors = [i for i in issues if i.level == "error"]
        if errors:
            raise ValueError("Invalid external metro stations CSV: " + "; ".join(i.message for i in errors))
    # Silver file path uses a stable name so downstream code can locate it without scanning directories.
    metro_out = silver_dir / "metro_stations.csv"
    # Write without index to keep the schema clean and portable across tools.
    metro_df.to_csv(metro_out, index=False)
    # Print output path so logs show what was produced.
    print(f"Wrote {metro_out}")
    _emit_event(build_id=build_id, stage="metro_stations", progress_pct=20, artifacts=[metro_out])

    # Bike stations (latest per city)
    # Same pattern as metro stations: read latest Bronze snapshot per city and normalize to `BikeStation`.
    bike_rows: list[dict[str, Any]] = []
    bike_station_inputs: list[dict[str, object]] = []
    for city in config.tdx.bike.cities:
        city_dir = bronze_dir / "tdx" / "bike" / "stations" / f"city={city}"
        latest = _latest_file(city_dir)
        try:
            st = latest.stat()
            bike_station_inputs.append(
                {
                    "city": city,
                    "path": str(latest),
                    "mtime_utc": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat(),
                    "size_bytes": int(st.st_size),
                }
            )
        except Exception:
            bike_station_inputs.append({"city": city, "path": str(latest)})
        bronze = read_bronze_json(latest)
        payload = bronze["payload"]
        for item in payload:
            bike_rows.append(asdict(TDXBikeClient.parse_station(item, city=city)))

    # Build a DataFrame for bike station metadata (used for map overlays and spatial joins).
    bike_df = pd.DataFrame(bike_rows)
    # Write a stable Silver CSV name.
    bike_out = silver_dir / "bike_stations.csv"
    bike_df.to_csv(bike_out, index=False)
    print(f"Wrote {bike_out}")
    _emit_event(build_id=build_id, stage="bike_stations", progress_pct=40, artifacts=[bike_out])

    # Bike availability snapshots (all files; capped)
    # Availability is time-varying, so we read multiple Bronze snapshot files to form a time series.
    availability_rows: list[dict[str, Any]] = []
    availability_inputs: list[dict[str, object]] = []
    for city in config.tdx.bike.cities:
        city_dir = bronze_dir / "tdx" / "bike" / "availability" / f"city={city}"
        # Cap to the most recent N files so long-running collections don't blow up memory/time.
        files = sorted(city_dir.glob("*.json"))[-args.max_availability_files :]
        if files:
            latest = files[-1]
            try:
                st = latest.stat()
                availability_inputs.append(
                    {
                        "city": city,
                        "file_count_used": int(len(files)),
                        "latest_path": str(latest),
                        "latest_mtime_utc": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat(),
                        "latest_size_bytes": int(st.st_size),
                    }
                )
            except Exception:
                availability_inputs.append(
                    {"city": city, "file_count_used": int(len(files)), "latest_path": str(latest)}
                )
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
        _emit_event(build_id=build_id, stage="bike_timeseries", progress_pct=60, artifacts=[ts_out])

        # Optional: write daily partitioned files for long-run scaling (repository can read only needed days).
        try:
            parts_dir = silver_dir / "bike_timeseries_parts"
            parts_dir.mkdir(parents=True, exist_ok=True)
            df = availability_df.copy()
            df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
            df = df.dropna(subset=["ts"])
            df["day"] = df["ts"].dt.strftime("%Y-%m-%d")
            for day, g in df.groupby("day"):
                out = parts_dir / f"{day}.csv"
                g.drop(columns=["day"]).to_csv(out, index=False)
        except Exception:
            # Best-effort: keep the primary monolithic CSV as the source of truth.
            pass
    else:
        # This message explains why later steps that depend on bike time series may show empty curves.
        print("No bike availability data found; skipping bike_timeseries.csv")
        _emit_event(
            build_id=build_id,
            stage="bike_timeseries",
            progress_pct=60,
            message="No bike availability data found; skipping bike_timeseries.csv",
        )

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
    _emit_event(build_id=build_id, stage="links", progress_pct=90, artifacts=[links_out])

    # External datasets (optional) → Silver dims/facts.
    external_sources: dict[str, object] = {}

    cal_path = Path(args.external_calendar_csv)
    if cal_path.exists():
        cal_df = load_external_calendar_csv(cal_path)
        issues = validate_external_calendar_df(cal_df)
        errors = [i for i in issues if i.level == "error"]
        if errors:
            raise ValueError("Invalid external calendar CSV: " + "; ".join(i.message for i in errors))
        cal_out = silver_dir / "calendar.csv"
        cal_df.to_csv(cal_out, index=False)
        print(f"Wrote {cal_out}")
        try:
            st = cal_path.stat()
            external_sources["calendar_csv"] = {
                "path": str(cal_path),
                "exists": True,
                "mtime_utc": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat(),
                "size_bytes": int(st.st_size),
                "row_count": int(len(cal_df)),
            }
        except Exception:
            external_sources["calendar_csv"] = {"path": str(cal_path), "exists": True, "row_count": int(len(cal_df))}

    w_path = Path(args.external_weather_hourly_csv)
    if w_path.exists():
        w_df = load_external_weather_hourly_csv(w_path)
        issues = validate_external_weather_hourly_df(w_df)
        errors = [i for i in issues if i.level == "error"]
        if errors:
            raise ValueError("Invalid external weather hourly CSV: " + "; ".join(i.message for i in errors))
        w_out = silver_dir / "weather_hourly.csv"
        w_df.to_csv(w_out, index=False)
        print(f"Wrote {w_out}")
        try:
            st = w_path.stat()
            external_sources["weather_hourly_csv"] = {
                "path": str(w_path),
                "exists": True,
                "mtime_utc": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat(),
                "size_bytes": int(st.st_size),
                "row_count": int(len(w_df)),
            }
        except Exception:
            external_sources["weather_hourly_csv"] = {"path": str(w_path), "exists": True, "row_count": int(len(w_df))}

    if args.write_sqlite:
        try:
            db_path = silver_dir / "metrobikeatlas.db"
            if db_path.exists():
                db_path.unlink()
            conn = sqlite3.connect(str(db_path))
            try:
                # Write only the heavy table for now (bike_timeseries). Others remain CSV.
                if availability_rows:
                    availability_df2 = availability_df.copy()
                    availability_df2["ts"] = pd.to_datetime(availability_df2["ts"], utc=True, errors="coerce")
                    availability_df2 = availability_df2.dropna(subset=["ts"])
                    availability_df2["ts"] = availability_df2["ts"].dt.strftime("%Y-%m-%dT%H:%M:%S%z")
                    availability_df2.to_sql("bike_timeseries", conn, if_exists="replace", index=False)
                    conn.execute("CREATE INDEX IF NOT EXISTS idx_bike_ts_station_ts ON bike_timeseries(station_id, ts)")
                    conn.commit()
            finally:
                conn.close()
            print(f"Wrote {db_path}")
        except Exception:
            print("Failed to write SQLite store; continuing with CSV outputs.")

    external_csv_meta: dict[str, object] | None = None
    if external_metro_path is not None:
        external_csv_meta = {"path": str(external_metro_path), "exists": bool(external_metro_path.exists())}
        if external_metro_path.exists():
            try:
                st = external_metro_path.stat()
                external_csv_meta.update(
                    {
                        "mtime_utc": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat(),
                        "size_bytes": int(st.st_size),
                        "row_count": external_metro_row_count,
                    }
                )
            except Exception:
                pass

    inputs: dict[str, object] = {
        "args": {
            "bronze_dir": str(args.bronze_dir),
            "silver_dir": str(args.silver_dir),
            "max_availability_files": int(args.max_availability_files),
            "write_sqlite": bool(args.write_sqlite),
            "external_metro_stations_csv": str(args.external_metro_stations_csv),
            "prefer_external_metro": bool(args.prefer_external_metro),
            "external_calendar_csv": str(args.external_calendar_csv),
            "external_weather_hourly_csv": str(args.external_weather_hourly_csv),
        },
        "sources": {
            "metro": metro_source,
            "external_metro_csv": external_csv_meta,
            **external_sources,
            "metro_bronze_latest_by_city": metro_station_inputs,
            "bike_stations_latest_by_city": bike_station_inputs,
            "bike_availability_summary_by_city": availability_inputs,
        },
    }
    inputs_hash = hashlib.sha256(json.dumps(inputs, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()

    finished_at = datetime.now(timezone.utc)
    build_meta = {
        "type": "silver_build_meta",
        "build_id": build_id,
        "started_at_utc": started_at.isoformat(),
        "finished_at_utc": finished_at.isoformat(),
        "duration_s": float((finished_at - started_at).total_seconds()),
        "inputs_hash": inputs_hash,
        "inputs": inputs,
        "artifacts": [
            _artifact_status(metro_out),
            _artifact_status(bike_out),
            _artifact_status(links_out),
            _artifact_status(silver_dir / "bike_timeseries.csv"),
            _artifact_status(silver_dir / "metro_timeseries.csv"),
            _artifact_status(silver_dir / "calendar.csv"),
            _artifact_status(silver_dir / "weather_hourly.csv"),
            _artifact_status(silver_dir / "metrobikeatlas.db"),
        ],
    }
    meta_out = silver_dir / "_build_meta.json"
    tmp = meta_out.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(build_meta, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(meta_out)
    print(f"Wrote {meta_out}")

    # Replayable schema contract for Silver tables.
    schema_meta = compute_schema_meta(silver_dir)
    schema_out = silver_dir / "_schema_meta.json"
    write_json(schema_out, schema_meta)
    print(f"Wrote {schema_out}")

    _emit_event(
        build_id=build_id,
        stage="done",
        progress_pct=100,
        message="build_silver completed",
        artifacts=[
            metro_out,
            bike_out,
            links_out,
            silver_dir / "bike_timeseries.csv",
            silver_dir / "metro_timeseries.csv",
            silver_dir / "calendar.csv",
            silver_dir / "weather_hourly.csv",
            silver_dir / "metrobikeatlas.db",
            meta_out,
            schema_out,
        ],
    )


if __name__ == "__main__":
    # Guard to prevent accidental execution when imported by tests or other scripts.
    try:
        main()
    except Exception as exc:
        _emit_event(stage="error", progress_pct=0, level="error", message=str(exc))
        raise
