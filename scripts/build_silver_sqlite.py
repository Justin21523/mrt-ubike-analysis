from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

import pandas as pd


def _ensure_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bike_timeseries (
          station_id TEXT NOT NULL,
          ts TEXT NOT NULL,
          city TEXT,
          available_bikes INTEGER,
          available_docks INTEGER,
          rent_proxy REAL,
          return_proxy REAL
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_bike_ts_station_ts ON bike_timeseries(station_id, ts)")
    conn.commit()


def _load_csv_to_table(conn: sqlite3.Connection, csv_path: Path) -> None:
    df = pd.read_csv(csv_path, parse_dates=["ts"])
    if df.empty:
        return
    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts"])
    df["ts"] = df["ts"].dt.strftime("%Y-%m-%dT%H:%M:%S%z")

    cols = ["station_id", "ts", "city", "available_bikes", "available_docks", "rent_proxy", "return_proxy"]
    present = [c for c in cols if c in df.columns]
    df = df[present].copy()
    if "station_id" in df.columns:
        df["station_id"] = df["station_id"].astype(str)

    df.to_sql("bike_timeseries", conn, if_exists="append", index=False)


def main() -> None:
    p = argparse.ArgumentParser(description="Build SQLite store from Silver CSVs (long-run query engine).")
    p.add_argument("--silver-dir", default="data/silver")
    p.add_argument("--db-path", default=None)
    p.add_argument("--replace", action="store_true", help="Replace existing DB")
    args = p.parse_args()

    silver_dir = Path(args.silver_dir)
    db_path = Path(args.db_path) if args.db_path else (silver_dir / "metrobikeatlas.db")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if args.replace and db_path.exists():
        db_path.unlink()

    conn = sqlite3.connect(str(db_path))
    try:
        _ensure_schema(conn)
        ts_path = silver_dir / "bike_timeseries.csv"
        if ts_path.exists():
            _load_csv_to_table(conn, ts_path)
    finally:
        conn.close()

    print(f"Wrote {db_path}")


if __name__ == "__main__":
    main()

