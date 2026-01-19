from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class ExternalCsvIssue:
    level: str  # "error" | "warning"
    message: str


def load_external_metro_stations_csv(path: Path) -> pd.DataFrame:
    """
    Load an external metro station CSV as a normalized DataFrame.

    Required columns:
    - station_id
    - name
    - lat
    - lon

    Optional columns:
    - city
    - system
    """

    df = pd.read_csv(Path(path), dtype={"station_id": str, "name": str})
    # Normalize column names
    df.columns = [str(c).strip() for c in df.columns]

    required = {"station_id", "name", "lat", "lon"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in external metro stations CSV: {sorted(missing)}")

    df = df.copy()
    df["station_id"] = df["station_id"].astype(str)
    df["name"] = df["name"].astype(str)
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")

    if "city" not in df.columns:
        df["city"] = "External"
    if "system" not in df.columns:
        df["system"] = "EXTERNAL"

    df["city"] = df["city"].astype(str)
    df["system"] = df["system"].astype(str)

    return df[["station_id", "name", "lat", "lon", "city", "system"]].copy()


def validate_external_metro_stations_df(df: pd.DataFrame) -> list[ExternalCsvIssue]:
    issues: list[ExternalCsvIssue] = []
    required = {"station_id", "name", "lat", "lon"}
    missing = required - set(df.columns)
    if missing:
        issues.append(ExternalCsvIssue("error", f"Missing columns: {sorted(missing)}"))
        return issues

    station_id = df["station_id"].astype(str)
    if (station_id.str.strip() == "").any():
        issues.append(ExternalCsvIssue("error", "Empty station_id values found."))
    dup = int(station_id.duplicated().sum())
    if dup:
        issues.append(ExternalCsvIssue("warning", f"Duplicate station_id rows: {dup}"))

    lat = pd.to_numeric(df["lat"], errors="coerce")
    lon = pd.to_numeric(df["lon"], errors="coerce")
    if lat.isna().any() or lon.isna().any():
        issues.append(ExternalCsvIssue("error", "Non-numeric lat/lon values found."))
    if ((lat < -90) | (lat > 90)).any():
        issues.append(ExternalCsvIssue("error", "Out-of-range lat values found."))
    if ((lon < -180) | (lon > 180)).any():
        issues.append(ExternalCsvIssue("error", "Out-of-range lon values found."))

    return issues

