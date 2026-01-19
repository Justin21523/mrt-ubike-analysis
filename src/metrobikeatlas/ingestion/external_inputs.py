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


def load_external_calendar_csv(path: Path) -> pd.DataFrame:
    """
    Load an external calendar CSV.

    Required columns:
    - date (YYYY-MM-DD)
    - is_holiday (0/1 or true/false)

    Optional columns:
    - name (holiday/event name)
    """

    df = pd.read_csv(Path(path), dtype={"date": str})
    df.columns = [str(c).strip() for c in df.columns]

    required = {"date", "is_holiday"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in external calendar CSV: {sorted(missing)}")

    out = df.copy()
    out["date"] = out["date"].astype(str).str.strip()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.strftime("%Y-%m-%d")

    v = out["is_holiday"]
    if v.dtype == bool:
        out["is_holiday"] = v.astype(int)
    else:
        s = v.astype(str).str.strip().str.lower()
        out["is_holiday"] = s.isin({"1", "true", "yes", "y", "on"}).astype(int)

    if "name" not in out.columns:
        out["name"] = ""
    out["name"] = out["name"].fillna("").astype(str)
    return out[["date", "is_holiday", "name"]].copy()


def validate_external_calendar_df(df: pd.DataFrame) -> list[ExternalCsvIssue]:
    issues: list[ExternalCsvIssue] = []
    required = {"date", "is_holiday"}
    missing = required - set(df.columns)
    if missing:
        issues.append(ExternalCsvIssue("error", f"Missing columns: {sorted(missing)}"))
        return issues

    date = pd.to_datetime(df["date"], errors="coerce")
    if date.isna().any():
        issues.append(ExternalCsvIssue("error", "Invalid `date` values found (expected YYYY-MM-DD)."))
    dup = int(pd.Series(df["date"].astype(str)).duplicated().sum())
    if dup:
        issues.append(ExternalCsvIssue("warning", f"Duplicate date rows: {dup}"))

    is_holiday = pd.to_numeric(df["is_holiday"], errors="coerce")
    if is_holiday.isna().any():
        issues.append(ExternalCsvIssue("error", "Non-numeric `is_holiday` values found."))
    if ((is_holiday < 0) | (is_holiday > 1)).any():
        issues.append(ExternalCsvIssue("warning", "`is_holiday` values outside {0,1} found (will be treated as truthy)."))
    return issues


def load_external_weather_hourly_csv(path: Path) -> pd.DataFrame:
    """
    Load an external hourly weather CSV.

    Required columns:
    - ts (timestamp; ISO8601 recommended)
    - city
    - temp_c
    - precip_mm
    """

    df = pd.read_csv(Path(path), dtype={"city": str})
    df.columns = [str(c).strip() for c in df.columns]

    # Allow common column synonyms so users can drop in datasets without rewriting.
    rename: dict[str, str] = {}
    if "ts" not in df.columns and "timestamp" in df.columns:
        rename["timestamp"] = "ts"
    if "temp_c" not in df.columns and "temperature_c" in df.columns:
        rename["temperature_c"] = "temp_c"
    if "precip_mm" not in df.columns and "rain_mm" in df.columns:
        rename["rain_mm"] = "precip_mm"
    if "humidity_pct" not in df.columns and "humidity" in df.columns:
        rename["humidity"] = "humidity_pct"
    if rename:
        df = df.rename(columns=rename)

    required = {"ts", "city", "temp_c", "precip_mm"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in external weather hourly CSV: {sorted(missing)}")

    out = df.copy()
    out["city"] = out["city"].astype(str)
    out["ts"] = pd.to_datetime(out["ts"], utc=True, errors="coerce")
    out = out.dropna(subset=["ts"])
    out["ts"] = out["ts"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    out["temp_c"] = pd.to_numeric(out["temp_c"], errors="coerce")
    out["precip_mm"] = pd.to_numeric(out["precip_mm"], errors="coerce")
    if "humidity_pct" in out.columns:
        out["humidity_pct"] = pd.to_numeric(out["humidity_pct"], errors="coerce")
    else:
        out["humidity_pct"] = pd.NA

    cols = ["ts", "city", "temp_c", "precip_mm", "humidity_pct"]
    return out[cols].copy()


def validate_external_weather_hourly_df(df: pd.DataFrame) -> list[ExternalCsvIssue]:
    issues: list[ExternalCsvIssue] = []
    required = {"ts", "city", "temp_c", "precip_mm"}
    missing = required - set(df.columns)
    if missing:
        issues.append(ExternalCsvIssue("error", f"Missing columns: {sorted(missing)}"))
        return issues

    ts = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    if ts.isna().any():
        issues.append(ExternalCsvIssue("error", "Invalid `ts` values found (expected timestamp)."))
    city = df["city"].astype(str)
    if (city.str.strip() == "").any():
        issues.append(ExternalCsvIssue("error", "Empty `city` values found."))

    for c in ["temp_c", "precip_mm"]:
        v = pd.to_numeric(df[c], errors="coerce")
        if v.isna().any():
            issues.append(ExternalCsvIssue("error", f"Non-numeric `{c}` values found."))
    return issues
