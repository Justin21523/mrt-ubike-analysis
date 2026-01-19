from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

import pandas as pd


@dataclass(frozen=True)
class OpenMeteoHourlyRow:
    ts: str  # ISO8601 UTC "YYYY-MM-DDTHH:MM:SSZ"
    city: str
    temp_c: float | None
    precip_mm: float | None
    humidity_pct: float | None


def _to_iso_z(dt: pd.Timestamp) -> str:
    if dt.tzinfo is None:
        dt = dt.tz_localize(timezone.utc)
    else:
        dt = dt.tz_convert(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_open_meteo_hourly(payload: Mapping[str, Any], *, city: str) -> pd.DataFrame:
    """
    Normalize Open-Meteo API JSON payload into the project's external weather schema:
    - ts (UTC ISO8601 "Z")
    - city
    - temp_c
    - precip_mm
    - humidity_pct

    Expected payload shape (subset):
      {
        "hourly": {
          "time": ["2026-01-01T00:00", ...],
          "temperature_2m": [...],
          "precipitation": [...],
          "relative_humidity_2m": [...]
        }
      }
    """

    hourly = payload.get("hourly")
    if not isinstance(hourly, Mapping):
        raise ValueError("Open-Meteo payload missing `hourly` object")

    times = hourly.get("time")
    if not isinstance(times, list) or not times:
        raise ValueError("Open-Meteo payload missing `hourly.time`")

    df = pd.DataFrame(
        {
            "ts": times,
            "temp_c": hourly.get("temperature_2m"),
            "precip_mm": hourly.get("precipitation"),
            "humidity_pct": hourly.get("relative_humidity_2m"),
        }
    )

    df["ts"] = pd.to_datetime(df["ts"], errors="coerce", utc=True)
    df = df.dropna(subset=["ts"]).copy()
    df["ts"] = df["ts"].map(_to_iso_z)
    df["city"] = str(city)

    for c in ["temp_c", "precip_mm", "humidity_pct"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    return df[["ts", "city", "temp_c", "precip_mm", "humidity_pct"]].copy()


def default_open_meteo_params(*, lat: float, lon: float, past_days: int) -> dict[str, str]:
    # Keep timezone in UTC so we can merge across cities consistently.
    return {
        "latitude": f"{float(lat):.6f}",
        "longitude": f"{float(lon):.6f}",
        "timezone": "UTC",
        "past_days": str(int(max(past_days, 0))),
        "hourly": "temperature_2m,precipitation,relative_humidity_2m",
    }


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

