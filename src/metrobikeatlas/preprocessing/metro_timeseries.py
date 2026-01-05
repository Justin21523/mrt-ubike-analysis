from __future__ import annotations

from typing import Literal, Optional

import pandas as pd


DedupAgg = Literal["sum", "mean"]


def normalize_metro_timeseries(
    df: pd.DataFrame,
    *,
    station_id_col: str = "station_id",
    ts_col: str = "ts",
    value_col: str = "value",
    ts_format: Optional[str] = None,
    ts_unit: Optional[str] = None,
    input_timezone: str = "Asia/Taipei",
    output_timezone: str = "Asia/Taipei",
    deduplicate: bool = True,
    dedup_agg: DedupAgg = "sum",
) -> pd.DataFrame:
    """
    Normalize an external metro ridership/flow dataset to the Silver schema:

    Output columns: `station_id` (str), `ts` (tz-aware), `value` (float).
    """

    missing = [c for c in (station_id_col, ts_col, value_col) if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    out = df[[station_id_col, ts_col, value_col]].copy()
    out = out.rename(columns={station_id_col: "station_id", ts_col: "ts", value_col: "value"})

    out["station_id"] = out["station_id"].astype(str).str.strip()
    out = out[out["station_id"] != ""]
    out = out[out["station_id"].str.lower() != "nan"]

    out["value"] = pd.to_numeric(out["value"], errors="coerce")

    if ts_unit is not None:
        out["ts"] = pd.to_datetime(out["ts"], errors="coerce", unit=ts_unit, utc=True)
    else:
        out["ts"] = pd.to_datetime(out["ts"], errors="coerce", format=ts_format)
        if out["ts"].dt.tz is None:
            out["ts"] = out["ts"].dt.tz_localize(input_timezone)

    out = out.dropna(subset=["ts", "value"])
    out["ts"] = out["ts"].dt.tz_convert(output_timezone)

    if deduplicate:
        if dedup_agg not in ("sum", "mean"):
            raise ValueError(f"Unsupported dedup_agg: {dedup_agg}")
        out = out.groupby(["station_id", "ts"], as_index=False)["value"].agg(dedup_agg)

    return out.sort_values(["station_id", "ts"]).reset_index(drop=True)

