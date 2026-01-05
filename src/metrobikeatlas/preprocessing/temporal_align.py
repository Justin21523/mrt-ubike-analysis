from __future__ import annotations

from typing import Iterable

import pandas as pd

from metrobikeatlas.config.models import Granularity


_FREQ_MAP: dict[Granularity, str] = {"15min": "15min", "hour": "1H", "day": "1D"}


def compute_rent_return_proxy(
    availability: pd.DataFrame,
    *,
    station_id_col: str = "station_id",
    ts_col: str = "ts",
    available_bikes_col: str = "available_bikes",
) -> pd.DataFrame:
    """
    Compute a simple rent/return proxy from availability snapshots.

    - Negative delta in available bikes => rent proxy (bike taken)
    - Positive delta => return proxy (bike docked)

    This is noisy (rebalancing, missing snapshots) but good enough for MVP exploration.
    """

    df = availability.copy()
    df = df.sort_values([station_id_col, ts_col])
    df["_delta"] = df.groupby(station_id_col)[available_bikes_col].diff()
    df["rent_proxy"] = (-df["_delta"]).clip(lower=0)
    df["return_proxy"] = (df["_delta"]).clip(lower=0)
    return df.drop(columns=["_delta"])


def align_timeseries(
    df: pd.DataFrame,
    *,
    ts_col: str = "ts",
    group_cols: Iterable[str] = (),
    value_cols: Iterable[str] = (),
    granularity: Granularity = "hour",
    timezone: str = "Asia/Taipei",
    agg: str = "mean",
) -> pd.DataFrame:
    """
    Align a time series to a fixed granularity.

    - Parses timestamps to tz-aware datetimes
    - Converts to the configured timezone
    - Resamples per group (if provided)
    """

    if granularity not in _FREQ_MAP:
        raise ValueError(f"Unsupported granularity: {granularity}")

    out = df.copy()
    out[ts_col] = pd.to_datetime(out[ts_col], utc=True, errors="coerce")
    out = out.dropna(subset=[ts_col])
    out[ts_col] = out[ts_col].dt.tz_convert(timezone)

    freq = _FREQ_MAP[granularity]
    group_cols_list = list(group_cols)
    value_cols_list = list(value_cols)
    if not value_cols_list:
        raise ValueError("value_cols must not be empty")

    agg_map = {col: agg for col in value_cols_list}

    if group_cols_list:
        return (
            out.set_index(ts_col)
            .groupby(group_cols_list)
            .resample(freq)
            .agg(agg_map)
            .reset_index()
        )

    return out.set_index(ts_col).resample(freq).agg(agg_map).reset_index()

