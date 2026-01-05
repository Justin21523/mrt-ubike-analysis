from __future__ import annotations

import pandas as pd

from metrobikeatlas.preprocessing.metro_timeseries import normalize_metro_timeseries


def test_normalize_localizes_naive_timestamps() -> None:
    df = pd.DataFrame(
        [
            {"sid": "M1", "time": "2026-01-01 08:00:00", "ridership": "100"},
        ]
    )
    out = normalize_metro_timeseries(
        df,
        station_id_col="sid",
        ts_col="time",
        value_col="ridership",
        input_timezone="Asia/Taipei",
        output_timezone="Asia/Taipei",
    )

    assert out.columns.tolist() == ["station_id", "ts", "value"]
    assert out["station_id"].tolist() == ["M1"]
    assert out["value"].tolist() == [100.0]
    assert str(out["ts"].dt.tz) == "Asia/Taipei"


def test_normalize_converts_timezone_when_aware() -> None:
    df = pd.DataFrame(
        [
            {"station_id": "M1", "ts": "2026-01-01T00:00:00+00:00", "value": 1},
        ]
    )
    out = normalize_metro_timeseries(df, input_timezone="UTC", output_timezone="Asia/Taipei")
    assert str(out["ts"].dt.tz) == "Asia/Taipei"
    assert out["ts"].iloc[0].hour == 8


def test_normalize_deduplicates_by_sum() -> None:
    df = pd.DataFrame(
        [
            {"station_id": "M1", "ts": "2026-01-01T00:00:00+08:00", "value": 10},
            {"station_id": "M1", "ts": "2026-01-01T00:00:00+08:00", "value": 5},
        ]
    )
    out = normalize_metro_timeseries(df, input_timezone="Asia/Taipei", output_timezone="Asia/Taipei")
    assert len(out) == 1
    assert out.iloc[0]["value"] == 15.0


def test_normalize_drops_invalid_rows() -> None:
    df = pd.DataFrame(
        [
            {"station_id": "M1", "ts": "bad", "value": 1},
            {"station_id": "M1", "ts": "2026-01-01T00:00:00+08:00", "value": "x"},
        ]
    )
    out = normalize_metro_timeseries(df, input_timezone="Asia/Taipei", output_timezone="Asia/Taipei")
    assert out.empty


def test_normalize_supports_epoch_seconds() -> None:
    # 2026-01-01T00:00:00Z
    df = pd.DataFrame([{"station_id": "M1", "ts": 1767225600, "value": 7}])
    out = normalize_metro_timeseries(
        df,
        ts_unit="s",
        input_timezone="UTC",
        output_timezone="Asia/Taipei",
    )
    assert str(out["ts"].dt.tz) == "Asia/Taipei"
    assert out["ts"].iloc[0].hour == 8

