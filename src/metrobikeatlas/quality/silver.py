from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pandas as pd


logger = logging.getLogger(__name__)

IssueLevel = Literal["error", "warning"]


@dataclass(frozen=True)
class ValidationIssue:
    level: IssueLevel
    table: str
    message: str


def validate_silver_dir(silver_dir: Path, *, strict: bool = True) -> list[ValidationIssue]:
    """
    Validate the Silver layer for basic schema and sanity checks.

    Returns a list of issues. If `strict=True`, raises ValueError when errors are present.
    """

    issues: list[ValidationIssue] = []
    silver_dir = Path(silver_dir)

    required_files = ["metro_stations.csv", "bike_stations.csv", "metro_bike_links.csv"]
    for filename in required_files:
        path = silver_dir / filename
        if not path.exists():
            issues.append(ValidationIssue("error", filename, f"Missing required file: {path}"))

    def _require_columns(df: pd.DataFrame, *, table: str, required: set[str]) -> bool:
        missing = required - set(df.columns)
        if missing:
            issues.append(ValidationIssue("error", table, f"Missing columns: {sorted(missing)}"))
            return False
        return True

    def _read_csv(name: str, *, parse_dates: list[str] | None = None) -> pd.DataFrame | None:
        path = silver_dir / name
        if not path.exists():
            return None
        return pd.read_csv(path, parse_dates=parse_dates)

    metro = _read_csv("metro_stations.csv")
    if metro is not None and _require_columns(
        metro, table="metro_stations.csv", required={"station_id", "name", "lat", "lon", "city", "system"}
    ):
        metro["station_id"] = metro["station_id"].astype(str)
        if (metro["station_id"].str.strip() == "").any():
            issues.append(ValidationIssue("error", "metro_stations.csv", "Empty station_id values found."))
        dup = int(metro["station_id"].duplicated().sum())
        if dup:
            issues.append(ValidationIssue("warning", "metro_stations.csv", f"Duplicate station_id rows: {dup}"))
        for col, lo, hi in (("lat", -90.0, 90.0), ("lon", -180.0, 180.0)):
            values = pd.to_numeric(metro[col], errors="coerce")
            if values.isna().any():
                issues.append(ValidationIssue("warning", "metro_stations.csv", f"Non-numeric {col} values found."))
            if ((values < lo) | (values > hi)).any():
                issues.append(ValidationIssue("warning", "metro_stations.csv", f"Out-of-range {col} values found."))

    bike = _read_csv("bike_stations.csv")
    if bike is not None and _require_columns(
        bike, table="bike_stations.csv", required={"station_id", "name", "lat", "lon", "city"}
    ):
        bike["station_id"] = bike["station_id"].astype(str)
        if (bike["station_id"].str.strip() == "").any():
            issues.append(ValidationIssue("error", "bike_stations.csv", "Empty station_id values found."))
        dup = int(bike["station_id"].duplicated().sum())
        if dup:
            issues.append(ValidationIssue("warning", "bike_stations.csv", f"Duplicate station_id rows: {dup}"))

    links = _read_csv("metro_bike_links.csv")
    if links is not None and _require_columns(
        links, table="metro_bike_links.csv", required={"metro_station_id", "bike_station_id", "distance_m"}
    ):
        links["metro_station_id"] = links["metro_station_id"].astype(str)
        links["bike_station_id"] = links["bike_station_id"].astype(str)
        dist = pd.to_numeric(links["distance_m"], errors="coerce")
        if dist.isna().any():
            issues.append(ValidationIssue("warning", "metro_bike_links.csv", "Non-numeric distance_m values found."))
        if (dist < 0).any():
            issues.append(ValidationIssue("warning", "metro_bike_links.csv", "Negative distance_m values found."))

        if metro is not None and "station_id" in metro.columns:
            known_metro = set(metro["station_id"].astype(str))
            missing_metro = links.loc[~links["metro_station_id"].isin(known_metro), "metro_station_id"].unique()
            if len(missing_metro):
                issues.append(
                    ValidationIssue(
                        "warning",
                        "metro_bike_links.csv",
                        f"Links reference unknown metro station_id values: {len(missing_metro)}",
                    )
                )
        if bike is not None and "station_id" in bike.columns:
            known_bike = set(bike["station_id"].astype(str))
            missing_bike = links.loc[~links["bike_station_id"].isin(known_bike), "bike_station_id"].unique()
            if len(missing_bike):
                issues.append(
                    ValidationIssue(
                        "warning",
                        "metro_bike_links.csv",
                        f"Links reference unknown bike station_id values: {len(missing_bike)}",
                    )
                )

    bike_ts = _read_csv("bike_timeseries.csv", parse_dates=["ts"])
    if bike_ts is None:
        issues.append(ValidationIssue("warning", "bike_timeseries.csv", "Missing bike_timeseries.csv (no snapshots?)."))
    else:
        _require_columns(bike_ts, table="bike_timeseries.csv", required={"station_id", "ts", "available_bikes"})
        neg = pd.to_numeric(bike_ts.get("available_bikes"), errors="coerce")
        if (neg < 0).any():
            issues.append(ValidationIssue("warning", "bike_timeseries.csv", "Negative available_bikes values found."))

    metro_ts = _read_csv("metro_timeseries.csv", parse_dates=["ts"])
    if metro_ts is not None:
        _require_columns(metro_ts, table="metro_timeseries.csv", required={"station_id", "ts", "value"})
        val = pd.to_numeric(metro_ts.get("value"), errors="coerce")
        if (val < 0).any():
            issues.append(ValidationIssue("warning", "metro_timeseries.csv", "Negative ridership values found."))

    errors = [i for i in issues if i.level == "error"]
    for issue in issues:
        log_fn = logger.error if issue.level == "error" else logger.warning
        log_fn("[%s] %s - %s", issue.level, issue.table, issue.message)

    if strict and errors:
        raise ValueError(f"Silver validation failed with {len(errors)} error(s).")

    return issues

