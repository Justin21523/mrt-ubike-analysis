from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Iterable, Literal

import pandas as pd

from metrobikeatlas.quality.silver import ValidationIssue, validate_silver_dir


IssueLevel = Literal["error", "warning", "info"]


def _sha256_file(path: Path, *, max_bytes: int | None = None) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        if max_bytes is None:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        else:
            remaining = int(max_bytes)
            while remaining > 0:
                chunk = f.read(min(1024 * 1024, remaining))
                if not chunk:
                    break
                h.update(chunk)
                remaining -= len(chunk)
    return h.hexdigest()


def compute_schema_meta(silver_dir: Path) -> dict[str, object]:
    """
    Compute a replayable schema contract for Silver artifacts.

    Output is intended to be written to `data/silver/_schema_meta.json`.
    """

    silver_dir = Path(silver_dir)
    now = datetime.now(timezone.utc)

    tables = {
        "metro_stations": silver_dir / "metro_stations.csv",
        "bike_stations": silver_dir / "bike_stations.csv",
        "metro_bike_links": silver_dir / "metro_bike_links.csv",
        "bike_timeseries": silver_dir / "bike_timeseries.csv",
        "metro_timeseries": silver_dir / "metro_timeseries.csv",
    }

    out_tables: dict[str, object] = {}
    issues: list[dict[str, object]] = []

    def _add_issue(level: IssueLevel, table: str, message: str) -> None:
        issues.append({"level": level, "table": table, "message": message})

    for name, path in tables.items():
        if not path.exists():
            _add_issue("warning", name, f"Missing file: {path}")
            continue
        st = path.stat()
        try:
            df = pd.read_csv(path, nrows=5000)
        except Exception as e:
            _add_issue("error", name, f"Failed to read CSV: {e}")
            continue

        cols = []
        for c in df.columns:
            s = df[c]
            cols.append(
                {
                    "name": str(c),
                    "dtype": str(s.dtype),
                    "nulls": int(s.isna().sum()),
                }
            )

        out_tables[name] = {
            "path": str(path),
            "exists": True,
            "size_bytes": int(st.st_size),
            "mtime_utc": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat(),
            "sha256": _sha256_file(path),
            "columns": cols,
            # Rowcount can be expensive; keep as best-effort by counting lines.
            "rowcount": max(sum(1 for _ in path.open("rb")) - 1, 0),
        }

    # Contract checks (PK/FK/nullability) for deterministic replay.
    try:
        metro = pd.read_csv(tables["metro_stations"])
        bike = pd.read_csv(tables["bike_stations"])
        links = pd.read_csv(tables["metro_bike_links"])
    except Exception:
        metro = None
        bike = None
        links = None

    checks: dict[str, object] = {}
    if isinstance(metro, pd.DataFrame) and "station_id" in metro.columns:
        s = metro["station_id"].astype(str)
        checks["metro_stations_pk_unique"] = int(s.duplicated().sum()) == 0
        checks["metro_stations_pk_duplicates"] = int(s.duplicated().sum())
    if isinstance(bike, pd.DataFrame) and "station_id" in bike.columns:
        s = bike["station_id"].astype(str)
        checks["bike_stations_pk_unique"] = int(s.duplicated().sum()) == 0
        checks["bike_stations_pk_duplicates"] = int(s.duplicated().sum())
    if isinstance(links, pd.DataFrame) and {"metro_station_id", "bike_station_id"} <= set(links.columns):
        pair = links[["metro_station_id", "bike_station_id"]].astype(str)
        dup = int(pair.duplicated().sum())
        checks["metro_bike_links_pk_unique"] = dup == 0
        checks["metro_bike_links_pk_duplicates"] = dup
        if isinstance(metro, pd.DataFrame) and "station_id" in metro.columns:
            known = set(metro["station_id"].astype(str))
            missing = int((~links["metro_station_id"].astype(str).isin(known)).sum())
            checks["links_fk_missing_metro_rows"] = missing
        if isinstance(bike, pd.DataFrame) and "station_id" in bike.columns:
            known = set(bike["station_id"].astype(str))
            missing = int((~links["bike_station_id"].astype(str).isin(known)).sum())
            checks["links_fk_missing_bike_rows"] = missing

    return {
        "type": "silver_schema_meta",
        "schema_version": 1,
        "generated_at_utc": now.isoformat(),
        "silver_dir": str(silver_dir),
        "tables": out_tables,
        "checks": checks,
        "issues": issues,
    }


def validate_silver_extended(silver_dir: Path, *, strict: bool = False) -> list[ValidationIssue]:
    """
    Extended DQ checks beyond `validate_silver_dir`.
    """

    issues = list(validate_silver_dir(Path(silver_dir), strict=False))
    silver_dir = Path(silver_dir)
    ts_path = silver_dir / "bike_timeseries.csv"
    if ts_path.exists():
        try:
            ts = pd.read_csv(ts_path, parse_dates=["ts"])
        except Exception as e:
            issues.append(ValidationIssue("error", "bike_timeseries.csv", f"Failed to read: {e}"))
        else:
            if {"station_id", "ts"} <= set(ts.columns):
                ts["station_id"] = ts["station_id"].astype(str)
                ts["ts"] = pd.to_datetime(ts["ts"], utc=True, errors="coerce")
                bad = int(ts["ts"].isna().sum())
                if bad:
                    issues.append(ValidationIssue("warning", "bike_timeseries.csv", f"Invalid ts rows: {bad}"))
                # Monotonic by station (best-effort; expensive for huge datasets, but OK as a script).
                sample = ts.sort_values(["station_id", "ts"]).groupby("station_id").head(5000)
                diffs = sample.groupby("station_id")["ts"].diff()
                nonmono = int((diffs < pd.Timedelta(0)).sum())
                if nonmono:
                    issues.append(ValidationIssue("warning", "bike_timeseries.csv", f"Non-monotonic ts samples: {nonmono}"))

    errors = [i for i in issues if i.level == "error"]
    if strict and errors:
        raise ValueError(f"Extended Silver validation failed with {len(errors)} error(s).")
    return issues


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)

