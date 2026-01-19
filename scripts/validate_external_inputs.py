from __future__ import annotations

# Allow running scripts without requiring an editable install (`pip install -e .`).
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_PATH))

import argparse

from metrobikeatlas.ingestion.external_inputs import (
    ExternalCsvIssue,
    load_external_calendar_csv,
    load_external_metro_stations_csv,
    load_external_weather_hourly_csv,
    validate_external_calendar_df,
    validate_external_metro_stations_df,
    validate_external_weather_hourly_df,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate external input CSVs (no network calls).")
    parser.add_argument(
        "--metro-stations-csv",
        default="data/external/metro_stations.csv",
        help="Path to external metro station CSV (fallback for TDX metro stations).",
    )
    parser.add_argument(
        "--require-metro-stations",
        action="store_true",
        help="Treat missing --metro-stations-csv as an error (useful for strict real-mode deployments).",
    )
    parser.add_argument(
        "--calendar-csv",
        default="data/external/calendar.csv",
        help="Optional external calendar CSV (date,is_holiday,name). If provided, it will be validated.",
    )
    parser.add_argument(
        "--require-calendar",
        action="store_true",
        help="Treat missing --calendar-csv as an error (useful when you rely on holiday effects).",
    )
    parser.add_argument(
        "--weather-hourly-csv",
        default="data/external/weather_hourly.csv",
        help="Optional external hourly weather CSV (ts,city,temp_c,precip_mm,...). If provided, it will be validated.",
    )
    parser.add_argument(
        "--require-weather-hourly",
        action="store_true",
        help="Treat missing --weather-hourly-csv as an error (useful when you rely on rain effects).",
    )
    parser.add_argument("--strict", action="store_true", help="Exit non-zero if any errors are found.")
    args = parser.parse_args()

    all_issues: list[ExternalCsvIssue] = []

    metro_path = Path(args.metro_stations_csv)
    if metro_path.exists():
        metro_df = load_external_metro_stations_csv(metro_path)
        metro_issues = validate_external_metro_stations_df(metro_df)
        all_issues.extend(metro_issues)
    elif args.require_metro_stations:
        all_issues.append(ExternalCsvIssue("error", f"Missing file: {metro_path}"))
    else:
        all_issues.append(ExternalCsvIssue("warning", f"Missing file (skipped): {metro_path}"))

    cal_path = Path(args.calendar_csv) if args.calendar_csv else None
    if cal_path is not None and cal_path.exists():
        cal_df = load_external_calendar_csv(cal_path)
        cal_issues = validate_external_calendar_df(cal_df)
        all_issues.extend(cal_issues)
    elif args.require_calendar and cal_path is not None:
        all_issues.append(ExternalCsvIssue("error", f"Missing file: {cal_path}"))
    elif cal_path is not None:
        all_issues.append(ExternalCsvIssue("warning", f"Missing file (skipped): {cal_path}"))

    w_path = Path(args.weather_hourly_csv) if args.weather_hourly_csv else None
    if w_path is not None and w_path.exists():
        w_df = load_external_weather_hourly_csv(w_path)
        w_issues = validate_external_weather_hourly_df(w_df)
        all_issues.extend(w_issues)
    elif args.require_weather_hourly and w_path is not None:
        all_issues.append(ExternalCsvIssue("error", f"Missing file: {w_path}"))
    elif w_path is not None:
        all_issues.append(ExternalCsvIssue("warning", f"Missing file (skipped): {w_path}"))

    errors = [i for i in all_issues if i.level == "error"]
    warnings = [i for i in all_issues if i.level == "warning"]

    for i in all_issues:
        print(f"[{i.level}] {i.message}")

    print(f"done: errors={len(errors)} warnings={len(warnings)}")
    if args.strict and errors:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
