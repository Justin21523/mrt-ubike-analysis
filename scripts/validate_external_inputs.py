from __future__ import annotations

# Allow running scripts without requiring an editable install (`pip install -e .`).
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_PATH))

import argparse

from metrobikeatlas.ingestion.external_inputs import load_external_metro_stations_csv, validate_external_metro_stations_df


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate external input CSVs (no network calls).")
    parser.add_argument(
        "--metro-stations-csv",
        default="data/external/metro_stations.csv",
        help="Path to external metro station CSV (fallback for TDX metro stations).",
    )
    parser.add_argument("--strict", action="store_true", help="Exit non-zero if any errors are found.")
    args = parser.parse_args()

    path = Path(args.metro_stations_csv)
    if not path.exists():
        raise FileNotFoundError(path)

    df = load_external_metro_stations_csv(path)
    issues = validate_external_metro_stations_df(df)
    errors = [i for i in issues if i.level == "error"]
    warnings = [i for i in issues if i.level == "warning"]

    for i in issues:
        print(f"[{i.level}] {i.message}")

    print(f"done: errors={len(errors)} warnings={len(warnings)} rows={len(df)}")
    if args.strict and errors:
        raise SystemExit(2)


if __name__ == "__main__":
    main()

