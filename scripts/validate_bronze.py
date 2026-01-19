from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

from metrobikeatlas.quality.contract import write_json


def main() -> None:
    p = argparse.ArgumentParser(description="Validate Bronze lake structure and basic parseability (no network).")
    p.add_argument("--bronze-dir", default="data/bronze")
    p.add_argument("--out", default="logs/dq/latest.json")
    p.add_argument("--strict", action="store_true")
    args = p.parse_args()

    bronze_dir = Path(args.bronze_dir)
    issues: list[dict[str, object]] = []

    def add(level: str, dataset: str, message: str) -> None:
        issues.append({"level": level, "dataset": dataset, "message": message})

    expected = [
        ("tdx:bike:stations", bronze_dir / "tdx" / "bike" / "stations"),
        ("tdx:bike:availability", bronze_dir / "tdx" / "bike" / "availability"),
        ("tdx:metro:stations", bronze_dir / "tdx" / "metro" / "stations"),
    ]

    for label, root in expected:
        if not root.exists():
            add("warning", label, f"Missing dataset dir: {root}")
            continue
        files = sorted(root.rglob("*.json"))
        if not files:
            add("warning", label, "No JSON files found")
            continue
        latest = files[-1]
        try:
            obj = json.loads(latest.read_text(encoding="utf-8"))
        except Exception as e:
            add("error", label, f"Failed to parse latest file {latest}: {e}")
            continue
        if not isinstance(obj, dict) or "payload" not in obj:
            add("error", label, f"Unexpected Bronze wrapper shape in {latest}")
            continue
        payload = obj.get("payload")
        if not isinstance(payload, list):
            add("warning", label, f"Payload is not a list in {latest}")
        if isinstance(payload, list) and not payload:
            add("warning", label, f"Empty payload in {latest}")

    report = {
        "type": "dq_report",
        "scope": "bronze",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "bronze_dir": str(bronze_dir),
        "issues": issues,
        "ok": not any(i["level"] == "error" for i in issues),
    }
    out = Path(args.out)
    write_json(out, report)
    print(f"Wrote {out}")

    if args.strict and not report["ok"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()

