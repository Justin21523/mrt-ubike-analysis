from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys


def _read_json(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


def _parse_ts(v: object) -> datetime | None:
    if not v:
        return None
    try:
        return datetime.fromisoformat(str(v)).astimezone(timezone.utc)
    except Exception:
        return None


def main() -> int:
    p = argparse.ArgumentParser(description="Collector health summary for ops/cron/docker healthchecks.")
    p.add_argument("--repo-root", default=".")
    p.add_argument("--heartbeat-stale-seconds", type=int, default=900)
    p.add_argument("--warn-stale-seconds", type=int, default=600)
    p.add_argument("--min-ok-snapshots", type=int, default=1)
    p.add_argument("--json", action="store_true", help="Emit JSON only.")
    args = p.parse_args()

    repo_root = Path(args.repo_root).resolve()
    hb_path = repo_root / "logs" / "collector_heartbeat.json"
    metrics_path = repo_root / "logs" / "collector_metrics.json"

    hb = _read_json(hb_path) or {}
    metrics = _read_json(metrics_path) or {}

    now = datetime.now(timezone.utc)
    hb_ts = _parse_ts(hb.get("ts_utc"))
    age_s = None if hb_ts is None else max((now - hb_ts).total_seconds(), 0.0)

    ok_snapshots = int(hb.get("ok_snapshots") or 0)
    disk = hb.get("disk") if isinstance(hb.get("disk"), dict) else {}
    disk_action = (disk.get("action") if isinstance(disk, dict) else None) or "unknown"

    level = "ok"
    reasons: list[str] = []
    if hb_ts is None:
        level = "critical"
        reasons.append("heartbeat_missing")
    elif age_s is not None and age_s >= max(int(args.heartbeat_stale_seconds), 1):
        level = "critical"
        reasons.append(f"heartbeat_stale({int(age_s)}s)")
    elif age_s is not None and age_s >= max(int(args.warn_stale_seconds), 1):
        level = "warn"
        reasons.append(f"heartbeat_old({int(age_s)}s)")

    if disk_action == "low_disk":
        level = "critical"
        reasons.append("low_disk")
    elif disk_action == "bronze_too_big" and level != "critical":
        level = "warn"
        reasons.append("bronze_too_big")

    if ok_snapshots < max(int(args.min_ok_snapshots), 0) and level != "critical":
        level = "warn"
        reasons.append(f"ok_snapshots<{int(args.min_ok_snapshots)}")

    datasets = hb.get("datasets") if isinstance(hb.get("datasets"), dict) else {}
    if isinstance(datasets, dict):
        # If availability keeps erroring, treat as warn (critical is handled via heartbeat staleness).
        avail = datasets.get("tdx:bike:availability") if isinstance(datasets.get("tdx:bike:availability"), dict) else None
        if isinstance(avail, dict) and int(avail.get("error") or 0) >= 3 and level == "ok":
            level = "warn"
            reasons.append("availability_errors")

    summary = {
        "ts_utc": now.isoformat(),
        "level": level,
        "reasons": reasons,
        "heartbeat": {"path": str(hb_path), "age_s": age_s, "ok_snapshots": ok_snapshots},
        "disk": disk,
        "datasets": datasets,
        "metrics": metrics,
    }

    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        print(f"collector_status level={level} reasons={','.join(reasons) if reasons else '-'}")
        if age_s is not None:
            print(f"heartbeat_age_s={int(age_s)} ok_snapshots={ok_snapshots}")
        if isinstance(disk, dict):
            free = disk.get("disk_free_bytes")
            bronze = disk.get("bronze_bytes_estimate")
            print(f"disk_action={disk_action} free_bytes={free} bronze_bytes={bronze}")
    return 0 if level == "ok" else 2 if level == "critical" else 1


if __name__ == "__main__":
    raise SystemExit(main())

