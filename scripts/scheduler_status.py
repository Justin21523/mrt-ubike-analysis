from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path


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
    p = argparse.ArgumentParser(description="Scheduler health summary for ops/docker healthchecks.")
    p.add_argument("--repo-root", default=".")
    p.add_argument("--heartbeat-stale-seconds", type=int, default=180)
    p.add_argument("--warn-stale-seconds", type=int, default=120)
    p.add_argument("--json", action="store_true", help="Emit JSON only.")
    args = p.parse_args()

    repo_root = Path(args.repo_root).resolve()
    hb_path = repo_root / "logs" / "scheduler_heartbeat.json"
    state_path = repo_root / "logs" / "scheduler_state.json"

    hb = _read_json(hb_path) or {}
    state = _read_json(state_path) or {}

    now = datetime.now(timezone.utc)
    hb_ts = _parse_ts(hb.get("ts_utc"))
    age_s = None if hb_ts is None else max((now - hb_ts).total_seconds(), 0.0)
    last_error = str(hb.get("last_error") or "") or None

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

    if last_error and level != "critical":
        # Scheduler can keep running despite transient errors; treat as warn unless heartbeat is stale.
        level = "warn"
        reasons.append(f"last_error={last_error}")

    summary = {
        "ts_utc": now.isoformat(),
        "level": level,
        "reasons": reasons,
        "heartbeat": {"path": str(hb_path), "age_s": age_s, "last_error": last_error, "last_action": hb.get("last_action")},
        "state": state,
        "silver_build_id": hb.get("silver_build_id"),
        "silver_inputs_hash": hb.get("silver_inputs_hash"),
    }

    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        print(f"scheduler_status level={level} reasons={','.join(reasons) if reasons else '-'}")
        if age_s is not None:
            print(f"heartbeat_age_s={int(age_s)} last_action={hb.get('last_action')}")
        if last_error:
            print(f"last_error={last_error}")
    return 0 if level == "ok" else 2 if level == "critical" else 1


if __name__ == "__main__":
    raise SystemExit(main())

