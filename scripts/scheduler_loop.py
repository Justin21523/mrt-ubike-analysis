from __future__ import annotations

import sys
from pathlib import Path

# Allow running scripts without requiring an editable install (`pip install -e .`).
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_PATH))

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
import os
import signal
import subprocess
import time

from metrobikeatlas.config.loader import load_config
from metrobikeatlas.utils.logging import configure_logging


logger = logging.getLogger(__name__)


def _parse_bool(val: object | None, *, default: bool = False) -> bool:
    if val is None:
        return bool(default)
    s = str(val).strip().lower()
    if s in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "f", "no", "n", "off"}:
        return False
    return bool(default)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _read_json(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _is_pid_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _acquire_lock(lock_path: Path) -> None:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    if lock_path.exists():
        try:
            existing = int(lock_path.read_text(encoding="utf-8").strip())
        except Exception:
            existing = None
        if existing and _is_pid_running(existing):
            raise RuntimeError(f"scheduler already running (lock pid={existing})")
        try:
            lock_path.unlink()
        except Exception:
            pass
    lock_path.write_text(str(os.getpid()), encoding="utf-8")


def _release_lock(lock_path: Path) -> None:
    try:
        lock_path.unlink()
    except Exception:
        pass


def _run(cmd: list[str], *, cwd: Path, timeout_s: float | None = None) -> tuple[int, str]:
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=(None if timeout_s is None else float(timeout_s)),
        )
        out = (proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else "")
        return int(proc.returncode), out
    except subprocess.TimeoutExpired as e:
        out = (e.stdout or "") + ("\n" + e.stderr if e.stderr else "")
        return 124, out
    except Exception as e:
        return 125, str(e)


def _tail(text: str, *, max_lines: int = 50) -> list[str]:
    lines = [ln.rstrip("\n") for ln in (text or "").splitlines()]
    return lines[-max(int(max_lines), 1) :]


@dataclass
class SchedulerState:
    last_silver_build_id: str | None = None
    last_silver_inputs_hash: str | None = None
    last_dq_bronze_utc: str | None = None
    last_dq_silver_utc: str | None = None
    last_gold_features_utc: str | None = None
    last_gold_analytics_utc: str | None = None
    last_archive_utc: str | None = None

    @classmethod
    def load(cls, path: Path) -> "SchedulerState":
        obj = _read_json(path) or {}
        return cls(
            last_silver_build_id=str(obj.get("last_silver_build_id")) if obj.get("last_silver_build_id") else None,
            last_silver_inputs_hash=str(obj.get("last_silver_inputs_hash")) if obj.get("last_silver_inputs_hash") else None,
            last_dq_bronze_utc=str(obj.get("last_dq_bronze_utc")) if obj.get("last_dq_bronze_utc") else None,
            last_dq_silver_utc=str(obj.get("last_dq_silver_utc")) if obj.get("last_dq_silver_utc") else None,
            last_gold_features_utc=str(obj.get("last_gold_features_utc")) if obj.get("last_gold_features_utc") else None,
            last_gold_analytics_utc=str(obj.get("last_gold_analytics_utc")) if obj.get("last_gold_analytics_utc") else None,
            last_archive_utc=str(obj.get("last_archive_utc")) if obj.get("last_archive_utc") else None,
        )

    def dump(self) -> dict[str, object]:
        return {
            "last_silver_build_id": self.last_silver_build_id,
            "last_silver_inputs_hash": self.last_silver_inputs_hash,
            "last_dq_bronze_utc": self.last_dq_bronze_utc,
            "last_dq_silver_utc": self.last_dq_silver_utc,
            "last_gold_features_utc": self.last_gold_features_utc,
            "last_gold_analytics_utc": self.last_gold_analytics_utc,
            "last_archive_utc": self.last_archive_utc,
        }


def _should_run_interval(last_ts_utc: str | None, *, every_s: int) -> bool:
    if every_s <= 0:
        return False
    if not last_ts_utc:
        return True
    try:
        last = datetime.fromisoformat(str(last_ts_utc)).astimezone(timezone.utc)
    except Exception:
        return True
    age_s = max((_utc_now() - last).total_seconds(), 0.0)
    return age_s >= float(every_s)


def main() -> int:
    p = argparse.ArgumentParser(description="Long-run scheduler loop (DQ, Gold builds, Bronze archiving).")
    p.add_argument("--repo-root", default=str(PROJECT_ROOT))
    p.add_argument("--tick-seconds", type=int, default=int(os.getenv("SCHEDULER_TICK_SECONDS", "30")))
    p.add_argument("--dq-interval-seconds", type=int, default=int(os.getenv("SCHEDULER_DQ_INTERVAL_SECONDS", "1800")))
    p.add_argument("--gold-interval-seconds", type=int, default=int(os.getenv("SCHEDULER_GOLD_INTERVAL_SECONDS", "1800")))
    p.add_argument("--archive-interval-seconds", type=int, default=int(os.getenv("SCHEDULER_ARCHIVE_INTERVAL_SECONDS", "86400")))
    p.add_argument("--archive-older-than-days", type=int, default=int(os.getenv("ARCHIVE_BRONZE_OLDER_THAN_DAYS", "30")))
    p.add_argument(
        "--archive-delete-after",
        action=argparse.BooleanOptionalAction,
        default=_parse_bool(os.getenv("ARCHIVE_DELETE_AFTER"), default=True),
    )
    p.add_argument("--archive-min-free-disk-bytes", type=int, default=int(os.getenv("ARCHIVE_MIN_FREE_DISK_BYTES", "0")) or 0)
    p.add_argument("--archive-max-bytes", type=int, default=int(os.getenv("ARCHIVE_MAX_BYTES", "0")) or 0)
    args = p.parse_args()

    cfg = load_config()
    configure_logging(cfg.logging)

    repo_root = Path(args.repo_root)
    logs_dir = repo_root / "logs"
    state_path = logs_dir / "scheduler_state.json"
    hb_path = logs_dir / "scheduler_heartbeat.json"
    lock_path = logs_dir / "locks" / "scheduler.lock"
    _acquire_lock(lock_path)

    stop = {"flag": False}

    def _handle(_sig: int, _frame: object) -> None:
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)

    state = SchedulerState.load(state_path)
    last_action = "startup"
    last_error: str | None = None

    try:
        while not stop["flag"]:
            now = _utc_now()
            silver_meta = _read_json(repo_root / "data" / "silver" / "_build_meta.json") or {}
            silver_build_id = str(silver_meta.get("build_id") or "") or None
            silver_inputs_hash = str(silver_meta.get("inputs_hash") or "") or None

            did_any = False

            # Bronze + Silver DQ gate.
            if _should_run_interval(state.last_dq_bronze_utc, every_s=int(args.dq_interval_seconds)):
                rc, out = _run(
                    [sys.executable, "scripts/validate_bronze.py", "--bronze-dir", "data/bronze", "--out", "logs/dq/bronze_latest.json"],
                    cwd=repo_root,
                    timeout_s=60.0,
                )
                state.last_dq_bronze_utc = now.isoformat()
                last_action = f"dq_bronze rc={rc}"
                last_error = None if rc == 0 else "dq_bronze_failed"
                did_any = True
                if rc != 0:
                    logger.warning("dq_bronze failed rc=%s tail=%s", rc, _tail(out, max_lines=10))

            if _should_run_interval(state.last_dq_silver_utc, every_s=int(args.dq_interval_seconds)):
                rc, out = _run(
                    [
                        sys.executable,
                        "scripts/validate_silver_extended.py",
                        "--silver-dir",
                        "data/silver",
                        "--out",
                        "logs/dq/silver_latest.json",
                    ],
                    cwd=repo_root,
                    timeout_s=120.0,
                )
                state.last_dq_silver_utc = now.isoformat()
                last_action = f"dq_silver rc={rc}"
                last_error = None if rc == 0 else "dq_silver_failed"
                did_any = True
                if rc != 0:
                    logger.warning("dq_silver failed rc=%s tail=%s", rc, _tail(out, max_lines=10))

            # Gold builds: run when Silver build id changes (or on interval if missing state).
            silver_changed = (
                silver_build_id
                and (silver_build_id != state.last_silver_build_id or silver_inputs_hash != state.last_silver_inputs_hash)
            )
            if silver_changed:
                state.last_silver_build_id = silver_build_id
                state.last_silver_inputs_hash = silver_inputs_hash

            want_gold = bool(silver_build_id)
            need_features = want_gold and (
                silver_changed
                or state.last_gold_features_utc is None
                or _should_run_interval(state.last_gold_features_utc, every_s=int(args.gold_interval_seconds))
            )
            need_analytics = want_gold and (
                silver_changed
                or state.last_gold_analytics_utc is None
                or _should_run_interval(state.last_gold_analytics_utc, every_s=int(args.gold_interval_seconds))
            )

            if need_features or need_analytics:
                # Gate on Silver DQ first.
                dq_rc, dq_out = _run(
                    [
                        sys.executable,
                        "scripts/validate_silver_extended.py",
                        "--silver-dir",
                        "data/silver",
                        "--out",
                        "logs/dq/silver_latest.json",
                    ],
                    cwd=repo_root,
                    timeout_s=120.0,
                )
                state.last_dq_silver_utc = now.isoformat()
                did_any = True
                if dq_rc != 0:
                    last_action = f"dq_silver rc={dq_rc}"
                    last_error = "dq_silver_failed"
                    logger.warning("dq_silver failed rc=%s tail=%s", dq_rc, _tail(dq_out, max_lines=10))
                else:
                    if need_features:
                        rc, out = _run(
                            [sys.executable, "scripts/build_features.py", "--silver-dir", "data/silver"],
                            cwd=repo_root,
                            timeout_s=300.0,
                        )
                        last_action = f"build_features rc={rc}"
                        last_error = None if rc == 0 else "build_features_failed"
                        if rc == 0:
                            state.last_gold_features_utc = now.isoformat()
                        else:
                            logger.warning("build_features failed rc=%s tail=%s", rc, _tail(out, max_lines=10))

                    if need_analytics:
                        rc2, out2 = _run(
                            [sys.executable, "scripts/build_analytics.py"],
                            cwd=repo_root,
                            timeout_s=300.0,
                        )
                        last_action = f"build_analytics rc={rc2}"
                        last_error = None if rc2 == 0 else "build_analytics_failed"
                        if rc2 == 0:
                            state.last_gold_analytics_utc = now.isoformat()
                        else:
                            logger.warning("build_analytics failed rc=%s tail=%s", rc2, _tail(out2, max_lines=10))

            # Archive old Bronze snapshots (safe default: no delete).
            if _should_run_interval(state.last_archive_utc, every_s=int(args.archive_interval_seconds)):
                cmd = [
                    sys.executable,
                    "scripts/archive_bronze.py",
                    "--bronze-dir",
                    "data/bronze",
                    "--archive-dir",
                    "data/archive/bronze",
                    "--older-than-days",
                    str(int(args.archive_older_than_days)),
                    "--datasets",
                    "tdx/bike/availability,tdx/bike/stations",
                ]
                if bool(args.archive_delete_after):
                    cmd.append("--delete-after-archive")
                if int(args.archive_min_free_disk_bytes) > 0:
                    cmd += ["--min-free-disk-bytes", str(int(args.archive_min_free_disk_bytes))]
                if int(args.archive_max_bytes) > 0:
                    cmd += ["--max-archive-bytes", str(int(args.archive_max_bytes))]
                rc, out = _run(cmd, cwd=repo_root, timeout_s=300.0)
                state.last_archive_utc = now.isoformat()
                last_action = f"archive_bronze rc={rc}"
                last_error = None if rc == 0 else "archive_failed"
                did_any = True
                if rc != 0:
                    logger.warning("archive_bronze failed rc=%s tail=%s", rc, _tail(out, max_lines=10))

            _write_json(state_path, state.dump())
            _write_json(
                hb_path,
                {
                    "ts_utc": now.isoformat(),
                    "silver_build_id": silver_build_id,
                    "silver_inputs_hash": silver_inputs_hash,
                    "last_action": last_action,
                    "last_error": last_error,
                    "state": state.dump(),
                },
            )

            sleep_s = float(max(int(args.tick_seconds), 1))
            if did_any:
                sleep_s = float(max(sleep_s, 1))
            time.sleep(sleep_s)
    finally:
        _release_lock(lock_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
