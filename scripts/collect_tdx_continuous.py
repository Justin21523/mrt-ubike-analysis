from __future__ import annotations

import sys
from pathlib import Path

# Allow running scripts without requiring an editable install (`pip install -e .`).
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_PATH))

import argparse
from datetime import datetime, timedelta, timezone
import logging
import os
import random
import subprocess
import time
import json
import signal
import shutil

from metrobikeatlas.config.loader import load_config
from metrobikeatlas.ingestion.bronze import write_bronze_json
from metrobikeatlas.ingestion.tdx_base import TDXClient, TDXCredentials, TDXRateLimitError
from metrobikeatlas.utils.cache import JsonFileCache
from metrobikeatlas.utils.logging import configure_logging


logger = logging.getLogger(__name__)


def _write_heartbeat(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _parse_bool(value: str | None, *, default: bool) -> bool:
    if value is None or value.strip() == "":
        return default
    v = value.strip().lower()
    if v in {"1", "true", "yes", "y", "on"}:
        return True
    if v in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _parse_cities(value: str | None) -> list[str] | None:
    if value is None:
        return None
    cities = [c.strip() for c in value.split(",")]
    cities = [c for c in cities if c]
    return cities or None


def _is_pid_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _pid_cmdline(pid: int) -> str | None:
    try:
        data = Path(f"/proc/{pid}/cmdline").read_bytes()
    except Exception:
        return None
    try:
        return data.replace(b"\x00", b" ").decode("utf-8", errors="replace").strip()
    except Exception:
        return None


def _pid_start_time_ticks(pid: int) -> int | None:
    # Linux only: /proc/<pid>/stat field 22 is process starttime (clock ticks since boot).
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8", errors="replace")
    except Exception:
        return None
    try:
        parts = stat.split()
        if len(parts) < 22:
            return None
        return int(parts[21])
    except Exception:
        return None


def _pid_looks_like_this_collector(pid: int) -> bool:
    cmd = _pid_cmdline(pid)
    if not cmd:
        return False
    return "collect_tdx_continuous.py" in cmd


def _read_lock(lock_path: Path) -> dict[str, object] | None:
    if not lock_path.exists():
        return None
    try:
        raw = lock_path.read_text(encoding="utf-8").strip()
    except Exception:
        return None
    if not raw:
        return None
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass
    # Backwards compatibility: older lock format was just a PID integer.
    try:
        return {"pid": int(raw)}
    except Exception:
        return None


def _write_lock(lock_path: Path, pid: int) -> None:
    payload: dict[str, object] = {
        "pid": int(pid),
        "start_time_ticks": _pid_start_time_ticks(pid),
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "cmdline": _pid_cmdline(pid),
    }
    lock_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _acquire_lock(lock_path: Path) -> None:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    if lock_path.exists():
        lock = _read_lock(lock_path) or {}
        existing = lock.get("pid")
        try:
            existing_pid = int(existing) if existing is not None else None
        except Exception:
            existing_pid = None

        # If a stale lock file happens to contain our current PID (PID reuse), do not self-deadlock.
        if existing_pid == os.getpid():
            _write_lock(lock_path, os.getpid())
            return

        if existing_pid and _is_pid_running(existing_pid):
            # In Docker, PID reuse across container restarts can cause false positives if a lock file is persisted.
            # Only treat it as "already running" if the PID is actually this collector script.
            if _pid_looks_like_this_collector(existing_pid):
                stored_start = lock.get("start_time_ticks")
                live_start = _pid_start_time_ticks(existing_pid)
                if stored_start is not None and live_start is not None:
                    try:
                        if int(stored_start) == int(live_start):
                            raise RuntimeError(f"Collector already running (lock pid={existing_pid})")
                    except Exception:
                        pass
                else:
                    raise RuntimeError(f"Collector already running (lock pid={existing_pid})")
    _write_lock(lock_path, os.getpid())


def _release_lock(lock_path: Path) -> None:
    try:
        if lock_path.exists():
            lock_path.unlink()
    except Exception:
        pass


def _cleanup_old_files(
    *,
    root: Path,
    pattern: str,
    keep_last: int | None,
    keep_days: float | None,
) -> int:
    """
    Delete old Bronze files under `root` matching `pattern`.
    """

    if not root.exists():
        return 0
    keep_n = None if keep_last is None else max(int(keep_last), 1)
    keep_td = None if keep_days is None else timedelta(days=max(float(keep_days), 0.0))
    now = datetime.now(timezone.utc)

    files = []
    for p in root.glob(pattern):
        try:
            st = p.stat()
        except Exception:
            continue
        files.append((p, st.st_mtime))
    files.sort(key=lambda x: x[1])  # oldest -> newest

    deleted = 0
    for idx, (p, mtime) in enumerate(files):
        # Keep newest N
        if keep_n is not None:
            # newest N are the last keep_n entries
            if idx >= max(len(files) - keep_n, 0):
                continue
        # Keep within days window
        if keep_td is not None:
            age = now - datetime.fromtimestamp(mtime, tz=timezone.utc)
            if age <= keep_td:
                continue
        try:
            p.unlink()
            deleted += 1
        except Exception:
            continue
    return deleted


def _ensure_disk_space(
    *,
    repo_root: Path,
    bronze_dir: Path,
    min_free_bytes: int | None,
    max_bronze_bytes: int | None,
) -> dict[str, object]:
    usage = shutil.disk_usage(str(repo_root))
    bronze_size = 0
    try:
        for p in bronze_dir.rglob("*.json"):
            try:
                bronze_size += int(p.stat().st_size)
            except Exception:
                continue
    except Exception:
        bronze_size = bronze_size

    action = "none"
    if min_free_bytes is not None and usage.free < int(min_free_bytes):
        action = "low_disk"
    if max_bronze_bytes is not None and bronze_size > int(max_bronze_bytes):
        action = "bronze_too_big"

    return {
        "disk_total_bytes": int(usage.total),
        "disk_used_bytes": int(usage.used),
        "disk_free_bytes": int(usage.free),
        "bronze_bytes_estimate": int(bronze_size),
        "action": action,
    }


def _collect_station_dataset(
    *,
    tdx: TDXClient,
    bronze_dir: Path,
    cache: JsonFileCache,
    use_cache: bool,
    source: str,
    domain: str,
    dataset: str,
    cities: list[str],
    path_template: str,
    cache_namespace: str,
    max_pages: int,
) -> int:
    wrote = 0
    for city in cities:
        path = path_template.format(city=city)
        params = {"$format": "JSON"}
        retrieved_at = datetime.now(timezone.utc)

        request_meta: dict[str, object] = {"path": path, "params": params, "cache": None}
        cache_key = cache.make_key(cache_namespace, {"path": path, "params": params, "city": city})
        cached = cache.get(cache_key) if use_cache else None
        if cached is not None:
            payload = cached
            request_meta["cache"] = "hit"
        else:
            payload = tdx.get_json_all(path, params=params, max_pages=max_pages)
            if use_cache:
                cache.set(cache_key, payload)
            request_meta["cache"] = "miss"

        out = write_bronze_json(
            bronze_dir,
            source=source,
            domain=domain,
            dataset=dataset,
            city=city,
            retrieved_at=retrieved_at,
            request=request_meta,
            payload=payload,
        )
        wrote += 1
        logger.info("Wrote %s", out)
    return wrote


def _collect_bike_availability(
    *,
    tdx: TDXClient,
    bronze_dir: Path,
    cities: list[str],
    path_template: str,
    max_pages: int,
) -> int:
    ok = 0
    for city in cities:
        path = path_template.format(city=city)
        params = {"$format": "JSON"}
        retrieved_at = datetime.now(timezone.utc)
        payload = tdx.get_json_all(path, params=params, max_pages=max_pages)
        out = write_bronze_json(
            bronze_dir,
            source="tdx",
            domain="bike",
            dataset="availability",
            city=city,
            retrieved_at=retrieved_at,
            request={"path": path, "params": params},
            payload=payload,
        )
        ok += 1
        logger.info("Wrote %s", out)
    return ok


def _run_build_silver(*, repo_root: Path, bronze_dir: Path, silver_dir: Path, max_availability_files: int) -> None:
    scripts_dir = repo_root / "scripts"
    external_metro = repo_root / "data" / "external" / "metro_stations.csv"
    cmd = [
        sys.executable,
        str(scripts_dir / "build_silver_locked.py"),
        "--wait-seconds",
        "0",
        "--bronze-dir",
        str(bronze_dir),
        "--silver-dir",
        str(silver_dir),
        "--max-availability-files",
        str(int(max_availability_files)),
    ]
    write_sqlite = _parse_bool(os.getenv("BUILD_SILVER_WRITE_SQLITE"), default=False) or (
        (os.getenv("METROBIKEATLAS_STORAGE") or "").strip().lower() == "sqlite"
    )
    if write_sqlite:
        cmd.append("--write-sqlite")
    if external_metro.exists():
        cmd += ["--external-metro-stations-csv", str(external_metro)]
    logger.info("Running: %s", " ".join(cmd))
    subprocess.run(cmd, check=True)


def _latest_bike_availability_by_city(*, bronze_dir: Path, cities: list[str]) -> dict[str, str | None]:
    out: dict[str, str | None] = {}
    for city in cities:
        city_dir = bronze_dir / "tdx" / "bike" / "availability" / f"city={city}"
        files = sorted(city_dir.glob("*.json"))
        out[city] = str(files[-1]) if files else None
    return out


def _load_json(path: Path) -> dict[str, object] | None:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _should_build_silver(
    *,
    repo_root: Path,
    bronze_dir: Path,
    silver_dir: Path,
    bike_cities: list[str],
) -> tuple[bool, str]:
    """
    Decide whether to run build_silver based on whether inputs changed.

    Goal: avoid rebuilding on a timer when no new Bronze snapshots arrived.
    """

    if _parse_bool(os.getenv("BUILD_SILVER_ALWAYS"), default=False):
        return True, "forced"

    build_meta_path = silver_dir / "_build_meta.json"
    build_meta = _load_json(build_meta_path) if build_meta_path.exists() else None

    current_latest = _latest_bike_availability_by_city(bronze_dir=bronze_dir, cities=bike_cities)
    if any(v is None for v in current_latest.values()):
        return True, "missing_bronze_availability"

    want_sqlite = _parse_bool(os.getenv("BUILD_SILVER_WRITE_SQLITE"), default=False) or (
        (os.getenv("METROBIKEATLAS_STORAGE") or "").strip().lower() == "sqlite"
    )
    if want_sqlite and not (silver_dir / "metrobikeatlas.db").exists():
        return True, "sqlite_missing"

    if not isinstance(build_meta, dict):
        return True, "no_previous_build_meta"

    try:
        inputs = build_meta.get("inputs")
        prev_sources = inputs.get("sources") if isinstance(inputs, dict) else {}
    except Exception:
        prev_sources = {}

    prev_avail = prev_sources.get("bike_availability_summary_by_city") if isinstance(prev_sources, dict) else None
    prev_latest_by_city: dict[str, str | None] = {}
    if isinstance(prev_avail, list):
        for it in prev_avail:
            if not isinstance(it, dict):
                continue
            city = str(it.get("city") or "").strip()
            if not city:
                continue
            prev_latest_by_city[city] = str(it.get("latest_path") or "") or None

    # Compare latest file path strings. build_silver stores repo-relative paths.
    for city, cur_path in current_latest.items():
        if cur_path is None:
            continue
        try:
            cur_rel = str(Path(cur_path).relative_to(repo_root))
        except Exception:
            cur_rel = str(cur_path)
        prev_rel = prev_latest_by_city.get(city)
        if not prev_rel or prev_rel != cur_rel:
            return True, "bronze_availability_changed"

    def _mtime(p: Path) -> float | None:
        try:
            return p.stat().st_mtime
        except Exception:
            return None

    # External inputs: if metro/calendar/weather CSV mtimes changed since the last build, rebuild.
    ext_metro = repo_root / "data" / "external" / "metro_stations.csv"
    ext_calendar = repo_root / "data" / "external" / "calendar.csv"
    ext_weather = repo_root / "data" / "external" / "weather_hourly.csv"
    for key, p in [
        ("external_metro_csv", ext_metro),
        ("calendar_csv", ext_calendar),
        ("weather_hourly_csv", ext_weather),
    ]:
        if not p.exists():
            continue
        prev = prev_sources.get(key) if isinstance(prev_sources, dict) else None
        prev_mtime = None
        if isinstance(prev, dict) and prev.get("mtime_utc"):
            try:
                prev_mtime = datetime.fromisoformat(str(prev.get("mtime_utc"))).astimezone(timezone.utc).timestamp()
            except Exception:
                prev_mtime = None
        cur_mtime = _mtime(p)
        if prev_mtime is None or (cur_mtime is not None and cur_mtime > prev_mtime + 1.0):
            return True, f"{key}_changed"

    return False, "no_new_inputs"


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Continuously collect TDX data into `data/bronze/` with rate-limit handling. "
            "Optionally rebuild Silver periodically."
        )
    )
    parser.add_argument("--bronze-dir", default="data/bronze")
    parser.add_argument("--silver-dir", default="data/silver")
    parser.add_argument("--max-availability-files", type=int, default=500)

    parser.add_argument("--bike-cities", default=None, help="Comma-separated list (overrides config).")
    parser.add_argument("--metro-cities", default=None, help="Comma-separated list (overrides config).")

    parser.add_argument("--availability-interval-seconds", type=int, default=300)
    parser.add_argument("--stations-refresh-interval-hours", type=float, default=24.0)
    parser.add_argument("--jitter-seconds", type=float, default=2.0)

    parser.add_argument("--duration-seconds", type=int, default=None)
    parser.add_argument("--max-iterations", type=int, default=None)

    parser.add_argument("--backoff-seconds", type=int, default=10)
    parser.add_argument("--max-backoff-seconds", type=int, default=300)

    # Retention & disk safety
    parser.add_argument("--retain-availability-files-per-city", type=int, default=int(os.getenv("BRONZE_RETAIN_AVAIL_FILES_PER_CITY", "288")))
    parser.add_argument("--retain-availability-days", type=float, default=float(os.getenv("BRONZE_RETAIN_AVAIL_DAYS", "2")))
    parser.add_argument("--retain-stations-files-per-city", type=int, default=int(os.getenv("BRONZE_RETAIN_STATIONS_FILES_PER_CITY", "4")))
    parser.add_argument("--max-bronze-bytes", type=int, default=int(os.getenv("BRONZE_MAX_BYTES", "0")) or None)
    parser.add_argument("--min-free-disk-bytes", type=int, default=int(os.getenv("MIN_FREE_DISK_BYTES", "0")) or None)
    parser.add_argument("--cleanup-interval-seconds", type=int, default=int(os.getenv("BRONZE_CLEANUP_INTERVAL_SECONDS", "600")))

    parser.add_argument("--min-request-interval-s", type=float, default=float(os.getenv("TDX_MIN_REQUEST_INTERVAL_S", "0.2")))
    parser.add_argument("--request-jitter-s", type=float, default=float(os.getenv("TDX_REQUEST_JITTER_S", "0.05")))
    parser.add_argument("--max-pages", type=int, default=int(os.getenv("TDX_MAX_PAGES", "100")))
    parser.add_argument("--cache-stations", type=str, default=os.getenv("TDX_CACHE_STATIONS", "true"))

    parser.add_argument(
        "--build-silver-interval-seconds",
        type=int,
        default=None,
        help="If set, periodically rebuild Silver from Bronze.",
    )
    args = parser.parse_args()

    config = load_config()
    configure_logging(config.logging)

    try:
        creds = TDXCredentials.from_env()
    except ValueError as exc:
        raise ValueError(
            "Missing TDX credentials. Set env vars `TDX_CLIENT_ID` and `TDX_CLIENT_SECRET` "
            "(for local runs, put them in `.env`; see `.env.example`)."
        ) from exc
    bronze_dir = Path(args.bronze_dir)
    bronze_dir.mkdir(parents=True, exist_ok=True)
    silver_dir = Path(args.silver_dir)
    silver_dir.mkdir(parents=True, exist_ok=True)

    cache = JsonFileCache(config.cache)
    use_cache = _parse_bool(args.cache_stations, default=True)

    bike_cities = _parse_cities(args.bike_cities) or list(config.tdx.bike.cities)
    metro_cities = _parse_cities(args.metro_cities) or list(config.tdx.metro.cities)
    if not bike_cities:
        raise ValueError("No bike cities configured or provided via --bike-cities")
    if not metro_cities:
        raise ValueError("No metro cities configured or provided via --metro-cities")

    interval_s = max(int(args.availability_interval_seconds), 1)
    jitter_s = max(float(args.jitter_seconds), 0.0)
    refresh_td = timedelta(hours=max(float(args.stations_refresh_interval_hours), 0.0))

    stop_at = None
    if args.duration_seconds is not None:
        stop_at = datetime.now(timezone.utc) + timedelta(seconds=int(args.duration_seconds))

    next_station_refresh = datetime.now(timezone.utc)  # run immediately on startup
    next_build_silver = None
    if args.build_silver_interval_seconds is not None:
        next_build_silver = datetime.now(timezone.utc) + timedelta(seconds=int(args.build_silver_interval_seconds))

    backoff_base = max(int(args.backoff_seconds), 1)
    max_backoff = max(int(args.max_backoff_seconds), backoff_base)

    repo_root = Path(__file__).resolve().parents[1]
    logs_dir = repo_root / "logs"
    heartbeat_path = logs_dir / "collector_heartbeat.json"
    metrics_path = logs_dir / "collector_metrics.json"
    lock_path = logs_dir / "collector.lock"
    external_metro_path = repo_root / "data" / "external" / "metro_stations.csv"
    skip_metro_stations = _parse_bool(
        os.getenv("TDX_SKIP_METRO_STATIONS"),
        default=external_metro_path.exists(),
    )
    last_success_utc: str | None = None
    last_error_utc: str | None = None
    last_error: str | None = None
    tdx_rate_limit_count = 0
    last_cleanup_utc: str | None = None

    def _new_backoff_state(name: str) -> dict[str, object]:
        return {"name": name, "current_s": float(backoff_base), "next_allowed_utc": None, "last_reason": None}

    def _backoff_ready(state: dict[str, object], now: datetime) -> bool:
        ts = state.get("next_allowed_utc")
        if not ts:
            return True
        try:
            t = datetime.fromisoformat(str(ts)).astimezone(timezone.utc)
        except Exception:
            return True
        return now >= t

    def _backoff_fail(state: dict[str, object], now: datetime, reason: str) -> None:
        cur = float(state.get("current_s") or float(backoff_base))
        nxt = min(cur * 2.0, float(max_backoff))
        state["current_s"] = float(nxt)
        state["next_allowed_utc"] = (now + timedelta(seconds=nxt)).isoformat()
        state["last_reason"] = str(reason)[:200]

    def _backoff_ok(state: dict[str, object]) -> None:
        state["current_s"] = float(backoff_base)
        state["next_allowed_utc"] = None
        state["last_reason"] = None

    backoff_states: dict[str, dict[str, object]] = {
        "tdx:metro:stations": _new_backoff_state("tdx:metro:stations"),
        "tdx:bike:stations": _new_backoff_state("tdx:bike:stations"),
        "tdx:bike:availability": _new_backoff_state("tdx:bike:availability"),
        "build_silver": _new_backoff_state("build_silver"),
    }

    _acquire_lock(lock_path)
    stop_requested = False

    def _handle_signal(_signum, _frame) -> None:  # type: ignore[no-untyped-def]
        nonlocal stop_requested
        stop_requested = True

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    with TDXClient(
        base_url=config.tdx.base_url,
        token_url=config.tdx.token_url,
        credentials=creds,
        min_request_interval_s=float(args.min_request_interval_s),
        request_jitter_s=float(args.request_jitter_s),
    ) as tdx:
        i = 0
        dataset_metrics: dict[str, object] = {
            "tdx:metro:stations": {"ok": 0, "error": 0, "last_ok_utc": None, "last_error_utc": None},
            "tdx:bike:stations": {"ok": 0, "error": 0, "last_ok_utc": None, "last_error_utc": None},
            "tdx:bike:availability": {"ok": 0, "error": 0, "last_ok_utc": None, "last_error_utc": None},
            "build_silver": {"ok": 0, "error": 0, "last_ok_utc": None, "last_error_utc": None},
        }

        while True:
            if stop_requested:
                logger.info("Stopping: signal received.")
                break
            if stop_at is not None and datetime.now(timezone.utc) >= stop_at:
                logger.info("Stopping: duration reached.")
                break
            if args.max_iterations is not None and i >= int(args.max_iterations):
                logger.info("Stopping: max iterations reached.")
                break
            i += 1

            loop_started = datetime.now(timezone.utc)
            ok = 0
            now = datetime.now(timezone.utc)

            # Disk safety snapshot (and opportunistic cleanup if needed).
            disk = _ensure_disk_space(
                repo_root=repo_root,
                bronze_dir=bronze_dir,
                min_free_bytes=args.min_free_disk_bytes,
                max_bronze_bytes=args.max_bronze_bytes,
            )

            # Emergency mode: if disk is critically low, skip network requests and only cleanup + heartbeat.
            if disk.get("action") == "low_disk":
                logger.error("Disk low (free=%s). Entering emergency mode: cleanup only.", disk.get("disk_free_bytes"))
                now = datetime.now(timezone.utc)
                deleted = 0
                for city in bike_cities:
                    avail_dir = bronze_dir / "tdx" / "bike" / "availability" / f"city={city}"
                    deleted += _cleanup_old_files(
                        root=avail_dir,
                        pattern="*.json",
                        keep_last=args.retain_availability_files_per_city,
                        keep_days=args.retain_availability_days,
                    )
                last_cleanup_utc = now.isoformat()
                # Re-check disk after cleanup.
                disk2 = _ensure_disk_space(
                    repo_root=repo_root,
                    bronze_dir=bronze_dir,
                    min_free_bytes=args.min_free_disk_bytes,
                    max_bronze_bytes=args.max_bronze_bytes,
                )
                _write_heartbeat(
                    heartbeat_path,
                    {
                        "ts_utc": datetime.now(timezone.utc).isoformat(),
                        "loop_started_utc": loop_started.isoformat(),
                        "last_success_utc": last_success_utc,
                        "last_error_utc": datetime.now(timezone.utc).isoformat(),
                        "last_error": "disk emergency mode",
                        "tdx_rate_limit_count": int(tdx_rate_limit_count),
                        "ok_snapshots": 0,
                        "backoff_s": float(max_backoff),
                        "disk": disk2,
                        "deleted_files": int(deleted),
                        "datasets": dataset_metrics,
                        "emergency_mode": True,
                    },
                )
                _write_heartbeat(
                    metrics_path,
                    {
                        "ts_utc": datetime.now(timezone.utc).isoformat(),
                        "datasets": dataset_metrics,
                        "disk": disk2,
                        "backoff_s": float(max_backoff),
                        "deleted_files": int(deleted),
                        "emergency_mode": True,
                    },
                )
                time.sleep(float(max_backoff))
                continue

            try:
                if refresh_td.total_seconds() == 0 or now >= next_station_refresh:
                    logger.info(
                        "Refreshing station metadata (metro=%s, bike=%s, skip_metro=%s)",
                        metro_cities,
                        bike_cities,
                        skip_metro_stations,
                    )
                    if not skip_metro_stations:
                        try:
                            st = backoff_states["tdx:metro:stations"]
                            if _backoff_ready(st, now):
                                _collect_station_dataset(
                                    tdx=tdx,
                                    bronze_dir=bronze_dir,
                                    cache=cache,
                                    use_cache=use_cache,
                                    source="tdx",
                                    domain="metro",
                                    dataset="stations",
                                    cities=metro_cities,
                                    path_template=config.tdx.metro.stations_path_template,
                                    cache_namespace="tdx:metro:stations",
                                    max_pages=int(args.max_pages),
                                )
                                _backoff_ok(st)
                                dm = dataset_metrics["tdx:metro:stations"]
                                dm["ok"] = int(dm.get("ok") or 0) + 1
                                dm["last_ok_utc"] = datetime.now(timezone.utc).isoformat()
                            else:
                                logger.warning(
                                    "Skipping metro stations refresh due to backoff until %s", st.get("next_allowed_utc")
                                )
                        except Exception:
                            logger.exception("Metro station refresh failed (will retry later).")
                            _backoff_fail(backoff_states["tdx:metro:stations"], now, "error")
                            dm = dataset_metrics["tdx:metro:stations"]
                            dm["error"] = int(dm.get("error") or 0) + 1
                            dm["last_error_utc"] = datetime.now(timezone.utc).isoformat()
                    else:
                        logger.info(
                            "Skipping metro stations refresh (TDX_SKIP_METRO_STATIONS=true or external metro CSV present at %s).",
                            external_metro_path,
                        )

                    try:
                        st = backoff_states["tdx:bike:stations"]
                        if _backoff_ready(st, now):
                            _collect_station_dataset(
                                tdx=tdx,
                                bronze_dir=bronze_dir,
                                cache=cache,
                                use_cache=use_cache,
                                source="tdx",
                                domain="bike",
                                dataset="stations",
                                cities=bike_cities,
                                path_template=config.tdx.bike.stations_path_template,
                                cache_namespace="tdx:bike:stations",
                                max_pages=int(args.max_pages),
                            )
                            _backoff_ok(st)
                            dm = dataset_metrics["tdx:bike:stations"]
                            dm["ok"] = int(dm.get("ok") or 0) + 1
                            dm["last_ok_utc"] = datetime.now(timezone.utc).isoformat()
                        else:
                            logger.warning("Skipping bike stations refresh due to backoff until %s", st.get("next_allowed_utc"))
                    except Exception:
                        logger.exception("Bike station refresh failed (will retry later).")
                        _backoff_fail(backoff_states["tdx:bike:stations"], now, "error")
                        dm = dataset_metrics["tdx:bike:stations"]
                        dm["error"] = int(dm.get("error") or 0) + 1
                        dm["last_error_utc"] = datetime.now(timezone.utc).isoformat()

                    next_station_refresh = now + refresh_td
            except Exception:
                logger.exception("Station refresh failed (will retry later).")
                last_error = "Station refresh failed"
                last_error_utc = datetime.now(timezone.utc).isoformat()

            try:
                st = backoff_states["tdx:bike:availability"]
                if _backoff_ready(st, now):
                    ok += _collect_bike_availability(
                        tdx=tdx,
                        bronze_dir=bronze_dir,
                        cities=bike_cities,
                        path_template=config.tdx.bike.availability_path_template,
                        max_pages=int(args.max_pages),
                    )
                    _backoff_ok(st)
                    dm = dataset_metrics["tdx:bike:availability"]
                    dm["ok"] = int(dm.get("ok") or 0) + int(ok)
                    dm["last_ok_utc"] = datetime.now(timezone.utc).isoformat()
                else:
                    logger.warning("Skipping availability due to backoff until %s", st.get("next_allowed_utc"))
            except TDXRateLimitError as exc:
                logger.exception("Availability collection failed.")
                msg = str(exc)
                last_error = msg[:500]
                last_error_utc = datetime.now(timezone.utc).isoformat()
                tdx_rate_limit_count += 1
                dm = dataset_metrics["tdx:bike:availability"]
                dm["error"] = int(dm.get("error") or 0) + 1
                dm["last_error_utc"] = datetime.now(timezone.utc).isoformat()
                _backoff_fail(backoff_states["tdx:bike:availability"], now, "rate_limited")
                # Respect Retry-After when present (more conservative than exponential backoff).
                if exc.retry_after_s is not None and exc.retry_after_s > 0:
                    backoff_states["tdx:bike:availability"]["current_s"] = min(float(exc.retry_after_s), float(max_backoff))
                    backoff_states["tdx:bike:availability"]["next_allowed_utc"] = (
                        now + timedelta(seconds=float(backoff_states["tdx:bike:availability"]["current_s"]))
                    ).isoformat()
            except Exception as exc:
                logger.exception("Availability collection failed.")
                msg = str(exc)
                last_error = msg[:500]
                last_error_utc = datetime.now(timezone.utc).isoformat()
                if "429" in msg or "rate limit" in msg.lower() or "retry-after" in msg.lower():
                    tdx_rate_limit_count += 1
                dm = dataset_metrics["tdx:bike:availability"]
                dm["error"] = int(dm.get("error") or 0) + 1
                dm["last_error_utc"] = datetime.now(timezone.utc).isoformat()
                _backoff_fail(backoff_states["tdx:bike:availability"], now, "error")

            # Retention cleanup (time-based and count-based), plus emergency cleanup when disk is low.
            now = datetime.now(timezone.utc)
            do_cleanup = False
            if last_cleanup_utc is None:
                do_cleanup = True
            else:
                try:
                    last_dt = datetime.fromisoformat(last_cleanup_utc).astimezone(timezone.utc)
                    do_cleanup = (now - last_dt).total_seconds() >= max(int(args.cleanup_interval_seconds), 30)
                except Exception:
                    do_cleanup = True
            if disk.get("action") in {"low_disk", "bronze_too_big"}:
                do_cleanup = True

            deleted = 0
            if do_cleanup:
                # Availability is high-frequency and dominates disk usage.
                for city in bike_cities:
                    avail_dir = bronze_dir / "tdx" / "bike" / "availability" / f"city={city}"
                    deleted += _cleanup_old_files(
                        root=avail_dir,
                        pattern="*.json",
                        keep_last=args.retain_availability_files_per_city,
                        keep_days=args.retain_availability_days,
                    )
                # Stations are low-frequency; keep only a few recent snapshots.
                for city in bike_cities:
                    st_dir = bronze_dir / "tdx" / "bike" / "stations" / f"city={city}"
                    deleted += _cleanup_old_files(
                        root=st_dir,
                        pattern="*.json",
                        keep_last=args.retain_stations_files_per_city,
                        keep_days=None,
                    )
                for city in metro_cities:
                    st_dir = bronze_dir / "tdx" / "metro" / "stations" / f"city={city}"
                    deleted += _cleanup_old_files(
                        root=st_dir,
                        pattern="*.json",
                        keep_last=args.retain_stations_files_per_city,
                        keep_days=None,
                    )
                last_cleanup_utc = now.isoformat()
                if deleted:
                    logger.info("Retention cleanup deleted %s files", deleted)

            if ok == 0:
                # Choose a conservative backoff: max of dataset backoffs.
                current_backoff = max(float(backoff_base), float(backoff_states["tdx:bike:availability"].get("current_s") or backoff_base))
                logger.warning("No snapshots collected; backing off %ss.", current_backoff)
                _write_heartbeat(
                    heartbeat_path,
                    {
                        "ts_utc": datetime.now(timezone.utc).isoformat(),
                        "loop_started_utc": loop_started.isoformat(),
                        "last_success_utc": last_success_utc,
                        "last_error_utc": last_error_utc,
                        "last_error": last_error,
                        "tdx_rate_limit_count": int(tdx_rate_limit_count),
                        "ok_snapshots": int(ok),
                        "backoff_s": float(current_backoff),
                        "disk": disk,
                        "deleted_files": int(deleted),
                        "datasets": dataset_metrics,
                        "backoffs": backoff_states,
                    },
                )
                _write_heartbeat(
                    metrics_path,
                    {
                        "ts_utc": datetime.now(timezone.utc).isoformat(),
                        "datasets": dataset_metrics,
                        "disk": disk,
                        "backoff_s": float(current_backoff),
                        "deleted_files": int(deleted),
                        "backoffs": backoff_states,
                    },
                )
                time.sleep(current_backoff)
                continue
            last_success_utc = datetime.now(timezone.utc).isoformat()
            last_error = None
            last_error_utc = None

            if next_build_silver is not None and datetime.now(timezone.utc) >= next_build_silver:
                try:
                    st = backoff_states["build_silver"]
                    if _backoff_ready(st, datetime.now(timezone.utc)):
                        should, reason = _should_build_silver(
                            repo_root=repo_root,
                            bronze_dir=bronze_dir,
                            silver_dir=silver_dir,
                            bike_cities=bike_cities,
                        )
                        if not should:
                            logger.info("Skipping build_silver: %s", reason)
                            next_build_silver = datetime.now(timezone.utc) + timedelta(
                                seconds=int(args.build_silver_interval_seconds)
                            )
                            continue
                        _run_build_silver(
                            repo_root=repo_root,
                            bronze_dir=bronze_dir,
                            silver_dir=silver_dir,
                            max_availability_files=int(args.max_availability_files),
                        )
                        _backoff_ok(st)
                        dm = dataset_metrics["build_silver"]
                        dm["ok"] = int(dm.get("ok") or 0) + 1
                        dm["last_ok_utc"] = datetime.now(timezone.utc).isoformat()
                    else:
                        logger.warning("Skipping build_silver due to backoff until %s", st.get("next_allowed_utc"))
                except Exception:
                    logger.exception("build_silver failed (will retry later).")
                    last_error = "build_silver failed"
                    last_error_utc = datetime.now(timezone.utc).isoformat()
                    dm = dataset_metrics["build_silver"]
                    dm["error"] = int(dm.get("error") or 0) + 1
                    dm["last_error_utc"] = datetime.now(timezone.utc).isoformat()
                    _backoff_fail(backoff_states["build_silver"], datetime.now(timezone.utc), "error")
                next_build_silver = datetime.now(timezone.utc) + timedelta(
                    seconds=int(args.build_silver_interval_seconds)
                )

            _write_heartbeat(
                heartbeat_path,
                {
                    "ts_utc": datetime.now(timezone.utc).isoformat(),
                    "loop_started_utc": loop_started.isoformat(),
                    "availability_interval_s": int(interval_s),
                    "jitter_s": float(jitter_s),
                    "stations_refresh_interval_hours": float(args.stations_refresh_interval_hours),
                    "build_silver_interval_s": None
                    if args.build_silver_interval_seconds is None
                    else int(args.build_silver_interval_seconds),
                    "last_success_utc": last_success_utc,
                    "last_error_utc": last_error_utc,
                    "last_error": last_error,
                    "tdx_rate_limit_count": int(tdx_rate_limit_count),
                    "ok_snapshots": int(ok),
                    "backoff_s": float(backoff_base),
                    "disk": disk,
                    "deleted_files": int(deleted),
                    "datasets": dataset_metrics,
                    "backoffs": backoff_states,
                },
            )
            _write_heartbeat(
                metrics_path,
                {
                    "ts_utc": datetime.now(timezone.utc).isoformat(),
                    "datasets": dataset_metrics,
                    "disk": disk,
                    "deleted_files": int(deleted),
                    "backoff_s": float(backoff_base),
                    "backoffs": backoff_states,
                },
            )

            elapsed = (datetime.now(timezone.utc) - loop_started).total_seconds()
            sleep_s = max(interval_s - elapsed, 0.0)
            if jitter_s:
                sleep_s += random.random() * jitter_s
            if sleep_s > 0:
                logger.info("Sleeping %.1fs", sleep_s)
                time.sleep(sleep_s)
    _release_lock(lock_path)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Stopped by user (KeyboardInterrupt).")
