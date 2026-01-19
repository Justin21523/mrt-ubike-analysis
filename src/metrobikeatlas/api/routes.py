from __future__ import annotations

# `datetime` gives a stable "now" timestamp for status endpoints.
from datetime import datetime, timezone
# `asyncio` powers lightweight SSE event streaming without extra dependencies.
import asyncio
# `json` serializes SSE payloads for the browser.
import json
# `os` is used for process checks (collector PID).
import os
# `Path` reads local artifacts (Bronze/Silver, logs).
from pathlib import Path
# `signal` is used to stop background collector processes.
import signal
# `subprocess` is used to start/trigger local maintenance jobs.
import subprocess
# `sys.executable` ensures we invoke scripts with the current interpreter.
import sys
# We use `Literal` to restrict certain query parameters to a small, documented set of values.
# We use `Optional[...]` for parameters that can be omitted so the server can fall back to config defaults.
from typing import Literal, Optional

# FastAPI primitives:
# - `APIRouter` groups endpoints (routes) so the app factory can include them cleanly.
# - `Depends` performs dependency injection (DI) per request (no global variables needed).
# - `HTTPException` converts Python errors into proper HTTP status codes + JSON error payloads.
# - `Request` gives access to `app.state` where we store our service object.
from fastapi import APIRouter, Depends, HTTPException, Request, UploadFile, File, Response
from fastapi.responses import FileResponse
from fastapi.responses import StreamingResponse

# Pydantic response models:
# These models define the JSON schema returned to the browser and validate payload shape at runtime.
# This matters for maintainability: the frontend (DOM + fetch) can rely on stable fields and types.
from metrobikeatlas.api.schemas import (
    AdminActionOut,
    AnalyticsOverviewOut,
    AlertOut,
    AppConfigOut,
    AppStatusOut,
    BikeStationOut,
    HeatAtResponseOut,
    HeatIndexResponseOut,
    CollectorStatusOut,
    CollectorStartIn,
    DatasetStatusOut,
    BriefingSnapshotIn,
    BriefingSnapshotOut,
    ExternalPreviewOut,
    ExternalValidationOut,
    FileStatusOut,
    HotspotStationOut,
    HotspotsOut,
    JobOut,
    JobRerunIn,
    JobEventsOut,
    JobEventOut,
    MetroAvailabilityPointOut,
    MetroHeatPointOut,
    NearbyBikeOut,
    NearbyBikeResponseOut,
    ReplayOut,
    SimilarStationOut,
    StationFactorsOut,
    StationOut,
    StationsResponseOut,
    StationTimeSeriesOut,
    MetaOut,
    TimeIndexOut,
)

# Local async job runner for maintenance tasks (localhost-only endpoints).
from metrobikeatlas.api.jobs import JobManager, parse_mba_event_timeline
from metrobikeatlas.api.briefing_store import BriefingSnapshotStore
# `StationService` is our thin application layer that hides whether we are in demo mode or real-data mode.
# Dataflow: HTTP request -> route handler -> StationService -> repository -> dict payload -> Pydantic model -> JSON response.
from metrobikeatlas.api.service import StationService


# A router is like a "mini app": it holds endpoints that can be attached to a FastAPI application.
router = APIRouter()


# Dependency provider: FastAPI will call this function per request when a handler declares `Depends(get_service)`.
# We keep the service on `app.state` so it is constructed once in the app factory (not per request).
def get_service(request: Request) -> StationService:
    # `app.state` is a generic container, so mypy cannot know the attribute exists; hence the `type: ignore`.
    # Pitfall: if `StationService` is not attached in `create_app`, this will raise `AttributeError` at runtime.
    return request.app.state.station_service  # type: ignore[attr-defined]


def get_job_manager(request: Request) -> JobManager:
    return request.app.state.job_manager  # type: ignore[attr-defined]


def get_briefing_store(request: Request) -> BriefingSnapshotStore:
    return request.app.state.briefing_store  # type: ignore[attr-defined]


# Config endpoint: the dashboard uses this to initialize UI defaults (e.g., join radius, granularity).
@router.get("/config", response_model=AppConfigOut)
def get_config(service: StationService = Depends(get_service)) -> AppConfigOut:
    # Read a snapshot of the typed config so the frontend can render defaults consistently with the backend.
    cfg = service.config
    # Return a Pydantic model instance; FastAPI will serialize it to JSON for the browser.
    return AppConfigOut(
        # Basic app metadata helps the UI show the correct title and which mode it is running in.
        app_name=cfg.app.name,
        demo_mode=cfg.app.demo_mode,
        # Temporal config controls how time series are aligned for visualization (15min/hour/day, timezone).
        temporal={
            "timezone": cfg.temporal.timezone,
            "granularity": cfg.temporal.granularity,
        },
        # Spatial config controls how bike stations are associated with metro stations (buffer radius vs nearest K).
        spatial={
            "join_method": cfg.spatial.join_method,
            "radius_m": cfg.spatial.radius_m,
            "nearest_k": cfg.spatial.nearest_k,
        },
        # Analytics config controls similarity and clustering (used by "similar stations" in the UI).
        analytics={
            "similarity": {
                "top_k": cfg.analytics.similarity.top_k,
                "metric": cfg.analytics.similarity.metric,
                "standardize": cfg.analytics.similarity.standardize,
            },
            "clustering": {
                "k": cfg.analytics.clustering.k,
                "standardize": cfg.analytics.clustering.standardize,
            },
        },
        # Map defaults let the UI start at a sensible location without hard-coding values in JS.
        web_map={
            "center_lat": cfg.web.map.center_lat,
            "center_lon": cfg.web.map.center_lon,
            "zoom": cfg.web.map.zoom,
        },
    )


@router.get("/meta", response_model=MetaOut)
def get_meta(service: StationService = Depends(get_service), response: Response = None) -> MetaOut:  # type: ignore[assignment]
    """
    Lightweight global metadata endpoint for traceability and UI consistency.
    """

    repo_root = _resolve_repo_root()
    bronze_dir = repo_root / "data" / "bronze"
    heartbeat = _read_collector_heartbeat(repo_root)
    build_meta = _read_silver_build_meta(repo_root)

    bronze_avail = _dataset_status("tdx:bike:availability", bronze_dir / "tdx" / "bike" / "availability")
    now = datetime.now(timezone.utc)
    hb = heartbeat if isinstance(heartbeat, dict) else None
    sla: dict[str, object] = {}
    if hb is not None:
        datasets = hb.get("datasets") if isinstance(hb.get("datasets"), dict) else {}
        backoffs = hb.get("backoffs") if isinstance(hb.get("backoffs"), dict) else {}
        for k, v in datasets.items():
            if not isinstance(v, dict):
                continue
            ok = int(v.get("ok") or 0)
            err = int(v.get("error") or 0)
            total = ok + err
            rate = (ok / total) if total else None
            next_allowed = None
            b = backoffs.get(k) if isinstance(backoffs.get(k), dict) else None
            if isinstance(b, dict):
                next_allowed = b.get("next_allowed_utc")
            sla[k] = {
                "ok": ok,
                "error": err,
                "success_rate": rate,
                "last_ok_utc": v.get("last_ok_utc"),
                "last_error_utc": v.get("last_error_utc"),
                "next_run_utc": next_allowed,
            }
    out = MetaOut(
        now_utc=now,
        demo_mode=bool(service.config.app.demo_mode),
        meta=_meta_contract(service=service, extra={"collector_sla": sla}),
        silver_build_meta=build_meta,
        collector_heartbeat=heartbeat,
        bronze={"bike_availability": bronze_avail.model_dump(mode="json")},
    )
    if response is not None:
        etag = None
        bid = out.meta.get("silver_build_id")
        hb_ts = None
        if isinstance(out.collector_heartbeat, dict):
            hb_ts = out.collector_heartbeat.get("ts_utc")
        if bid or hb_ts:
            etag = f"W/\"meta-{bid or 'none'}-{hb_ts or 'none'}\""
        _set_cache_headers(response, etag=etag, max_age_s=5)
    return out


def _file_status(path: Path) -> FileStatusOut:
    if not path.exists():
        return FileStatusOut(path=str(path), exists=False, mtime_utc=None, size_bytes=None)
    st = path.stat()
    return FileStatusOut(
        path=str(path),
        exists=True,
        mtime_utc=datetime.fromtimestamp(st.st_mtime, tz=timezone.utc),
        size_bytes=int(st.st_size),
    )


def _dataset_status(label: str, dataset_dir: Path) -> DatasetStatusOut:
    dataset_dir = Path(dataset_dir)
    latest: Path | None = None
    count = 0
    if dataset_dir.exists():
        for p in dataset_dir.rglob("*.json"):
            count += 1
            if latest is None or p.name > latest.name:
                latest = p
    return DatasetStatusOut(
        label=label,
        dir=str(dataset_dir),
        file_count=count,
        latest_file=None if latest is None else _file_status(latest),
    )


def _tail_lines(path: Path, *, max_lines: int = 30, max_bytes: int = 64_000) -> list[str]:
    if max_lines <= 0:
        return []
    if not path.exists():
        return []
    try:
        data = path.read_bytes()
    except Exception:
        return []
    if len(data) > max_bytes:
        data = data[-max_bytes:]
    text = data.decode("utf-8", errors="replace")
    lines = [ln.rstrip("\n") for ln in text.splitlines()]
    return lines[-max_lines:]


def _read_collector_heartbeat(repo_root: Path) -> dict[str, object] | None:
    path = repo_root / "logs" / "collector_heartbeat.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _collector_running_from_heartbeat(
    heartbeat: dict[str, object] | None,
    *,
    now: datetime,
    default_stale_seconds: float = 900.0,
) -> bool:
    if not isinstance(heartbeat, dict):
        return False
    ts = heartbeat.get("ts_utc")
    if not ts:
        return False
    try:
        hb_ts = datetime.fromisoformat(str(ts)).astimezone(timezone.utc)
    except Exception:
        return False
    age_s = max((now - hb_ts).total_seconds(), 0.0)

    interval_s = heartbeat.get("availability_interval_s")
    try:
        interval_f = float(interval_s) if interval_s is not None else None
    except Exception:
        interval_f = None

    if interval_f is None or interval_f <= 0:
        return age_s <= float(default_stale_seconds)

    # Consider the collector "running" if it has written a heartbeat within ~2 intervals (plus slack).
    # This avoids false negatives for conservative collection intervals (e.g. 10 minutes).
    stale_s = max(2.0 * interval_f + 60.0, 180.0)
    return age_s <= stale_s


def _read_silver_build_meta(repo_root: Path) -> dict[str, object] | None:
    path = repo_root / "data" / "silver" / "_build_meta.json"
    if not path.exists():
        return None
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


def _meta_contract(
    *,
    service: StationService,
    fallback_source: str | None = None,
    extra: dict[str, object] | None = None,
) -> dict[str, object]:
    repo_root = _resolve_repo_root()
    build_meta = _read_silver_build_meta(repo_root)
    build_id = build_meta.get("build_id") if isinstance(build_meta, dict) else None
    inputs_hash = build_meta.get("inputs_hash") if isinstance(build_meta, dict) else None

    silver_dir = repo_root / "data" / "silver"
    calendar_silver = silver_dir / "calendar.csv"
    weather_silver = silver_dir / "weather_hourly.csv"
    db_path = silver_dir / "metrobikeatlas.db"
    has_calendar = calendar_silver.exists()
    has_weather = weather_silver.exists()
    has_sqlite = db_path.exists()

    external_sources = (build_meta.get("inputs") or {}).get("sources") if isinstance(build_meta, dict) else None
    ext: dict[str, object] = {
        "has_calendar": bool(has_calendar),
        "has_weather_hourly": bool(has_weather),
        "has_sqlite": bool(has_sqlite),
        "silver": {
            "calendar": _file_status(calendar_silver).model_dump(mode="json"),
            "weather_hourly": _file_status(weather_silver).model_dump(mode="json"),
            "sqlite_db": _file_status(db_path).model_dump(mode="json"),
        },
        "sources": external_sources if isinstance(external_sources, dict) else {},
    }

    if service.config.app.demo_mode:
        source = "demo"
    else:
        source = fallback_source or "silver"

    out: dict[str, object] = {
        "demo_mode": bool(service.config.app.demo_mode),
        "fallback_source": source,
        "silver_build_id": build_id,
        "inputs_hash": inputs_hash,
        "silver_build_meta": build_meta,
        "external": ext,
    }
    if extra:
        out.update(extra)
    return out


def _set_cache_headers(resp: Response, *, etag: str | None, max_age_s: int = 10) -> None:
    if etag:
        resp.headers["ETag"] = etag
    resp.headers["Cache-Control"] = f"private, max-age={int(max_age_s)}, must-revalidate"


def _read_jobs_index(repo_root: Path) -> dict[str, object] | None:
    path = repo_root / "logs" / "jobs" / "index.json"
    if not path.exists():
        return None
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


def _recent_build_silver_failures(repo_root: Path, *, window_seconds: int = 3600) -> tuple[int, datetime | None]:
    idx = _read_jobs_index(repo_root)
    items = idx.get("jobs") if isinstance(idx, dict) else None
    if not isinstance(items, list):
        return 0, None

    now = datetime.now(timezone.utc)
    count = 0
    last: datetime | None = None
    for it in items:
        if not isinstance(it, dict):
            continue
        if str(it.get("kind") or "build_silver") != "build_silver":
            continue
        rc = it.get("returncode")
        if rc is None:
            continue
        try:
            rc_i = int(rc)
        except Exception:
            continue
        if rc_i == 0:
            continue
        finished_raw = it.get("finished_at_utc")
        if not finished_raw:
            continue
        try:
            finished = datetime.fromisoformat(str(finished_raw)).astimezone(timezone.utc)
        except Exception:
            continue
        age_s = max((now - finished).total_seconds(), 0.0)
        if age_s > max(int(window_seconds), 1):
            continue
        count += 1
        if last is None or finished > last:
            last = finished
    return count, last


def _send_webhook(url: str, *, kind: str, payload: dict[str, object], timeout_s: float = 2.0) -> None:
    import urllib.request

    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, method="POST", data=data)
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=timeout_s) as _resp:
        return


def _notify_critical_alerts(repo_root: Path, alerts: list[AlertOut]) -> None:
    url = os.getenv("METROBIKEATLAS_ALERT_WEBHOOK_URL")
    if not url:
        return

    critical = [a for a in alerts if str(a.level).lower() == "critical"]
    if not critical:
        return

    kind = (os.getenv("METROBIKEATLAS_ALERT_WEBHOOK_KIND") or "").strip().lower()
    if not kind:
        if "slack.com" in url:
            kind = "slack"
        elif "discord.com" in url or "discordapp.com" in url:
            kind = "discord"
        else:
            kind = "generic"

    # Dedupe so polling `/status` doesn't spam notifications.
    state_dir = repo_root / "logs" / "alerts"
    state_dir.mkdir(parents=True, exist_ok=True)
    state_path = state_dir / "notify_state.json"

    sig = json.dumps(
        [{"title": a.title, "message": a.message} for a in critical],
        sort_keys=True,
        ensure_ascii=False,
    )
    now = datetime.now(timezone.utc)
    ttl_s = int(os.getenv("METROBIKEATLAS_ALERT_WEBHOOK_TTL_S") or "900")

    last_sig: str | None = None
    last_sent: datetime | None = None
    if state_path.exists():
        try:
            st = json.loads(state_path.read_text(encoding="utf-8"))
            if isinstance(st, dict):
                last_sig = st.get("last_sig")
                sent_raw = st.get("last_sent_utc")
                if sent_raw:
                    last_sent = datetime.fromisoformat(str(sent_raw)).astimezone(timezone.utc)
        except Exception:
            pass

    if last_sig == sig and last_sent is not None:
        age_s = max((now - last_sent).total_seconds(), 0.0)
        if age_s < max(ttl_s, 1):
            return

    text_lines = ["MetroBikeAtlas critical alert(s):"]
    for a in critical[:5]:
        text_lines.append(f"- {a.title}: {a.message}")
    text = "\n".join(text_lines)

    payload: dict[str, object]
    if kind == "slack":
        payload = {"text": text}
    elif kind == "discord":
        payload = {"content": text}
    else:
        payload = {
            "ts_utc": now.isoformat(),
            "level": "critical",
            "alerts": [a.model_dump(mode="json") for a in critical],
            "text": text,
        }

    try:
        _send_webhook(url, kind=kind, payload=payload, timeout_s=2.0)
    except Exception:
        return

    tmp = state_path.with_suffix(".json.tmp")
    tmp.write_text(
        json.dumps({"last_sig": sig, "last_sent_utc": now.isoformat()}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    tmp.replace(state_path)


def _is_pid_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _resolve_repo_root() -> Path:
    # Keep this local to avoid circular imports (`api.app` imports this module).
    return Path(__file__).resolve().parents[3]


def _parse_log_timestamp_utc(line: str) -> datetime | None:
    # Example: "2026-01-18 14:28:17,785 INFO ..."
    try:
        prefix = line[:23]
        dt = datetime.strptime(prefix, "%Y-%m-%d %H:%M:%S,%f")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _count_metro_404s(log_path: Path, *, max_bytes: int = 512_000) -> tuple[int, datetime | None]:
    if not log_path.exists():
        return 0, None
    try:
        data = log_path.read_bytes()
    except Exception:
        return 0, None
    if len(data) > max_bytes:
        data = data[-max_bytes:]
    text = data.decode("utf-8", errors="replace")
    count = 0
    last_ts: datetime | None = None
    for ln in text.splitlines():
        if "Rail/Metro/Station" in ln and "TDX request failed (404)" in ln:
            count += 1
            ts = _parse_log_timestamp_utc(ln)
            if ts is not None:
                last_ts = ts
    return count, last_ts


def _require_localhost(request: Request) -> None:
    host = getattr(getattr(request, "client", None), "host", None)
    if host in {"127.0.0.1", "::1"}:
        return

    # Optional remote admin: require a shared token and apply lightweight rate limiting.
    admin_token = os.getenv("METROBIKEATLAS_ADMIN_TOKEN")
    provided = request.headers.get("X-Admin-Token")
    if not admin_token or not provided or provided != admin_token:
        raise HTTPException(
            status_code=403,
            detail={
                "code": "admin_forbidden",
                "message": "Admin endpoints are only available on localhost (or require X-Admin-Token).",
            },
        )

    # Rate limit per client host to reduce accidental abuse.
    limiter = getattr(request.app.state, "admin_rate_limiter", None)
    if not isinstance(limiter, dict):
        return
    key = host or "unknown"
    now = datetime.now(timezone.utc).timestamp()
    window_s = 60.0
    limit = 30
    entry = limiter.get(key)
    if not entry or not isinstance(entry, dict):
        limiter[key] = {"window_start": now, "count": 1}
        return
    start = float(entry.get("window_start") or now)
    count = int(entry.get("count") or 0)
    if now - start > window_s:
        limiter[key] = {"window_start": now, "count": 1}
        return
    count += 1
    entry["count"] = count
    if count > limit:
        raise HTTPException(
            status_code=429,
            detail={"code": "admin_rate_limited", "message": "Too many admin requests. Please slow down."},
        )


def _script_paths(repo_root: Path) -> tuple[Path, Path]:
    logs_dir = repo_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    pid_path = logs_dir / "tdx_collect.pid"
    log_path = logs_dir / "tdx_collect.log"
    return pid_path, log_path


def _read_pid(path: Path) -> int | None:
    try:
        return int(path.read_text(encoding="utf-8").strip())
    except Exception:
        return None


def _stop_pid(pid: int) -> None:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return

    # Prefer killing the process group (collector spawns subprocesses for build_silver).
    try:
        os.killpg(pid, signal.SIGTERM)
        return
    except Exception:
        pass
    try:
        os.kill(pid, signal.SIGTERM)
    except Exception:
        return


def _start_collector_process(repo_root: Path, body: CollectorStartIn) -> tuple[int, list[str]]:
    pid_path, log_path = _script_paths(repo_root)
    scripts_dir = repo_root / "scripts"
    cmd = [
        sys.executable,
        str(scripts_dir / "collect_tdx_continuous.py"),
        "--availability-interval-seconds",
        str(int(body.availability_interval_seconds)),
        "--stations-refresh-interval-hours",
        str(float(body.stations_refresh_interval_hours)),
        "--jitter-seconds",
        str(float(body.jitter_seconds)),
    ]
    if body.build_silver_interval_seconds is not None:
        cmd += ["--build-silver-interval-seconds", str(int(body.build_silver_interval_seconds))]

    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("ab") as f:
        proc = subprocess.Popen(
            cmd,
            cwd=str(repo_root),
            stdout=f,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            env=os.environ.copy(),
        )
    pid_path.write_text(str(proc.pid), encoding="utf-8")
    return proc.pid, cmd


def _stop_collector_if_running(repo_root: Path) -> int | None:
    pid_path, _log_path = _script_paths(repo_root)
    pid = _read_pid(pid_path)
    if pid is None:
        return None
    _stop_pid(pid)
    try:
        pid_path.unlink()
    except Exception:
        pass
    return pid


def _build_silver_plan(repo_root: Path) -> dict[str, object]:
    scripts_dir = repo_root / "scripts"
    silver_dir = repo_root / "data" / "silver"
    cmd = [sys.executable, str(scripts_dir / "build_silver_locked.py")]
    # Best-effort: infer the metro station source the build will likely use.
    bronze_metro_root = repo_root / "data" / "bronze" / "tdx" / "metro" / "stations"
    external_csv = repo_root / "data" / "external" / "metro_stations.csv"
    metro_source = "unknown"
    reason = ""
    try:
        has_bronze = bool(list(bronze_metro_root.rglob("*.json"))[:1])
    except Exception:
        has_bronze = False
    if has_bronze:
        metro_source = "tdx_bronze"
        reason = "found Bronze metro station snapshots"
    elif external_csv.exists():
        metro_source = "external_csv"
        reason = "Bronze missing; external CSV exists"
    else:
        metro_source = "missing"
        reason = "no Bronze metro snapshots and external CSV missing"
    artifacts = [
        _file_status(silver_dir / "metro_stations.csv").model_dump(mode="json"),
        _file_status(silver_dir / "bike_stations.csv").model_dump(mode="json"),
        _file_status(silver_dir / "metro_bike_links.csv").model_dump(mode="json"),
        _file_status(silver_dir / "bike_timeseries.csv").model_dump(mode="json"),
        _file_status(silver_dir / "metro_timeseries.csv").model_dump(mode="json"),
        _file_status(silver_dir / "_build_meta.json").model_dump(mode="json"),
    ]
    would_overwrite = [a for a in artifacts if a.get("exists")]
    return {
        "command": cmd,
        "cwd": str(repo_root),
        "silver_dir": str(silver_dir),
        "artifacts": artifacts,
        "would_overwrite": would_overwrite,
        "metro_source": metro_source,
        "metro_source_reason": reason,
    }


def _read_external_metro_stations(repo_root: Path) -> list[dict[str, object]] | None:
    """
    Optional /stations fallback when TDX metro stations are unavailable.

    Expected CSV: `data/external/metro_stations.csv`
    Columns: station_id,name,lat,lon,(city),(system)
    """

    path = repo_root / "data" / "external" / "metro_stations.csv"
    if not path.exists():
        return None
    import csv

    rows: list[dict[str, object]] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            station_id = (r.get("station_id") or r.get("id") or "").strip()
            name = (r.get("name") or "").strip()
            if not station_id or not name:
                continue
            try:
                lat = float(r.get("lat") or "")
                lon = float(r.get("lon") or "")
            except ValueError:
                continue
            rows.append(
                {
                    "station_id": station_id,
                    "name": name,
                    "lat": lat,
                    "lon": lon,
                    "city": (r.get("city") or None),
                    "system": (r.get("system") or None),
                    "district": (r.get("district") or None),
                    "cluster": None,
                    "source": "external_csv",
                }
            )
    return rows


def _resolved_station_query_meta(
    *,
    station_id: str,
    service: StationService,
    endpoint: str,
    join_method: str | None,
    radius_m: float | None,
    nearest_k: int | None,
    granularity: str | None = None,
    timezone: str | None = None,
    window_days: int | None = None,
    metro_series: str | None = None,
    limit: int | None = None,
) -> dict[str, object]:
    cfg = service.config
    repo_root = _resolve_repo_root()
    silver_dir = repo_root / "data" / "silver"
    silver_build_meta = _read_silver_build_meta(repo_root)
    build_id = silver_build_meta.get("build_id") if isinstance(silver_build_meta, dict) else None
    inputs_hash = silver_build_meta.get("inputs_hash") if isinstance(silver_build_meta, dict) else None

    resolved = {
        "join_method": join_method or cfg.spatial.join_method,
        "radius_m": cfg.spatial.radius_m if radius_m is None else float(radius_m),
        "nearest_k": cfg.spatial.nearest_k if nearest_k is None else int(nearest_k),
        "granularity": cfg.temporal.granularity if granularity is None else str(granularity),
        "timezone": cfg.temporal.timezone if timezone is None else str(timezone),
        "window_days": None if window_days is None else int(window_days),
        "metro_series": metro_series,
        "limit": None if limit is None else int(limit),
    }

    artifacts = [
        _file_status(silver_dir / "metro_stations.csv").model_dump(mode="json"),
        _file_status(silver_dir / "bike_stations.csv").model_dump(mode="json"),
        _file_status(silver_dir / "metro_bike_links.csv").model_dump(mode="json"),
        _file_status(silver_dir / "bike_timeseries.csv").model_dump(mode="json"),
        _file_status(silver_dir / "metro_timeseries.csv").model_dump(mode="json"),
        _file_status(silver_dir / "calendar.csv").model_dump(mode="json"),
        _file_status(silver_dir / "weather_hourly.csv").model_dump(mode="json"),
        _file_status(silver_dir / "metrobikeatlas.db").model_dump(mode="json"),
    ]

    query: dict[str, object] = {}
    for k, v in {
        "join_method": join_method,
        "radius_m": radius_m,
        "nearest_k": nearest_k,
        "granularity": granularity,
        "timezone": timezone,
        "window_days": window_days,
        "metro_series": metro_series,
        "limit": limit,
    }.items():
        if v is not None:
            query[k] = v

    return {
        "endpoint": endpoint,
        "station_id": station_id,
        "demo_mode": bool(cfg.app.demo_mode),
        "fallback_source": "demo" if bool(cfg.app.demo_mode) else "silver",
        "silver_build_id": build_id,
        "inputs_hash": inputs_hash,
        "query": query,
        "resolved": resolved,
        "silver_artifacts": artifacts,
        "silver_build_meta": silver_build_meta,
        "external": _meta_contract(service=service).get("external"),
    }


def _external_metro_is_sample(repo_root: Path) -> bool:
    path = repo_root / "data" / "external" / "metro_stations.csv"
    if not path.exists():
        return False
    import csv

    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            ids = []
            for r in reader:
                if r.get("station_id"):
                    ids.append(str(r["station_id"]).strip())
                if len(ids) > 10:
                    break
    except Exception:
        return False
    if not ids:
        return False
    if len(ids) <= 3 and all(i.startswith("MRT_EX_") for i in ids):
        return True
    return False


@router.get("/status", response_model=AppStatusOut)
def get_status(service: StationService = Depends(get_service), response: Response = None) -> AppStatusOut:  # type: ignore[assignment]
    repo_root = _resolve_repo_root()
    bronze_dir = repo_root / "data" / "bronze"
    silver_dir = repo_root / "data" / "silver"

    silver_tables = [
        _file_status(silver_dir / "metro_stations.csv"),
        _file_status(silver_dir / "bike_stations.csv"),
        _file_status(silver_dir / "metro_bike_links.csv"),
        _file_status(silver_dir / "bike_timeseries.csv"),
        _file_status(silver_dir / "metro_timeseries.csv"),
    ]

    bronze_datasets = [
        _dataset_status("tdx:bike:stations", bronze_dir / "tdx" / "bike" / "stations"),
        _dataset_status("tdx:bike:availability", bronze_dir / "tdx" / "bike" / "availability"),
        _dataset_status("tdx:metro:stations", bronze_dir / "tdx" / "metro" / "stations"),
    ]

    logs_dir = repo_root / "logs"
    pid_path = logs_dir / "tdx_collect.pid"
    log_path = logs_dir / "tdx_collect.log"
    collector: CollectorStatusOut | None = None
    if pid_path.exists() or log_path.exists():
        pid: int | None = None
        running = False
        try:
            pid = int(pid_path.read_text(encoding="utf-8").strip()) if pid_path.exists() else None
        except Exception:
            pid = None
        if pid is not None:
            running = _is_pid_running(pid)

        collector = CollectorStatusOut(
            pid_path=str(pid_path),
            log_path=str(log_path),
            running=running,
            pid=pid,
            log_tail=_tail_lines(log_path, max_lines=30),
        )

    metro_404_count, metro_404_last = _count_metro_404s(log_path)
    heartbeat = _read_collector_heartbeat(repo_root)
    silver_build_meta = _read_silver_build_meta(repo_root)

    alerts: list[AlertOut] = []
    now = datetime.now(timezone.utc)
    collector_running_from_hb = _collector_running_from_heartbeat(heartbeat, now=now)

    required_silver = [
        silver_dir / "metro_stations.csv",
        silver_dir / "bike_stations.csv",
        silver_dir / "metro_bike_links.csv",
    ]
    missing_required = [p for p in required_silver if not p.exists()]
    if not service.config.app.demo_mode and missing_required:
        alerts.append(
            AlertOut(
                level="warning",
                title="Real-data onboarding: Silver missing",
                message="Real data mode is enabled but required Silver tables are missing.",
                commands=[
                    "python scripts/run_pipeline_mvp.py --collect-duration-seconds 1800 --collect-interval-seconds 300",
                    "python scripts/build_silver.py",
                    "python scripts/validate_silver.py --strict",
                ],
            )
        )

    if collector is not None and collector.pid is not None and not collector.running and not collector_running_from_hb:
        alerts.append(
            AlertOut(
                level="warning",
                title="Collector not running",
                message="Collector PID file exists but the process is not running.",
                commands=[
                    "python scripts/collect_tdx_continuous.py --availability-interval-seconds 600 --stations-refresh-interval-hours 24 --jitter-seconds 5 --build-silver-interval-seconds 1800",
                ],
            )
        )

    if (collector is not None and collector.running) or collector_running_from_hb:
        if heartbeat is None:
            alerts.append(
                AlertOut(
                    level="critical",
                    title="Collector heartbeat missing",
                    message="Collector seems active but heartbeat file is missing. Collector may be outdated or stuck.",
                    commands=[
                        "python scripts/run_api.py  # open Data page to restart collector",
                        "python scripts/restart_collector_if_stale.py --force",
                    ],
                )
            )
        else:
            try:
                hb_ts = datetime.fromisoformat(str(heartbeat.get("ts_utc"))).astimezone(timezone.utc)
                age_s = max((now - hb_ts).total_seconds(), 0.0)
                if age_s > 900:
                    alerts.append(
                        AlertOut(
                            level="critical",
                            title="Collector heartbeat stale",
                            message=f"Collector heartbeat is stale ({int(age_s)}s). Collector may be stuck.",
                            commands=[
                                "python scripts/run_api.py  # open Data page to stop/start collector",
                                "python scripts/restart_collector_if_stale.py",
                            ],
                        )
                    )
            except Exception:
                pass

    if metro_404_count:
        alerts.append(
            AlertOut(
                level="warning",
                title="TDX metro stations 404",
                message=(
                    "TDX metro station endpoint is returning 404 for the configured path. "
                    "Bike datasets can still be collected."
                ),
                commands=[
                    "METROBIKEATLAS_DEMO_MODE=true python scripts/run_api.py",
                    "# Optional fallback: provide `data/external/metro_stations.csv` (station_id,name,lat,lon,city,system)",
                ],
            )
        )

    external_metro = repo_root / "data" / "external" / "metro_stations.csv"
    if external_metro.exists():
        alerts.append(
            AlertOut(
                level="info",
                title="External metro station source detected",
                message="`data/external/metro_stations.csv` exists; `/stations` can fall back to it if Silver is missing.",
                commands=[],
            )
        )
        if _external_metro_is_sample(repo_root):
            alerts.append(
                AlertOut(
                    level="warning",
                    title="External metro stations look like sample data",
                    message="`data/external/metro_stations.csv` appears to be the example file (very small). Replace it with real station data.",
                    commands=[
                        "cp data/external/metro_stations.csv.example data/external/metro_stations.csv  # then edit",
                        "python scripts/validate_external_inputs.py --strict",
                        "python scripts/build_silver.py --external-metro-stations-csv data/external/metro_stations.csv",
                    ],
                )
            )

    # `now` is defined earlier (used by heartbeat-derived collector status)
    def _age_seconds(ts: datetime | None) -> float | None:
        if ts is None:
            return None
        return max((now - ts).total_seconds(), 0.0)

    def _mtime_for(path: str) -> datetime | None:
        for f in silver_tables:
            if f.path == path and f.mtime_utc is not None:
                return f.mtime_utc
        return None

    bronze_avail_latest = next((d.latest_file for d in bronze_datasets if d.label == "tdx:bike:availability"), None)
    silver_links_mtime = _mtime_for(str(silver_dir / "metro_bike_links.csv"))
    silver_bike_ts_mtime = _mtime_for(str(silver_dir / "bike_timeseries.csv"))
    build_fail_count_1h, build_fail_last = _recent_build_silver_failures(repo_root, window_seconds=3600)
    if build_fail_count_1h >= 3:
        alerts.append(
            AlertOut(
                level="critical",
                title="Repeated Silver build failures",
                message=f"{build_fail_count_1h} build_silver jobs failed in the last hour.",
                commands=[
                    "python scripts/run_api.py  # Data → Job Center → download logs",
                    "python scripts/build_silver.py",
                ],
            )
        )

    collector_running_effective = (collector.running if collector is not None else False) or collector_running_from_hb

    health = {
        "collector_running": bool(collector_running_effective),
        "collector_pid": collector.pid if (collector is not None and collector.running) else None,
        "bronze_bike_availability_last_utc": None if bronze_avail_latest is None else bronze_avail_latest.mtime_utc,
        "bronze_bike_availability_age_s": None
        if bronze_avail_latest is None
        else _age_seconds(bronze_avail_latest.mtime_utc),
        "silver_metro_bike_links_last_utc": silver_links_mtime,
        "silver_metro_bike_links_age_s": _age_seconds(silver_links_mtime),
        "silver_bike_timeseries_last_utc": silver_bike_ts_mtime,
        "silver_bike_timeseries_age_s": _age_seconds(silver_bike_ts_mtime),
        "build_silver_failures_1h": int(build_fail_count_1h),
        "build_silver_last_failure_utc": build_fail_last,
        "metro_tdx_404_count": int(metro_404_count),
        "metro_tdx_404_last_utc": metro_404_last,
    }
    if heartbeat is not None:
        health["collector_heartbeat"] = heartbeat
    if silver_build_meta is not None:
        health["silver_build_id"] = silver_build_meta.get("build_id")
        health["silver_build_finished_utc"] = silver_build_meta.get("finished_at_utc")
        health["silver_inputs_hash"] = silver_build_meta.get("inputs_hash")

    _notify_critical_alerts(repo_root, alerts)

    out = AppStatusOut(
        now_utc=now,
        demo_mode=service.config.app.demo_mode,
        bronze_dir=str(bronze_dir),
        silver_dir=str(silver_dir),
        tdx={
            "base_url": service.config.tdx.base_url,
            "metro_cities": list(service.config.tdx.metro.cities),
            "metro_stations_path_template": service.config.tdx.metro.stations_path_template,
            "bike_cities": list(service.config.tdx.bike.cities),
            "bike_stations_path_template": service.config.tdx.bike.stations_path_template,
            "bike_availability_path_template": service.config.tdx.bike.availability_path_template,
        },
        health=health,
        silver_tables=silver_tables,
        bronze_datasets=bronze_datasets,
        collector=collector,
        alerts=alerts,
        metro_tdx_404_count=int(metro_404_count),
        metro_tdx_404_last_utc=metro_404_last,
    )
    if response is not None:
        etag = None
        bid = None
        if "silver_build_id" in health:
            bid = health.get("silver_build_id")
        hb_ts = None
        if isinstance(heartbeat, dict):
            hb_ts = heartbeat.get("ts_utc")
        etag = f"W/\"status-{bid or 'none'}-{hb_ts or 'none'}\""
        _set_cache_headers(response, etag=etag, max_age_s=3)
    return out


@router.get("/events")
async def events(
    request: Request,
    service: StationService = Depends(get_service),
    job_manager: JobManager = Depends(get_job_manager),
    interval_s: float = 3.0,
) -> StreamingResponse:
    """
    Server-Sent Events stream for lightweight live updates.

    This intentionally publishes only operational snapshots (status + jobs) and avoids sending large datasets.
    """

    tick_s = max(float(interval_s), 1.0)

    async def gen():
        # Tell the browser to retry the connection on transient failures.
        yield "retry: 3000\n\n"
        last_status_sig: str | None = None
        last_alerts_sig: str | None = None
        last_heat_sig: str | None = None
        last_hb_sig: str | None = None
        last_silver_sig: str | None = None
        last_jobs_sig: str | None = None
        last_job_by_id: dict[str, str] = {}
        while True:
            if await request.is_disconnected():
                return

            try:
                status = get_status(service)
                status_dict = status.model_dump(mode="json")
                jobs = [
                    JobOut.model_validate(job_manager.to_public_dict(j)).model_dump(mode="json")
                    for j in job_manager.list_jobs(limit=20)
                ]

                heat_latest_ts: str | None = None
                try:
                    idx = service.metro_heat_index(limit=1)
                    if idx and idx[-1].get("ts") is not None:
                        heat_latest_ts = str(idx[-1]["ts"].isoformat())  # type: ignore[union-attr]
                except Exception:
                    heat_latest_ts = None

                now = datetime.now(timezone.utc).isoformat()

                # status (ignore now_utc for change detection)
                status_for_sig = dict(status_dict)
                status_for_sig.pop("now_utc", None)
                status_sig = json.dumps(status_for_sig, sort_keys=True, ensure_ascii=False)
                if status_sig != last_status_sig:
                    last_status_sig = status_sig
                    status_payload = json.dumps({"ts_utc": now, "status": status_dict}, ensure_ascii=False)
                    yield f"event: status\ndata: {status_payload}\n\n"

                # alerts
                alerts_list = status_dict.get("alerts", [])
                alerts_sig = json.dumps(alerts_list, sort_keys=True, ensure_ascii=False)
                if alerts_sig != last_alerts_sig:
                    last_alerts_sig = alerts_sig
                    alerts_payload = json.dumps({"ts_utc": now, "alerts": alerts_list}, ensure_ascii=False)
                    yield f"event: alerts\ndata: {alerts_payload}\n\n"

                # heat latest ts
                heat_sig = json.dumps({"heat_latest_ts": heat_latest_ts}, sort_keys=True, ensure_ascii=False)
                if heat_sig != last_heat_sig:
                    last_heat_sig = heat_sig
                    heat_payload = json.dumps({"ts_utc": now, "heat_latest_ts": heat_latest_ts}, ensure_ascii=False)
                    yield f"event: heat\ndata: {heat_payload}\n\n"

                # collector heartbeat
                hb = (status_dict.get("health") or {}).get("collector_heartbeat")
                hb_sig = json.dumps(hb, sort_keys=True, ensure_ascii=False) if hb is not None else ""
                if hb_sig != last_hb_sig:
                    last_hb_sig = hb_sig
                    hb_payload = json.dumps({"ts_utc": now, "collector_heartbeat": hb}, ensure_ascii=False)
                    yield f"event: collector_heartbeat\ndata: {hb_payload}\n\n"

                # silver freshness changed (based on key silver mtimes)
                health = status_dict.get("health") or {}
                silver_sig = json.dumps(
                    {
                        "silver_metro_bike_links_last_utc": health.get("silver_metro_bike_links_last_utc"),
                        "silver_bike_timeseries_last_utc": health.get("silver_bike_timeseries_last_utc"),
                    },
                    sort_keys=True,
                    ensure_ascii=False,
                )
                if silver_sig != last_silver_sig:
                    last_silver_sig = silver_sig
                    sil_payload = json.dumps(
                        {
                            "ts_utc": now,
                            "silver_freshness": {
                                "silver_metro_bike_links_last_utc": health.get("silver_metro_bike_links_last_utc"),
                                "silver_bike_timeseries_last_utc": health.get("silver_bike_timeseries_last_utc"),
                            },
                        },
                        ensure_ascii=False,
                    )
                    yield f"event: silver_freshness_changed\ndata: {sil_payload}\n\n"

                # jobs list + per-job updates
                jobs_sig = json.dumps(jobs, sort_keys=True, ensure_ascii=False)
                if jobs_sig != last_jobs_sig:
                    last_jobs_sig = jobs_sig
                    jobs_payload = json.dumps({"ts_utc": now, "jobs": jobs}, ensure_ascii=False)
                    yield f"event: jobs\ndata: {jobs_payload}\n\n"

                current = {}
                for j in jobs:
                    jid = str(j.get("id") or "")
                    if not jid:
                        continue
                    summary = {
                        "id": jid,
                        "status": j.get("status"),
                        "stage": j.get("stage"),
                        "progress_pct": j.get("progress_pct"),
                        "kind": j.get("kind"),
                    }
                    current[jid] = json.dumps(summary, sort_keys=True, ensure_ascii=False)
                    if last_job_by_id.get(jid) != current[jid]:
                        last_job_by_id[jid] = current[jid]
                        upd = json.dumps({"ts_utc": now, "job": j}, ensure_ascii=False)
                        yield f"event: job_update\ndata: {upd}\n\n"
            except Exception as e:
                # Never crash the stream on serialization/runtime issues; send an error event instead.
                err = json.dumps(
                    {"type": "error", "ts_utc": datetime.now(timezone.utc).isoformat(), "detail": str(e)},
                    ensure_ascii=False,
                )
                yield f"event: sse_error\ndata: {err}\n\n"

            # Explicit heartbeat event for proxies/browsers that ignore comments.
            hb_payload = json.dumps({"ts_utc": datetime.now(timezone.utc).isoformat()}, ensure_ascii=False)
            yield f"event: heartbeat\ndata: {hb_payload}\n\n"

            # Keep-alive comment (helps some proxies/browsers keep the connection open).
            yield ": ping\n\n"
            await asyncio.sleep(tick_s)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/admin/collector/start", response_model=AdminActionOut)
def admin_start_collector(
    body: CollectorStartIn,
    request: Request,
) -> AdminActionOut:
    _require_localhost(request)
    repo_root = _resolve_repo_root()
    pid_path, log_path = _script_paths(repo_root)

    existing_pid = _read_pid(pid_path)
    if existing_pid is not None and _is_pid_running(existing_pid):
        return AdminActionOut(ok=True, pid=existing_pid, detail="Collector already running.")

    pid, cmd = _start_collector_process(repo_root, body)
    return AdminActionOut(ok=True, pid=pid, detail="Collector started.", meta={"command": cmd})


@router.post("/admin/collector/stop", response_model=AdminActionOut)
def admin_stop_collector(request: Request) -> AdminActionOut:
    _require_localhost(request)
    repo_root = _resolve_repo_root()
    pid_path, _log_path = _script_paths(repo_root)

    pid = _read_pid(pid_path)
    if pid is None:
        return AdminActionOut(ok=True, pid=None, detail="No PID file found.")
    stopped = _stop_collector_if_running(repo_root)
    return AdminActionOut(ok=True, pid=pid, detail="Stop signal sent.", meta={"stopped_pid": stopped})


@router.post("/admin/collector/restart_if_stale", response_model=AdminActionOut)
def admin_restart_collector_if_stale(
    body: CollectorStartIn,
    request: Request,
    stale_after_seconds: int = 900,
    force: bool = False,
) -> AdminActionOut:
    """
    Restart the background collector only when it appears stale.

    Criteria (unless `force=true`):
    - Collector process is not running, OR
    - Heartbeat file is missing while collector is running, OR
    - Heartbeat timestamp older than `stale_after_seconds` while collector is running.
    """

    _require_localhost(request)
    repo_root = _resolve_repo_root()
    pid_path, _log_path = _script_paths(repo_root)

    existing_pid = _read_pid(pid_path)
    running = existing_pid is not None and _is_pid_running(existing_pid)

    heartbeat = _read_collector_heartbeat(repo_root)
    hb_age_s: float | None = None
    try:
        if heartbeat is not None and heartbeat.get("ts_utc"):
            hb_ts = datetime.fromisoformat(str(heartbeat.get("ts_utc"))).astimezone(timezone.utc)
            hb_age_s = max((datetime.now(timezone.utc) - hb_ts).total_seconds(), 0.0)
    except Exception:
        hb_age_s = None

    should_restart = False
    reason = "healthy"
    if force:
        should_restart = True
        reason = "forced"
    elif not running and existing_pid is not None:
        should_restart = True
        reason = "pid_not_running"
    elif running and heartbeat is None:
        should_restart = True
        reason = "heartbeat_missing"
    elif running and hb_age_s is not None and hb_age_s > max(int(stale_after_seconds), 1):
        should_restart = True
        reason = "heartbeat_stale"

    if not should_restart:
        return AdminActionOut(
            ok=True,
            pid=existing_pid,
            detail="Collector is healthy; no action taken.",
            meta={
                "action": "noop",
                "reason": reason,
                "existing_pid": existing_pid,
                "running": running,
                "heartbeat_age_s": hb_age_s,
                "stale_after_seconds": int(stale_after_seconds),
            },
        )

    stopped_pid = _stop_collector_if_running(repo_root) if existing_pid is not None else None
    new_pid, cmd = _start_collector_process(repo_root, body)
    return AdminActionOut(
        ok=True,
        pid=new_pid,
        detail="Collector restarted." if stopped_pid else "Collector started.",
        meta={
            "action": "restarted" if stopped_pid else "started",
            "reason": reason,
            "existing_pid": existing_pid,
            "stopped_pid": stopped_pid,
            "new_pid": new_pid,
            "heartbeat_age_s": hb_age_s,
            "stale_after_seconds": int(stale_after_seconds),
            "command": cmd,
        },
    )


@router.post("/admin/build_silver", response_model=AdminActionOut)
def admin_build_silver(request: Request, dry_run: bool = False) -> AdminActionOut:
    _require_localhost(request)
    repo_root = _resolve_repo_root()
    scripts_dir = repo_root / "scripts"

    cmd = [sys.executable, str(scripts_dir / "build_silver_locked.py")]
    if dry_run:
        plan = _build_silver_plan(repo_root)
        return AdminActionOut(
            ok=True,
            detail="Dry run: build_silver would execute.",
            meta=plan,
            artifacts=[FileStatusOut.model_validate(a) for a in (plan.get("artifacts") or [])],
        )
    started = datetime.now(timezone.utc)
    try:
        proc = subprocess.run(cmd, cwd=str(repo_root), check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        out = (e.stdout or "") + ("\n" + e.stderr if e.stderr else "")
        tail = [ln for ln in out.splitlines()[-30:]]
        return AdminActionOut(
            ok=False,
            detail=f"build_silver failed: exit={e.returncode}",
            duration_s=(datetime.now(timezone.utc) - started).total_seconds(),
            stdout_tail=tail,
        )

    silver_dir = repo_root / "data" / "silver"
    artifacts = [
        _file_status(silver_dir / "metro_stations.csv"),
        _file_status(silver_dir / "bike_stations.csv"),
        _file_status(silver_dir / "metro_bike_links.csv"),
        _file_status(silver_dir / "bike_timeseries.csv"),
        _file_status(silver_dir / "metro_timeseries.csv"),
    ]
    tail = [ln for ln in (proc.stdout or "").splitlines()[-30:]]
    return AdminActionOut(
        ok=True,
        detail="build_silver completed.",
        duration_s=(datetime.now(timezone.utc) - started).total_seconds(),
        artifacts=artifacts,
        stdout_tail=tail,
    )


@router.post("/admin/build_silver_async", response_model=AdminActionOut)
def admin_build_silver_async(
    request: Request,
    job_manager: JobManager = Depends(get_job_manager),
    dry_run: bool = False,
) -> AdminActionOut:
    _require_localhost(request)
    if dry_run:
        plan = _build_silver_plan(_resolve_repo_root())
        return AdminActionOut(
            ok=True,
            detail="Dry run: build_silver would start as an async job.",
            meta=plan,
            artifacts=[FileStatusOut.model_validate(a) for a in (plan.get("artifacts") or [])],
        )
    job = job_manager.start_build_silver()
    return AdminActionOut(
        ok=True,
        detail="build_silver started.",
        pid=job.pid,
        job_id=job.id,
        meta={"command": list(job.cmd) if job.cmd else None},
    )


@router.get("/admin/jobs", response_model=list[JobOut])
def admin_list_jobs(
    request: Request,
    limit: int = 20,
    job_manager: JobManager = Depends(get_job_manager),
) -> list[JobOut]:
    _require_localhost(request)
    jobs = job_manager.list_jobs(limit=limit)
    return [JobOut.model_validate(job_manager.to_public_dict(j)) for j in jobs]


@router.get("/admin/jobs/{job_id}", response_model=JobOut)
def admin_get_job(
    job_id: str,
    request: Request,
    job_manager: JobManager = Depends(get_job_manager),
) -> JobOut:
    _require_localhost(request)
    job = job_manager.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return JobOut.model_validate(job_manager.to_public_dict(job))


@router.get("/admin/jobs/{job_id}/events", response_model=JobEventsOut)
def admin_get_job_events(
    job_id: str,
    request: Request,
    job_manager: JobManager = Depends(get_job_manager),
    limit: int = 500,
) -> JobEventsOut:
    _require_localhost(request)
    job = job_manager.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    raw_events = parse_mba_event_timeline(job.log_path, max_events=max(int(limit), 1))
    events: list[JobEventOut] = []
    for ev in raw_events:
        ts = None
        try:
            if ev.get("ts_utc"):
                ts = datetime.fromisoformat(str(ev.get("ts_utc"))).astimezone(timezone.utc)
        except Exception:
            ts = None
        artifacts = ev.get("artifacts")
        artifacts_list = list(artifacts) if isinstance(artifacts, list) else []
        events.append(
            JobEventOut(
                ts_utc=ts,
                level=str(ev.get("level") or "info"),
                stage=str(ev.get("stage") or "") or None,
                progress_pct=(int(ev.get("progress_pct")) if ev.get("progress_pct") is not None else None),
                message=(str(ev.get("message")) if ev.get("message") is not None else None),
                artifacts=artifacts_list,
                raw=ev,
            )
        )

    latest = events[-1] if events else None
    latest_artifacts = latest.artifacts if latest is not None else []
    return JobEventsOut(
        job_id=job.id,
        kind=job.kind,
        events=events,
        latest=latest,
        artifacts=latest_artifacts,
    )


@router.post("/admin/jobs/{job_id}/cancel", response_model=AdminActionOut)
def admin_cancel_job(
    job_id: str,
    request: Request,
    job_manager: JobManager = Depends(get_job_manager),
) -> AdminActionOut:
    _require_localhost(request)
    ok = job_manager.cancel(job_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Job not found or not cancelable")
    return AdminActionOut(ok=True, detail="Cancel signal sent.", job_id=job_id)


@router.post("/admin/jobs/{job_id}/rerun", response_model=AdminActionOut)
def admin_rerun_job(
    job_id: str,
    request: Request,
    job_manager: JobManager = Depends(get_job_manager),
    body: JobRerunIn | None = None,
) -> AdminActionOut:
    _require_localhost(request)
    overrides: dict[str, object] | None = None
    if body is not None:
        overrides = body.model_dump(mode="json", exclude_none=True)
    job = job_manager.rerun(job_id, overrides=overrides)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found or not rerunnable")
    return AdminActionOut(
        ok=True,
        detail="Job re-run started.",
        pid=job.pid,
        job_id=job.id,
        meta={"command": list(job.cmd) if job.cmd else None},
    )


@router.get("/admin/jobs/{job_id}/log", response_class=FileResponse)
def admin_download_job_log(
    job_id: str,
    request: Request,
    job_manager: JobManager = Depends(get_job_manager),
) -> FileResponse:
    _require_localhost(request)
    job = job_manager.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    if not job.log_path.exists():
        raise HTTPException(status_code=404, detail="Job log not found")
    return FileResponse(job.log_path)


@router.post("/briefing/snapshots", response_model=BriefingSnapshotOut)
def create_briefing_snapshot(
    body: BriefingSnapshotIn,
    request: Request,
    store: BriefingSnapshotStore = Depends(get_briefing_store),
) -> BriefingSnapshotOut:
    _require_localhost(request)
    stored = store.create(body.model_dump(mode="json"))
    return BriefingSnapshotOut(id=stored.id, created_at_utc=stored.created_at_utc, snapshot=body)


@router.get("/briefing/snapshots", response_model=list[BriefingSnapshotOut])
def list_briefing_snapshots(
    request: Request,
    limit: int = 30,
    store: BriefingSnapshotStore = Depends(get_briefing_store),
) -> list[BriefingSnapshotOut]:
    _require_localhost(request)
    snaps = store.list(limit=limit)
    out: list[BriefingSnapshotOut] = []
    for s in snaps:
        out.append(
            BriefingSnapshotOut(
                id=s.id,
                created_at_utc=s.created_at_utc,
                snapshot=BriefingSnapshotIn.model_validate(s.snapshot),
            )
        )
    return out


@router.get("/insights/hotspots", response_model=HotspotsOut)
def hotspots(
    request: Request,
    metric: str = "available",
    agg: str = "sum",
    ts: str | None = None,
    top_k: int = 5,
    service: StationService = Depends(get_service),
) -> HotspotsOut:
    # Public endpoint (safe): it only returns station ids/names and derived values.
    # We keep it accessible for the story UI even when opened remotely.
    top = max(int(top_k), 1)

    # Choose timestamp
    if ts is None:
        idx = service.metro_heat_index(limit=1)
        if not idx or idx[-1].get("ts") is None:
            raise HTTPException(status_code=503, detail="Heat index unavailable")
        target_dt = idx[-1]["ts"]
        ts = target_dt.isoformat()
    try:
        rows = service.metro_heat_at(ts, metric=metric, agg=agg)
    except FileNotFoundError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Station name lookup
    try:
        stations = service.list_stations()
        name_by_id = {str(s["station_id"]): str(s.get("name") or s["station_id"]) for s in stations}
    except Exception:
        name_by_id = {}

    vals = []
    for r in rows:
        sid = str(r.get("station_id") or "")
        if not sid:
            continue
        v = float(r.get("value") or 0.0)
        vals.append((sid, v))
    vals.sort(key=lambda x: x[1], reverse=True)
    hot = vals[:top]
    cold = list(reversed(vals[-top:])) if len(vals) >= top else list(reversed(vals))

    import pandas as pd

    target_ts = pd.to_datetime(ts, utc=True, errors="coerce")
    if pd.isna(target_ts):
        raise HTTPException(status_code=400, detail=f"Invalid ts: {ts}")
    target_py = target_ts.to_pydatetime()

    def _reason(value: float, kind: str) -> str:
        m = str(metric).strip().lower()
        if m == "available":
            return "Low availability suggests potential shortage." if kind == "cold" else "High availability suggests strong supply."
        if m == "rent_proxy":
            return "High rent pressure suggests strong take demand." if kind == "hot" else "Low rent pressure suggests weaker take demand."
        if m == "return_proxy":
            return "High return pressure suggests strong return demand (possible docking pressure)." if kind == "hot" else "Low return pressure suggests weaker return demand."
        return ""

    hot_out = [
        HotspotStationOut(
            station_id=sid,
            name=name_by_id.get(sid),
            value=float(v),
            rank=i + 1,
            reason=_reason(float(v), "hot"),
        )
        for i, (sid, v) in enumerate(hot)
    ]
    cold_out = [
        HotspotStationOut(
            station_id=sid,
            name=name_by_id.get(sid),
            value=float(v),
            rank=i + 1,
            reason=_reason(float(v), "cold"),
        )
        for i, (sid, v) in enumerate(cold)
    ]

    explanation = (
        "Hotspots/coldspots are derived by ranking metro stations using the selected heat metric at a single timestamp. "
        "Use this list for narrative exploration; validate with longer windows before policy decisions."
    )
    return HotspotsOut(metric=str(metric), agg=str(agg), ts=target_py, hot=hot_out, cold=cold_out, explanation=explanation)


@router.get("/briefing/export")
def briefing_export(
    request: Request,
    station_id: str | None = None,
    join_method: Optional[Literal["buffer", "nearest"]] = None,
    radius_m: Optional[float] = None,
    nearest_k: Optional[int] = None,
    granularity: Optional[Literal["15min", "hour", "day"]] = None,
    timezone: Optional[str] = None,
    window_days: Optional[int] = None,
    metro_series: Literal["auto", "ridership", "proxy"] = "auto",
    heat_metric: str = "available",
    heat_agg: str = "sum",
    heat_ts: str | None = None,
    top_k: int = 5,
    format: Literal["json", "zip"] = "json",
    service: StationService = Depends(get_service),
) -> object:
    # Public export endpoint: designed for copying into slides/notes.
    # Keep payload reasonably small and avoid leaking filesystem paths beyond already exposed /status.
    from io import BytesIO
    import zipfile

    status = get_status(service).model_dump(mode="json")

    ts_payload = None
    nearby_payload = None
    if station_id:
        try:
            ts_payload = service.station_timeseries(
                station_id,
                join_method=join_method,
                radius_m=radius_m,
                nearest_k=nearest_k,
                granularity=granularity,
                timezone=timezone,
                window_days=window_days,
                metro_series=metro_series,
            )
            # Nearby (with meta) comes from the v2 endpoint payload shape; build locally.
            bikes = service.nearby_bike(
                station_id,
                join_method=join_method,
                radius_m=radius_m,
                nearest_k=nearest_k,
                limit=50,
            )
            nearby_payload = {"station_id": station_id, "items": bikes}
        except Exception:
            ts_payload = None
            nearby_payload = None

    # Insights hotspot list
    insights = hotspots(
        request,
        metric=heat_metric,
        agg=heat_agg,
        ts=heat_ts,
        top_k=top_k,
        service=service,
    ).model_dump(mode="json")

    export = {
        "exported_at_utc": datetime.now(timezone.utc).isoformat(),
        "station_id": station_id,
        "status": status,
        "timeseries": ts_payload,
        "nearby_bike": nearby_payload,
        "insights_hotspots": insights,
        "suggested_commands": [cmd for a in status.get("alerts", []) for cmd in (a.get("commands") or [])][:8],
    }

    if format == "json":
        return export

    buf = BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("brief.json", json.dumps(export, ensure_ascii=False, indent=2))
        if ts_payload is not None:
            z.writestr("timeseries.json", json.dumps(ts_payload, ensure_ascii=False, indent=2))
        if nearby_payload is not None:
            z.writestr("nearby_bike.json", json.dumps(nearby_payload, ensure_ascii=False, indent=2))
        z.writestr("insights_hotspots.json", json.dumps(insights, ensure_ascii=False, indent=2))

    buf.seek(0)
    headers = {"Content-Disposition": "attachment; filename=metrobikeatlas-brief.zip"}
    return StreamingResponse(buf, media_type="application/zip", headers=headers)

@router.get("/external/metro_stations/validate", response_model=ExternalValidationOut)
def validate_external_metro_stations(request: Request) -> ExternalValidationOut:
    _require_localhost(request)
    repo_root = _resolve_repo_root()
    path = repo_root / "data" / "external" / "metro_stations.csv"

    if not path.exists():
        return ExternalValidationOut(
            ok=False,
            path=str(path),
            row_count=0,
            issues=[{"level": "error", "message": "File not found"}],
            head=[],
        )

    from metrobikeatlas.ingestion.external_inputs import (
        load_external_metro_stations_csv,
        validate_external_metro_stations_df,
    )

    try:
        df = load_external_metro_stations_csv(path)
    except Exception as e:
        return ExternalValidationOut(
            ok=False,
            path=str(path),
            row_count=0,
            issues=[{"level": "error", "message": f"Failed to load CSV: {e}"}],
            head=[],
        )

    issues = validate_external_metro_stations_df(df)
    ok = not any(i.level == "error" for i in issues)
    head = df.head(5).to_dict(orient="records") if not df.empty else []
    return ExternalValidationOut(
        ok=ok,
        path=str(path),
        row_count=int(len(df)),
        issues=[{"level": i.level, "message": i.message} for i in issues],
        head=head,
    )


@router.get("/external/metro_stations/preview", response_model=ExternalPreviewOut)
def preview_external_metro_stations(
    request: Request,
    limit: int = 50,
) -> ExternalPreviewOut:
    _require_localhost(request)
    repo_root = _resolve_repo_root()
    path = repo_root / "data" / "external" / "metro_stations.csv"
    if not path.exists():
        return ExternalPreviewOut(
            ok=False,
            path=str(path),
            row_count=0,
            issues=[{"level": "error", "message": "File not found"}],
            columns=[],
            rows=[],
        )

    from metrobikeatlas.ingestion.external_inputs import (
        load_external_metro_stations_csv,
        validate_external_metro_stations_df,
    )

    import csv

    # Full validation via normalized loader.
    try:
        df = load_external_metro_stations_csv(path)
        issues = validate_external_metro_stations_df(df)
        ok = not any(i.level == "error" for i in issues)
        row_count = int(len(df))
    except Exception as e:
        return ExternalPreviewOut(
            ok=False,
            path=str(path),
            row_count=0,
            issues=[{"level": "error", "message": f"Failed to load CSV: {e}"}],
            columns=[],
            rows=[],
        )

    # Preview raw rows (preserve original columns as much as possible).
    rows: list[dict[str, object]] = []
    columns: list[str] = []
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            columns = list(reader.fieldnames or [])
            for i, r in enumerate(reader):
                if i >= max(int(limit), 1):
                    break
                row = {"_row_number": i + 2}
                row.update({k: (v if v is not None else "") for k, v in r.items()})
                rows.append(row)
    except Exception:
        rows = []
        columns = []

    return ExternalPreviewOut(
        ok=ok,
        path=str(path),
        row_count=row_count,
        issues=[{"level": i.level, "message": i.message} for i in issues],
        columns=(["_row_number"] + columns) if columns else [],
        rows=rows,
    )


@router.get("/external/metro_stations/download", response_class=FileResponse)
def download_external_metro_stations(request: Request) -> FileResponse:
    _require_localhost(request)
    repo_root = _resolve_repo_root()
    path = repo_root / "data" / "external" / "metro_stations.csv"
    if not path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path)


@router.post("/external/metro_stations/upload", response_model=ExternalValidationOut)
async def upload_external_metro_stations(
    request: Request,
    file: UploadFile = File(...),
    dry_run: bool = False,
) -> ExternalValidationOut:
    _require_localhost(request)
    repo_root = _resolve_repo_root()
    out_path = repo_root / "data" / "external" / "metro_stations.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_suffix(".csv.uploading")

    data = await file.read()
    tmp_path.write_bytes(data)

    from metrobikeatlas.ingestion.external_inputs import (
        load_external_metro_stations_csv,
        validate_external_metro_stations_df,
    )

    try:
        df = load_external_metro_stations_csv(tmp_path)
    except Exception as e:
        try:
            tmp_path.unlink()
        except Exception:
            pass
        return ExternalValidationOut(
            ok=False,
            path=str(out_path),
            row_count=0,
            issues=[{"level": "error", "message": f"Failed to load CSV: {e}"}],
            head=[],
        )

    issues = validate_external_metro_stations_df(df)
    ok = not any(i.level == "error" for i in issues)
    if not ok:
        try:
            tmp_path.unlink()
        except Exception:
            pass
        head = df.head(5).to_dict(orient="records") if not df.empty else []
        return ExternalValidationOut(
            ok=False,
            path=str(out_path),
            row_count=int(len(df)),
            issues=[{"level": i.level, "message": i.message} for i in issues],
            head=head,
        )

    if dry_run:
        try:
            tmp_path.unlink()
        except Exception:
            pass
    else:
        tmp_path.replace(out_path)
    head = df.head(5).to_dict(orient="records") if not df.empty else []
    return ExternalValidationOut(
        ok=True,
        path=str(out_path),
        row_count=int(len(df)),
        issues=[{"level": i.level, "message": i.message} for i in issues],
        head=head,
    )


@router.get("/external/calendar/validate", response_model=ExternalValidationOut)
def validate_external_calendar(request: Request) -> ExternalValidationOut:
    _require_localhost(request)
    repo_root = _resolve_repo_root()
    path = repo_root / "data" / "external" / "calendar.csv"
    if not path.exists():
        return ExternalValidationOut(
            ok=False,
            path=str(path),
            row_count=0,
            issues=[{"level": "error", "message": "File not found"}],
            head=[],
        )

    from metrobikeatlas.ingestion.external_inputs import load_external_calendar_csv, validate_external_calendar_df

    try:
        df = load_external_calendar_csv(path)
        issues = validate_external_calendar_df(df)
    except Exception as e:
        return ExternalValidationOut(
            ok=False,
            path=str(path),
            row_count=0,
            issues=[{"level": "error", "message": f"Failed to load CSV: {e}"}],
            head=[],
        )

    ok = not any(i.level == "error" for i in issues)
    head = df.head(5).to_dict(orient="records") if not df.empty else []
    return ExternalValidationOut(
        ok=ok,
        path=str(path),
        row_count=int(len(df)),
        issues=[{"level": i.level, "message": i.message} for i in issues],
        head=head,
    )


@router.get("/external/calendar/preview", response_model=ExternalPreviewOut)
def preview_external_calendar(request: Request, limit: int = 50) -> ExternalPreviewOut:
    _require_localhost(request)
    repo_root = _resolve_repo_root()
    path = repo_root / "data" / "external" / "calendar.csv"
    if not path.exists():
        return ExternalPreviewOut(
            ok=False,
            path=str(path),
            row_count=0,
            issues=[{"level": "error", "message": "File not found"}],
            columns=[],
            rows=[],
        )

    from metrobikeatlas.ingestion.external_inputs import load_external_calendar_csv, validate_external_calendar_df
    import csv

    try:
        df = load_external_calendar_csv(path)
        issues = validate_external_calendar_df(df)
        ok = not any(i.level == "error" for i in issues)
        row_count = int(len(df))
    except Exception as e:
        return ExternalPreviewOut(
            ok=False,
            path=str(path),
            row_count=0,
            issues=[{"level": "error", "message": f"Failed to load CSV: {e}"}],
            columns=[],
            rows=[],
        )

    rows: list[dict[str, object]] = []
    columns: list[str] = []
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            columns = list(reader.fieldnames or [])
            for i, r in enumerate(reader):
                if i >= max(int(limit), 1):
                    break
                row = {"_row_number": i + 2}
                row.update({k: (v if v is not None else "") for k, v in r.items()})
                rows.append(row)
    except Exception:
        rows = []
        columns = []

    return ExternalPreviewOut(
        ok=ok,
        path=str(path),
        row_count=row_count,
        issues=[{"level": i.level, "message": i.message} for i in issues],
        columns=(["_row_number"] + columns) if columns else [],
        rows=rows,
    )


@router.get("/external/calendar/download", response_class=FileResponse)
def download_external_calendar(request: Request) -> FileResponse:
    _require_localhost(request)
    repo_root = _resolve_repo_root()
    path = repo_root / "data" / "external" / "calendar.csv"
    if not path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path)


@router.post("/external/calendar/upload", response_model=ExternalValidationOut)
async def upload_external_calendar(
    request: Request,
    file: UploadFile = File(...),
    dry_run: bool = False,
) -> ExternalValidationOut:
    _require_localhost(request)
    repo_root = _resolve_repo_root()
    out_path = repo_root / "data" / "external" / "calendar.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_suffix(".csv.uploading")

    data = await file.read()
    tmp_path.write_bytes(data)

    from metrobikeatlas.ingestion.external_inputs import load_external_calendar_csv, validate_external_calendar_df

    try:
        df = load_external_calendar_csv(tmp_path)
        issues = validate_external_calendar_df(df)
    except Exception as e:
        try:
            tmp_path.unlink()
        except Exception:
            pass
        return ExternalValidationOut(
            ok=False,
            path=str(out_path),
            row_count=0,
            issues=[{"level": "error", "message": f"Failed to load CSV: {e}"}],
            head=[],
        )

    ok = not any(i.level == "error" for i in issues)
    if not ok:
        try:
            tmp_path.unlink()
        except Exception:
            pass
        head = df.head(5).to_dict(orient="records") if not df.empty else []
        return ExternalValidationOut(
            ok=False,
            path=str(out_path),
            row_count=int(len(df)),
            issues=[{"level": i.level, "message": i.message} for i in issues],
            head=head,
        )

    if dry_run:
        try:
            tmp_path.unlink()
        except Exception:
            pass
    else:
        tmp_path.replace(out_path)
    head = df.head(5).to_dict(orient="records") if not df.empty else []
    return ExternalValidationOut(
        ok=True,
        path=str(out_path),
        row_count=int(len(df)),
        issues=[{"level": i.level, "message": i.message} for i in issues],
        head=head,
    )


@router.get("/external/weather_hourly/validate", response_model=ExternalValidationOut)
def validate_external_weather_hourly(request: Request) -> ExternalValidationOut:
    _require_localhost(request)
    repo_root = _resolve_repo_root()
    path = repo_root / "data" / "external" / "weather_hourly.csv"
    if not path.exists():
        return ExternalValidationOut(
            ok=False,
            path=str(path),
            row_count=0,
            issues=[{"level": "error", "message": "File not found"}],
            head=[],
        )

    from metrobikeatlas.ingestion.external_inputs import load_external_weather_hourly_csv, validate_external_weather_hourly_df

    try:
        df = load_external_weather_hourly_csv(path)
        issues = validate_external_weather_hourly_df(df)
    except Exception as e:
        return ExternalValidationOut(
            ok=False,
            path=str(path),
            row_count=0,
            issues=[{"level": "error", "message": f"Failed to load CSV: {e}"}],
            head=[],
        )

    ok = not any(i.level == "error" for i in issues)
    head = df.head(5).to_dict(orient="records") if not df.empty else []
    return ExternalValidationOut(
        ok=ok,
        path=str(path),
        row_count=int(len(df)),
        issues=[{"level": i.level, "message": i.message} for i in issues],
        head=head,
    )


@router.get("/external/weather_hourly/preview", response_model=ExternalPreviewOut)
def preview_external_weather_hourly(request: Request, limit: int = 50) -> ExternalPreviewOut:
    _require_localhost(request)
    repo_root = _resolve_repo_root()
    path = repo_root / "data" / "external" / "weather_hourly.csv"
    if not path.exists():
        return ExternalPreviewOut(
            ok=False,
            path=str(path),
            row_count=0,
            issues=[{"level": "error", "message": "File not found"}],
            columns=[],
            rows=[],
        )

    from metrobikeatlas.ingestion.external_inputs import load_external_weather_hourly_csv, validate_external_weather_hourly_df
    import csv

    try:
        df = load_external_weather_hourly_csv(path)
        issues = validate_external_weather_hourly_df(df)
        ok = not any(i.level == "error" for i in issues)
        row_count = int(len(df))
    except Exception as e:
        return ExternalPreviewOut(
            ok=False,
            path=str(path),
            row_count=0,
            issues=[{"level": "error", "message": f"Failed to load CSV: {e}"}],
            columns=[],
            rows=[],
        )

    rows: list[dict[str, object]] = []
    columns: list[str] = []
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            columns = list(reader.fieldnames or [])
            for i, r in enumerate(reader):
                if i >= max(int(limit), 1):
                    break
                row = {"_row_number": i + 2}
                row.update({k: (v if v is not None else "") for k, v in r.items()})
                rows.append(row)
    except Exception:
        rows = []
        columns = []

    return ExternalPreviewOut(
        ok=ok,
        path=str(path),
        row_count=row_count,
        issues=[{"level": i.level, "message": i.message} for i in issues],
        columns=(["_row_number"] + columns) if columns else [],
        rows=rows,
    )


@router.get("/external/weather_hourly/download", response_class=FileResponse)
def download_external_weather_hourly(request: Request) -> FileResponse:
    _require_localhost(request)
    repo_root = _resolve_repo_root()
    path = repo_root / "data" / "external" / "weather_hourly.csv"
    if not path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path)


@router.post("/external/weather_hourly/upload", response_model=ExternalValidationOut)
async def upload_external_weather_hourly(
    request: Request,
    file: UploadFile = File(...),
    dry_run: bool = False,
) -> ExternalValidationOut:
    _require_localhost(request)
    repo_root = _resolve_repo_root()
    out_path = repo_root / "data" / "external" / "weather_hourly.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_suffix(".csv.uploading")

    data = await file.read()
    tmp_path.write_bytes(data)

    from metrobikeatlas.ingestion.external_inputs import load_external_weather_hourly_csv, validate_external_weather_hourly_df

    try:
        df = load_external_weather_hourly_csv(tmp_path)
        issues = validate_external_weather_hourly_df(df)
    except Exception as e:
        try:
            tmp_path.unlink()
        except Exception:
            pass
        return ExternalValidationOut(
            ok=False,
            path=str(out_path),
            row_count=0,
            issues=[{"level": "error", "message": f"Failed to load CSV: {e}"}],
            head=[],
        )

    ok = not any(i.level == "error" for i in issues)
    if not ok:
        try:
            tmp_path.unlink()
        except Exception:
            pass
        head = df.head(5).to_dict(orient="records") if not df.empty else []
        return ExternalValidationOut(
            ok=False,
            path=str(out_path),
            row_count=int(len(df)),
            issues=[{"level": i.level, "message": i.message} for i in issues],
            head=head,
        )

    if dry_run:
        try:
            tmp_path.unlink()
        except Exception:
            pass
    else:
        tmp_path.replace(out_path)
    head = df.head(5).to_dict(orient="records") if not df.empty else []
    return ExternalValidationOut(
        ok=True,
        path=str(out_path),
        row_count=int(len(df)),
        issues=[{"level": i.level, "message": i.message} for i in issues],
        head=head,
    )

# Bike station metadata endpoint (used for overlays and debug tooling in the dashboard).
@router.get("/bike_stations", response_model=list[BikeStationOut])
def list_bike_stations(service: StationService = Depends(get_service)) -> list[BikeStationOut]:
    # Fetch the raw list from the repository via the service layer.
    try:
        bikes = service.list_bike_stations()
    except FileNotFoundError as e:
        raise HTTPException(status_code=503, detail=str(e))
    # Convert internal dicts to a stable output schema (`BikeStationOut`) for the frontend.
    return [
        BikeStationOut(
            id=s["station_id"],
            name=s["name"],
            lat=s["lat"],
            lon=s["lon"],
            city=s.get("city"),
            operator=s.get("operator"),
            capacity=s.get("capacity"),
        )
        for s in bikes
    ]


# Metro station metadata endpoint (used to render markers on the Leaflet map in the browser).
@router.get("/stations", response_model=list[StationOut])
def list_stations(service: StationService = Depends(get_service), response: Response = None) -> list[StationOut]:  # type: ignore[assignment]
    # Fetch metro stations (optionally enriched with district/cluster if Gold tables are present).
    try:
        stations = service.list_stations()
    except FileNotFoundError as e:
        repo_root = _resolve_repo_root()
        fallback = _read_external_metro_stations(repo_root)
        if fallback is None:
            raise HTTPException(status_code=503, detail=str(e))
        stations = fallback
    # Provide lightweight meta via headers without breaking the legacy response schema.
    if response is not None:
        meta = _meta_contract(service=service)
        ext = meta.get("external") if isinstance(meta, dict) else None
        if isinstance(ext, dict):
            response.headers["X-MBA-Has-Calendar"] = "1" if ext.get("has_calendar") else "0"
            response.headers["X-MBA-Has-Weather-Hourly"] = "1" if ext.get("has_weather_hourly") else "0"
            response.headers["X-MBA-Has-SQLite"] = "1" if ext.get("has_sqlite") else "0"
        bid = meta.get("silver_build_id") if isinstance(meta, dict) else None
        if bid:
            response.headers["X-MBA-Silver-Build-Id"] = str(bid)
    # Map internal dict keys to the public API field names expected by the frontend.
    return [
        StationOut(
            id=s["station_id"],
            name=s["name"],
            lat=s["lat"],
            lon=s["lon"],
            city=s.get("city"),
            system=s.get("system"),
            district=s.get("district"),
            cluster=s.get("cluster"),
            source=s.get("source"),
        )
        for s in stations
    ]


@router.get("/stations2", response_model=StationsResponseOut)
def list_stations2(service: StationService = Depends(get_service), response: Response = None) -> StationsResponseOut:  # type: ignore[assignment]
    fallback_source: str | None = None
    try:
        stations = service.list_stations()
        # In real mode, LocalRepository labels station rows with `source="silver"`.
        # In demo mode, DemoRepository labels with `source="demo"`.
    except FileNotFoundError as e:
        repo_root = _resolve_repo_root()
        fallback = _read_external_metro_stations(repo_root)
        if fallback is None:
            raise HTTPException(status_code=503, detail=str(e))
        stations = fallback
        fallback_source = "external_csv"

    items = [
        StationOut(
            id=s["station_id"],
            name=s["name"],
            lat=s["lat"],
            lon=s["lon"],
            city=s.get("city"),
            system=s.get("system"),
            district=s.get("district"),
            cluster=s.get("cluster"),
            source=s.get("source"),
        )
        for s in stations
    ]
    meta = _meta_contract(service=service, fallback_source=fallback_source)
    if response is not None:
        etag = None
        if meta.get("silver_build_id"):
            etag = f"W/\"stations2-{meta.get('silver_build_id')}\""
        _set_cache_headers(response, etag=etag, max_age_s=30)
    return StationsResponseOut(items=items, meta=meta)


@router.get("/replay", response_model=ReplayOut)
def replay(
    station_id: str,
    join_method: Optional[Literal["buffer", "nearest"]] = None,
    radius_m: Optional[float] = None,
    nearest_k: Optional[int] = None,
    granularity: Optional[Literal["15min", "hour", "day"]] = None,
    timezone: Optional[str] = None,
    window_days: Optional[int] = None,
    metro_series: Literal["auto", "ridership", "proxy"] = "auto",
    service: StationService = Depends(get_service),
) -> ReplayOut:
    """
    Reproducible payload endpoint for debugging/ops: returns the same artifacts the UI would query.
    """

    base_meta = _meta_contract(service=service)

    stations = list_stations2(service=service)
    try:
        ts = station_timeseries(
            station_id=station_id,
            join_method=join_method,
            radius_m=radius_m,
            nearest_k=nearest_k,
            granularity=granularity,
            timezone=timezone,
            window_days=window_days,
            metro_series=metro_series,
            service=service,
        )
    except HTTPException:
        ts = None

    try:
        nb = nearby_bike2(
            station_id=station_id,
            join_method=join_method,
            radius_m=radius_m,
            nearest_k=nearest_k,
            limit=50,
            service=service,
        )
    except HTTPException:
        nb = None

    return ReplayOut(
        station_id=station_id,
        meta=base_meta,
        stations=stations,
        timeseries=ts,
        nearby_bike=nb,
    )


@router.get("/stations/bike_availability_index", response_model=TimeIndexOut)
def bike_availability_index(
    limit: int = 200, service: StationService = Depends(get_service)
) -> TimeIndexOut:
    try:
        rows = service.metro_bike_availability_index(limit=limit)
    except FileNotFoundError as e:
        raise HTTPException(status_code=503, detail=str(e))
    ts = [r["ts"] for r in rows if r.get("ts") is not None]
    return TimeIndexOut(timestamps=ts)


@router.get("/stations/bike_availability_at", response_model=list[MetroAvailabilityPointOut])
def bike_availability_at(
    ts: str, service: StationService = Depends(get_service)
) -> list[MetroAvailabilityPointOut]:
    try:
        rows = service.metro_bike_availability_at(ts)
    except FileNotFoundError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return [MetroAvailabilityPointOut.model_validate(r) for r in rows]


@router.get("/stations/heat_index", response_model=TimeIndexOut)
def heat_index(limit: int = 200, service: StationService = Depends(get_service)) -> TimeIndexOut:
    try:
        rows = service.metro_heat_index(limit=limit)
    except FileNotFoundError as e:
        raise HTTPException(status_code=503, detail=str(e))
    ts = [r["ts"] for r in rows if r.get("ts") is not None]
    return TimeIndexOut(timestamps=ts)


@router.get("/stations/heat_index2", response_model=HeatIndexResponseOut)
def heat_index2(
    limit: int = 200, service: StationService = Depends(get_service), response: Response = None  # type: ignore[assignment]
) -> HeatIndexResponseOut:
    try:
        rows = service.metro_heat_index(limit=limit)
    except FileNotFoundError as e:
        raise HTTPException(status_code=503, detail=str(e))
    ts = [r["ts"] for r in rows if r.get("ts") is not None]
    out = HeatIndexResponseOut(
        timestamps=ts,
        meta=_meta_contract(service=service),
    )
    if response is not None:
        etag = None
        bid = out.meta.get("silver_build_id") if isinstance(out.meta, dict) else None
        if bid:
            etag = f"W/\"heat_index2-{bid}-{int(limit)}\""
        _set_cache_headers(response, etag=etag, max_age_s=10)
    return out


@router.get("/stations/heat_at", response_model=list[MetroHeatPointOut])
def heat_at(
    ts: str,
    metric: str = "available",
    agg: str = "sum",
    service: StationService = Depends(get_service),
) -> list[MetroHeatPointOut]:
    try:
        rows = service.metro_heat_at(ts, metric=metric, agg=agg)
    except FileNotFoundError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return [MetroHeatPointOut.model_validate(r) for r in rows]


@router.get("/stations/heat_at2", response_model=HeatAtResponseOut)
def heat_at2(
    ts: str,
    metric: str = "available",
    agg: str = "sum",
    service: StationService = Depends(get_service),
) -> HeatAtResponseOut:
    try:
        rows = service.metro_heat_at(ts, metric=metric, agg=agg)
    except FileNotFoundError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    points = [MetroHeatPointOut.model_validate(r) for r in rows]
    repo_root = _resolve_repo_root()
    # `ts` in the response is the parsed timestamp of the first point when available,
    # otherwise we parse the query param best-effort for UI display.
    parsed_ts: datetime
    if points:
        parsed_ts = points[0].ts
    else:
        try:
            parsed_ts = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            parsed_ts = datetime.now(timezone.utc)
    return HeatAtResponseOut(
        ts=parsed_ts,
        metric=str(metric),
        agg=str(agg),
        points=points,
        meta=_meta_contract(service=service),
    )


# Timeseries endpoint: returns aligned metro + bike series for a selected metro station.
# The dashboard passes query params so the user can adjust parameters (granularity, radius, etc.) at runtime.
@router.get("/station/{station_id}/timeseries", response_model=StationTimeSeriesOut)
def station_timeseries(
    # Path parameter: the unique metro station id (used as a stable key across tables).
    station_id: str,
    # Query params: spatial join controls for which bike stations get aggregated.
    join_method: Optional[Literal["buffer", "nearest"]] = None,
    radius_m: Optional[float] = None,
    nearest_k: Optional[int] = None,
    # Query params: temporal alignment controls for bucketing timestamps.
    granularity: Optional[Literal["15min", "hour", "day"]] = None,
    timezone: Optional[str] = None,
    window_days: Optional[int] = None,
    # Query param: choose whether to prefer real ridership or a bike-derived proxy in the response.
    metro_series: Literal["auto", "ridership", "proxy"] = "auto",
    # Dependency injection: FastAPI calls `get_service` and passes the result here.
    service: StationService = Depends(get_service),
) -> StationTimeSeriesOut:
    try:
        # Delegate to the service layer so route code stays "thin" and focused on HTTP concerns.
        payload = service.station_timeseries(
            station_id,
            join_method=join_method,
            radius_m=radius_m,
            nearest_k=nearest_k,
            granularity=granularity,
            timezone=timezone,
            window_days=window_days,
            metro_series=metro_series,
        )
    except KeyError:
        # A missing station id maps to 404 so the frontend can show a "not found" message.
        raise HTTPException(status_code=404, detail="Station not found")
    except FileNotFoundError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except ValueError as e:
        # Bad user input (e.g., unsupported granularity) maps to 400 for a clear client-side error.
        # Note: FastAPI may also return 422 for validation errors before this handler runs.
        raise HTTPException(status_code=400, detail=str(e))
    base_meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    payload["meta"] = {
        **base_meta,
        **_resolved_station_query_meta(
        station_id=station_id,
        service=service,
        endpoint="timeseries",
        join_method=join_method,
        radius_m=radius_m,
        nearest_k=nearest_k,
        granularity=granularity,
        timezone=timezone,
        window_days=window_days,
        metro_series=metro_series,
        ),
    }
    # Validate the payload shape against the Pydantic model (defensive programming for API stability).
    return StationTimeSeriesOut.model_validate(payload)


# Nearby bike endpoint: returns bike stations associated with a metro station under the chosen join parameters.
@router.get("/station/{station_id}/nearby_bike", response_model=list[NearbyBikeOut])
def nearby_bike(
    # Path parameter: selected metro station.
    station_id: str,
    # Query params: choose join method and its parameterization.
    join_method: Optional[Literal["buffer", "nearest"]] = None,
    radius_m: Optional[float] = None,
    nearest_k: Optional[int] = None,
    # Query param: limit keeps payload small (important for UI performance).
    limit: Optional[int] = None,
    # Dependency injection: provides access to our service/repository without globals.
    service: StationService = Depends(get_service),
) -> list[NearbyBikeOut]:
    try:
        # Delegate the selection logic (buffer / nearest) to the repository via the service layer.
        payload = service.nearby_bike(
            station_id,
            join_method=join_method,
            radius_m=radius_m,
            nearest_k=nearest_k,
            limit=limit,
        )
    except KeyError:
        # Unknown station id -> 404.
        raise HTTPException(status_code=404, detail="Station not found")
    except FileNotFoundError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except ValueError as e:
        # Invalid join settings -> 400.
        raise HTTPException(status_code=400, detail=str(e))
    # Convert each dict into a validated response object so the frontend gets predictable fields.
    return [
        NearbyBikeOut(
            id=s["station_id"],
            name=s["name"],
            lat=s["lat"],
            lon=s["lon"],
            distance_m=s["distance_m"],
            capacity=s.get("capacity"),
        )
        for s in payload
    ]


@router.get("/station/{station_id}/nearby_bike2", response_model=NearbyBikeResponseOut)
def nearby_bike2(
    station_id: str,
    join_method: Optional[Literal["buffer", "nearest"]] = None,
    radius_m: Optional[float] = None,
    nearest_k: Optional[int] = None,
    limit: Optional[int] = None,
    service: StationService = Depends(get_service),
) -> NearbyBikeResponseOut:
    try:
        payload = service.nearby_bike(
            station_id,
            join_method=join_method,
            radius_m=radius_m,
            nearest_k=nearest_k,
            limit=limit,
        )
    except KeyError:
        raise HTTPException(status_code=404, detail="Station not found")
    except FileNotFoundError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    items = [
        NearbyBikeOut(
            id=s["station_id"],
            name=s["name"],
            lat=s["lat"],
            lon=s["lon"],
            distance_m=s["distance_m"],
            capacity=s.get("capacity"),
        )
        for s in payload
    ]
    return NearbyBikeResponseOut(
        station_id=station_id,
        items=items,
        meta=_resolved_station_query_meta(
            station_id=station_id,
            service=service,
            endpoint="nearby_bike",
            join_method=join_method,
            radius_m=radius_m,
            nearest_k=nearest_k,
            limit=limit,
        ),
    )


# Station factors endpoint: returns the feature table row for the station (if Gold features exist).
@router.get("/station/{station_id}/factors", response_model=StationFactorsOut)
def station_factors(
    station_id: str, service: StationService = Depends(get_service)
) -> StationFactorsOut:
    try:
        # Features are computed offline (Gold) and served here for UI inspection.
        payload = service.station_factors(station_id)
    except KeyError:
        # Unknown station id -> 404.
        raise HTTPException(status_code=404, detail="Station not found")
    except FileNotFoundError as e:
        raise HTTPException(status_code=503, detail=str(e))
    # Validate payload and return; the UI uses this to render the factors table in the DOM.
    return StationFactorsOut.model_validate(payload)


# Similar stations endpoint: returns a k-nearest list in feature space (for quick exploration).
@router.get("/station/{station_id}/similar", response_model=list[SimilarStationOut])
def similar_stations(
    # Path parameter: anchor station id.
    station_id: str,
    # Query params: runtime overrides for similarity settings (useful for experimentation).
    top_k: Optional[int] = None,
    metric: Optional[Literal["euclidean", "cosine"]] = None,
    standardize: Optional[bool] = None,
    # Dependency injection: provides the service.
    service: StationService = Depends(get_service),
) -> list[SimilarStationOut]:
    try:
        # Delegate the similarity computation (or lookup) to the repository layer.
        payload = service.similar_stations(
            station_id,
            top_k=top_k,
            metric=metric,
            standardize=standardize,
        )
    except KeyError:
        # Unknown station id -> 404.
        raise HTTPException(status_code=404, detail="Station not found")
    except FileNotFoundError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except ValueError as e:
        # Invalid similarity params -> 400.
        raise HTTPException(status_code=400, detail=str(e))
    # Map dict payload into the public schema consumed by the UI.
    return [
        SimilarStationOut(
            id=s["station_id"],
            name=s.get("name"),
            distance=s["distance"],
            cluster=s.get("cluster"),
        )
        for s in payload
    ]


# Analytics overview endpoint: returns precomputed global stats (correlations/regression/clusters) if present.
@router.get("/analytics/overview", response_model=AnalyticsOverviewOut)
def analytics_overview(service: StationService = Depends(get_service)) -> AnalyticsOverviewOut:
    # This is a lightweight endpoint so the UI can show a small summary without loading all Gold tables.
    try:
        payload = service.analytics_overview()
    except FileNotFoundError:
        payload = {"available": False, "correlations": [], "regression": None, "clusters": None}
    # Validate and return; the UI renders this into the sidebar list.
    return AnalyticsOverviewOut.model_validate(payload)
