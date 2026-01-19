from __future__ import annotations

import sys
from pathlib import Path

# Allow running scripts without requiring an editable install (`pip install -e .`).
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_PATH))

import argparse
from datetime import datetime, timedelta, timezone
import json
import logging
import os
import random
import time

import pandas as pd
import requests

from metrobikeatlas.ingestion.external_inputs import load_external_weather_hourly_csv, validate_external_weather_hourly_df
from metrobikeatlas.ingestion.open_meteo import default_open_meteo_params, normalize_open_meteo_hourly, utc_now_iso
from metrobikeatlas.utils.logging import configure_logging
from metrobikeatlas.config.loader import load_config


logger = logging.getLogger(__name__)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _read_existing(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["ts", "city", "temp_c", "precip_mm", "humidity_pct"])
    try:
        df = load_external_weather_hourly_csv(path)
        # Loader normalizes columns and ts already; just ensure correct order.
        return df[["ts", "city", "temp_c", "precip_mm", "humidity_pct"]].copy()
    except Exception as e:
        logger.warning("Failed to read existing weather CSV (%s); starting fresh. err=%s", path, e)
        return pd.DataFrame(columns=["ts", "city", "temp_c", "precip_mm", "humidity_pct"])


def _dedupe_sort(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["ts_dt"] = pd.to_datetime(out["ts"], utc=True, errors="coerce")
    out = out.dropna(subset=["ts_dt"]).copy()
    out["city"] = out["city"].astype(str)
    out = out.drop_duplicates(subset=["city", "ts_dt"], keep="last")
    out = out.sort_values(["city", "ts_dt"]).copy()
    out["ts"] = out["ts_dt"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    out = out.drop(columns=["ts_dt"])
    return out[["ts", "city", "temp_c", "precip_mm", "humidity_pct"]].copy()


def _retain_window(df: pd.DataFrame, *, retain_days: int | None) -> pd.DataFrame:
    if df.empty or not retain_days or retain_days <= 0:
        return df
    ts = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    if ts.isna().all():
        return df
    cutoff = datetime.now(timezone.utc) - timedelta(days=int(retain_days))
    keep = ts >= cutoff
    return df.loc[keep].copy()


def _fetch_open_meteo(*, session: requests.Session, base_url: str, params: dict[str, str]) -> dict[str, object]:
    resp = session.get(base_url, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict):
        raise ValueError("Open-Meteo response is not an object")
    return data


def _once(
    *,
    out_path: Path,
    city: str,
    lat: float,
    lon: float,
    past_days: int,
    retain_days: int | None,
    heartbeat_path: Path,
    min_request_interval_s: float,
    request_jitter_s: float,
) -> None:
    started = time.time()
    now_iso = utc_now_iso()
    session = requests.Session()
    base_url = os.getenv("OPEN_METEO_BASE_URL", "https://api.open-meteo.com/v1/forecast")

    # Client-side throttle (keeps behavior predictable and polite).
    sleep_s = max(float(min_request_interval_s), 0.0) + random.uniform(0.0, max(float(request_jitter_s), 0.0))
    if sleep_s > 0:
        time.sleep(sleep_s)

    params = default_open_meteo_params(lat=lat, lon=lon, past_days=past_days)
    data = _fetch_open_meteo(session=session, base_url=base_url, params=params)
    fresh = normalize_open_meteo_hourly(data, city=city)

    existing = _read_existing(out_path)
    merged = pd.concat([existing, fresh], ignore_index=True)
    merged = _dedupe_sort(merged)
    merged = _retain_window(merged, retain_days=retain_days)
    merged = _dedupe_sort(merged)

    issues = validate_external_weather_hourly_df(merged)
    errors = [i for i in issues if i.level == "error"]
    if errors:
        raise RuntimeError("Weather validation failed: " + "; ".join(i.message for i in errors[:5]))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(".csv.tmp")
    merged.to_csv(tmp, index=False)
    tmp.replace(out_path)

    duration_s = time.time() - started
    _write_json(
        heartbeat_path,
        {
            "ts_utc": now_iso,
            "city": city,
            "lat": float(lat),
            "lon": float(lon),
            "past_days": int(past_days),
            "retain_days": None if not retain_days else int(retain_days),
            "rows": int(len(merged)),
            "duration_s": float(duration_s),
            "out_path": str(out_path),
            "ok": True,
            "last_error": None,
        },
    )

    logger.info("Updated %s rows=%s duration_s=%.2f", out_path, len(merged), duration_s)


def main() -> int:
    p = argparse.ArgumentParser(description="Continuously fetch hourly weather data from Open-Meteo into data/external.")
    p.add_argument("--out", default="data/external/weather_hourly.csv")
    p.add_argument("--city", default=os.getenv("WEATHER_CITY", "Taipei"))
    p.add_argument("--lat", type=float, default=float(os.getenv("WEATHER_LAT", "25.0330")))
    p.add_argument("--lon", type=float, default=float(os.getenv("WEATHER_LON", "121.5654")))
    p.add_argument("--interval-seconds", type=int, default=int(os.getenv("WEATHER_INTERVAL_SECONDS", "1800")))
    p.add_argument("--past-days", type=int, default=int(os.getenv("WEATHER_PAST_DAYS", "7")))
    p.add_argument("--retain-days", type=int, default=int(os.getenv("WEATHER_RETAIN_DAYS", "365")))
    p.add_argument("--min-request-interval-s", type=float, default=float(os.getenv("WEATHER_MIN_REQUEST_INTERVAL_S", "1.0")))
    p.add_argument("--request-jitter-s", type=float, default=float(os.getenv("WEATHER_REQUEST_JITTER_S", "0.5")))
    args = p.parse_args()

    cfg = load_config()
    configure_logging(cfg.logging)

    repo_root = PROJECT_ROOT
    heartbeat_path = repo_root / "logs" / "weather_heartbeat.json"

    out_path = Path(args.out)
    interval_s = max(int(args.interval_seconds), 60)
    retain_days = int(args.retain_days) if int(args.retain_days) > 0 else None

    last_error: str | None = None
    while True:
        try:
            _once(
                out_path=out_path,
                city=str(args.city),
                lat=float(args.lat),
                lon=float(args.lon),
                past_days=int(args.past_days),
                retain_days=retain_days,
                heartbeat_path=heartbeat_path,
                min_request_interval_s=float(args.min_request_interval_s),
                request_jitter_s=float(args.request_jitter_s),
            )
            last_error = None
        except Exception as e:
            last_error = str(e)
            logger.exception("Weather collection failed; will retry.")
            _write_json(
                heartbeat_path,
                {
                    "ts_utc": utc_now_iso(),
                    "ok": False,
                    "last_error": last_error,
                    "out_path": str(out_path),
                },
            )

        time.sleep(float(interval_s))


if __name__ == "__main__":
    raise SystemExit(main())

