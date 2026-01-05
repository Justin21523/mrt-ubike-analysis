from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Mapping, Optional

from metrobikeatlas.config.models import (
    AppConfig,
    AppSettings,
    CacheSettings,
    LoggingSettings,
    SpatialSettings,
    TDXBikeSettings,
    TDXMetroSettings,
    TDXSettings,
    TemporalSettings,
    WebMapSettings,
    WebSettings,
)


def load_dotenv_if_available(path: str = ".env") -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
    except ModuleNotFoundError:
        return
    load_dotenv(path)


def _as_path(value: str, *, base_dir: Path) -> Path:
    candidate = Path(value)
    return candidate if candidate.is_absolute() else (base_dir / candidate)


def load_config(path: Optional[str | Path] = None, *, base_dir: Optional[Path] = None) -> AppConfig:
    """
    Load typed application config from JSON.

    - Path resolution is relative to `base_dir` (defaults to current working directory).
    - `.env` is loaded when python-dotenv is installed (dev convenience).
    """

    load_dotenv_if_available()

    config_path = Path(
        path
        or os.getenv("METROBIKEATLAS_CONFIG_PATH", "config/default.json")
    ).resolve()
    base_dir = (base_dir or Path.cwd()).resolve()

    raw = json.loads(config_path.read_text(encoding="utf-8"))

    app_raw: Mapping[str, Any] = raw.get("app", {})
    app = AppSettings(
        name=str(app_raw.get("name", "MetroBikeAtlas")),
        demo_mode=bool(app_raw.get("demo_mode", True)),
    )

    tdx_raw: Mapping[str, Any] = raw.get("tdx", {})
    base_url = tdx_raw.get("base_url")
    token_url = tdx_raw.get("token_url")
    if not base_url or not token_url:
        raise ValueError("Config missing required fields: tdx.base_url and/or tdx.token_url")
    metro_raw: Mapping[str, Any] = tdx_raw.get("metro", {})
    bike_raw: Mapping[str, Any] = tdx_raw.get("bike", {})
    tdx = TDXSettings(
        base_url=str(base_url),
        token_url=str(token_url),
        metro=TDXMetroSettings(
            cities=list(metro_raw.get("cities", [])),
            stations_path_template=str(metro_raw.get("stations_path_template")),
            ridership_path_template=metro_raw.get("ridership_path_template"),
        ),
        bike=TDXBikeSettings(
            cities=list(bike_raw.get("cities", [])),
            stations_path_template=str(bike_raw.get("stations_path_template")),
            availability_path_template=str(bike_raw.get("availability_path_template")),
        ),
    )

    temporal_raw: Mapping[str, Any] = raw.get("temporal", {})
    temporal = TemporalSettings(
        timezone=str(temporal_raw.get("timezone", "Asia/Taipei")),
        granularity=str(temporal_raw.get("granularity", "hour")),  # type: ignore[arg-type]
    )
    if temporal.granularity not in ("15min", "hour", "day"):
        raise ValueError(f"Unsupported granularity: {temporal.granularity}")

    spatial_raw: Mapping[str, Any] = raw.get("spatial", {})
    spatial = SpatialSettings(
        join_method=str(spatial_raw.get("join_method", "buffer")),  # type: ignore[arg-type]
        radius_m=float(spatial_raw.get("radius_m", 500)),
        nearest_k=int(spatial_raw.get("nearest_k", 3)),
    )
    if spatial.join_method not in ("buffer", "nearest"):
        raise ValueError(f"Unsupported join_method: {spatial.join_method}")

    cache_raw: Mapping[str, Any] = raw.get("cache", {})
    cache = CacheSettings(
        dir=_as_path(str(cache_raw.get("dir", "data/cache")), base_dir=base_dir),
        ttl_seconds=int(cache_raw.get("ttl_seconds", 3600)),
    )

    logging_raw: Mapping[str, Any] = raw.get("logging", {})
    file_value = logging_raw.get("file")
    log_file = None if not file_value else _as_path(str(file_value), base_dir=base_dir)
    logging_settings = LoggingSettings(
        level=str(logging_raw.get("level", "INFO")),
        format=str(logging_raw.get("format", "%(asctime)s %(levelname)s %(name)s - %(message)s")),
        file=log_file,
    )

    web_raw: Mapping[str, Any] = raw.get("web", {})
    map_raw: Mapping[str, Any] = web_raw.get("map", {})
    web = WebSettings(
        static_dir=_as_path(str(web_raw.get("static_dir", "web")), base_dir=base_dir),
        map=WebMapSettings(
            center_lat=float(map_raw.get("center_lat", 25.0375)),
            center_lon=float(map_raw.get("center_lon", 121.5637)),
            zoom=int(map_raw.get("zoom", 12)),
        ),
    )

    return AppConfig(
        app=app,
        tdx=tdx,
        temporal=temporal,
        spatial=spatial,
        cache=cache,
        logging=logging_settings,
        web=web,
    )
