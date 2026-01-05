from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from metrobikeatlas.api.routes import router
from metrobikeatlas.api.service import StationService
from metrobikeatlas.config.models import AppConfig
from metrobikeatlas.utils.logging import configure_logging


def create_app(config: AppConfig) -> FastAPI:
    configure_logging(config.logging)

    app = FastAPI(title=config.app.name)
    app.state.station_service = StationService(config)
    app.include_router(router)

    static_dir = config.web.static_dir
    assets_dir = static_dir / "static"
    index_html = static_dir / "index.html"

    if assets_dir.exists():
        app.mount("/static", StaticFiles(directory=assets_dir), name="static")

    @app.get("/", include_in_schema=False)
    def index() -> FileResponse:
        return FileResponse(index_html)

    return app


def resolve_repo_root() -> Path:
    return Path(__file__).resolve().parents[3]

