# Use postponed evaluation of annotations so type hints stay as strings at runtime.
# This reduces import-time coupling (helpful for larger apps with many modules).
from __future__ import annotations

# We use `Path` to work with filesystem paths in a cross-platform way (no manual string joins).
from pathlib import Path

# `FastAPI` is the Python web framework that exposes our data as HTTP endpoints for the web UI.
from fastapi import FastAPI

# `FileResponse` efficiently streams a file from disk (used for serving `web/index.html`).
from fastapi.responses import FileResponse

# `StaticFiles` serves assets (JS/CSS) so the browser (DOM) can load the dashboard bundle.
from fastapi.staticfiles import StaticFiles

# API routes are defined in a separate module to keep the app factory small and testable.
from metrobikeatlas.api.routes import router

# `StationService` is our application service layer: it reads data (demo or local Silver/Gold)
# and shapes it into payloads that the frontend consumes.
from metrobikeatlas.api.service import StationService

# `AppConfig` is the typed config model so we can avoid globals and magic strings.
from metrobikeatlas.config.models import AppConfig

# Central logging configuration keeps operational debugging consistent across scripts and the API.
from metrobikeatlas.utils.logging import configure_logging


# This app factory builds the FastAPI application from a typed config.
# Keeping app construction in a function (instead of module-level globals) improves testability and reuse.
def create_app(config: AppConfig) -> FastAPI:
    # Configure Python logging early so every subsequent log line follows the same format/level.
    # Pitfall: `logging.basicConfig(...)` is a no-op if handlers already exist (common in notebooks/tests),
    # so treat this as best-effort for local/dev.
    configure_logging(config.logging)

    # Create the FastAPI application instance; the title shows up in the OpenAPI docs.
    app = FastAPI(title=config.app.name)

    # Store the service on `app.state` so route handlers can access it without global variables.
    # This is a simple dependency-injection pattern that keeps the dataflow explicit.
    app.state.station_service = StationService(config)

    # Register all API endpoints (e.g., `/stations`, `/station/{id}/timeseries`, `/config`).
    app.include_router(router)

    # The web UI lives under `web/` (configurable). It is a plain HTML page + static assets.
    static_dir = config.web.static_dir

    # We serve JS/CSS from `/static/*` (browser will request these files to render the dashboard DOM).
    assets_dir = static_dir / "static"

    # The root route (`/`) returns `index.html` so the user can open the dashboard in a browser.
    index_html = static_dir / "index.html"

    # Only mount `/static` if the directory exists so the API can still run in minimal environments.
    if assets_dir.exists():
        # `StaticFiles` does basic file serving; for high-traffic production you would typically
        # put a CDN / reverse proxy in front (and add caching headers).
        app.mount("/static", StaticFiles(directory=assets_dir), name="static")

    @app.get("/", include_in_schema=False)
    def index() -> FileResponse:
        # Serve the HTML entrypoint; the browser then loads `/static/app.js` and renders the UI.
        # Pitfall: if `index.html` is missing, this will 500; keep the repo layout consistent.
        return FileResponse(index_html)

    # Return the fully constructed app so callers (scripts/tests) can decide how to run it.
    return app


# This helper resolves a stable repo root path for scripts that need to locate files reliably.
def resolve_repo_root() -> Path:
    # Resolve the repository root from this file location.
    # This is useful for scripts that need paths relative to the project without relying on CWD.
    return Path(__file__).resolve().parents[3]
