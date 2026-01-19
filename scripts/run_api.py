# Use postponed evaluation of annotations so type hints don't require importing types at runtime.
# This helps avoid import cycles and keeps startup lightweight (especially important in CLI scripts).
from __future__ import annotations

# Allow running scripts without requiring an editable install (`pip install -e .`).
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_PATH))

# We import `uvicorn` to run our FastAPI application as an ASGI server during local development.
# In production you would typically run Uvicorn via a process manager (e.g., systemd, Docker, k8s).
import uvicorn
import os

# We use a factory function so the FastAPI app can be created with a typed config (no global state).
from metrobikeatlas.api.app import create_app

# We load config at runtime so settings can be changed via `config/default.json` or environment variables
# without modifying code (production-minded configuration management).
from metrobikeatlas.config.loader import load_config


# This `main()` function is the script's single entrypoint, which keeps all side effects
# (config IO, app creation, server startup) in one place and makes the module import-safe.
def main() -> None:
    # Read the typed application config (timezone, join radius, demo mode, etc.).
    config = load_config()

    # Build the FastAPI app with routes + dependency wiring based on the config.
    app = create_app(config)

    # Start a local dev server on localhost; the static web UI is served by the same app.
    host = os.getenv("METROBIKEATLAS_HOST", "127.0.0.1")
    port = int(os.getenv("METROBIKEATLAS_PORT", "8000"))
    proxy_headers = os.getenv("METROBIKEATLAS_PROXY_HEADERS", "false").strip().lower() in {"1", "true", "yes", "on"}
    forwarded_allow_ips = os.getenv("METROBIKEATLAS_FORWARDED_ALLOW_IPS", "127.0.0.1")
    timeout_keep_alive = int(os.getenv("METROBIKEATLAS_TIMEOUT_KEEP_ALIVE", "75"))
    timeout_graceful_shutdown = int(os.getenv("METROBIKEATLAS_TIMEOUT_GRACEFUL_SHUTDOWN", "30"))

    uvicorn.run(
        app,
        host=host,
        port=port,
        proxy_headers=proxy_headers,
        forwarded_allow_ips=forwarded_allow_ips,
        timeout_keep_alive=timeout_keep_alive,
        timeout_graceful_shutdown=timeout_graceful_shutdown,
    )


if __name__ == "__main__":
    # This guard prevents accidental side effects when the module is imported by tests or other scripts.
    main()
