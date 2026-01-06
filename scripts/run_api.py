# Use postponed evaluation of annotations so type hints don't require importing types at runtime.
# This helps avoid import cycles and keeps startup lightweight (especially important in CLI scripts).
from __future__ import annotations

# We import `uvicorn` to run our FastAPI application as an ASGI server during local development.
# In production you would typically run Uvicorn via a process manager (e.g., systemd, Docker, k8s).
import uvicorn

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
    uvicorn.run(app, host="127.0.0.1", port=8000)


if __name__ == "__main__":
    # This guard prevents accidental side effects when the module is imported by tests or other scripts.
    main()
