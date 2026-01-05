from __future__ import annotations

import uvicorn

from metrobikeatlas.api.app import create_app
from metrobikeatlas.config.loader import load_config


def main() -> None:
    config = load_config()
    app = create_app(config)
    uvicorn.run(app, host="127.0.0.1", port=8000)


if __name__ == "__main__":
    main()

