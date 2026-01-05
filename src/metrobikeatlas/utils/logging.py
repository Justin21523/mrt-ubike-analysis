from __future__ import annotations

import logging
from typing import Optional

from metrobikeatlas.config.models import LoggingSettings


def configure_logging(settings: LoggingSettings) -> None:
    level = getattr(logging, settings.level.upper(), None)
    if not isinstance(level, int):
        raise ValueError(f"Invalid log level: {settings.level}")

    handlers: Optional[list[logging.Handler]] = None
    if settings.file is not None:
        settings.file.parent.mkdir(parents=True, exist_ok=True)
        handlers = [logging.FileHandler(settings.file, encoding="utf-8"), logging.StreamHandler()]

    logging.basicConfig(level=level, format=settings.format, handlers=handlers)

