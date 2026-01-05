from __future__ import annotations

from typing import Any

from metrobikeatlas.demo.repository import DemoRepository
from metrobikeatlas.config.models import AppConfig
from metrobikeatlas.repository.local import LocalRepository


class StationService:
    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self._repo = DemoRepository(config) if config.app.demo_mode else LocalRepository(config)

    def list_stations(self) -> list[dict[str, Any]]:
        return self._repo.list_metro_stations()

    def station_timeseries(self, station_id: str) -> dict[str, Any]:
        return self._repo.station_timeseries(station_id)

    def nearby_bike(self, station_id: str) -> list[dict[str, Any]]:
        return self._repo.nearby_bike(station_id)
