from __future__ import annotations

from typing import Any

from metrobikeatlas.demo.repository import DemoRepository
from metrobikeatlas.config.models import AppConfig
from metrobikeatlas.repository.local import LocalRepository


class StationService:
    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self._repo = DemoRepository(config) if config.app.demo_mode else LocalRepository(config)

    @property
    def config(self) -> AppConfig:
        return self._config

    def list_stations(self) -> list[dict[str, Any]]:
        return self._repo.list_metro_stations()

    def list_bike_stations(self) -> list[dict[str, Any]]:
        return self._repo.list_bike_stations()

    def station_timeseries(
        self,
        station_id: str,
        *,
        join_method: str | None = None,
        radius_m: float | None = None,
        nearest_k: int | None = None,
        granularity: str | None = None,
        timezone: str | None = None,
        window_days: int | None = None,
        metro_series: str = "auto",
    ) -> dict[str, Any]:
        return self._repo.station_timeseries(
            station_id,
            join_method=join_method,
            radius_m=radius_m,
            nearest_k=nearest_k,
            granularity=granularity,
            timezone=timezone,
            window_days=window_days,
            metro_series=metro_series,
        )

    def nearby_bike(
        self,
        station_id: str,
        *,
        join_method: str | None = None,
        radius_m: float | None = None,
        nearest_k: int | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        return self._repo.nearby_bike(
            station_id,
            join_method=join_method,
            radius_m=radius_m,
            nearest_k=nearest_k,
            limit=limit,
        )

    def station_factors(self, station_id: str) -> dict[str, Any]:
        return self._repo.station_factors(station_id)

    def similar_stations(
        self,
        station_id: str,
        *,
        top_k: int | None = None,
        metric: str | None = None,
        standardize: bool | None = None,
    ) -> list[dict[str, Any]]:
        return self._repo.similar_stations(
            station_id,
            top_k=top_k,
            metric=metric,
            standardize=standardize,
        )

    def analytics_overview(self) -> dict[str, Any]:
        return self._repo.analytics_overview()
