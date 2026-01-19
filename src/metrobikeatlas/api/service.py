from __future__ import annotations

# `Any` is used for our internal "dict payload" boundary between repository and service.
# In a later phase we may tighten these to TypedDicts/Pydantic models, but `Any` keeps MVP flexible.
from typing import Any

# DemoRepository provides deterministic in-memory/sample data so the UI always has something to render.
from metrobikeatlas.demo.repository import DemoRepository
# `AppConfig` is the single source of truth for runtime settings (demo mode, spatial/temporal defaults, etc.).
from metrobikeatlas.config.models import AppConfig
# LocalRepository reads our local data lake tables (Silver/Gold) from disk in "real data" mode.
from metrobikeatlas.repository.local import LocalRepository


# `StationService` is a thin application layer between HTTP routes and repositories.
# Why have a service at all (instead of calling the repository directly from routes)?
# - Keeps route handlers focused on HTTP concerns (status codes, query params, response models).
# - Centralizes "mode switching" (demo vs real data) without global variables.
# - Provides a stable API for the frontend: route -> service -> repo -> dict -> Pydantic -> JSON -> DOM.
class StationService:
    # We inject config so this object is easy to construct in tests and does not read globals/env at import time.
    def __init__(self, config: AppConfig) -> None:
        # Store the typed config so routes (and `/config`) can expose defaults to the UI.
        self._config = config
        # Lazily initialize the repository so the API can start even when real-data artifacts
        # (Silver/Gold files) are not built yet.
        self._repo: DemoRepository | LocalRepository | None = None

    @property
    def config(self) -> AppConfig:
        # Expose config as a read-only property so callers cannot mutate settings accidentally.
        return self._config

    def _get_repo(self) -> DemoRepository | LocalRepository:
        if self._repo is not None:
            return self._repo

        if self._config.app.demo_mode:
            self._repo = DemoRepository(self._config)
            return self._repo

        self._repo = LocalRepository(self._config)
        return self._repo

    def list_stations(self) -> list[dict[str, Any]]:
        # Return metro station metadata for the map marker layer in the frontend.
        return self._get_repo().list_metro_stations()

    def list_bike_stations(self) -> list[dict[str, Any]]:
        # Return bike station metadata for overlays (nearby bikes, debug layers, etc.).
        return self._get_repo().list_bike_stations()

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
        # Delegate to the repository which implements the actual retrieval + aggregation logic.
        # We keep the service signature close to the HTTP layer so runtime overrides stay explicit.
        return self._get_repo().station_timeseries(
            # `station_id` is the stable key that links station metadata, bike links, and time series.
            station_id,
            # Spatial join params control which bike stations are considered "near" this metro station.
            join_method=join_method,
            radius_m=radius_m,
            nearest_k=nearest_k,
            # Temporal params control how raw timestamps are bucketed for charting.
            granularity=granularity,
            timezone=timezone,
            window_days=window_days,
            # `metro_series` lets the API choose real ridership when available, otherwise fall back to a proxy.
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
        # Return the bike stations linked to a metro station under the chosen join parameters.
        # The frontend uses this for map overlays and for explaining "why this curve looks like this".
        return self._get_repo().nearby_bike(
            # Reuse `station_id` as the lookup key across all API endpoints for consistent routing.
            station_id,
            # Join params are optional so callers can omit them and rely on config defaults.
            join_method=join_method,
            radius_m=radius_m,
            nearest_k=nearest_k,
            # `limit` helps keep response payloads small to avoid UI lag on slower devices.
            limit=limit,
        )

    def station_factors(self, station_id: str) -> dict[str, Any]:
        # Return station-level factor/features (Gold table) so the UI can render a "factor breakdown" panel.
        return self._get_repo().station_factors(station_id)

    def similar_stations(
        self,
        station_id: str,
        *,
        top_k: int | None = None,
        metric: str | None = None,
        standardize: bool | None = None,
    ) -> list[dict[str, Any]]:
        # Return a k-nearest list in feature space for quick exploration (not heavy ML in MVP).
        # Pitfall: similarity metrics are sensitive to scale; `standardize=True` can avoid domination by 1 feature.
        return self._get_repo().similar_stations(
            # Anchor station to compare against.
            station_id,
            # Optional overrides allow experimenting from the UI without restarting the server.
            top_k=top_k,
            metric=metric,
            standardize=standardize,
        )

    def analytics_overview(self) -> dict[str, Any]:
        # Return small precomputed global stats (e.g., correlations) for a dashboard summary panel.
        # Keeping this endpoint lightweight avoids shipping large tables over the network to the browser.
        return self._get_repo().analytics_overview()

    def metro_bike_availability_index(self, *, limit: int = 200) -> list[dict[str, Any]]:
        return self._get_repo().metro_bike_availability_index(limit=limit)

    def metro_bike_availability_at(self, ts: str) -> list[dict[str, Any]]:
        return self._get_repo().metro_bike_availability_at(ts)

    def metro_heat_index(self, *, limit: int = 200) -> list[dict[str, Any]]:
        repo = self._get_repo()
        fn = getattr(repo, "metro_heat_index", None)
        if callable(fn):
            return fn(limit=limit)
        return repo.metro_bike_availability_index(limit=limit)

    def metro_heat_at(self, ts: str, *, metric: str = "available", agg: str = "sum") -> list[dict[str, Any]]:
        repo = self._get_repo()
        fn = getattr(repo, "metro_heat_at", None)
        if callable(fn):
            return fn(ts, metric=metric, agg=agg)
        rows = repo.metro_bike_availability_at(ts)
        out = []
        for r in rows:
            out.append(
                {
                    "station_id": r.get("station_id"),
                    "ts": r.get("ts"),
                    "metric": "available",
                    "agg": "sum",
                    "value": r.get("available_bikes_total"),
                }
            )
        return out
